"""
main.py — Telegram-бот для создания поставок на Озон.

Минималистичный интерфейс:
  /start - запуск
  /setup - установка Client-ID и API-Key
  /create - создание новой поставки (пошаговый диалог)

FSM состояния:
  - waiting_for_sku
  - waiting_for_delivery_type
  - waiting_for_clusters
  - waiting_for_date
  - confirming_supply
"""

import os
import sys
import asyncio
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

# Добавляем родительскую директорию в path для импорта модулей проекта
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ozon_client import OzonClient, OzonAPIError
import tgbot_db
import supply_flow

# ── Конфиг ────────────────────────────────────────────────────────────────
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

if not TELEGRAM_TOKEN:
    print("❌ Ошибка: TELEGRAM_BOT_TOKEN не найден в .env")
    sys.exit(1)

# ── FSM состояния ─────────────────────────────────────────────────────────

class SupplyForm(StatesGroup):
    """Состояния для диалога создания поставки."""
    waiting_for_sku = State()
    waiting_for_quantity = State()
    waiting_for_delivery_type = State()
    waiting_for_clusters = State()
    waiting_for_date = State()
    confirming_supply = State()


class SetupForm(StatesGroup):
    """Состояния для диалога установки учетных данных."""
    waiting_for_client_id = State()
    waiting_for_api_key = State()


# ── Основной класс бота ───────────────────────────────────────────────────

class OzonSupplyBot:
    def __init__(self, token: str):
        self.bot = Bot(token=token)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)

        # Регистрируем обработчики
        self._register_handlers()

        # Временное хранилище для диалога (для сессии)
        self.user_sessions: dict = {}

    def _register_handlers(self):
        """Регистрирует все обработчики команд."""

        # Команда /start
        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            await self._handle_start(message)

        # Команда /setup
        @self.dp.message(Command("setup"))
        async def cmd_setup(message: types.Message, state: FSMContext):
            await self._handle_setup_start(message, state)

        # Ввод Client-ID при setup
        @self.dp.message(SetupForm.waiting_for_client_id)
        async def process_client_id(message: types.Message, state: FSMContext):
            await state.update_data(client_id=message.text)
            await message.answer("Отправь свой API-Key от Озона:")
            await state.set_state(SetupForm.waiting_for_api_key)

        # Ввод API-Key при setup
        @self.dp.message(SetupForm.waiting_for_api_key)
        async def process_api_key(message: types.Message, state: FSMContext):
            data = await state.get_data()
            client_id = data.get("client_id")
            api_key = message.text

            # Сохраняем в БД
            username = message.from_user.username or message.from_user.first_name or "User"
            tgbot_db.save_user_credentials(message.from_user.id, username, client_id, api_key)

            await message.answer(
                "✅ Учетные данные сохранены!\n\n"
                "Теперь ты можешь создавать поставки. Напиши /create чтобы начать."
            )
            await state.clear()

        # Команда /create
        @self.dp.message(Command("create"))
        async def cmd_create(message: types.Message, state: FSMContext):
            await self._handle_create_start(message, state)

        # Команда /list — показать все товары с SKU
        @self.dp.message(Command("list"))
        async def cmd_list(message: types.Message):
            await self._handle_list(message)

        # Ввод SKU
        @self.dp.message(SupplyForm.waiting_for_sku)
        async def process_sku(message: types.Message, state: FSMContext):
            await state.update_data(sku=message.text)
            await message.answer("Сколько единиц отправить?")
            await state.set_state(SupplyForm.waiting_for_quantity)

        # Ввод количества
        @self.dp.message(SupplyForm.waiting_for_quantity)
        async def process_quantity(message: types.Message, state: FSMContext):
            try:
                quantity = int(message.text)
                if quantity <= 0:
                    await message.answer("❌ Количество должно быть больше 0")
                    return
                await state.update_data(quantity=quantity)

                # Показываем варианты доставки
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📦 Прямая поставка", callback_data="delivery_direct")],
                    [InlineKeyboardButton(text="🚚 Кросс-докинг", callback_data="delivery_crossdock")],
                ])
                await message.answer("Выбери схему доставки:", reply_markup=kb)
                await state.set_state(SupplyForm.waiting_for_delivery_type)
            except ValueError:
                await message.answer("❌ Напиши число (количество товара)")

        # Выбор типа доставки
        @self.dp.callback_query(SupplyForm.waiting_for_delivery_type)
        async def process_delivery_type(callback: types.CallbackQuery, state: FSMContext):
            delivery_type = "direct" if callback.data == "delivery_direct" else "crossdock"
            await state.update_data(delivery_type=delivery_type)

            # Получаем кластеры
            await callback.answer()
            await self._show_clusters(callback.message, state)

        # Выбор кластеров
        @self.dp.callback_query(SupplyForm.waiting_for_clusters)
        async def process_clusters(callback: types.CallbackQuery, state: FSMContext):
            # Проверяем если это кнопка "Далее"
            if callback.data == "clusters_done":
                data = await state.get_data()
                selected = data.get("selected_clusters", [])

                if not selected:
                    await callback.answer("❌ Выбери хотя бы один кластер", show_alert=True)
                    return

                await callback.answer()
                await self._show_dates(callback.message, state)
                return

            # Парсим ID кластера
            cluster_id = int(callback.data.split("_")[1])

            data = await state.get_data()
            selected = data.get("selected_clusters", [])

            if cluster_id in selected:
                selected.remove(cluster_id)
            else:
                selected.append(cluster_id)

            await state.update_data(selected_clusters=selected)
            await callback.answer()

            # Обновляем только клавиатуру (без перестройки текста)
            await self._update_clusters_keyboard(callback.message, state)

        # Выбор даты
        @self.dp.callback_query(SupplyForm.waiting_for_date)
        async def process_date(callback: types.CallbackQuery, state: FSMContext):
            selected_date = callback.data.split("_")[1]
            await state.update_data(selected_date=selected_date)

            await callback.answer()
            await self._show_summary(callback.message, state)

        # Подтверждение создания поставки
        @self.dp.callback_query(F.data == "confirm_supply")
        async def confirm_supply(callback: types.CallbackQuery, state: FSMContext):
            await callback.answer()
            await self._create_supply(callback.message, state)

        @self.dp.callback_query(F.data == "cancel_supply")
        async def cancel_supply(callback: types.CallbackQuery, state: FSMContext):
            await callback.answer()
            await callback.message.answer("❌ Отменено")
            await state.clear()

    # ── Обработчики команд ────────────────────────────────────────────────

    async def _handle_start(self, message: types.Message):
        """Обработчик /start."""
        user_id = message.from_user.id

        if tgbot_db.user_exists(user_id):
            await message.answer(
                "👋 Привет! Я помогу тебе создавать поставки на Озон.\n\n"
                "Команды:\n"
                "/create - создать новую поставку\n"
                "/setup - изменить API-Key и Client-ID"
            )
        else:
            await message.answer(
                "👋 Привет! Я — бот для создания поставок на Озон.\n\n"
                "Сначала мне нужны твои учетные данные. Напиши /setup"
            )

    async def _handle_setup_start(self, message: types.Message, state: FSMContext):
        """Обработчик /setup."""
        await message.answer("Отправь свой Client-ID от Озона:")
        await state.set_state(SetupForm.waiting_for_client_id)

    async def _handle_create_start(self, message: types.Message, state: FSMContext):
        """Обработчик /create."""
        user_id = message.from_user.id

        if not tgbot_db.user_exists(user_id):
            await message.answer("❌ Сначала установи учетные данные: /setup")
            return

        # Инициализируем сессию
        self.user_sessions[user_id] = {}

        await message.answer("Напиши SKU товара, который хочешь отправить:")
        await state.set_state(SupplyForm.waiting_for_sku)

    async def _handle_list(self, message: types.Message):
        """Показывает все товары с их SKU."""
        user_id = message.from_user.id

        if not tgbot_db.user_exists(user_id):
            await message.answer("❌ Сначала установи учетные данные: /setup")
            return

        try:
            creds = tgbot_db.get_user_credentials(user_id)
            if not creds:
                await message.answer("❌ Учетные данные не найдены. /setup")
                return

            client = OzonClient(creds["client_id"], creds["api_key"])
            all_product_ids = client.get_all_product_ids()

            if not all_product_ids:
                await message.answer("❌ Товары не найдены")
                return

            products_info = client.get_product_info(all_product_ids)

            # Форматируем список товаров
            text = "📦 *Все твои товары:*\n\n"
            for i, product in enumerate(products_info, 1):
                sku = product.get("sku")
                name = product.get("name", "Unknown")
                offer_id = product.get("offer_id", "?")

                # Обрезаем длинное название
                if len(name) > 40:
                    name = name[:37] + "..."

                text += f"{i}\. SKU: `{sku}`\n   Название: {name}\n   Артикул: {offer_id}\n\n"

                # Telegram ограничивает длину сообщения, делим на части
                if i % 10 == 0:
                    await message.answer(text, parse_mode="Markdown")
                    text = ""

            if text:
                await message.answer(text, parse_mode="Markdown")

        except OzonAPIError as e:
            await message.answer(f"❌ Ошибка API: {e.message}")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")

    def _build_clusters_keyboard(self, clusters: list, selected: list) -> InlineKeyboardMarkup:
        """Создает клавиатуру с кластерами."""
        buttons = []
        for cluster in clusters:
            cluster_id = cluster.get("macrolocal_cluster_id")
            cluster_name = cluster.get("name", "Unknown")

            # Галочка если выбран
            check = "✅" if cluster_id in selected else "☐"
            text = f"{check} {cluster_name}"

            buttons.append([
                InlineKeyboardButton(text=text, callback_data=f"cluster_{cluster_id}")
            ])

        # Кнопка "Далее"
        buttons.append([
            InlineKeyboardButton(text="✅ Далее", callback_data="clusters_done")
        ])

        return InlineKeyboardMarkup(inline_keyboard=buttons)

    async def _show_clusters(self, message: types.Message, state: FSMContext):
        """Показывает список доступных кластеров с чекбоксами."""
        data = await state.get_data()
        user_id = message.chat.id

        try:
            creds = tgbot_db.get_user_credentials(user_id)
            if not creds:
                await message.answer("❌ Учетные данные не найдены. /setup")
                return

            client = OzonClient(creds["client_id"], creds["api_key"])
            clusters = client.get_clusters()

            if not clusters:
                await message.answer("❌ Не удалось получить список кластеров")
                return

            selected = data.get("selected_clusters", [])
            kb = self._build_clusters_keyboard(clusters, selected)

            await message.edit_text("Выбери кластеры (нажимай на них для выбора):", reply_markup=kb)
            await state.set_state(SupplyForm.waiting_for_clusters)

        except OzonAPIError as e:
            await message.answer(f"❌ Ошибка Ozon API: {e.message}")

    async def _update_clusters_keyboard(self, message: types.Message, state: FSMContext):
        """Обновляет только клавиатуру кластеров (без перестройки текста)."""
        data = await state.get_data()
        user_id = message.chat.id

        try:
            creds = tgbot_db.get_user_credentials(user_id)
            if not creds:
                return

            client = OzonClient(creds["client_id"], creds["api_key"])
            clusters = client.get_clusters()

            if not clusters:
                return

            selected = data.get("selected_clusters", [])
            kb = self._build_clusters_keyboard(clusters, selected)

            # Обновляем только клавиатуру
            await message.edit_reply_markup(reply_markup=kb)

        except Exception:
            pass

    async def _show_dates(self, message: types.Message, state: FSMContext):
        """Показывает доступные даты для поставки (следующие 30 дней)."""
        await message.edit_text(
            "⏳ Загружаю доступные даты...\n\n"
            "_Это может занять несколько секунд_",
            parse_mode="Markdown"
        )

        buttons = []

        # Показываем все даты на месяц вперед
        # Система Озона покажет какие из них доступны после создания черновика
        for i in range(1, 31):
            next_date = date.today() + timedelta(days=i)
            date_str = next_date.strftime("%Y-%m-%d")

            # Форматируем дату
            day_name = next_date.strftime("%A")
            day_names = {
                "Monday": "ПН", "Tuesday": "ВТ", "Wednesday": "СР",
                "Thursday": "ЧТ", "Friday": "ПТ", "Saturday": "СБ", "Sunday": "ВС"
            }
            day_short = day_names.get(day_name, "?")
            date_display = f"{next_date.day:02d}.{next_date.month:02d} ({day_short})"

            buttons.append([
                InlineKeyboardButton(text=date_display, callback_data=f"date_{date_str}")
            ])

        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.edit_text(
            "📅 Выбери дату поставки (все даты):\n\n"
            "_Если дата недоступна, API покажет ближайшие альтернативы_",
            reply_markup=kb,
            parse_mode="Markdown"
        )
        await state.set_state(SupplyForm.waiting_for_date)

    async def _show_summary(self, message: types.Message, state: FSMContext):
        """Показывает итоговую информацию перед подтверждением."""
        data = await state.get_data()

        sku = data.get("sku", "?")
        qty = data.get("quantity", "?")
        delivery = "Прямая поставка" if data.get("delivery_type") == "direct" else "Кросс-докинг"
        clusters_count = len(data.get("selected_clusters", []))
        date_str = data.get("selected_date", "?")

        text = (
            "📋 *Проверь информацию:*\n\n"
            f"SKU: `{sku}`\n"
            f"Количество: {qty} шт.\n"
            f"Доставка: {delivery}\n"
            f"Кластеры: {clusters_count} шт.\n"
            f"Дата: {date_str}\n\n"
            f"Всё правильно?"
        )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Создать поставку", callback_data="confirm_supply")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_supply")],
        ])

        await message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        await state.set_state(SupplyForm.confirming_supply)

    async def _create_supply(self, message: types.Message, state: FSMContext):
        """Создаёт поставку в Озоне."""
        data = await state.get_data()
        user_id = message.chat.id

        try:
            await message.edit_text("⏳ Создаю поставку...", reply_markup=None)

            creds = tgbot_db.get_user_credentials(user_id)
            if not creds:
                await message.answer("❌ Учетные данные не найдены")
                await state.clear()
                return

            client = OzonClient(creds["client_id"], creds["api_key"])

            # Запускаем полный поток создания поставки
            result = await supply_flow.create_supply_full_flow(
                client=client,
                sku=data.get("sku"),
                quantity=data.get("quantity"),
                delivery_type=data.get("delivery_type"),
                cluster_ids=data.get("selected_clusters", []),
                target_date=data.get("selected_date"),
            )

            if result["success"]:
                await message.edit_text(
                    f"✅ Поставка создана!\n\n{result['message']}"
                )
            else:
                await message.edit_text(result["message"])

        except OzonAPIError as e:
            await message.answer(f"❌ Ошибка API Озона: {e.message}")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")
        finally:
            await state.clear()

    async def start(self):
        """Запускает бот."""
        try:
            print("🤖 Инициализирую БД...")
            tgbot_db.init_users_table()

            print("🚀 Бот запущен!")
            await self.dp.start_polling(self.bot)
        except KeyboardInterrupt:
            print("\n👋 Бот остановлен")
        finally:
            await self.bot.session.close()


# ── Точка входа ────────────────────────────────────────────────────────────

async def main():
    """Главная функция."""
    bot = OzonSupplyBot(TELEGRAM_TOKEN)
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
