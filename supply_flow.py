"""
supply_flow.py — полный поток создания поставки.

Обрабатывает:
1. Получение product_id и fbo_sku по артикулу продавца
2. Создание черновика поставки
3. Проверку скоринга (доступность товара на складах)
4. Получение таймслотов
5. Создание финальной поставки
"""

import sys
import os
import time
from datetime import datetime, timedelta

# Добавляем родительскую директорию в path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ozon_client import OzonClient, OzonAPIError


def get_product_by_sku(client: OzonClient, sku: str) -> dict | None:
    """
    Получает информацию о товаре по реальному SKU через API Озона.
    sku: может быть либо числовой SKU (например '3329196407'), либо буквенный offer_id (например 'в36')

    Возвращает {'product_id': int, 'fbo_sku': int, 'name': str} или None.
    """
    try:
        # Получаем все товары продавца
        all_product_ids = client.get_all_product_ids()

        if not all_product_ids:
            return None

        print(f"[DEBUG] Найдено {len(all_product_ids)} товаров в кабинете")

        # Небольшая задержка чтобы не получить Rate Limit
        time.sleep(0.5)

        # Получаем информацию о всех товарах
        products_info = client.get_product_info(all_product_ids)

        # Пытаемся конвертировать sku в число (если это числовой SKU)
        try:
            sku_numeric = int(sku)
            is_numeric_sku = True
        except ValueError:
            sku_numeric = None
            is_numeric_sku = False

        # Ищем товар по SKU (числовой) или offer_id (буквенный)
        for i, product in enumerate(products_info):
            product_id = all_product_ids[i] if i < len(all_product_ids) else None
            real_sku = product.get("sku")
            offer_id = product.get("offer_id")

            # Проверяем совпадение
            match = False
            if is_numeric_sku and real_sku:
                # Ищем по числовому SKU
                if int(real_sku) == sku_numeric:
                    match = True
            else:
                # Ищем по offer_id (буквенный артикул)
                if str(offer_id).strip() == str(sku).strip():
                    match = True

            if match:
                print(f"[DEBUG] ✅ Товар найден: {product.get('name')}, product_id={product_id}, SKU={real_sku}")

                if not product_id or not real_sku:
                    print(f"[DEBUG] ❌ Нет product_id или SKU для товара {sku}")
                    return None

                return {
                    "product_id": product_id,
                    "fbo_sku": int(real_sku),
                    "name": product.get("name", ""),
                    "offer_id": offer_id,
                }

        print(f"[DEBUG] ❌ Товар с SKU '{sku}' не найден")
        return None

    except OzonAPIError as e:
        print(f"❌ Ошибка получения товара через API: {e.message}")
        return None
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при поиске товара: {str(e)}")
        return None


async def prepare_supply_draft(
    client: OzonClient,
    sku: str,
    quantity: int,
    delivery_type: str,  # "direct" или "crossdock"
    cluster_ids: list[int],
    fbo_sku: int | None = None,
    product_id: int | None = None,
) -> dict:
    """
    Создаёт черновик, делает скоринг, получает реальные таймслоты от Ozon.
    Вызывается при выборе кластеров, чтобы /api/create-supply был быстрым.

    Возвращает:
    {
        "success": bool,
        "message": str,
        "draft_id": int,
        "warehouse_id": int,
        "cluster_id": int,
        "timeslots_by_date": {"YYYY-MM-DD": [{"from": "...", "to": "..."}]},
    }
    """
    result = {
        "success": False,
        "message": "",
        "draft_id": None,
        "warehouse_id": None,
        "cluster_id": None,
        "timeslots_by_date": {},
    }

    try:
        # Шаг 1: fbo_sku уже должен быть известен из verify-sku
        if not fbo_sku:
            product = get_product_by_sku(client, sku)
            if not product:
                result["message"] = f"❌ Товар '{sku}' не найден"
                return result
            fbo_sku = product["fbo_sku"]
            product_id = product["product_id"]

        print(f"[PREP] fbo_sku={fbo_sku}, product_id={product_id}")

        # Шаг 2: Создаём черновик
        items = [{"sku": fbo_sku, "quantity": quantity}]
        first_cluster_id = cluster_ids[0] if cluster_ids else None
        print(f"[PREP] Создаю черновик для кластера {first_cluster_id}...")
        time.sleep(2)
        draft_result = client.create_draft(items=items, cluster_id=first_cluster_id)
        draft_id = draft_result.get("draft_id")
        result["draft_id"] = draft_id
        print(f"[PREP] Черновик создан: draft_id={draft_id}")

        # Шаг 3: Скоринг
        print(f"[PREP] ⏳ Ожидаю скоринг...")
        draft_info = client.get_draft_info(draft_id, timeout=120)
        clusters_data = draft_info.get("clusters", [])
        suitable_clusters = []

        for cluster in clusters_data:
            cluster_id = cluster.get("macrolocal_cluster_id")
            cluster_name = cluster.get("cluster_name", "Unknown")
            if cluster_id not in cluster_ids:
                continue
            warehouses = cluster.get("warehouses", [])
            available_warehouses = []
            for wh in warehouses:
                state = wh.get("availability_status", {}).get("state", "UNAVAILABLE")
                wh_id = wh.get("storage_warehouse", {}).get("warehouse_id")
                wh_name = wh.get("storage_warehouse", {}).get("name", "Unknown")
                if state in ["AVAILABLE", "FULL_AVAILABLE"]:
                    available_warehouses.append({"warehouse_id": wh_id, "name": wh_name})
            if available_warehouses:
                suitable_clusters.append({
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "warehouses": available_warehouses,
                })
                print(f"[PREP] ✅ Кластер {cluster_name}: {len(available_warehouses)} складов")

        if not suitable_clusters:
            result["message"] = "❌ Нет доступных складов для выбранных кластеров"
            return result

        cluster_id = suitable_clusters[0]["cluster_id"]
        warehouse_id = suitable_clusters[0]["warehouses"][0]["warehouse_id"]
        result["cluster_id"] = cluster_id
        result["warehouse_id"] = warehouse_id

        # Шаг 4: Получаем таймслоты на 14 дней вперёд
        today = datetime.now()
        date_from = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        date_to = (today + timedelta(days=14)).strftime("%Y-%m-%d")
        print(f"[PREP] ⏳ Получаю таймслоты {date_from} — {date_to}...")
        time.sleep(25)

        timeslots = client.get_timeslots_v2(
            draft_id=draft_id,
            cluster_id=cluster_id,
            warehouse_id=warehouse_id,
            date_from=date_from,
            date_to=date_to,
            supply_type="DIRECT" if delivery_type == "direct" else "CROSSDOCK",
        )

        timeslots_by_date = {}
        for day in timeslots:
            timeslots_by_date[day["date"]] = day["timeslots"]

        result["timeslots_by_date"] = timeslots_by_date
        result["success"] = True
        result["message"] = f"✅ Готово: {len(timeslots_by_date)} дат доступно"
        print(f"[PREP] ✅ Таймслоты получены: {list(timeslots_by_date.keys())}")

    except OzonAPIError as e:
        result["message"] = f"❌ Ошибка Ozon API: {e.message}"
    except Exception as e:
        result["message"] = f"❌ Непредвиденная ошибка: {str(e)}"

    return result


async def create_supply_full_flow(
    client: OzonClient,
    sku: str,
    quantity: int,
    delivery_type: str,  # "direct" или "crossdock"
    cluster_ids: list[int],
    target_date: str,  # "YYYY-MM-DD"
    seller_warehouse_id: int | None = None,
    drop_off_warehouse_id: int | None = None,
    fbo_sku: int | None = None,  # если уже известен — пропускаем поиск
    product_id: int | None = None,
) -> dict:
    """
    Полный поток создания поставки.

    Возвращает результат:
    {
        "success": bool,
        "message": str,
        "order_id": int | None,
        "draft_id": int | None,
        "cluster_info": dict,
    }
    """

    result = {
        "success": False,
        "message": "",
        "order_id": None,
        "draft_id": None,
        "cluster_info": {},
    }

    try:
        # ─── Шаг 1: Получаем информацию о товаре через API ─────────────────
        if fbo_sku and product_id:
            # Уже известны — пропускаем лишние API запросы
            print(f"[DEBUG] Используем кэшированный fbo_sku={fbo_sku}, product_id={product_id}")
        else:
            product = get_product_by_sku(client, sku)
            if not product:
                result["message"] = f"❌ Товар с артикулом '{sku}' не найден в кабинете Озона"
                return result

            product_id = product["product_id"]
            fbo_sku = product.get("fbo_sku")

            if not fbo_sku:
                result["message"] = f"❌ Не удалось получить SKU для товара '{sku}'"
                return result

        # ─── Шаг 3: Создаём черновик поставки ───────────────────────────────
        items = [{"sku": fbo_sku, "quantity": quantity}]

        try:
            if delivery_type == "direct":
                # Прямая поставка — используем первый выбранный кластер
                first_cluster_id = cluster_ids[0] if cluster_ids else None
                print(f"[DEBUG] Создаю черновик для кластера: {first_cluster_id}")

                # Озон лимит: 2 раза в минуту, поэтому ждем перед запросом
                print(f"[DEBUG] ⏳ Ожидаю перед созданием черновика (лимит Озона: 2/мин)...")
                time.sleep(2)

                draft_result = client.create_draft(items=items, cluster_id=first_cluster_id)
            else:
                # Кросс-докинг (нужны warehouse_id)
                if not seller_warehouse_id or not drop_off_warehouse_id:
                    result["message"] = "❌ Для кросс-докинга нужны ID складов"
                    return result

                draft_result = client.create_draft_crossdock(
                    items=items,
                    cluster_id=cluster_ids[0] if cluster_ids else 1,
                    drop_off_warehouse_id=drop_off_warehouse_id,
                    seller_warehouse_id=seller_warehouse_id,
                )

            draft_id = draft_result.get("draft_id")
            result["draft_id"] = draft_id

        except OzonAPIError as e:
            result["message"] = f"❌ Ошибка создания черновика: {e.message}"
            return result

        # ─── Шаг 4: Получаем информацию о черновике (скоринг) ────────────────
        try:
            # Озон лимит: минимум 2-3 секунды между запросами
            print(f"[DEBUG] ⏳ Ожидаю перед проверкой скоринга...")
            time.sleep(3)
            draft_info = client.get_draft_info(draft_id, timeout=120)

            # Анализируем результаты скоринга для выбранных кластеров
            clusters_data = draft_info.get("clusters", [])
            suitable_clusters = []

            print(f"[DEBUG] Всего кластеров в черновике: {len(clusters_data)}")
            print(f"[DEBUG] Выбранные кластеры: {cluster_ids}")

            for cluster in clusters_data:
                cluster_id = cluster.get("macrolocal_cluster_id")
                cluster_name = cluster.get("cluster_name", "Unknown")
                print(f"[DEBUG] Проверяю кластер {cluster_id} ({cluster_name})")

                if cluster_id not in cluster_ids:
                    print(f"  → Пропускаю, не выбран")
                    continue  # Пропускаем, если не выбран

                warehouses = cluster.get("warehouses", [])
                print(f"  → Найдено {len(warehouses)} складов")
                available_warehouses = []

                for i, wh in enumerate(warehouses):
                    availability = wh.get("availability_status", {})
                    state = availability.get("state", "UNAVAILABLE")
                    wh_name = wh.get("storage_warehouse", {}).get("name", "Unknown")
                    wh_id = wh.get("storage_warehouse", {}).get("warehouse_id", "?")
                    print(f"    Склад {i+1}: {wh_name} (ID={wh_id}) — {state}")

                    # Проверяем если склад доступен (может быть AVAILABLE или FULL_AVAILABLE)
                    if state in ["AVAILABLE", "FULL_AVAILABLE"]:
                        available_warehouses.append({
                            "warehouse_id": wh_id,
                            "name": wh_name,
                        })

                if available_warehouses:
                    suitable_clusters.append({
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                        "warehouses": available_warehouses,
                    })
                    print(f"  ✅ Кластер подходит ({len(available_warehouses)} доступных складов)")
                else:
                    print(f"  ❌ Нет доступных складов")

            print(f"[DEBUG] Итого подходящих кластеров: {len(suitable_clusters)}")

            if not suitable_clusters:
                result["message"] = (
                    f"❌ Товар '{sku}' недоступен на выбранных кластерах\n"
                    "Попробуй выбрать другие кластеры"
                )
                return result

            result["cluster_info"] = suitable_clusters[0]  # Берем первый подходящий

        except OzonAPIError as e:
            result["message"] = f"❌ Ошибка скоринга: {e.message}"
            return result

        # ─── Шаг 5: Получаем таймслоты ──────────────────────────────────────
        cluster_id = suitable_clusters[0]["cluster_id"]
        warehouse_id = suitable_clusters[0]["warehouses"][0]["warehouse_id"]

        try:
            from datetime import timedelta
            date_to = datetime.fromisoformat(target_date + "T23:59:59").strftime("%Y-%m-%d")

            # Озон имеет строгий лимит на запросы в секунду
            # get_draft_info() делает несколько внутренних запросов
            # Ждем дольше перед get_timeslots_v2()
            print(f"[DEBUG] ⏳ Ожидаю перед получением таймслотов (лимит Озона: per second)...")
            time.sleep(15)

            timeslots = client.get_timeslots_v2(
                draft_id=draft_id,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
                date_from=target_date,
                date_to=date_to,
                supply_type="DIRECT" if delivery_type == "direct" else "CROSSDOCK",
            )

            if not timeslots:
                # Если выбранная дата недоступна, ищем доступные даты на месяц вперед
                available_dates = []
                search_date = datetime.fromisoformat(target_date)

                for days_offset in range(30):
                    check_date = search_date + timedelta(days=days_offset)
                    check_date_str = check_date.strftime("%Y-%m-%d")

                    try:
                        check_timeslots = client.get_timeslots_v2(
                            draft_id=draft_id,
                            cluster_id=cluster_id,
                            warehouse_id=warehouse_id,
                            date_from=check_date_str,
                            date_to=check_date_str,
                            supply_type="DIRECT" if delivery_type == "direct" else "CROSSDOCK",
                        )
                        if check_timeslots:
                            available_dates.append(check_date_str)
                    except:
                        pass

                if available_dates:
                    dates_str = ", ".join(available_dates[:5])  # Показываем первые 5 доступных дат
                    result["message"] = (
                        f"❌ Дата {target_date} недоступна\n\n"
                        f"Доступные даты:\n{dates_str}"
                    )
                else:
                    result["message"] = f"❌ Нет доступных таймслотов на месяц вперед"
                return result

            # Берем первый доступный таймслот
            first_day = timeslots[0]
            first_slot = first_day["timeslots"][0]

        except OzonAPIError as e:
            result["message"] = f"❌ Ошибка получения таймслотов: {e.message}"
            return result

        # ─── Шаг 6: Создаём финальную поставку ─────────────────────────────
        try:
            # Задержка перед созданием финальной поставки
            print(f"[DEBUG] ⏳ Ожидаю перед созданием поставки...")
            time.sleep(1)

            client.create_supply_v2(
                draft_id=draft_id,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
                timeslot_from=first_slot["from"],
                timeslot_to=first_slot["to"],
                supply_type="DIRECT" if delivery_type == "direct" else "CROSSDOCK",
            )

            # Получаем статус создания
            order_id = client.get_supply_create_status(draft_id, timeout=120)
            result["order_id"] = order_id
            result["success"] = True
            result["message"] = (
                f"✅ Поставка создана!\n"
                f"Order ID: {order_id}\n"
                f"Кластер: {result['cluster_info'].get('cluster_name', 'Unknown')}\n"
                f"Дата: {target_date} {first_slot['from']}"
            )

        except OzonAPIError as e:
            result["message"] = f"❌ Ошибка создания поставки: {e.message}"
            return result

    except Exception as e:
        result["message"] = f"❌ Непредвиденная ошибка: {str(e)}"

    return result
