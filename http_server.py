"""
HTTP сервер для Mini App backend.
Обрабатывает запросы от frontend и вызывает real Ozon API.
"""

import sys
import os
import json
import asyncio
from datetime import datetime, timedelta
import time

# Добавляем родительскую директорию в path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Импорты из локальной папки TGBOT
from tgbot_db import get_user_credentials
from supply_flow import get_product_by_sku, create_supply_full_flow, prepare_supply_draft
from ozon_client import OzonClient

# ──────────────────────────────────────────────────────────────────────
# In-memory кэш черновиков (draft_id, warehouse_id, таймслоты)
# Ключ: "sku|quantity|delivery_type|clusters"
# ──────────────────────────────────────────────────────────────────────
supply_draft_cache: dict = {}

# ──────────────────────────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Ozon Supply Backend", version="1.0.0")

# CORS для Mini App
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────

def extract_user_id_from_init_data(init_data: str) -> int | None:
    """
    Извлекает user_id из Telegram initData.
    initData это URL-encoded строка: "user=%7B%22id%22%3A123..."
    """
    if not init_data:
        return None

    try:
        from urllib.parse import parse_qs
        params = parse_qs(init_data)
        user_str = params.get('user', [None])[0]

        if not user_str:
            return None

        user_data = json.loads(user_str)
        return user_data.get('id')
    except Exception as e:
        print(f"[Auth] Ошибка парсинга initData: {str(e)}")
        return None


# ──────────────────────────────────────────────────────────────────────
# Request Models
# ──────────────────────────────────────────────────────────────────────

class VerifySkuRequest(BaseModel):
    sku: str

class GetDatesRequest(BaseModel):
    sku: str
    quantity: int
    clusters: list[int]
    delivery_type: str
    fbo_sku: int | None = None
    product_id: int | None = None

class CreateSupplyRequest(BaseModel):
    sku: str
    quantity: int
    clusters: list[int]
    delivery_type: str
    date: str
    fbo_sku: int | None = None
    product_id: int | None = None

# ──────────────────────────────────────────────────────────────────────
# API Endpoints
# ──────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/clusters")
async def get_clusters(x_telegram_init_data: str = Header(None), client_id: str = None, api_key: str = None):
    """Получает доступные кластеры пользователя"""
    try:
        credentials = None
        user_id = None

        # Приоритет 1: Telegram initData
        if x_telegram_init_data:
            user_id = extract_user_id_from_init_data(x_telegram_init_data)
            if user_id:
                credentials = get_user_credentials(user_id)

        # Приоритет 2: Query параметры (для локального тестирования)
        if not credentials and client_id and api_key:
            credentials = {"client_id": client_id, "api_key": api_key}

        if not credentials:
            raise HTTPException(
                status_code=401,
                detail="Missing credentials. Use ?client_id=...&api_key=... or Telegram initData"
            )

        print(f"[API] /clusters для пользователя {user_id}")

        # Создаем Ozon client и получаем кластеры
        client = OzonClient(
            client_id=credentials['client_id'],
            api_key=credentials['api_key']
        )

        # Получаем информацию о товарах чтобы определить доступные кластеры
        # На самом деле, кластеры стандартные для Озона
        clusters = [
            {"id": 4039, "name": "Москва, МО и Дальние регионы"},
            {"id": 4002, "name": "Дальний Восток"},
            {"id": 4038, "name": "Санкт-Петербург и ЦСР"},
            {"id": 4040, "name": "Нижний Новгород и Средняя Волга"},
            {"id": 4001, "name": "Свердловская область"},
            {"id": 4037, "name": "Екатеринбург и УФО"},
            {"id": 4041, "name": "Казань и Поволжье"},
            {"id": 4042, "name": "Новосибирск и СФО"},
        ]

        return {"clusters": clusters}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[API] Ошибка /clusters: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/verify-sku")
async def verify_sku(request: VerifySkuRequest, x_telegram_init_data: str = Header(None), client_id: str = None, api_key: str = None):
    """Проверяет наличие товара по SKU"""
    try:
        credentials = None
        user_id = None

        # Приоритет 1: Telegram initData
        if x_telegram_init_data:
            user_id = extract_user_id_from_init_data(x_telegram_init_data)
            if user_id:
                credentials = get_user_credentials(user_id)

        # Приоритет 2: Query параметры (для локального тестирования)
        if not credentials and client_id and api_key:
            credentials = {"client_id": client_id, "api_key": api_key}

        if not credentials:
            raise HTTPException(status_code=401, detail="Missing credentials")

        print(f"[API] /verify-sku для SKU {request.sku}, пользователь {user_id}")

        # Создаем Ozon client
        client = OzonClient(
            client_id=credentials['client_id'],
            api_key=credentials['api_key']
        )

        # Получаем информацию о товаре
        product = get_product_by_sku(client, request.sku)

        if product:
            print(f"[API] ✅ SKU {request.sku} найден: {product.get('name')}")
            return {
                "found": True,
                "sku": request.sku,
                "name": product.get("name", "Unknown"),
                "fbo_sku": product.get("fbo_sku"),
                "product_id": product.get("product_id"),
            }
        else:
            print(f"[API] ❌ SKU {request.sku} не найден")
            raise HTTPException(status_code=404, detail="Товар не найден")

    except HTTPException:
        raise
    except Exception as e:
        print(f"[API] Ошибка /verify-sku: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/get-dates")
async def get_dates(request: GetDatesRequest, x_telegram_init_data: str = Header(None), client_id: str = None, api_key: str = None):
    """Создаёт черновик, делает скоринг, получает реальные таймслоты. Возвращает доступные даты."""
    try:
        credentials = None
        user_id = None

        if x_telegram_init_data:
            user_id = extract_user_id_from_init_data(x_telegram_init_data)
            if user_id:
                credentials = get_user_credentials(user_id)

        if not credentials and client_id and api_key:
            credentials = {"client_id": client_id, "api_key": api_key}

        if not credentials:
            raise HTTPException(status_code=401, detail="Missing credentials")

        print(f"[API] /get-dates для SKU {request.sku}, пользователь {user_id}")

        client = OzonClient(
            client_id=credentials['client_id'],
            api_key=credentials['api_key']
        )

        # Создаём черновик и получаем реальные таймслоты от Ozon
        prep = await prepare_supply_draft(
            client=client,
            sku=request.sku,
            quantity=request.quantity,
            delivery_type=request.delivery_type.lower(),
            cluster_ids=request.clusters,
            fbo_sku=request.fbo_sku,
            product_id=request.product_id,
        )

        if not prep["success"]:
            raise HTTPException(status_code=400, detail=prep["message"])

        # Кэшируем черновик по ключу
        cache_key = f"{request.sku}|{request.quantity}|{request.delivery_type}|{sorted(request.clusters)}"
        supply_draft_cache[cache_key] = {
            "draft_id": prep["draft_id"],
            "warehouse_id": prep["warehouse_id"],
            "cluster_id": prep["cluster_id"],
            "timeslots_by_date": prep["timeslots_by_date"],
            "delivery_type": request.delivery_type,
        }
        print(f"[API] Кэш сохранён: {cache_key}")

        # Форматируем даты для UI
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        dates = []
        for date_str in sorted(prep["timeslots_by_date"].keys()):
            from datetime import date as date_cls
            d = date_cls.fromisoformat(date_str)
            day_name = day_names[d.weekday()]
            dates.append(f"{date_str} ({day_name})")

        return {"dates": dates}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[API] Ошибка /get-dates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/create-supply")
async def create_supply(request: CreateSupplyRequest, x_telegram_init_data: str = Header(None), client_id: str = None, api_key: str = None):
    """Создаёт поставку используя кэшированный черновик — только 1 запрос к Ozon."""
    try:
        credentials = None
        user_id = None

        if x_telegram_init_data:
            user_id = extract_user_id_from_init_data(x_telegram_init_data)
            if user_id:
                credentials = get_user_credentials(user_id)

        if not credentials and client_id and api_key:
            credentials = {"client_id": client_id, "api_key": api_key}

        if not credentials:
            raise HTTPException(status_code=401, detail="Missing credentials")

        print(f"[API] /create-supply для SKU {request.sku}, пользователь {user_id}")

        # Берём черновик из кэша
        cache_key = f"{request.sku}|{request.quantity}|{request.delivery_type}|{sorted(request.clusters)}"
        cached = supply_draft_cache.get(cache_key)

        if not cached:
            raise HTTPException(status_code=400, detail="Черновик не найден. Пожалуйста, вернитесь к выбору даты.")

        draft_id = cached["draft_id"]
        warehouse_id = cached["warehouse_id"]
        cluster_id = cached["cluster_id"]
        timeslots_by_date = cached["timeslots_by_date"]
        delivery_type = cached["delivery_type"]

        # Ищем таймслот для выбранной даты
        target_date = request.date.split(' ')[0]
        slots = timeslots_by_date.get(target_date)
        if not slots:
            raise HTTPException(status_code=400, detail=f"Нет таймслотов для даты {target_date}")

        first_slot = slots[0]
        print(f"[API] Создаю поставку: draft_id={draft_id}, дата={target_date}, слот={first_slot['from']}")

        client = OzonClient(
            client_id=credentials['client_id'],
            api_key=credentials['api_key']
        )

        # Только 1 запрос к Ozon — финальное создание поставки
        client.create_supply_v2(
            draft_id=draft_id,
            cluster_id=cluster_id,
            warehouse_id=warehouse_id,
            timeslot_from=first_slot["from"],
            timeslot_to=first_slot["to"],
            supply_type="DIRECT" if delivery_type.lower() == "direct" else "CROSSDOCK",
        )

        order_id = client.get_supply_create_status(draft_id, timeout=120)

        # Очищаем кэш после успешного создания
        supply_draft_cache.pop(cache_key, None)

        print(f"[API] ✅ Поставка создана: draft_id={draft_id}, order_id={order_id}")
        return {
            "success": True,
            "draft_id": draft_id,
            "order_id": order_id,
            "message": "Поставка успешно создана!"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[API] Ошибка /create-supply: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ──────────────────────────────────────────────────────────────────────
# Start Server
# ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Для Railway используем переменную PORT, иначе 8000 локально
    port = int(os.getenv("PORT", 8000))
    host = "0.0.0.0"  # Слушаем на всех интерфейсах

    print("\n" + "="*60)
    print("🚀 Ozon Supply HTTP Server")
    print("="*60)
    print(f"📍 Server URL: http://0.0.0.0:{port}")
    print(f"📚 Docs: http://localhost:{port}/docs")
    print("="*60 + "\n")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )
