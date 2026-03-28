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

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from tgbot_db import get_user_credentials
from supply_flow import get_product_by_sku, prepare_supply_drafts_pipeline
from ozon_client import OzonClient

# ─────────────────────────────────────────────────────────────────────────────
# In-memory кэши
# ─────────────────────────────────────────────────────────────────────────────

# Кэш черновиков: ключ = "sku|qty|type|clusters" -> {clusters: {...}}
supply_draft_cache: dict = {}

# Кэш кластеров от Ozon (обновляется после каждого скоринга)
ozon_clusters_cache: list = [
    {"id": 4039, "name": "Москва, МО и Дальние регионы"},
    {"id": 4038, "name": "Санкт-Петербург и ЦСР"},
    {"id": 4002, "name": "Дальний Восток"},
    {"id": 4040, "name": "Нижний Новгород и Средняя Волга"},
    {"id": 4037, "name": "Екатеринбург и УФО"},
    {"id": 4041, "name": "Казань и Поволжье"},
    {"id": 4042, "name": "Новосибирск и СФО"},
    {"id": 4001, "name": "Свердловская область"},
]

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Ozon Supply Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def extract_user_id_from_init_data(init_data: str) -> int | None:
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


def get_credentials(x_telegram_init_data, client_id, api_key):
    user_id = None
    credentials = None
    if x_telegram_init_data:
        user_id = extract_user_id_from_init_data(x_telegram_init_data)
        if user_id:
            credentials = get_user_credentials(user_id)
    if not credentials and client_id and api_key:
        credentials = {"client_id": client_id, "api_key": api_key}
    return credentials, user_id


def make_ozon_client(credentials: dict) -> OzonClient:
    return OzonClient(client_id=credentials['client_id'], api_key=credentials['api_key'])

# ─────────────────────────────────────────────────────────────────────────────
# Request Models
# ─────────────────────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/clusters")
async def get_clusters(
    x_telegram_init_data: str = Header(None),
    client_id: str = None,
    api_key: str = None
):
    """Возвращает список кластеров. Динамически обновляется после первого скоринга."""
    credentials, user_id = get_credentials(x_telegram_init_data, client_id, api_key)
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")
    print(f"[API] /clusters user={user_id}, кластеров в кэше: {len(ozon_clusters_cache)}")
    return {"clusters": ozon_clusters_cache}


@app.post("/api/verify-sku")
async def verify_sku(
    request: VerifySkuRequest,
    x_telegram_init_data: str = Header(None),
    client_id: str = None,
    api_key: str = None
):
    """Проверяет наличие товара по SKU."""
    credentials, user_id = get_credentials(x_telegram_init_data, client_id, api_key)
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")

    print(f"[API] /verify-sku SKU={request.sku}, user={user_id}")
    client = make_ozon_client(credentials)
    product = get_product_by_sku(client, request.sku)

    if product:
        print(f"[API] ✅ SKU {request.sku}: {product.get('name')}")
        return {
            "found": True,
            "sku": request.sku,
            "name": product.get("name", ""),
            "fbo_sku": product.get("fbo_sku"),
            "product_id": product.get("product_id"),
        }
    else:
        print(f"[API] ❌ SKU {request.sku} не найден")
        raise HTTPException(status_code=404, detail="Товар не найден")


@app.post("/api/get-dates")
async def get_dates(
    request: GetDatesRequest,
    x_telegram_init_data: str = Header(None),
    client_id: str = None,
    api_key: str = None
):
    """
    Pipeline: черновики → скоринг параллельно → таймслоты.
    Возвращает общие даты для всех выбранных кластеров.
    """
    credentials, user_id = get_credentials(x_telegram_init_data, client_id, api_key)
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")

    print(f"[API] /get-dates SKU={request.sku}, qty={request.quantity}, "
          f"clusters={request.clusters}, type={request.delivery_type}")

    if not request.fbo_sku or not request.product_id:
        raise HTTPException(status_code=400, detail="fbo_sku и product_id обязательны")

    client = make_ozon_client(credentials)

    pipe = await prepare_supply_drafts_pipeline(
        client=client,
        sku=request.sku,
        quantity=request.quantity,
        delivery_type=request.delivery_type.lower(),
        cluster_ids=request.clusters,
        fbo_sku=request.fbo_sku,
        product_id=request.product_id,
    )

    # Обновляем кэш кластеров
    global ozon_clusters_cache
    if pipe.get("all_clusters"):
        ozon_clusters_cache = pipe["all_clusters"]
        print(f"[API] Кэш кластеров обновлён: {len(ozon_clusters_cache)} кластеров")

    if not pipe["success"]:
        errors = []
        for cid, cdata in pipe["clusters"].items():
            if cdata.get("error"):
                errors.append(f"{cdata['name']}: {cdata['error']}")
        detail = "\n".join(errors) if errors else "Нет доступных складов"
        raise HTTPException(status_code=400, detail=detail)

    # Кэшируем данные
    cache_key = f"{request.sku}|{request.quantity}|{request.delivery_type}|{sorted(request.clusters)}"
    supply_draft_cache[cache_key] = {
        "clusters": pipe["clusters"],
        "delivery_type": request.delivery_type,
    }
    print(f"[API] Кэш сохранён: {cache_key}")

    # Форматируем общие даты
    day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    dates = []
    from datetime import date as date_cls
    for date_str in pipe["common_dates"]:
        d = date_cls.fromisoformat(date_str)
        dates.append(f"{date_str} ({day_names[d.weekday()]})")

    # Статус по каждому кластеру
    clusters_status = {}
    for cid, cdata in pipe["clusters"].items():
        clusters_status[str(cid)] = {
            "name": cdata["name"],
            "available": cdata["success"],
            "error": cdata.get("error"),
        }

    return {"dates": dates, "clusters_status": clusters_status}


@app.post("/api/create-supply")
async def create_supply(
    request: CreateSupplyRequest,
    x_telegram_init_data: str = Header(None),
    client_id: str = None,
    api_key: str = None
):
    """Создаёт поставки для всех выбранных кластеров из кэша."""
    credentials, user_id = get_credentials(x_telegram_init_data, client_id, api_key)
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")

    print(f"[API] /create-supply SKU={request.sku}, date={request.date}, clusters={request.clusters}")

    cache_key = f"{request.sku}|{request.quantity}|{request.delivery_type}|{sorted(request.clusters)}"
    cached = supply_draft_cache.get(cache_key)
    if not cached:
        raise HTTPException(status_code=400, detail="Черновик не найден. Вернитесь к выбору даты.")

    target_date = request.date.split(' ')[0]
    client = make_ozon_client(credentials)
    supply_type = "DIRECT" if request.delivery_type.upper() == "DIRECT" else "CROSSDOCK"
    clusters_data = cached["clusters"]
    results = []

    for cluster_id in request.clusters:
        cdata = clusters_data.get(cluster_id)
        if not cdata or not cdata["success"]:
            err = cdata.get("error", "Данные не найдены") if cdata else "Данные не найдены"
            name = cdata.get("name", f"Кластер {cluster_id}") if cdata else f"Кластер {cluster_id}"
            results.append({"cluster_id": cluster_id, "cluster_name": name, "success": False, "error": err})
            print(f"[API] ⚠️  Кластер {cluster_id} пропущен: {err}")
            continue

        draft_id = cdata["draft_id"]
        warehouse_id = cdata["warehouse_id"]
        cluster_name = cdata["name"]
        timeslots_by_date = cdata["timeslots_by_date"]

        # Ищем таймслот для выбранной даты
        slots = timeslots_by_date.get(target_date)
        if not slots:
            available = sorted(timeslots_by_date.keys())
            if available:
                slots = timeslots_by_date[available[0]]
                print(f"[API] Дата {target_date} недоступна для {cluster_name}, использую {available[0]}")
            else:
                results.append({
                    "cluster_id": cluster_id, "cluster_name": cluster_name,
                    "success": False, "error": f"Нет таймслотов для {target_date}"
                })
                continue

        first_slot = slots[0]
        print(f"[API] Создаю поставку: {cluster_name}, draft={draft_id}, слот={first_slot.get('from','?')}")

        try:
            t0 = time.time()
            client.create_supply_v2(
                draft_id=draft_id,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
                timeslot_from=first_slot["from"],
                timeslot_to=first_slot["to"],
                supply_type=supply_type,
            )
            order_id = client.get_supply_create_status(draft_id, timeout=120)
            results.append({
                "cluster_id": cluster_id, "cluster_name": cluster_name,
                "success": True, "order_id": order_id, "draft_id": draft_id,
            })
            print(f"[API] ✅ {cluster_name}: order_id={order_id} ({time.time()-t0:.1f}с)")

        except Exception as e:
            results.append({
                "cluster_id": cluster_id, "cluster_name": cluster_name,
                "success": False, "error": str(e),
            })
            print(f"[API] ❌ {cluster_name}: {str(e)}")

    supply_draft_cache.pop(cache_key, None)

    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    print(f"[API] Итог: {len(successful)} успешно, {len(failed)} ошибок")

    if not successful:
        raise HTTPException(
            status_code=400,
            detail="Не удалось создать ни одной поставки: " + "; ".join(r.get("error","") for r in failed)
        )

    return {
        "success": True,
        "results": results,
        "message": f"Создано {len(successful)} из {len(results)} поставок",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Start
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print("\n" + "="*60)
    print("🚀 Ozon Supply HTTP Server")
    print("="*60)
    print(f"📍 Server URL: http://0.0.0.0:{port}")
    print(f"📚 Docs: http://localhost:{port}/docs")
    print("="*60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
