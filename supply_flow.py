"""
supply_flow.py — полный поток создания поставки.

Обрабатывает:
1. Получение product_id и fbo_sku по артикулу продавца
2. Создание черновиков поставки (pipeline для нескольких кластеров)
3. Проверку скоринга (доступность товара на складах) — параллельно
4. Получение таймслотов
5. Создание финальной поставки
"""

import sys
import os
import time
import asyncio
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ozon_client import OzonClient, OzonAPIError


# ─────────────────────────────────────────────────────────────────────────────
# Поиск товара по SKU
# ─────────────────────────────────────────────────────────────────────────────

def get_product_by_sku(client: OzonClient, sku: str) -> dict | None:
    """
    Получает информацию о товаре по реальному SKU через API Озона.
    sku: числовой SKU (например '3329196407') или буквенный offer_id (например 'в36')

    Возвращает {'product_id': int, 'fbo_sku': int, 'name': str} или None.
    """
    try:
        t0 = time.time()
        all_product_ids = client.get_all_product_ids()
        print(f"[SKU] get_all_product_ids: {len(all_product_ids)} товаров ({time.time()-t0:.1f}с)")

        if not all_product_ids:
            return None

        time.sleep(0.5)

        t0 = time.time()
        products_info = client.get_product_info(all_product_ids)
        print(f"[SKU] get_product_info: {len(products_info)} товаров ({time.time()-t0:.1f}с)")

        try:
            sku_numeric = int(sku)
            is_numeric_sku = True
        except ValueError:
            sku_numeric = None
            is_numeric_sku = False

        for i, product in enumerate(products_info):
            product_id = all_product_ids[i] if i < len(all_product_ids) else None
            real_sku = product.get("sku")
            offer_id = product.get("offer_id")

            match = False
            if is_numeric_sku and real_sku:
                if int(real_sku) == sku_numeric:
                    match = True
            else:
                if str(offer_id).strip() == str(sku).strip():
                    match = True

            if match:
                print(f"[SKU] ✅ Найден: {product.get('name')}, product_id={product_id}, SKU={real_sku}")
                if not product_id or not real_sku:
                    return None
                return {
                    "product_id": product_id,
                    "fbo_sku": int(real_sku),
                    "name": product.get("name", ""),
                    "offer_id": offer_id,
                }

        print(f"[SKU] ❌ Товар '{sku}' не найден")
        return None

    except OzonAPIError as e:
        print(f"[SKU] ❌ API ошибка: {e.message}")
        return None
    except Exception as e:
        print(f"[SKU] ❌ Ошибка: {str(e)}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline для нескольких кластеров
# ─────────────────────────────────────────────────────────────────────────────

async def prepare_supply_drafts_pipeline(
    client: OzonClient,
    sku: str,
    quantity: int,
    delivery_type: str,       # "direct" или "crossdock"
    cluster_ids: list[int],
    fbo_sku: int,
    product_id: int,
) -> dict:
    """
    Pipeline подготовка черновиков для нескольких кластеров одновременно.

    Фазы:
      1. Создаём черновики последовательно (rate limit 2/мин)
      2. Скоринг ПАРАЛЛЕЛЬНО через asyncio.gather
      3. Одна общая пауза 25 сек перед таймслотами
      4. Таймслоты последовательно (с паузой 8 сек между)

    Возвращает:
    {
        "success": bool,
        "clusters": {
            cluster_id: {
                "success": bool,
                "name": str,
                "draft_id": int | None,
                "warehouse_id": int | None,
                "timeslots_by_date": {"YYYY-MM-DD": [...]},
                "error": str | None,
            }
        },
        "common_dates": ["YYYY-MM-DD", ...],   # пересечение дат всех кластеров
        "all_clusters": [{"id": int, "name": str}, ...],  # все кластеры из скоринга
    }
    """
    total_t0 = time.time()

    # Инициализируем результат
    results = {}
    for cid in cluster_ids:
        results[cid] = {
            "success": False,
            "name": f"Кластер {cid}",
            "draft_id": None,
            "warehouse_id": None,
            "timeslots_by_date": {},
            "error": None,
        }

    items = [{"sku": fbo_sku, "quantity": quantity}]
    supply_type = "DIRECT" if delivery_type.lower() == "direct" else "CROSSDOCK"

    # ─── Фаза 1: Создаём черновики последовательно ──────────────────────────
    print(f"\n[PIPE] ═══ ФАЗА 1: Создание черновиков ({len(cluster_ids)} кластеров) ═══")
    draft_map = {}  # cluster_id -> draft_id

    for i, cluster_id in enumerate(cluster_ids):
        if i > 0:
            print(f"[PIPE] ⏳ Пауза 2 сек (rate limit create_draft)...")
            time.sleep(2)
        try:
            t0 = time.time()
            draft_result = client.create_draft(items=items, cluster_id=cluster_id)
            draft_id = draft_result.get("draft_id")
            draft_map[cluster_id] = draft_id
            results[cluster_id]["draft_id"] = draft_id
            print(f"[PIPE] ✅ [{i+1}/{len(cluster_ids)}] Кластер {cluster_id} → draft_id={draft_id} ({time.time()-t0:.1f}с)")
        except OzonAPIError as e:
            results[cluster_id]["error"] = f"Ошибка черновика: {e.message}"
            print(f"[PIPE] ❌ [{i+1}/{len(cluster_ids)}] Кластер {cluster_id}: {e.message}")

    if not draft_map:
        print(f"[PIPE] ❌ Не удалось создать ни одного черновика")
        return {"success": False, "clusters": results, "common_dates": [], "all_clusters": []}

    # ─── Фаза 2: Параллельный скоринг ───────────────────────────────────────
    print(f"\n[PIPE] ═══ ФАЗА 2: Скоринг ПАРАЛЛЕЛЬНО ({len(draft_map)} черновиков) ═══")

    async def do_scoring(cluster_id: int, draft_id: int):
        """Запускает get_draft_info в executor (не блокирует event loop)."""
        loop = asyncio.get_event_loop()
        t0 = time.time()
        try:
            draft_info = await loop.run_in_executor(
                None,
                lambda: client.get_draft_info(draft_id, timeout=120)
            )
            elapsed = time.time() - t0
            clusters_count = len(draft_info.get("clusters", []))
            print(f"[PIPE] ✅ Скоринг cluster {cluster_id}: {clusters_count} кластеров в ответе ({elapsed:.1f}с)")
            return cluster_id, draft_info
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[PIPE] ❌ Скоринг cluster {cluster_id}: {e} ({elapsed:.1f}с)")
            return cluster_id, None

    scoring_tasks = [do_scoring(cid, did) for cid, did in draft_map.items()]
    scoring_results = await asyncio.gather(*scoring_tasks)

    # Разбираем результаты скоринга
    warehouse_map = {}        # cluster_id -> warehouse_id
    all_clusters_seen = {}    # cluster_id -> name (все кластеры из ответа Ozon)

    for cluster_id, draft_info in scoring_results:
        if not draft_info:
            if not results[cluster_id]["error"]:
                results[cluster_id]["error"] = "Скоринг не вернул данных"
            continue

        clusters_data = draft_info.get("clusters", [])

        # Собираем все кластеры которые Ozon показывает (для кэша)
        for cluster in clusters_data:
            cid = cluster.get("macrolocal_cluster_id")
            cname = cluster.get("cluster_name", f"Кластер {cid}")
            if cid:
                all_clusters_seen[cid] = cname

        # Берём первый кластер из ответа скоринга (Ozon может вернуть другой ID)
        if not clusters_data:
            results[cluster_id]["error"] = "Скоринг вернул пустой список кластеров"
            print(f"[PIPE] ❌ Кластер {cluster_id}: пустой ответ скоринга")
            continue

        # Логируем все кластеры из ответа для диагностики
        print(f"[PIPE] Скоринг для cluster_id={cluster_id}: реальные ID в ответе = "
              f"{[c.get('macrolocal_cluster_id') for c in clusters_data]}")

        # Используем первый кластер из ответа (Ozon может переназначить ID)
        cluster = clusters_data[0]
        real_cid = cluster.get("macrolocal_cluster_id")
        cname = cluster.get("cluster_name", f"Кластер {real_cid}")

        if real_cid != cluster_id:
            print(f"[PIPE] ⚠️  Ozon переназначил кластер {cluster_id} → {real_cid} ({cname})")

        results[cluster_id]["name"] = cname
        results[cluster_id]["real_cluster_id"] = real_cid

        warehouses = cluster.get("warehouses", [])
        print(f"[PIPE] Кластер {cname} ({real_cid}): {len(warehouses)} складов")

        found_wh = False
        for wh in warehouses:
            state = wh.get("availability_status", {}).get("state", "UNAVAILABLE")
            wh_id = wh.get("storage_warehouse", {}).get("warehouse_id")
            wh_name = wh.get("storage_warehouse", {}).get("name", "Unknown")
            print(f"[PIPE]   Склад {wh_name} ({wh_id}): {state}")

            if state in ["AVAILABLE", "FULL_AVAILABLE"] and not found_wh:
                # Ключ в warehouse_map — реальный cluster_id от Ozon
                warehouse_map[cluster_id] = wh_id
                results[cluster_id]["warehouse_id"] = wh_id
                results[cluster_id]["real_cluster_id"] = real_cid
                found_wh = True
                print(f"[PIPE] ✅ Выбран склад: {wh_name}")

        if not found_wh:
            results[cluster_id]["error"] = "Нет доступных складов"
            print(f"[PIPE] ❌ Кластер {cname}: нет доступных складов")

    all_clusters_list = [{"id": cid, "name": cname} for cid, cname in sorted(all_clusters_seen.items())]
    print(f"[PIPE] Всего кластеров от Ozon: {len(all_clusters_list)}")

    if not warehouse_map:
        print(f"[PIPE] ❌ Нет доступных складов ни в одном кластере")
        return {
            "success": False,
            "clusters": results,
            "common_dates": [],
            "all_clusters": all_clusters_list,
        }

    # ─── Фаза 3: Пауза перед таймслотами ────────────────────────────────────
    print(f"\n[PIPE] ═══ ФАЗА 3: Пауза 25 сек (rate limit timeslots) ═══")
    time.sleep(25)

    # ─── Фаза 4: Таймслоты последовательно ──────────────────────────────────
    print(f"\n[PIPE] ═══ ФАЗА 4: Таймслоты ({len(warehouse_map)} кластеров) ═══")

    today = datetime.now()
    date_from = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = (today + timedelta(days=14)).strftime("%Y-%m-%d")
    common_dates = None

    for i, (cluster_id, warehouse_id) in enumerate(warehouse_map.items()):
        if i > 0:
            print(f"[PIPE] ⏳ Пауза 8 сек между таймслотами...")
            time.sleep(8)

        draft_id = draft_map[cluster_id]
        t0 = time.time()

        try:
            print(f"[PIPE] ⏳ Таймслоты кластера {cluster_id} ({date_from}—{date_to})...")
            timeslots = client.get_timeslots_v2(
                draft_id=draft_id,
                cluster_id=cluster_id,
                warehouse_id=warehouse_id,
                date_from=date_from,
                date_to=date_to,
                supply_type=supply_type,
            )

            timeslots_by_date = {}
            for day in timeslots:
                timeslots_by_date[day["date"]] = day["timeslots"]

            results[cluster_id]["timeslots_by_date"] = timeslots_by_date
            results[cluster_id]["success"] = True
            print(f"[PIPE] ✅ Кластер {cluster_id}: {len(timeslots_by_date)} дат ({time.time()-t0:.1f}с)")
            print(f"[PIPE]   Даты: {list(timeslots_by_date.keys())}")

            if common_dates is None:
                common_dates = set(timeslots_by_date.keys())
            else:
                common_dates &= set(timeslots_by_date.keys())

        except OzonAPIError as e:
            results[cluster_id]["error"] = f"Ошибка таймслотов: {e.message}"
            print(f"[PIPE] ❌ Таймслоты кластера {cluster_id}: {e.message}")

    total_elapsed = time.time() - total_t0
    successful = sum(1 for r in results.values() if r["success"])
    print(f"\n[PIPE] ═══ ИТОГ: {successful}/{len(cluster_ids)} кластеров, {total_elapsed:.1f}с ═══")

    return {
        "success": successful > 0,
        "clusters": results,
        "common_dates": sorted(common_dates) if common_dates else [],
        "all_clusters": all_clusters_list,
    }
