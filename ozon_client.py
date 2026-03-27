"""
ozon_client.py — обёртка над Ozon Seller API.

Все запросы к Ozon проходят через этот класс.
Не содержит бизнес-логики — только HTTP-вызовы и разбор ответов.
"""

import requests
import time
import logging

logger = logging.getLogger(__name__)


class OzonAPIError(Exception):
    """Ошибка от Ozon API (HTTP != 2xx или code != 0 в теле)."""
    def __init__(self, status: int, message: str):
        self.status  = status
        self.message = message
        super().__init__(f"Ozon API {status}: {message}")


class OzonClient:
    BASE_URL = "https://api-seller.ozon.ru"

    def __init__(self, client_id: str, api_key: str):
        self.headers = {
            "Client-Id":    client_id,
            "Api-Key":      api_key,
            "Content-Type": "application/json",
        }

    # ── Низкоуровневый HTTP ────────────────────────────────────────────────────

    def _post(self, endpoint: str, body: dict, *, retries: int = 3) -> dict:
        """
        Выполняет POST-запрос. При временных сбоях делает повторы.
        Возвращает распарсенный JSON.
        Бросает OzonAPIError при ошибке.
        """
        url = self.BASE_URL + endpoint
        for attempt in range(retries + 1):
            try:
                resp = requests.post(url, json=body, headers=self.headers, timeout=20)
            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    time.sleep(2)
                    continue
                raise OzonAPIError(0, f"Сетевая ошибка: {e}") from e

            if resp.status_code in (429, 503) and attempt < retries:
                # Превышен лимит запросов — ждём и повторяем
                wait = 10 * (attempt + 1)  # 10, 20, 30 секунд
                print(f"  ⚠️  API {resp.status_code}, повтор через {wait} сек (попытка {attempt+1}/{retries})...")
                time.sleep(wait)
                continue

            if resp.status_code in (401, 403):
                raise OzonAPIError(resp.status_code,
                    "Ошибка авторизации: проверь Client-Id и Api-Key в .env")

            try:
                data = resp.json()
            except Exception:
                data = {"_raw": resp.text}

            if not resp.ok:
                msg = data.get("message") or data.get("_raw", "")
                raise OzonAPIError(resp.status_code, str(msg)[:300])

            # Ozon иногда возвращает 200 с {"code": N, "message": "..."} — это тоже ошибка
            if data.get("code") and data["code"] != 0:
                raise OzonAPIError(200, data.get("message", str(data)))

            return data

        raise OzonAPIError(0, "Все попытки исчерпаны")

    # ── Товары ─────────────────────────────────────────────────────────────────

    def get_all_product_ids(self) -> list[int]:
        """
        Возвращает полный список product_id из каталога продавца.
        Автоматически листает все страницы.
        """
        ids: list[int] = []
        last_id = ""
        while True:
            data = self._post("/v3/product/list", {
                "filter":  {},
                "last_id": last_id,
                "limit":   1000,
            })
            result = data.get("result", {})
            items  = result.get("items", [])
            if not items:
                break
            ids.extend(item["product_id"] for item in items)
            last_id = result.get("last_id", "")
            if not last_id or len(items) < 1000:
                break
        return ids

    def get_product_info(self, product_ids: list[int]) -> list[dict]:
        """
        Возвращает детали товаров: название, артикул, статус архивации.
        Разбивает на батчи по 100 автоматически.
        """
        result: list[dict] = []
        BATCH = 100
        for i in range(0, len(product_ids), BATCH):
            batch = product_ids[i:i + BATCH]
            data  = self._post("/v3/product/info/list", {"product_id": batch})
            # v3 возвращает {"items": [...]}, без обёртки result
            items = data.get("items") or data.get("result", {}).get("items", [])
            # Диагностика: если вернулись не словари — показываем образец
            bad = [x for x in items if not isinstance(x, dict)]
            if bad:
                logger.warning("get_product_info: %d элементов — не dict. Образец: %r",
                               len(bad), bad[:2])
            # Защита: берём только словари (API иногда возвращает строки/None)
            result.extend(item for item in items if isinstance(item, dict))
        return result

    # ── Остатки ────────────────────────────────────────────────────────────────

    def get_stocks(self, product_ids: list[int]) -> list[dict]:
        """
        Возвращает остатки FBO/FBS для указанных товаров.
        Формат каждого элемента:
          {product_id, offer_id, stocks: [{type, present, reserved, sku, ...}]}
        """
        result: list[dict] = []
        BATCH = 100
        for i in range(0, len(product_ids), BATCH):
            batch = product_ids[i:i + BATCH]
            data  = self._post("/v4/product/info/stocks", {
                "filter": {
                    "product_id": batch,
                    "visibility": "ALL",
                },
                "last_id": "",
                "limit":   BATCH,
            })
            items = data.get("items") or data.get("result", {}).get("items", [])
            result.extend(items)
        return result

    # ── Склады FBO ─────────────────────────────────────────────────────────────

    def get_warehouses_fbo(self, max_postings: int = 500) -> list[dict]:
        """
        Получает список FBO-складов из FBO-постингов (заказов покупателей).
        Листает страницы пока не наберёт max_postings или не кончатся данные.
        Возвращает список {'warehouse_id': ..., 'name': ...}.
        """
        from datetime import date, timedelta
        date_to   = date.today().isoformat()
        date_from = (date.today() - timedelta(days=365 * 3)).isoformat()  # 3 года

        seen: dict[int, str] = {}
        offset = 0
        STEP   = 1000  # максимум за запрос

        while offset < max_postings:
            limit = min(STEP, max_postings - offset)
            try:
                data = self._post("/v2/posting/fbo/list", {
                    "dir": "desc",
                    "filter": {
                        "since":  f"{date_from}T00:00:00Z",
                        "to":     f"{date_to}T23:59:59Z",
                        "status": "",
                    },
                    "limit":  limit,
                    "offset": offset,
                    "with": {"analytics_data": True, "financial_data": False},
                })
            except OzonAPIError as e:
                logger.warning("FBO-постинги offset=%d: %s", offset, e)
                break

            result   = data.get("result", [])
            postings = result if isinstance(result, list) else result.get("postings", [])

            if not postings:
                break  # данных больше нет

            for p in postings:
                ad   = p.get("analytics_data") or {}
                wid  = ad.get("warehouse_id") or p.get("warehouse_id")
                name = ad.get("warehouse_name") or p.get("warehouse_name", "")
                if wid and wid not in seen:
                    seen[wid] = name or f"Склад {wid}"

            offset += len(postings)
            if len(postings) < limit:
                break  # последняя страница

        logger.info("Склады из FBO-постингов (%d обработано): %d шт.", offset, len(seen))
        return [{"warehouse_id": wid, "name": name} for wid, name in seen.items()]

    # ── Кластеры и черновик поставки (v2 API) ────────────────────────────────

    def get_clusters(self, cluster_type: str = "CLUSTER_TYPE_OZON") -> list[dict]:
        """
        Получает список доступных кластеров для поставки (FBO).
        Возвращает RAW-ответ clusters[] из /v1/cluster/list.
        """
        try:
            data = self._post("/v1/cluster/list", {
                "cluster_type": cluster_type,
            })
            clusters = data.get("clusters", data.get("result", {}).get("clusters", []))
            logger.info(f"Получено кластеров: {len(clusters)}")
            return clusters
        except OzonAPIError as e:
            logger.error(f"Ошибка получения кластеров ({cluster_type}): {e}")
            return []

    def create_draft(
        self,
        items: list[dict],      # [{"sku": int, "quantity": int}, ...]
        cluster_id: int | None = None,
    ) -> dict:
        """
        Создаёт черновик заявки на прямую поставку через /v1/draft/direct/create.

        Если cluster_id не указан — перебирает все кластеры, пока не найдёт подходящий.

        items: [{"sku": fbo_sku, "quantity": кол-во}, ...]
        cluster_id: macrolocal_cluster_id (опционально)

        Возвращает {"draft_id": int, "cluster_id": int, "cluster_name": str}.
        """
        clusters_raw = self.get_clusters()
        if not clusters_raw:
            raise OzonAPIError(0, "Не удалось получить список кластеров")

        # Если указан конкретный кластер — пробуем только его
        if cluster_id is not None:
            clusters_to_try = [
                c for c in clusters_raw
                if c.get("macrolocal_cluster_id") == cluster_id
            ]
            if not clusters_to_try:
                clusters_to_try = clusters_raw  # fallback
        else:
            clusters_to_try = clusters_raw

        last_error = ""
        for cluster in clusters_to_try:
            macro_id = cluster.get("macrolocal_cluster_id")
            name = cluster.get("name", "")
            if not macro_id:
                continue

            try:
                data = self._post("/v1/draft/direct/create", {
                    "cluster_info": {
                        "items": items,
                        "macrolocal_cluster_id": macro_id,
                    },
                    "deletion_sku_mode": "PARTIAL",
                })
            except OzonAPIError as e:
                last_error = str(e)
                logger.debug(f"Кластер {name} ({macro_id}): ошибка API — {e}")
                time.sleep(1)
                continue

            draft_id = data.get("draft_id")
            errors = data.get("errors", [])

            if not draft_id or draft_id == 0:
                reasons = []
                for err in errors:
                    reasons.extend(err.get("error_reasons", []))
                last_error = ", ".join(reasons) or str(errors)
                logger.debug(f"Кластер {name} ({macro_id}): {last_error}")
                time.sleep(1)
                continue

            logger.info(f"Создан черновик {draft_id} для кластера {name} ({macro_id})")
            return {
                "draft_id": int(draft_id),
                "cluster_id": int(macro_id),
                "cluster_name": name,
            }

        raise OzonAPIError(0, f"Ни один кластер не подошёл для товаров: {last_error}")

    def create_draft_crossdock(
        self,
        items: list[dict],              # [{"sku": int, "quantity": int}, ...]
        cluster_id: int,
        drop_off_warehouse_id: int,
        seller_warehouse_id: int,
    ) -> dict:
        """
        Создаёт черновик заявки на кроссдокинг через /v1/draft/crossdock/create.

        items: [{"sku": fbo_sku, "quantity": кол-во}, ...]
        cluster_id: macrolocal_cluster_id кластера-назначения
        drop_off_warehouse_id: ID склада кроссдокинга (точка сдачи)
        seller_warehouse_id: ID склада продавца

        Возвращает {"draft_id": int, "cluster_id": int, "cluster_name": str}.
        """
        clusters_raw = self.get_clusters()
        cluster_name = ""
        for c in clusters_raw:
            if c.get("macrolocal_cluster_id") == cluster_id:
                cluster_name = c.get("name", "")
                break

        try:
            data = self._post("/v1/draft/crossdock/create", {
                "cluster_info": {
                    "items": items,
                    "macrolocal_cluster_id": cluster_id,
                },
                "deletion_sku_mode": "PARTIAL",
                "delivery_info": {
                    "drop_off_warehouse": {
                        "warehouse_id": drop_off_warehouse_id,
                        "warehouse_type": "DELIVERY_POINT",
                    },
                    "seller_warehouse_id": seller_warehouse_id,
                    "type": "DROPOFF",
                },
            })
        except OzonAPIError as e:
            raise OzonAPIError(e.status, f"Кросс-докинг {cluster_name}: {e.message}")

        draft_id = data.get("draft_id")
        errors = data.get("errors", [])

        if not draft_id or draft_id == 0:
            reasons = []
            for err in errors:
                reasons.extend(err.get("error_reasons", []))
            msg = ", ".join(reasons) or str(errors)
            raise OzonAPIError(0, f"Кросс-докинг {cluster_name}: {msg}")

        logger.info(f"Создан crossdock черновик {draft_id} для кластера {cluster_name} ({cluster_id})")
        return {
            "draft_id": int(draft_id),
            "cluster_id": int(cluster_id),
            "cluster_name": cluster_name,
        }

    # ── v2: Информация о черновике (скоринг) ─────────────────────────────────

    def get_draft_info(self, draft_id: int, *, timeout: int = 60) -> dict:
        """
        Проверяет статус черновика через /v2/draft/create/info.
        Поллит до статуса SUCCESS или FAILED (макс. timeout секунд).

        Возвращает полный ответ:
          {"status": "SUCCESS", "clusters": [{
              "cluster_name": "...",
              "macrolocal_cluster_id": int,
              "warehouses": [{
                  "storage_warehouse": {"warehouse_id": int, "name": str, "address": str},
                  "availability_status": {"state": "...", "invalid_reason": "..."},
                  ...
              }]
          }], "errors": [...]}
        """
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            data = self._post("/v2/draft/create/info", {"draft_id": draft_id})

            status = data.get("status", "UNSPECIFIED")
            if status == "SUCCESS":
                logger.info(f"Черновик {draft_id}: скоринг завершён (SUCCESS)")
                return data
            if status == "FAILED":
                errors = data.get("errors", [])
                msg = str(errors)[:200] if errors else "скоринг не прошёл"
                logger.error(f"Черновик {draft_id}: FAILED — {msg}")
                raise OzonAPIError(0, f"Скоринг черновика не прошёл: {msg}")
            # IN_PROGRESS — ждём
            attempt += 1
            wait = min(3 + attempt, 10)
            logger.info(f"Черновик {draft_id}: статус {status}, ждём {wait}с...")
            time.sleep(wait)

        raise OzonAPIError(0, f"Черновик {draft_id}: таймаут ожидания скоринга ({timeout}с)")

    # ── v2: Таймслоты ────────────────────────────────────────────────────────

    def get_timeslots_v2(
        self,
        draft_id: int,
        cluster_id: int,
        warehouse_id: int | None,
        date_from: str,     # "YYYY-MM-DD"
        date_to: str,       # "YYYY-MM-DD"
        supply_type: str = "DIRECT",
    ) -> list[dict]:
        """
        Получает таймслоты через /v2/draft/timeslot/info.
        Максимальный период ~14 дней, при превышении автоматически уменьшает.

        supply_type: "DIRECT" для прямых поставок, "CROSSDOCK" для кросс-докинга.
        warehouse_id: может быть None для кросс-докинга (Ozon сам определяет склад).

        Возвращает список дней с таймслотами:
          [{"date": "YYYY-MM-DD", "timeslots": [{"from": "...", "to": "..."}, ...]}]
        """
        from datetime import datetime, timedelta

        d_from = datetime.strptime(date_from, "%Y-%m-%d")
        d_to   = datetime.strptime(date_to, "%Y-%m-%d")

        # Ограничиваем период до 14 дней (API не принимает больше)
        if (d_to - d_from).days > 14:
            d_to = d_from + timedelta(days=14)
            date_to = d_to.strftime("%Y-%m-%d")
            logger.info(f"Период ограничен до 14 дней: {date_from} — {date_to}")

        wh_entry = {"macrolocal_cluster_id": cluster_id}
        if warehouse_id is not None:
            wh_entry["storage_warehouse_id"] = warehouse_id

        data = self._post("/v2/draft/timeslot/info", {
            "draft_id": draft_id,
            "date_from": date_from,
            "date_to": date_to,
            "supply_type": supply_type,
            "selected_cluster_warehouses": [wh_entry],
        })

        # Проверяем ошибку периода
        err_reason = data.get("error_reason", "")
        if err_reason and err_reason not in ("UNSPECIFIED", ""):
            logger.warning(f"get_timeslots_v2: error_reason={err_reason}")
            return []

        # Ответ: {"result": {"drop_off_warehouse_timeslots": {"days": [...]}}}
        result = data.get("result") or data

        dwt = result.get("drop_off_warehouse_timeslots")
        if isinstance(dwt, dict):
            days = dwt.get("days", [])
        elif isinstance(dwt, list) and dwt:
            days = dwt[0].get("days", [])
        else:
            days = []

        # Нормализуем формат: [{date, timeslots: [{from, to}]}]
        normalized: list[dict] = []
        for day in days:
            date_str = day.get("date_in_timezone", "")[:10]
            slots = day.get("timeslots", [])
            if date_str and slots:
                norm_slots = []
                for s in slots:
                    norm_slots.append({
                        "from": s.get("from_in_timezone", ""),
                        "to": s.get("to_in_timezone", ""),
                    })
                normalized.append({"date": date_str, "timeslots": norm_slots})

        logger.info(f"get_timeslots_v2: draft={draft_id}, warehouse={warehouse_id}, "
                     f"дней с таймслотами: {len(normalized)}")
        return normalized

    # ── v2: Создание заявки на поставку ──────────────────────────────────────

    def create_supply_v2(
        self,
        draft_id: int,
        cluster_id: int,
        warehouse_id: int | None,
        timeslot_from: str,     # ISO datetime (from_in_timezone)
        timeslot_to: str,       # ISO datetime (to_in_timezone)
        supply_type: str = "DIRECT",
    ) -> dict:
        """
        Создаёт заявку на поставку через /v2/draft/supply/create.

        supply_type: "DIRECT" для прямых поставок, "CROSSDOCK" для кросс-докинга.
        warehouse_id: может быть None для кросс-докинга.

        Возвращает ответ: {"draft_id": int, "error_reasons": [...]}
        """
        wh_entry = {"macrolocal_cluster_id": cluster_id}
        if warehouse_id is not None:
            wh_entry["storage_warehouse_id"] = warehouse_id

        data = self._post("/v2/draft/supply/create", {
            "draft_id": draft_id,
            "supply_type": supply_type,
            "selected_cluster_warehouses": [wh_entry],
            "timeslot": {
                "from_in_timezone": timeslot_from,
                "to_in_timezone": timeslot_to,
            },
        })

        errors = data.get("error_reasons", [])
        errors = [e for e in errors if e != "UNSPECIFIED"]
        if errors:
            raise OzonAPIError(0, f"Ошибка создания заявки: {', '.join(errors)}")

        return data

    def get_supply_create_status(self, draft_id: int, *, timeout: int = 60) -> int:
        """
        Поллит /v2/draft/supply/create/status до завершения.
        Возвращает order_id при успехе.
        """
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            data = self._post("/v2/draft/supply/create/status", {"draft_id": draft_id})

            status = data.get("status", "UNSPECIFIED")
            if status == "SUCCESS":
                order_id = data.get("order_id", 0)
                logger.info(f"Заявка создана! draft={draft_id}, order_id={order_id}")
                return order_id
            if status == "FAILED":
                reasons = data.get("error_reasons", [])
                reasons = [r for r in reasons if r != "UNSPECIFIED"]
                msg = ", ".join(reasons) if reasons else "неизвестная ошибка"
                raise OzonAPIError(0, f"Создание заявки не удалось: {msg}")
            # IN_PROGRESS
            attempt += 1
            wait = min(2 + attempt, 8)
            time.sleep(wait)

        raise OzonAPIError(0, f"Таймаут создания заявки ({timeout}с)")

    # ── Заявки на поставку ─────────────────────────────────────────────────────

    def cancel_supply_order(self, order_id: int) -> str:
        """
        Отменяет заявку на поставку через Ozon API.
        order_id — целое число из URL (order_id из /v3/supply-order/get).
        Возвращает operation_id или бросает OzonAPIError.
        """
        data = self._post("/v1/supply-order/cancel", {"order_id": order_id})
        return data.get("operation_id", "")

    # Все известные состояния заявок FBO
    SUPPLY_ORDER_STATES = [
        "DATA_FILLING", "ACCEPTED", "SUPPLY_ARRIVAL", "PROCESSING",
        "PROCESSED", "CANCELLED", "REJECTED", "COMPLETED", "PENDING",
        "CREATED", "CONFIRMED", "AT_DROP_OFF", "IN_TRANSIT", "DELIVERED",
    ]

    def list_supply_order_ids(self, states: list[str] | None = None) -> list[int]:
        """
        Возвращает все ID заявок на поставку через /v3/supply-order/list.
        Автоматически листает страницы.
        """
        if states is None:
            states = self.SUPPLY_ORDER_STATES

        all_ids: list[int] = []
        last_id = ""
        while True:
            body: dict = {
                "limit":   100,
                "filter":  {"states": states},
                "sort_by": 1,  # 1 = по дате создания
            }
            if last_id:
                body["last_id"] = last_id

            try:
                data = self._post("/v3/supply-order/list", body)
            except OzonAPIError:
                break

            ids = data.get("order_ids") or []
            all_ids.extend(ids)
            last_id = data.get("last_id", "")
            if not last_id or len(ids) < 100:
                break

        return all_ids

    def get_supply_orders(self, order_ids: list[int]) -> list[dict]:
        """
        Возвращает детали заявок по их ID через /v3/supply-order/get.
        Разбивает на батчи по 50 (ограничение API).
        """
        result: list[dict] = []
        BATCH = 50
        for i in range(0, len(order_ids), BATCH):
            batch = order_ids[i:i + BATCH]
            try:
                data = self._post("/v3/supply-order/get", {"order_ids": batch})
                result.extend(data.get("orders") or [])
            except OzonAPIError as e:
                logger.warning("get_supply_orders batch %s: %s", batch[:3], e)
        return result

    def update_supply_driver(
        self,
        supply_order_id: str,
        driver_name: str,
        driver_phone: str,
        car_number: str,
    ) -> dict:
        """Обновляет данные водителя для заявки на поставку."""
        data = self._post("/v1/supply/driver", {
            "supply_order_id": supply_order_id,
            "driver": {
                "name":       driver_name,
                "phone":      driver_phone,
                "car_number": car_number,
            },
        })
        return data.get("result") or data

    def set_supply_order_vehicle(
        self,
        supply_order_id: int,
        driver_name: str,
        driver_phone: str,
        vehicle_model: str,
        vehicle_number: str,
    ) -> dict:
        """
        Указывает данные водителя и автомобиля через
        POST /v1/supply-order/pass/create.
        Возвращает {"error_reasons": [...], "operation_id": "..."}.
        """
        data = self._post("/v1/supply-order/pass/create", {
            "supply_order_id": supply_order_id,
            "vehicle": {
                "driver_name":    driver_name,
                "driver_phone":   driver_phone,
                "vehicle_model":  vehicle_model,
                "vehicle_number": vehicle_number,
            },
        })
        return data

    def get_supply_order_vehicle_status(self, operation_id: str) -> dict:
        """
        Проверяет статус операции указания данных водителя через
        POST /v1/supply-order/pass/status.
        Возвращает {"errors": [...], "result": "Success|InProgress|Failed|Unknown"}.
        """
        data = self._post("/v1/supply-order/pass/status", {
            "operation_id": operation_id,
        })
        return data

    def get_supply_labels_pdf_url(self, supply_order_id: str) -> str:
        """Возвращает URL PDF-этикеток для поставки."""
        data = self._post("/v1/supply/label/get", {
            "supply_order_id": supply_order_id,
        })
        return (data.get("result", {}).get("url")
                or data.get("url", ""))

    # ── Грузоместа (cargoes) ─────────────────────────────────────────────────

    def create_cargoes(
        self,
        supply_id: int,
        cargoes: list[dict],
        delete_current: bool = True,
    ) -> str:
        """
        Устанавливает грузоместа для поставки через /v1/cargoes/create.

        supply_id:  ID поставки (из orders.supplies.supply_id)
        cargoes:    список грузомест, каждый:
            {
                "key": "box-1",           # уникальный ключ
                "value": {
                    "type": "BOX",        # BOX или PALLET
                    "items": [
                        {"offer_id": "арт1", "quantity": 5, "barcode": "...", "quant": 1},
                        ...
                    ]
                }
            }
        delete_current: удалить предыдущие грузоместа (True)

        Возвращает operation_id для polling.
        """
        data = self._post("/v1/cargoes/create", {
            "supply_id": supply_id,
            "cargoes": cargoes,
            "delete_current_version": delete_current,
        })

        errors = data.get("errors", {})
        err_reasons = errors.get("error_reasons", [])
        err_reasons = [r for r in err_reasons if r != "UNSPECIFIED" and r]
        if err_reasons:
            raise OzonAPIError(0, f"Ошибка создания грузомест: {', '.join(err_reasons)}")

        items_valid = errors.get("items_validation", [])
        if items_valid:
            msgs = [f"{v.get('cargo_key','?')}: {v.get('type','?')}" for v in items_valid[:5]]
            logger.warning(f"Валидация товаров в грузоместах: {msgs}")

        return data.get("operation_id", "")

    def get_cargoes_create_status(self, operation_id: str, *, timeout: int = 60) -> list[dict]:
        """
        Поллит /v2/cargoes/create/info до завершения.
        Возвращает список созданных грузомест: [{"key": "box-1", "value": {"cargo_id": 123}}, ...]
        """
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            data = self._post("/v2/cargoes/create/info", {"operation_id": operation_id})

            status = data.get("status", "STATUS_UNSPECIFIED")
            if status == "SUCCESS":
                result = data.get("result", {})
                cargoes = result.get("cargoes", [])
                logger.info(f"Грузоместа созданы: {len(cargoes)} шт.")
                return cargoes
            if status == "FAILED":
                errors = data.get("errors", {})
                err_reasons = errors.get("error_reasons", [])
                items_err = errors.get("items_validation", [])
                msg_parts = [r for r in err_reasons if r not in ("", "UNSPECIFIED")]
                for iv in items_err[:3]:
                    msg_parts.append(f"{iv.get('cargo_key','')}: {iv.get('type','')}")
                msg = ", ".join(msg_parts) or "неизвестная ошибка"
                raise OzonAPIError(0, f"Грузоместа не созданы: {msg}")
            # IN_PROGRESS
            attempt += 1
            wait = min(2 + attempt, 8)
            time.sleep(wait)

        raise OzonAPIError(0, f"Таймаут создания грузомест ({timeout}с)")

    def get_cargoes(self, supply_ids: list[int]) -> list[dict]:
        """
        Получает информацию о грузоместах через /v1/cargoes/get.
        Возвращает: [{"supply_id": int, "cargoes": [...], "bundle_id": "..."}, ...]
        """
        data = self._post("/v1/cargoes/get", {
            "supply_ids": [str(sid) for sid in supply_ids],
        })
        return data.get("supply", [])

    def delete_cargoes(self, supply_id: int, cargo_ids: list[int]) -> str:
        """Удаляет грузоместа. Возвращает operation_id."""
        data = self._post("/v1/cargoes/delete", {
            "supply_id": supply_id,
            "cargo_ids": [str(cid) for cid in cargo_ids],
        })
        return data.get("operation_id", "")

    # ── Этикетки грузомест ────────────────────────────────────────────────────

    def create_cargo_labels(self, supply_id: int, cargo_ids: list[int]) -> str:
        """
        Генерирует этикетки для грузомест через /v1/cargoes-label/create.
        Возвращает operation_id.
        """
        data = self._post("/v1/cargoes-label/create", {
            "supply_id": supply_id,
            "cargoes": [{"cargo_id": cid} for cid in cargo_ids],
        })
        errors = data.get("errors", {})
        err_reasons = errors.get("error_reasons", [])
        err_reasons = [r for r in err_reasons if r not in ("", "UNSPECIFIED")]
        if err_reasons:
            raise OzonAPIError(0, f"Ошибка генерации этикеток: {', '.join(err_reasons)}")
        return data.get("operation_id", "")

    def get_cargo_labels(self, operation_id: str, *, timeout: int = 60) -> str:
        """
        Поллит /v1/cargoes-label/get до готовности.
        Возвращает URL PDF-файла с этикетками.
        """
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            data = self._post("/v1/cargoes-label/get", {"operation_id": operation_id})

            status = data.get("status", "IN_PROGRESS")
            if status == "SUCCESS":
                result = data.get("result", {})
                url = result.get("file_url", "")
                logger.info(f"Этикетки готовы: {url[:80]}")
                return url
            if status == "FAILED":
                errors = data.get("errors", {})
                reasons = errors.get("error_reasons", [])
                msg = ", ".join(r for r in reasons if r) or "ошибка генерации"
                raise OzonAPIError(0, f"Этикетки не сгенерированы: {msg}")
            # IN_PROGRESS
            attempt += 1
            wait = min(2 + attempt, 8)
            time.sleep(wait)

        raise OzonAPIError(0, f"Таймаут генерации этикеток ({timeout}с)")

    # ── Пропуска (arrival passes) ─────────────────────────────────────────────

    def list_passes(self, warehouse_ids: list[int] | None = None,
                    only_active: bool = True) -> list[dict]:
        """
        Получает список пропусков через /v1/pass/list.
        Возвращает arrival_passes[].
        """
        body: dict = {"limit": 1000, "cursor": "", "filter": {
            "only_active_passes": only_active,
        }}
        if warehouse_ids:
            body["filter"]["warehouse_ids"] = [str(w) for w in warehouse_ids]
        all_passes: list[dict] = []
        while True:
            data = self._post("/v1/pass/list", body)
            all_passes.extend(data.get("arrival_passes", []))
            cursor = data.get("cursor", "")
            if not cursor:
                break
            body["cursor"] = cursor
        return all_passes

    def create_pass(self, carriage_id: int, driver_name: str,
                    driver_phone: str, vehicle_model: str,
                    vehicle_license_plate: str,
                    with_returns: bool = False) -> list[str]:
        """
        Создаёт пропуск через /v1/carriage/pass/create.
        Возвращает список arrival_pass_ids.
        """
        data = self._post("/v1/carriage/pass/create", {
            "carriage_id": carriage_id,
            "arrival_passes": [{
                "driver_name": driver_name,
                "driver_phone": driver_phone,
                "vehicle_model": vehicle_model,
                "vehicle_license_plate": vehicle_license_plate,
                "with_returns": with_returns,
            }],
        })
        return data.get("arrival_pass_ids", [])

    def update_pass(self, carriage_id: int, pass_id: int,
                    driver_name: str, driver_phone: str,
                    vehicle_model: str,
                    vehicle_license_plate: str,
                    with_returns: bool = False) -> dict:
        """
        Обновляет пропуск через /v1/carriage/pass/update.
        """
        data = self._post("/v1/carriage/pass/update", {
            "carriage_id": carriage_id,
            "arrival_passes": [{
                "id": pass_id,
                "driver_name": driver_name,
                "driver_phone": driver_phone,
                "vehicle_model": vehicle_model,
                "vehicle_license_plate": vehicle_license_plate,
                "with_returns": with_returns,
            }],
        })
        return data

    def delete_passes(self, carriage_id: int,
                      pass_ids: list[int]) -> dict:
        """Удаляет пропуска через /v1/carriage/pass/delete."""
        data = self._post("/v1/carriage/pass/delete", {
            "carriage_id": carriage_id,
            "arrival_pass_ids": [str(pid) for pid in pass_ids],
        })
        return data

    # ── Редактирование товарного состава поставки ─────────────────────────────

    def update_supply_content(self, order_id: int, supply_id: int,
                              items: list[dict]) -> str:
        """
        Редактирует товарный состав через /v1/supply-order/content/update.
        items: [{"sku": int, "quantity": int, "quant": int}, ...]
        Возвращает operation_id.
        """
        data = self._post("/v1/supply-order/content/update", {
            "order_id": order_id,
            "supply_id": supply_id,
            "items": items,
        })
        errors = data.get("errors", [])
        errors = [e for e in errors if e and e != "UNSPECIFIED"]
        if errors:
            raise OzonAPIError(0,
                f"Ошибка редактирования состава: {', '.join(errors)}")
        return data.get("operation_id", "")

    def get_supply_content_update_status(self, operation_id: str,
                                          *, timeout: int = 60) -> str:
        """
        Поллит /v1/supply-order/content/update/status.
        Возвращает new_bundle_id при SUCCESS.
        """
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            data = self._post("/v1/supply-order/content/update/status", {
                "operation_id": operation_id,
            })
            status = data.get("status", "IN_PROGRESS")
            if status == "SUCCESS":
                return data.get("new_bundle_id", "")
            if status == "ERROR":
                errors = data.get("errors", [])
                errors = [e for e in errors if e and e != "UNSPECIFIED"]
                msg = ", ".join(errors) or "неизвестная ошибка"
                raise OzonAPIError(0, f"Редактирование состава: {msg}")
            attempt += 1
            time.sleep(min(2 + attempt, 8))
        raise OzonAPIError(0,
            f"Таймаут редактирования состава ({timeout}с)")

    # ── Чек-лист ───────────────────────────────────────────────────────────────

    def get_cargoes_checklist(self, supply_ids: list[int]) -> list[dict]:
        """
        Получает чек-лист по грузоместам через /v1/cargoes/rules/get.
        """
        data = self._post("/v1/cargoes/rules/get", {
            "supply_ids": [str(sid) for sid in supply_ids],
        })
        return data.get("supply_check_lists", [])
