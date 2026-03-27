"""
tgbot_db.py — расширение БД для хранения учетных данных пользователей Telegram.

Добавляет таблицу users для хранения Client-ID и API-Key.
"""

import sqlite3
import os
from datetime import datetime

# Используем ту же базу, что и основной проект
PARENT_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), "warehouse.db")


def get_conn() -> sqlite3.Connection:
    """Открывает соединение с БД."""
    conn = sqlite3.connect(PARENT_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_users_table() -> None:
    """Создаёт таблицу users, если её ещё нет."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_users (
                telegram_id     INTEGER PRIMARY KEY,  -- Telegram User ID
                username        TEXT,                  -- @username или имя пользователя
                client_id       TEXT NOT NULL,         -- Ozon Client-ID
                api_key         TEXT NOT NULL,         -- Ozon API-Key
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


def save_user_credentials(telegram_id: int, username: str, client_id: str, api_key: str) -> None:
    """Сохраняет или обновляет учетные данные пользователя."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO tg_users (telegram_id, username, client_id, api_key)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username = excluded.username,
                client_id = excluded.client_id,
                api_key = excluded.api_key,
                updated_at = datetime('now')
        """, (telegram_id, username, client_id, api_key))
        conn.commit()


def get_user_credentials(telegram_id: int) -> dict | None:
    """Получает учетные данные пользователя. Возвращает None если не найдены."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT client_id, api_key FROM tg_users WHERE telegram_id = ?
        """, (telegram_id,)).fetchone()
        if row:
            return dict(row)
    return None


def user_exists(telegram_id: int) -> bool:
    """Проверяет, зарегистрирован ли пользователь."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT 1 FROM tg_users WHERE telegram_id = ?
        """, (telegram_id,)).fetchone()
    return row is not None
