"""
ОДНОРАЗОВЫЙ скрипт.

Выкачивает список пользователей из Remnawave (с пагинацией) и заливает
в локальную SQLite-таблицу:

    subscriptions (
        user_id INTEGER PRIMARY KEY,
        subscription_expires_at TEXT NOT NULL,
        runout_notified INTEGER DEFAULT 0,
        expiring_tomorrow_notified INTEGER DEFAULT 0
    )

Логика:
- Берём только тех пользователей, у кого есть telegramId и expireAt.
- subscription_expires_at сохраняем как YYYY-MM-DD (так же, как делает
  databases.upsert_subscription_days — это важно для совместимости с
  ikbs.py/expire_functions.py, которые сравнивают через date()).
- UPSERT: если строки ещё нет — создаём с флагами notified = 0.
  Если строка есть — ОБНОВЛЯЕМ только subscription_expires_at, флаги
  notified НЕ трогаем (чтобы не сдвинуть логику уведомлений в проде).
- Если у одного telegramId несколько пользователей в панели — берём
  максимальный expireAt.

Запуск:
    1) DRY_RUN=True — посмотреть сводку, ничего не пишется в БД.
    2) DRY_RUN=False — залить в БД.
    3) Удалить файл.
"""

import os
import sys
import time
import sqlite3 as sq
from datetime import datetime

import dotenv
import requests

dotenv.load_dotenv()

# =========================== НАСТРОЙКИ ===========================
DRY_RUN = False                 # <-- сначала True. После проверки — False.
DB_PATH = 'database.db'
PAGE_SIZE = 200
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES = 0.2
MAX_PAGES_SAFETY = 500         # предохранитель от бесконечного цикла
# =================================================================

base_url = os.getenv("REMNAWAVE_BASE_URL")
token = os.getenv("REMNAWAVE_TOKEN")

if not base_url or not token:
    print("[FATAL] REMNAWAVE_BASE_URL / REMNAWAVE_TOKEN не заданы в .env")
    sys.exit(1)


def fetch_page(start: int, size: int):
    """Возвращает (users_list, total_or_None)."""
    r = requests.get(
        f"{base_url}/api/users",
        headers={"Authorization": f"Bearer {token}"},
        params={"size": str(size), "start": str(start)},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()

    # Возможные структуры ответа Remnawave:
    #   { "response": { "users": [...], "total": N } }
    #   { "response": [...] }
    #   { "users": [...], "total": N }
    #   [...]
    def extract(d):
        if isinstance(d, list):
            return d, None
        if isinstance(d, dict):
            if "users" in d and isinstance(d["users"], list):
                return d["users"], d.get("total")
            if "response" in d:
                return extract(d["response"])
        return [], None

    users, total = extract(data)
    return users, total


def fetch_all_users():
    """Скачивает всех пользователей с пагинацией."""
    all_users = []
    start = 0
    total = None
    for page_idx in range(MAX_PAGES_SAFETY):
        users, page_total = fetch_page(start, PAGE_SIZE)
        if page_total is not None and total is None:
            total = page_total

        print(f"  page #{page_idx}: start={start} получено={len(users)} total={total}")
        if not users:
            break
        all_users.extend(users)

        # условия остановки
        if total is not None and len(all_users) >= total:
            break
        if len(users) < PAGE_SIZE:
            break

        start += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_PAGES)
    return all_users


def extract_tg_and_expire(user: dict):
    """Возвращает (tg_id_int_or_None, expires_date_str_YYYY_MM_DD_or_None)."""
    if not isinstance(user, dict):
        return None, None

    tg_raw = user.get("telegramId")
    if tg_raw in (None, "", 0):
        return None, None
    try:
        tg_id = int(tg_raw)
    except (TypeError, ValueError):
        return None, None

    exp_raw = user.get("expireAt") or user.get("expire_at")
    if not exp_raw:
        return tg_id, None

    exp_str = str(exp_raw).strip()
    # Normalize trailing 'Z'
    if exp_str.endswith("Z"):
        exp_str = exp_str[:-1] + "+00:00"
    # Попытки парсинга
    parsed = None
    try:
        parsed = datetime.fromisoformat(exp_str)
    except Exception:
        # Fallback: обрезаем миллисекунды / таймзону
        try:
            parsed = datetime.strptime(exp_str[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            try:
                parsed = datetime.strptime(exp_str[:10], "%Y-%m-%d")
            except Exception:
                return tg_id, None

    return tg_id, parsed.date().isoformat()


def ensure_table():
    """Создаёт таблицу, если вдруг её ещё нет. Колонки должны совпадать с используемыми в проекте."""
    with sq.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS subscriptions '
            '(user_id INTEGER PRIMARY KEY, subscription_expires_at TEXT NOT NULL)'
        )
        for alter in (
            'ALTER TABLE subscriptions ADD COLUMN runout_notified INTEGER DEFAULT 0',
            'ALTER TABLE subscriptions ADD COLUMN expiring_tomorrow_notified INTEGER DEFAULT 0',
        ):
            try:
                cur.execute(alter)
            except Exception:
                pass
        con.commit()


def upsert_rows(rows):
    """rows: list[(user_id, expires_yyyy_mm_dd)]. Обновляет только дату, флаги не трогает."""
    if not rows:
        return 0, 0
    inserted = 0
    updated = 0
    with sq.connect(DB_PATH) as con:
        cur = con.cursor()
        for user_id, exp_date in rows:
            cur.execute('SELECT 1 FROM subscriptions WHERE user_id = ?', (user_id,))
            exists = cur.fetchone() is not None
            cur.execute(
                """
                INSERT INTO subscriptions (user_id, subscription_expires_at, runout_notified, expiring_tomorrow_notified)
                VALUES (?, ?, 0, 0)
                ON CONFLICT(user_id) DO UPDATE SET
                    subscription_expires_at = excluded.subscription_expires_at
                """,
                (user_id, exp_date),
            )
            if exists:
                updated += 1
            else:
                inserted += 1
        con.commit()
    return inserted, updated


def main():
    print("=" * 72)
    print(f"[fillbd] DRY_RUN={DRY_RUN}  DB={DB_PATH}  BASE={base_url}")
    print("=" * 72)

    print("Скачиваю пользователей из Remnawave...")
    users = fetch_all_users()
    print(f"Всего получено пользователей: {len(users)}")

    prepared = []            # (tg_id, expires_yyyy_mm_dd) — без дублей, max expireAt
    by_tg: dict[int, str] = {}
    stats = {
        "total_api": len(users),
        "no_tg_id": 0,
        "no_expire": 0,
        "duplicates_collapsed": 0,
        "to_write": 0,
    }

    for u in users:
        tg_id, exp_date = extract_tg_and_expire(u)
        if tg_id is None:
            stats["no_tg_id"] += 1
            continue
        if exp_date is None:
            stats["no_expire"] += 1
            continue
        prev = by_tg.get(tg_id)
        if prev is None:
            by_tg[tg_id] = exp_date
        else:
            stats["duplicates_collapsed"] += 1
            if exp_date > prev:
                by_tg[tg_id] = exp_date

    prepared = sorted(by_tg.items(), key=lambda p: p[1], reverse=True)
    stats["to_write"] = len(prepared)

    print("Первые 10 записей для заливки:")
    for tg_id, exp_date in prepared[:10]:
        print(f"  tg_id={tg_id}  subscription_expires_at={exp_date}")

    if DRY_RUN:
        print("DRY_RUN=True: в БД ничего не пишу.")
    else:
        ensure_table()
        inserted, updated = upsert_rows(prepared)
        stats["inserted"] = inserted
        stats["updated_expires_only"] = updated

    print("=" * 72)
    print("ИТОГО:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print("=" * 72)
    if DRY_RUN:
        print("Это был DRY_RUN. Если всё ок — поставьте DRY_RUN=False и запустите снова.")


if __name__ == "__main__":
    main()
