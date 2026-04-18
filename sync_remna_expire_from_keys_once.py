"""
ОДНОРАЗОВЫЙ скрипт миграции.

Для каждого buyer_id из таблицы `keys`, у которого expiration_date > сегодня,
создаёт пользователя в Remnawave с expireAt = expiration_date (конец дня).
Тело запроса ПОЛНОСТЬЮ повторяет vpn.py Vpn.create_new_user (строки 26-42),
меняется только значение expireAt (вместо now+30 дней — реальная дата из БД).

Безопасность (прод):
- DRY_RUN = True — по умолчанию ничего не отправляет, только печатает план.
- Если пользователь с таким telegram_id уже есть в Remnawave — ПРОПУСКАЕМ
  (чтобы случайно не сломать уже работающую подписку).
- На каждую запись — свой try/except; одна ошибка не валит весь прогон.
- Между вызовами небольшая пауза, чтобы не душить панель.

Запуск:
    1) Проверить вывод в DRY_RUN.
    2) Поставить DRY_RUN = False и запустить повторно.
    3) После успешного прогона — удалить файл.
"""

import os
import sys
import time
import sqlite3 as sq
from datetime import datetime, date, timedelta

import dotenv
import requests

dotenv.load_dotenv()

# =========================== НАСТРОЙКИ ===========================
DRY_RUN = False                  # <-- сначала True. После проверки — False.
DB_PATH = 'database.db'
SLEEP_BETWEEN_CALLS = 0.3       # секунд между запросами к Remnawave
REQUEST_TIMEOUT = 20            # таймаут HTTP-запросов
# =================================================================

BASE_URL = os.getenv("REMNAWAVE_BASE_URL")
TOKEN = os.getenv("REMNAWAVE_TOKEN")

if not BASE_URL or not TOKEN:
    print("[FATAL] REMNAWAVE_BASE_URL / REMNAWAVE_TOKEN не заданы в .env")
    sys.exit(1)


def get_user_by_tg_id(tg_id: int):
    """Возвращает (status_code, json_or_none)."""
    try:
        r = requests.get(
            f"{BASE_URL}/api/users/by-telegram-id/{tg_id}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            timeout=REQUEST_TIMEOUT,
        )
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, None
    except Exception as e:
        print(f"  [WARN] get_user_by_tg_id({tg_id}) exception: {e}")
        return None, None


def user_exists_in_panel(tg_id: int) -> bool:
    """True если пользователь уже существует в Remnawave.

    Формат ответа панели может быть:
      - {"response": {...}} или {"response": [...]}
      - {"response": null} или {} при отсутствии
      - 404 при отсутствии
    Любое сомнение трактуем как "СУЩЕСТВУЕТ" — безопаснее пропустить, чем продублировать.
    """
    status, payload = get_user_by_tg_id(tg_id)
    if status is None:
        # сетевая ошибка — считаем, что существует (чтобы не создать дубль)
        return True
    if status == 404:
        return False
    if status >= 500:
        # ошибка сервера — безопаснее считать, что существует
        return True
    if status == 200 and isinstance(payload, dict):
        resp = payload.get("response", payload)
        if resp is None:
            return False
        if isinstance(resp, list):
            return len(resp) > 0
        if isinstance(resp, dict):
            # непустой словарь с данными пользователя
            return bool(resp)
    # остальные случаи — на всякий случай True
    return True


def create_user_with_expire(tg_id: int, expire_at_iso: str):
    """Создание пользователя, ТЕЛО 1:1 как в vpn.py:26-42, кроме expireAt."""
    body = requests.post(
        f"{BASE_URL}/api/users",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}",
        },
        json={
            "username": f'user_{tg_id}',
            "trafficLimitBytes": 300000000000,
            "expireAt": expire_at_iso,                     # <-- отличие от vpn.py
            "createdAt": datetime.now().isoformat(),
            "telegramId": tg_id,
            "hwidDeviceLimit": 3,
            "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
        },
        timeout=REQUEST_TIMEOUT,
    )
    try:
        return body.status_code, body.json()
    except Exception:
        return body.status_code, None


def fetch_targets():
    """Возвращает список (buyer_id, max_expiration_date_str) для expiration_date > today."""
    today_str = date.today().isoformat()
    with sq.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT buyer_id, MAX(expiration_date) AS exp
            FROM keys
            WHERE buyer_id IS NOT NULL
              AND expiration_date IS NOT NULL
              AND TRIM(expiration_date) <> ''
              AND expiration_date > ?
            GROUP BY buyer_id
            ORDER BY exp DESC
            """,
            (today_str,),
        )
        return cur.fetchall()


def parse_expiration_to_iso(exp_str: str):
    """Преобразует 'YYYY-MM-DD' (или с временем) в ISO-строку конца дня.

    Возвращает None, если распарсить не удалось.
    """
    s = (exp_str or "").strip()
    if not s:
        return None
    # Пробуем сначала как 'YYYY-MM-DD'
    try:
        d = datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        try:
            d = datetime.fromisoformat(s).date()
        except Exception:
            return None
    # Конец дня, чтобы подписка действовала полностью в день expiration_date
    dt = datetime.combine(d, datetime.min.time()) + timedelta(hours=23, minutes=59, seconds=59)
    return dt.isoformat()


def main():
    print("=" * 72)
    print(f"[sync_remna_expire_from_keys_once] DRY_RUN={DRY_RUN}")
    print(f"BASE_URL={BASE_URL}")
    print("=" * 72)

    targets = fetch_targets()
    print(f"Найдено кандидатов (buyer_id с expiration_date > сегодня): {len(targets)}")

    stats = {
        "total": len(targets),
        "skipped_bad_date": 0,
        "skipped_exists": 0,
        "would_create": 0,
        "created_ok": 0,
        "create_already_exists": 0,
        "errors": 0,
    }

    for idx, (buyer_id, exp_str) in enumerate(targets, 1):
        prefix = f"[{idx}/{len(targets)}] tg_id={buyer_id} exp={exp_str!r}"
        try:
            expire_at_iso = parse_expiration_to_iso(exp_str)
            if not expire_at_iso:
                stats["skipped_bad_date"] += 1
                print(f"{prefix} SKIP: не удалось распарсить дату")
                continue

            # Доп. проверка: дата в будущем
            try:
                exp_date_obj = datetime.fromisoformat(expire_at_iso).date()
            except Exception:
                exp_date_obj = None
            if exp_date_obj is not None and exp_date_obj <= date.today():
                stats["skipped_bad_date"] += 1
                print(f"{prefix} SKIP: дата не в будущем после парсинга")
                continue

            if user_exists_in_panel(int(buyer_id)):
                stats["skipped_exists"] += 1
                print(f"{prefix} SKIP: пользователь уже есть в Remnawave")
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            if DRY_RUN:
                stats["would_create"] += 1
                print(f"{prefix} DRY-RUN: создал бы с expireAt={expire_at_iso}")
            else:
                status, payload = create_user_with_expire(int(buyer_id), expire_at_iso)
                msg = ""
                if isinstance(payload, dict):
                    msg = str(payload.get("message", "") or "")
                if status in (200, 201):
                    stats["created_ok"] += 1
                    print(f"{prefix} OK: создан, expireAt={expire_at_iso}")
                elif msg and "already exists" in msg.lower():
                    stats["create_already_exists"] += 1
                    print(f"{prefix} SKIP: панель сказала already exists — {msg}")
                else:
                    stats["errors"] += 1
                    print(f"{prefix} ERROR: status={status} payload={payload}")
            time.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as e:
            stats["errors"] += 1
            print(f"{prefix} EXCEPTION: {e}")
            continue

    print("=" * 72)
    print("ИТОГО:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print("=" * 72)
    if DRY_RUN:
        print("Это был DRY_RUN. Если всё ок — поставьте DRY_RUN=False и запустите снова.")


if __name__ == "__main__":
    main()
