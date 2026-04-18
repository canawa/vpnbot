#!/usr/bin/env python3
"""
Одноразовый скрипт: для каждого buyer_id с активным ключом в таблице keys
синхронизирует пользователя в Remnawave с expireAt = конец дня expiration_date.

Не импортирует vpn.py (в vpn.py при импорте выполняется тестовый запрос).

Запуск из каталога проекта (где лежит database.db):
  py -3 sync_remna_expire_from_keys_once.py --dry-run
  py -3 sync_remna_expire_from_keys_once.py --apply

По умолчанию --dry-run: только печать, без POST/PATCH.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import dotenv
import requests

dotenv.load_dotenv()

BASE_URL = (os.getenv("REMNAWAVE_BASE_URL") or "").rstrip("/")
TOKEN = os.getenv("REMNAWAVE_TOKEN") or ""

DEFAULT_SQUAD = "6f11955f-6b95-4f96-bba4-3d866de8ce83"
TRAFFIC_LIMIT = 300_000_000_000
HWID_LIMIT = 3


def _expire_at_iso_for_key_date(expiration_date: date) -> str:
    """Конец календарного дня expiration_date (локальное время процесса)."""
    end = datetime.combine(expiration_date, time(23, 59, 59))
    return end.isoformat()


def _parse_expiration(raw: str | None) -> date | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _user_payload(tg_id: int, expire_at_iso: str) -> dict:
    return {
        "username": f"user_{tg_id}",
        "trafficLimitBytes": TRAFFIC_LIMIT,
        "expireAt": expire_at_iso,
        "createdAt": datetime.now().isoformat(),
        "telegramId": tg_id,
        "hwidDeviceLimit": HWID_LIMIT,
        "activeInternalSquads": [DEFAULT_SQUAD],
    }


def _headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


def _user_already_exists(resp_json: dict) -> bool:
    if not isinstance(resp_json, dict):
        return False
    msg = str(resp_json.get("message", "") or "")
    if msg == "User username already exists":
        return True
    return "already exists" in msg.lower()


def create_user(tg_id: int, expire_at_iso: str) -> tuple[int, dict]:
    r = requests.post(
        f"{BASE_URL}/api/users",
        headers=_headers(),
        json=_user_payload(tg_id, expire_at_iso),
        timeout=60,
    )
    try:
        body = r.json()
    except Exception:
        body = {"_raw": r.text[:500]}
    return r.status_code, body


def patch_user(tg_id: int, expire_at_iso: str) -> tuple[int, dict]:
    r = requests.patch(
        f"{BASE_URL}/api/users",
        headers=_headers(),
        json=_user_payload(tg_id, expire_at_iso),
        timeout=60,
    )
    try:
        body = r.json()
    except Exception:
        body = {"_raw": r.text[:500]}
    return r.status_code, body


def get_user_by_telegram(tg_id: int) -> tuple[int, dict | list | None]:
    r = requests.get(
        f"{BASE_URL}/api/users/by-telegram-id/{tg_id}",
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=60,
    )
    try:
        body = r.json()
    except Exception:
        body = None
    return r.status_code, body


def load_active_buyers(db_path: Path) -> list[tuple[int, date]]:
    """
    Один buyer_id — одна дата окончания: максимальная expiration_date среди активных ключей.
    Активный: date(expiration_date) >= сегодня (включительно).
    """
    today = date.today()
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT buyer_id, MAX(expiration_date) AS exp
            FROM keys
            WHERE buyer_id IS NOT NULL
              AND expiration_date IS NOT NULL
              AND TRIM(COALESCE(expiration_date, '')) != ''
              AND date(expiration_date) >= date(?)
            GROUP BY buyer_id
            """,
            (today.isoformat(),),
        )
        rows = cur.fetchall()
    out: list[tuple[int, date]] = []
    for buyer_id, exp_raw in rows:
        try:
            uid = int(buyer_id)
        except (TypeError, ValueError):
            continue
        exp_d = _parse_expiration(exp_raw if isinstance(exp_raw, str) else str(exp_raw))
        if exp_d is None:
            continue
        if exp_d >= today:
            out.append((uid, exp_d))
    return sorted(out, key=lambda x: x[0])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / "database.db",
        help="Путь к SQLite (по умолчанию database.db рядом со скриптом)",
    )
    p.add_argument("--apply", action="store_true", help="Выполнить POST/PATCH (без флага — только план)")
    args = p.parse_args()

    if not BASE_URL or not TOKEN:
        print("ERROR: задайте REMNAWAVE_BASE_URL и REMNAWAVE_TOKEN в окружении или .env", file=sys.stderr)
        return 2

    if not args.db.is_file():
        print(f"ERROR: файл БД не найден: {args.db}", file=sys.stderr)
        return 2

    buyers = load_active_buyers(args.db)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] записей (buyer_id + max expiration): {len(buyers)}")
    if not buyers:
        return 0

    ok = 0
    fail = 0
    for tg_id, exp_d in buyers:
        expire_iso = _expire_at_iso_for_key_date(exp_d)
        print(f"- user_{tg_id}: expiration_date={exp_d.isoformat()} -> expireAt={expire_iso}")

        if not args.apply:
            ok += 1
            continue

        st_get, body_get = get_user_by_telegram(tg_id)
        exists = st_get == 200 and body_get not in (None, {}, [])

        if exists:
            st, body = patch_user(tg_id, expire_iso)
            action = "PATCH"
        else:
            st, body = create_user(tg_id, expire_iso)
            action = "POST"
            if st not in (200, 201) and _user_already_exists(body):
                st, body = patch_user(tg_id, expire_iso)
                action = "POST->PATCH"

        if st in (200, 201):
            ok += 1
            print(f"  OK {action} HTTP {st}")
        else:
            fail += 1
            print(f"  FAIL {action} HTTP {st}: {json.dumps(body, ensure_ascii=False)[:400]}")

    print(f"Итого: ok={ok}, fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
