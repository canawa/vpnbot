import sqlite3 as sq
from datetime import datetime, timedelta


def upsert_subscription_days(user_id: int, duration_days: int = None, expires_at: str = None) -> str:
    if expires_at:
        expires = expires_at
    else:
        expires = (datetime.now() + timedelta(days=int(duration_days))).date().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO subscriptions (user_id, subscription_expires_at, runout_notified, expiring_tomorrow_notified)
            VALUES (?, ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                subscription_expires_at = excluded.subscription_expires_at,
                runout_notified = 0,
                expiring_tomorrow_notified = 0
            """,
            (user_id, expires),
        )
        con.commit()
    return expires


def create_tables():
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT, balance INTEGER, ref_balance INTEGER DEFAULT 0, ref_amount INTEGER DEFAULT 0, keys TEXT, role TEXT DEFAULT NULL, had_trial INTEGER DEFAULT 0, runout_notified INTEGER DEFAULT 0, has_active_keys INTEGER DEFAULT 0)")
        cur.execute(
            'CREATE TABLE IF NOT EXISTS referal_users (id INTEGER PRIMARY KEY, referral_id INTEGER UNIQUE, ref_master_id INTEGER, registration_date TEXT, referral_username TEXT, ref_master_username TEXT)')
        cur.execute(
            'CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER, type TEXT, date TEXT)')
        # Добавляем поле role, если его еще нет
        try:
            cur.execute('ALTER TABLE users ADD COLUMN role TEXT DEFAULT NULL')
        except:
            pass  # Поле уже существует
        # Добавляем поле runout_notified, если его еще нет
        try:
            cur.execute('ALTER TABLE users ADD COLUMN runout_notified INTEGER DEFAULT 0')
        except:
            pass  # Поле уже существует
        # Добавляем поле had_trial, если его еще нет
        try:
            cur.execute('ALTER TABLE users ADD COLUMN had_trial INTEGER DEFAULT 0')
        except:
            pass  # Поле уже существует
        # Добавляем поле has_active_keys, если его еще нет
        try:
            cur.execute('ALTER TABLE users ADD COLUMN has_active_keys INTEGER DEFAULT 0')
        except:
            pass  # Поле уже существует
        # Добавляем поле expiring_tomorrow_notified, если его еще нет
        try:
            cur.execute('ALTER TABLE users ADD COLUMN expiring_tomorrow_notified INTEGER DEFAULT 0')
        except:
            pass  # Поле уже существует
        # Добавляем поле registration_date в таблицу referal_users, если его еще нет
        try:
            cur.execute('ALTER TABLE referal_users ADD COLUMN registration_date TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute('ALTER TABLE referal_users ADD COLUMN referral_username TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute('ALTER TABLE referal_users ADD COLUMN ref_master_username TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute('ALTER TABLE users ADD COLUMN sub_expires_at TEXT')
        except:
            pass
        try:
            cur.execute('ALTER TABLE users ADD COLUMN ref_withdraw INTEGER DEFAULT 0')
        except:
            pass
        try:
            cur.execute('ALTER TABLE users ADD COLUMN received_bonus INTEGER DEFAULT 0')
        except:
            pass
        cur.execute('CREATE TABLE IF NOT EXISTS vpn_pay_pending (user_id INTEGER PRIMARY KEY, country TEXT NOT NULL)')
        cur.execute('CREATE TABLE IF NOT EXISTS subscriptions (user_id INTEGER PRIMARY KEY, subscription_expires_at TEXT NOT NULL)')
        try:
            cur.execute('ALTER TABLE subscriptions ADD COLUMN runout_notified INTEGER DEFAULT 0')
        except Exception:
            pass
        try:
            cur.execute('ALTER TABLE subscriptions ADD COLUMN expiring_tomorrow_notified INTEGER DEFAULT 0')
        except Exception:
            pass