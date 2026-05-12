import logging
import sqlite3 as sq
from datetime import datetime, timedelta


def upsert_subscription_days(user_id: int, duration_days: int = None, expires_at: str = None) -> str:
    if expires_at:
        expires = expires_at
    else:
        expires = (datetime.now() + timedelta(days=int(duration_days))).isoformat()
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

        # USERS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER,
            ref_balance INTEGER DEFAULT 0,
            ref_amount INTEGER DEFAULT 0,
            keys TEXT,
            role TEXT DEFAULT NULL,
            had_trial INTEGER DEFAULT 0,
            runout_notified INTEGER DEFAULT 0,
            has_active_keys INTEGER DEFAULT 0,
            expiring_tomorrow_notified INTEGER DEFAULT 0,
            ref_withdraw INTEGER DEFAULT 0,
            received_bonus INTEGER DEFAULT 0,
            has_active_subscription INTEGER,
            sub_expires_at TEXT
        )
        """)

        # REFERAL USERS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS referal_users (
            id INTEGER PRIMARY KEY,
            referral_id INTEGER UNIQUE,
            ref_master_id INTEGER,
            registration_date TEXT,
            referral_username TEXT,
            ref_master_username TEXT
        )
        """)

        # TRANSACTIONS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount INTEGER,
            type TEXT,
            date TEXT,
            external_payment_id TEXT
        )
        """)

        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_type_external_payment
        ON transactions(type, external_payment_id)
        """)

        # SUBSCRIPTIONS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            subscription_expires_at TEXT NOT NULL,
            runout_notified INTEGER DEFAULT 0,
            expiring_tomorrow_notified INTEGER DEFAULT 0,
            traffic_leftover_bytes INTEGER DEFAULT 0,
            notified_low_traffic INTEGER DEFAULT 0
        )
        """)

        # ADV CAMPAIGNS
        cur.execute("""
        CREATE TABLE IF NOT EXISTS adv_campaigns (
            campaign_name TEXT NOT NULL,
            campaign_description TEXT,
            campaign_link TEXT NOT NULL
        )
        """)

        con.commit()
        try:
            cur.execute('ALTER TABLE users ADD COLUMN is_legacy INTEGER DEFAULT 0;')
        except Exception as e:
            logging.exception(e)

        try:
            cur.execute('ALTER TABLE adv_campaigns ADD COLUMN custom_link TEXT DEFAULT NULL')
        except Exception as e:
            logging.exception(e)