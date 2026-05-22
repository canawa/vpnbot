import re
import sqlite3 as sq
from datetime import datetime, timedelta

BOT_USERNAME = 'coffemaniaVPNbot'
_REF_CODE_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')


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

        # Воронка на покупку
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_funnel (
            user_id INTEGER PRIMARY KEY,
            branch TEXT NOT NULL DEFAULT 'no_trial',
            first_seen_at TEXT NOT NULL,
            trial_started_at TEXT,
            trial_ended_at TEXT,
            last_paid_at TEXT,
            nt_30m INTEGER DEFAULT 0,
            nt_24h INTEGER DEFAULT 0,
            nt_48h INTEGER DEFAULT 0,
            nt_72h INTEGER DEFAULT 0,
            pt_1h INTEGER DEFAULT 0,
            pt_24h INTEGER DEFAULT 0,
            pt_3d INTEGER DEFAULT 0,
            pt_7d INTEGER DEFAULT 0,
            extra_trial_once INTEGER DEFAULT 0
        )
        """)

        con.commit()
        try:
            cur.execute('ALTER TABLE users ADD COLUMN is_legacy INTEGER DEFAULT 0;')
        except Exception as e:
            print(e)
        try:
            cur.execute('ALTER TABLE users ADD COLUMN custom_ref_code TEXT;')
        except Exception as e:
            print(e)
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_users_custom_ref_code
            ON users(custom_ref_code) WHERE custom_ref_code IS NOT NULL
            """
        )
        con.commit()
        cur.execute(
            """
            INSERT OR IGNORE INTO user_funnel (user_id, branch, first_seen_at)
            SELECT
                u.id,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM subscriptions s
                        WHERE s.user_id = u.id
                          AND date(s.subscription_expires_at) >= date('now')
                    ) AND COALESCE(u.had_trial, 0) = 0 THEN 'paid'
                    WHEN EXISTS (
                        SELECT 1 FROM subscriptions s
                        WHERE s.user_id = u.id
                          AND date(s.subscription_expires_at) >= date('now')
                    ) THEN 'trial_active'
                    WHEN COALESCE(u.had_trial, 0) = 1 THEN 'post_trial'
                    ELSE 'no_trial'
                END,
                datetime('now')
            FROM users u
            WHERE u.id NOT IN (SELECT user_id FROM user_funnel)
            """
        )
        con.commit()


def normalize_ref_code(code: str) -> str | None:
    if code is None:
        return None
    code = str(code).strip()
    if not code or not _REF_CODE_RE.fullmatch(code):
        return None
    return code


def resolve_ref_master_id(start_payload: str) -> int | None:
    """Код автора в start=… или числовой Telegram ID."""
    payload = (start_payload or '').strip()
    if not payload:
        return None
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users WHERE custom_ref_code = ?', (payload,))
        row = cur.fetchone()
        if row:
            return int(row[0])
    try:
        return int(payload)
    except ValueError:
        return None


def get_referral_start_param(user_id: int) -> str:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT custom_ref_code FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return str(user_id)


def referral_link(user_id: int) -> str:
    return f'https://t.me/{BOT_USERNAME}?start={get_referral_start_param(user_id)}'


def set_custom_ref_code(user_id: int, code: str | None) -> tuple[bool, str]:
    normalized = normalize_ref_code(code) if code else None
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT 1 FROM users WHERE id = ?', (user_id,))
        if not cur.fetchone():
            return False, f'Пользователь {user_id} не найден в users'
        if normalized:
            cur.execute(
                'SELECT id FROM users WHERE custom_ref_code = ? AND id != ?',
                (normalized, user_id),
            )
            if cur.fetchone():
                return False, f'Код «{normalized}» уже занят другим пользователем'
        cur.execute(
            'UPDATE users SET custom_ref_code = ? WHERE id = ?',
            (normalized, user_id),
        )
        con.commit()
    if normalized:
        return True, f'OK: {referral_link(user_id)}'
    return True, f'Код снят. Ссылка снова: {referral_link(user_id)}'


def fetch_all_referrers_progress() -> list[tuple]:
    """Статистика по каждому ref_master_id из referal_users (как в экране refmaster)."""
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                r.ref_master_id,
                u.username,
                COALESCE(u.role, ''),
                u.custom_ref_code,
                COALESCE(u.ref_amount, 0),
                COALESCE(u.ref_balance, 0),
                COALESCE(u.ref_withdraw, 0),
                COUNT(DISTINCT r.referral_id) AS refs_count,
                COUNT(DISTINCT CASE WHEN t.id IS NOT NULL THEN r.referral_id END) AS paying_refs,
                COUNT(t.id) AS deposits_count,
                COALESCE(SUM(CAST(t.amount AS INTEGER)), 0) AS deposits_total
            FROM referal_users r
            LEFT JOIN users u ON u.id = r.ref_master_id
            LEFT JOIN transactions t ON t.user_id = r.referral_id
                AND t.type IN ('CryptoBot', 'yookassa')
            GROUP BY r.ref_master_id
            ORDER BY refs_count DESC, deposits_total DESC
            """
        )
        return cur.fetchall()
