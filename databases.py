import os
import re
import sqlite3 as sq
import time
from datetime import datetime, timedelta

BOT_USERNAME = 'coffemaniaVPNbot'
_REF_CODE_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}$')
ADV_LINK_START_PREFIX = 'l'

_DB_TIMEOUT_SEC = float(os.getenv('SQLITE_TIMEOUT_SEC', '30'))


def db_connect():
    """SQLite с ожиданием блокировки (не WAL)."""
    return sq.connect('database.db', timeout=_DB_TIMEOUT_SEC)


def db_retry(fn, *, attempts: int = 10, base_delay: float = 0.2):
    """Повтор при database is locked / database is busy."""
    last_err = None
    for attempt in range(attempts):
        try:
            return fn()
        except sq.OperationalError as e:
            msg = str(e).lower()
            if 'locked' not in msg and 'busy' not in msg:
                raise
            last_err = e
            time.sleep(base_delay * (attempt + 1))
    if last_err is not None:
        raise last_err
    return None


def grant_month_promo_99(user_id: int, *, hours: int = 24) -> str:
    """Выдаёт персональную скидку 99₽ на месяц на указанное число часов."""
    expires = (datetime.now() + timedelta(hours=int(hours))).isoformat()

    def _write():
        with db_connect() as con:
            con.execute(
                'UPDATE users SET promo_99_until = ? WHERE id = ?',
                (expires, user_id),
            )
            con.commit()

    db_retry(_write)
    return expires


def month_promo_99_active(user_id: int) -> bool:
    with db_connect() as con:
        cur = con.cursor()
        cur.execute('SELECT promo_99_until FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
    if not row or not row[0]:
        return False
    try:
        until = datetime.fromisoformat(str(row[0]).strip())
        return until > datetime.now()
    except Exception:
        return False


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
            campaign_link TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS adv_campaign_links (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NOT NULL,
            link_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        cur.execute(
            'CREATE INDEX IF NOT EXISTS ix_adv_campaign_links_campaign '
            'ON adv_campaign_links(campaign_id)'
        )

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
            extra_trial_once INTEGER DEFAULT 0,
            survey_answer TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS funnel_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            meta TEXT,
            created_at TEXT NOT NULL
        )
        """)
        cur.execute(
            'CREATE INDEX IF NOT EXISTS ix_funnel_events_user ON funnel_events(user_id)'
        )
        cur.execute(
            'CREATE INDEX IF NOT EXISTS ix_funnel_events_type ON funnel_events(event_type)'
        )

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_renewal_funnel (
            user_id INTEGER PRIMARY KEY,
            subscription_expires_at TEXT,
            entered_at TEXT,
            stopped INTEGER DEFAULT 0,
            stopped_at TEXT,
            rn_m7 INTEGER DEFAULT 0,
            rn_m3 INTEGER DEFAULT 0,
            rn_d0 INTEGER DEFAULT 0,
            rn_p1d INTEGER DEFAULT 0,
            rn_p3d INTEGER DEFAULT 0,
            rn_p7d INTEGER DEFAULT 0,
            rn_p30d INTEGER DEFAULT 0
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
        try:
            cur.execute('ALTER TABLE user_funnel ADD COLUMN survey_answer TEXT;')
        except Exception as e:
            print(e)
        try:
            cur.execute(
                'ALTER TABLE users ADD COLUMN ref_notify_new_referral INTEGER DEFAULT 1;'
            )
        except Exception as e:
            print(e)
        try:
            cur.execute(
                'ALTER TABLE users ADD COLUMN ref_notify_new_deposit INTEGER DEFAULT 1;'
            )
        except Exception as e:
            print(e)
        try:
            cur.execute('ALTER TABLE users ADD COLUMN bot_blocked INTEGER DEFAULT 0;')
        except Exception as e:
            print(e)
        try:
            cur.execute('ALTER TABLE referal_users ADD COLUMN adv_link_id INTEGER;')
        except Exception as e:
            print(e)
        try:
            cur.execute('ALTER TABLE adv_campaigns ADD COLUMN owner_id INTEGER;')
        except Exception as e:
            print(e)
        try:
            cur.execute('ALTER TABLE users ADD COLUMN promo_99_until TEXT;')
        except Exception as e:
            print(e)
        _migrate_adv_campaign_links(cur)
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


def _migrate_adv_campaign_links(cur) -> None:
    """У каждой кампании — минимум одна запись в adv_campaign_links."""
    cur.execute('SELECT rowid FROM adv_campaigns ORDER BY rowid')
    for (campaign_id,) in cur.fetchall():
        cur.execute(
            'SELECT 1 FROM adv_campaign_links WHERE campaign_id = ? LIMIT 1',
            (campaign_id,),
        )
        if cur.fetchone():
            continue
        cur.execute(
            """
            INSERT INTO adv_campaign_links (campaign_id, link_name, created_at)
            VALUES (?, ?, ?)
            """,
            (campaign_id, 'Ссылка 1', datetime.now().isoformat()),
        )


def build_adv_link_url(link_id: int) -> str:
    return f'https://t.me/{BOT_USERNAME}?start={ADV_LINK_START_PREFIX}{int(link_id)}'


def normalize_ref_code(code: str) -> str | None:
    if code is None:
        return None
    code = str(code).strip()
    if not code or not _REF_CODE_RE.fullmatch(code):
        return None
    return code


def resolve_referral_start(start_payload: str) -> tuple[int | None, int | None]:
    """
    Разбор ?start=… → (ref_master_id, adv_link_id).
    Для кампании ref_master_id = campaign_id; для рефовода = telegram id.
    """
    payload = (start_payload or '').strip()
    if not payload:
        return None, None

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users WHERE custom_ref_code = ?', (payload,))
        row = cur.fetchone()
        if row:
            return int(row[0]), None

        prefix = ADV_LINK_START_PREFIX
        if payload.lower().startswith(prefix) and payload[len(prefix):].isdigit():
            link_id = int(payload[len(prefix):])
            cur.execute(
                'SELECT campaign_id FROM adv_campaign_links WHERE rowid = ?',
                (link_id,),
            )
            link_row = cur.fetchone()
            if link_row:
                return int(link_row[0]), link_id
            return None, None

        if not payload.isdigit():
            return None, None

        numeric_id = int(payload)
        cur.execute('SELECT 1 FROM adv_campaigns WHERE rowid = ?', (numeric_id,))
        if cur.fetchone():
            return numeric_id, None

        return numeric_id, None


def resolve_ref_master_id(start_payload: str) -> int | None:
    """Код автора, ссылка кампании lID, ID кампании или Telegram ID."""
    ref_master_id, _adv_link_id = resolve_referral_start(start_payload)
    return ref_master_id


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


def fetch_refmaster_partner_rows() -> list[dict]:
    """Пользователи с ролью refmaster / refmaster_20 и сводкой по рефералам."""
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                u.id,
                u.username,
                COALESCE(u.role, '') AS role,
                u.custom_ref_code,
                COALESCE(u.ref_amount, 0),
                COALESCE(u.ref_balance, 0),
                COALESCE(u.ref_withdraw, 0),
                (
                    SELECT COUNT(*) FROM referal_users r
                    WHERE r.ref_master_id = u.id
                ) AS refs_total,
                (
                    SELECT COUNT(DISTINCT r.referral_id)
                    FROM referal_users r
                    INNER JOIN transactions t ON t.user_id = r.referral_id
                        AND t.type IN ('CryptoBot', 'yookassa')
                    WHERE r.ref_master_id = u.id
                ) AS paying_refs,
                (
                    SELECT COUNT(*)
                    FROM transactions t
                    JOIN referal_users r ON r.referral_id = t.user_id
                    WHERE r.ref_master_id = u.id
                      AND t.type IN ('CryptoBot', 'yookassa')
                ) AS deposits_count,
                (
                    SELECT COALESCE(SUM(CAST(t.amount AS INTEGER)), 0)
                    FROM transactions t
                    JOIN referal_users r ON r.referral_id = t.user_id
                    WHERE r.ref_master_id = u.id
                      AND t.type IN ('CryptoBot', 'yookassa')
                ) AS deposits_total,
                (
                    SELECT COUNT(*)
                    FROM transactions t
                    JOIN referal_users r ON r.referral_id = t.user_id
                    WHERE r.ref_master_id = u.id
                      AND t.type IN ('CryptoBot', 'yookassa')
                      AND CAST(t.amount AS INTEGER) >= 149
                ) AS bonus_deposits_count,
                (
                    SELECT COUNT(*)
                    FROM transactions t
                    JOIN referal_users r ON r.referral_id = t.user_id
                    WHERE r.ref_master_id = u.id
                      AND t.type IN ('CryptoBot', 'yookassa')
                      AND CAST(t.amount AS INTEGER) >= 149
                      AND date(t.date) >= date(r.registration_date)
                ) AS qualified_deposits_count
            FROM users u
            WHERE LOWER(TRIM(COALESCE(u.role, ''))) IN ('refmaster', 'refmaster_20')
            ORDER BY (COALESCE(u.ref_balance, 0) - COALESCE(u.ref_withdraw, 0)) DESC,
                     u.id DESC
            """
        )
        rows = []
        for row in cur.fetchall():
            rows.append({
                'id': int(row[0]),
                'username': row[1],
                'role': row[2],
                'custom_ref_code': row[3],
                'ref_amount': int(row[4] or 0),
                'ref_balance': int(row[5] or 0),
                'ref_withdraw': int(row[6] or 0),
                'refs_total': int(row[7] or 0),
                'paying_refs': int(row[8] or 0),
                'deposits_count': int(row[9] or 0),
                'deposits_total': int(row[10] or 0),
                'bonus_deposits_count': int(row[11] or 0),
                'qualified_deposits_count': int(row[12] or 0),
            })
        return rows


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
                COALESCE(SUM(CAST(t.amount AS INTEGER)), 0) AS deposits_total,
                SUM(
                    CASE
                        WHEN t.id IS NOT NULL AND CAST(t.amount AS INTEGER) >= 149 THEN 1
                        ELSE 0
                    END
                ) AS bonus_deposits_count
            FROM referal_users r
            LEFT JOIN users u ON u.id = r.ref_master_id
            LEFT JOIN transactions t ON t.user_id = r.referral_id
                AND t.type IN ('CryptoBot', 'yookassa')
            GROUP BY r.ref_master_id
            ORDER BY refs_count DESC, deposits_total DESC
            """
        )
        return cur.fetchall()


def list_adv_campaigns(*, viewer_id: int | None = None, admin_sees_all: bool = False) -> list[tuple]:
    """rowid, campaign_name, campaign_description. Менеджер — только свои (owner_id)."""
    with sq.connect('database.db') as con:
        cur = con.cursor()
        if admin_sees_all:
            cur.execute(
                """
                SELECT rowid, campaign_name, campaign_description
                FROM adv_campaigns
                ORDER BY rowid DESC
                """
            )
        elif viewer_id is not None:
            cur.execute(
                """
                SELECT rowid, campaign_name, campaign_description
                FROM adv_campaigns
                WHERE owner_id = ?
                ORDER BY rowid DESC
                """,
                (int(viewer_id),),
            )
        else:
            return []
        return cur.fetchall()


def get_adv_campaign_owner_id(campaign_id: int) -> int | None:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT owner_id FROM adv_campaigns WHERE rowid = ?', (int(campaign_id),))
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def user_can_view_adv_campaign(
    viewer_id: int,
    campaign_id: int,
    *,
    admin_sees_all: bool,
) -> bool:
    if not get_adv_campaign(campaign_id):
        return False
    if admin_sees_all:
        return True
    owner_id = get_adv_campaign_owner_id(campaign_id)
    return owner_id is not None and owner_id == int(viewer_id)


def user_can_view_adv_link(
    viewer_id: int,
    link_id: int,
    *,
    admin_sees_all: bool,
) -> bool:
    link = get_adv_campaign_link(link_id)
    if not link:
        return False
    return user_can_view_adv_campaign(
        viewer_id, link['campaign_id'], admin_sees_all=admin_sees_all,
    )


def get_adv_campaign(campaign_id: int) -> tuple | None:
    """rowid, campaign_name, campaign_description."""
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT rowid, campaign_name, campaign_description
            FROM adv_campaigns
            WHERE rowid = ?
            """,
            (campaign_id,),
        )
        return cur.fetchone()


def list_adv_campaign_links(campaign_id: int) -> list[dict]:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT rowid, link_name, created_at
            FROM adv_campaign_links
            WHERE campaign_id = ?
            ORDER BY rowid ASC
            """,
            (campaign_id,),
        )
        return [
            {'id': int(row[0]), 'link_name': row[1], 'created_at': row[2]}
            for row in cur.fetchall()
        ]


def get_adv_campaign_link(link_id: int) -> dict | None:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT rowid, campaign_id, link_name, created_at
            FROM adv_campaign_links
            WHERE rowid = ?
            """,
            (link_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        'id': int(row[0]),
        'campaign_id': int(row[1]),
        'link_name': row[2],
        'created_at': row[3],
        'link': build_adv_link_url(int(row[0])),
    }


def create_adv_campaign_with_link(
    name: str,
    description: str,
    link_name: str = 'Ссылка 1',
    *,
    owner_id: int | None = None,
) -> tuple[int, int, str]:
    """Создаёт кампанию и первую ссылку. Возвращает (campaign_id, link_id, link_url)."""
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO adv_campaigns (campaign_name, campaign_description, campaign_link, owner_id)
            VALUES (?, ?, ?, ?)
            """,
            (name, description, '', owner_id),
        )
        campaign_id = int(cur.lastrowid)
        cur.execute(
            """
            INSERT INTO adv_campaign_links (campaign_id, link_name, created_at)
            VALUES (?, ?, ?)
            """,
            (campaign_id, link_name, datetime.now().isoformat()),
        )
        link_id = int(cur.lastrowid)
        link_url = build_adv_link_url(link_id)
        cur.execute(
            'UPDATE adv_campaigns SET campaign_link = ? WHERE rowid = ?',
            (link_url, campaign_id),
        )
        con.commit()
    return campaign_id, link_id, link_url


def add_adv_campaign_link(campaign_id: int, link_name: str | None = None) -> tuple[int, str]:
    """Новая ссылка в кампании. Возвращает (link_id, link_url)."""
    campaign_id = int(campaign_id)
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT COUNT(*) FROM adv_campaign_links WHERE campaign_id = ?',
            (campaign_id,),
        )
        count = int((cur.fetchone() or (0,))[0])
        name = (link_name or '').strip() or f'Ссылка {count + 1}'
        cur.execute(
            """
            INSERT INTO adv_campaign_links (campaign_id, link_name, created_at)
            VALUES (?, ?, ?)
            """,
            (campaign_id, name, datetime.now().isoformat()),
        )
        link_id = int(cur.lastrowid)
        con.commit()
    return link_id, build_adv_link_url(link_id)


def _referral_filter_sql(
    ref_master_id: int,
    adv_link_id: int | None = None,
) -> tuple[str, list]:
    if adv_link_id is not None:
        return 'r.ref_master_id = ? AND r.adv_link_id = ?', [ref_master_id, adv_link_id]
    return 'r.ref_master_id = ?', [ref_master_id]


def _fetch_referral_stats(cur, ref_master_id: int, adv_link_id: int | None = None) -> dict:
    where_sql, params = _referral_filter_sql(ref_master_id, adv_link_id)

    cur.execute(f'SELECT COUNT(*) FROM referal_users r WHERE {where_sql}', params)
    refs_total = int((cur.fetchone() or (0,))[0])

    cur.execute(
        f"""
        SELECT COUNT(DISTINCT r.referral_id)
        FROM referal_users r
        INNER JOIN transactions t ON t.user_id = r.referral_id
            AND t.type IN ('CryptoBot', 'yookassa')
        WHERE {where_sql}
        """,
        params,
    )
    paying_refs = int((cur.fetchone() or (0,))[0])

    cur.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(CAST(t.amount AS INTEGER)), 0)
        FROM transactions t
        JOIN referal_users r ON r.referral_id = t.user_id
        WHERE {where_sql}
          AND t.type IN ('CryptoBot', 'yookassa')
        """,
        params,
    )
    dep_row = cur.fetchone() or (0, 0)
    deposits_count = int(dep_row[0] or 0)
    deposits_total = int(dep_row[1] or 0)

    cur.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(CAST(t.amount AS INTEGER)), 0)
        FROM transactions t
        JOIN referal_users r ON r.referral_id = t.user_id
        WHERE {where_sql}
          AND t.type IN ('CryptoBot', 'yookassa')
          AND CAST(t.amount AS INTEGER) >= 149
          AND date(t.date) >= date(r.registration_date)
        """,
        params,
    )
    qual_row = cur.fetchone() or (0, 0)
    qualified_deposits_count = int(qual_row[0] or 0)
    qualified_deposits_total = int(qual_row[1] or 0)

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM transactions t
        JOIN referal_users r ON r.referral_id = t.user_id
        WHERE {where_sql}
          AND t.type IN ('CryptoBot', 'yookassa')
          AND CAST(t.amount AS INTEGER) >= 149
        """,
        params,
    )
    bonus_deposits_count = int((cur.fetchone() or (0,))[0] or 0)

    cur.execute(
        f"""
        SELECT
            r.referral_id,
            COALESCE(r.referral_username, u.username, ''),
            CAST(t.amount AS INTEGER),
            t.type,
            t.date,
            CASE
                WHEN date(t.date) >= date(r.registration_date) THEN 1 ELSE 0
            END AS counts_for_bonus
        FROM transactions t
        JOIN referal_users r ON r.referral_id = t.user_id
        LEFT JOIN users u ON u.id = r.referral_id
        WHERE {where_sql}
          AND t.type IN ('CryptoBot', 'yookassa')
        ORDER BY t.date DESC
        LIMIT 20
        """,
        params,
    )
    deposit_rows = [
        {
            'referral_id': row[0],
            'referral_username': row[1] or '',
            'amount': int(row[2] or 0),
            'pay_type': row[3],
            'date': row[4],
            'counts_for_bonus': bool(row[5]),
        }
        for row in cur.fetchall()
    ]

    return {
        'refs_total': refs_total,
        'paying_refs': paying_refs,
        'deposits_count': deposits_count,
        'deposits_total': deposits_total,
        'qualified_deposits_count': qualified_deposits_count,
        'qualified_deposits_total': qualified_deposits_total,
        'bonus_deposits_count': bonus_deposits_count,
        'deposit_rows': deposit_rows,
    }


def _link_brief_stats(cur, campaign_id: int, link_id: int) -> dict:
    stats = _fetch_referral_stats(cur, campaign_id, link_id)
    return {
        'id': link_id,
        'refs_total': stats['refs_total'],
        'paying_refs': stats['paying_refs'],
        'deposits_total': stats['deposits_total'],
    }


def get_adv_campaign_dashboard(campaign_id: int) -> dict | None:
    """Сводка по кампании целиком (все ссылки)."""
    campaign_id = int(campaign_id)
    campaign = get_adv_campaign(campaign_id)
    if not campaign:
        return None

    _rowid, name, description = campaign
    with sq.connect('database.db') as con:
        cur = con.cursor()
        stats = _fetch_referral_stats(cur, campaign_id)
        links_raw = list_adv_campaign_links(campaign_id)
        links = []
        for link in links_raw:
            brief = _link_brief_stats(cur, campaign_id, link['id'])
            links.append({
                **link,
                'link': build_adv_link_url(link['id']),
                'refs_total': brief['refs_total'],
                'paying_refs': brief['paying_refs'],
                'deposits_total': brief['deposits_total'],
            })

    return {
        'campaign_id': campaign_id,
        'ref_master_id': campaign_id,
        'is_campaign': True,
        'is_link': False,
        'campaign_name': name,
        'campaign_description': description or '',
        'campaign_link': None,
        'links': links,
        'username': None,
        'role': None,
        'ref_balance': 0,
        'ref_withdraw': 0,
        'ref_amount': stats['refs_total'],
        'custom_ref_code': None,
        **stats,
    }


def get_adv_link_dashboard(link_id: int) -> dict | None:
    """Сводка по одной ссылке кампании."""
    link = get_adv_campaign_link(link_id)
    if not link:
        return None

    campaign = get_adv_campaign(link['campaign_id'])
    if not campaign:
        return None

    _rowid, name, description = campaign
    with sq.connect('database.db') as con:
        cur = con.cursor()
        stats = _fetch_referral_stats(cur, link['campaign_id'], link_id)

    return {
        'campaign_id': link['campaign_id'],
        'link_id': link_id,
        'link_name': link['link_name'],
        'ref_master_id': link['campaign_id'],
        'is_campaign': False,
        'is_link': True,
        'campaign_name': name,
        'campaign_description': description or '',
        'campaign_link': link['link'],
        'links': [],
        'username': None,
        'role': None,
        'ref_balance': 0,
        'ref_withdraw': 0,
        'ref_amount': stats['refs_total'],
        'custom_ref_code': None,
        **stats,
    }


def resolve_admin_adv_lookup(lookup_id: int) -> dict | None:
    """Карточка по ID: кампания → ссылка → рефовод."""
    lookup_id = int(lookup_id)
    if get_adv_campaign(lookup_id):
        return get_adv_campaign_dashboard(lookup_id)
    if get_adv_campaign_link(lookup_id):
        return get_adv_link_dashboard(lookup_id)
    return get_ref_partner_dashboard(lookup_id)


def get_ref_partner_dashboard(ref_master_id: int) -> dict | None:
    """
    Сводка по ref_master_id: пользователь-рефовод (не кампания).
    Кампании и ссылки — через get_adv_campaign_dashboard / get_adv_link_dashboard.
    """
    ref_master_id = int(ref_master_id)
    if get_adv_campaign(ref_master_id):
        return get_adv_campaign_dashboard(ref_master_id)

    campaign_name = f'Рефовод #{ref_master_id}'
    campaign_description = ''
    campaign_link = referral_link(ref_master_id)
    is_campaign = False

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT username, role, COALESCE(ref_balance, 0), COALESCE(ref_withdraw, 0), '
            'COALESCE(ref_amount, 0), custom_ref_code '
            'FROM users WHERE id = ?',
            (ref_master_id,),
        )
        user_row = cur.fetchone()
        username = user_row[0] if user_row else None
        role = user_row[1] if user_row else None
        ref_balance = int(user_row[2]) if user_row else 0
        ref_withdraw = int(user_row[3]) if user_row else 0
        ref_amount = int(user_row[4]) if user_row else 0
        custom_ref_code = user_row[5] if user_row else None

        stats = _fetch_referral_stats(cur, ref_master_id)

    if not user_row and stats['refs_total'] == 0:
        return None

    return {
        'ref_master_id': ref_master_id,
        'campaign_id': None,
        'is_campaign': is_campaign,
        'is_link': False,
        'campaign_name': campaign_name,
        'campaign_description': campaign_description,
        'campaign_link': campaign_link,
        'links': [],
        'username': username,
        'role': role,
        'ref_balance': ref_balance,
        'ref_withdraw': ref_withdraw,
        'ref_amount': ref_amount,
        'custom_ref_code': custom_ref_code,
        **stats,
    }
