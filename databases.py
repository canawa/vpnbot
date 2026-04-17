import sqlite3 as sq
def create_tables():
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS "keys" (
            "key"   TEXT,
            "duration"      INTEGER,
            "sold"  INTEGER DEFAULT 0,
            "buyer_id"      INTEGER
    , expiration_date, expired INTEGER, buy_date, username, location TEXT, marzban_username TEXT)""")
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
            cur.execute('ALTER TABLE keys ADD COLUMN location TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute('ALTER TABLE keys ADD COLUMN marzban_username TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute('ALTER TABLE keys ADD COLUMN bundle_id TEXT')
        except:
            pass  # Поле уже существует
        try:
            cur.execute(
                "UPDATE keys SET location = 'germany' WHERE location IS NULL OR TRIM(COALESCE(location, '')) = ''")
            con.commit()
        except Exception:
            pass
        cur.execute('CREATE TABLE IF NOT EXISTS vpn_pay_pending (user_id INTEGER PRIMARY KEY, country TEXT NOT NULL)')
