import os
import dotenv
import requests
import sqlite3 as sq
from databases import *
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential

dotenv.load_dotenv()

BASE_LIMIT = 26843545600  # 25 ГБ
TRIAL_LIMIT = 3221225472  # 3 ГБ

MAIN_SQUAD = "6f11955f-6b95-4f96-bba4-3d866de8ce83"
TRIAL_SQUAD = "ffa0ca48-bb6e-447b-a404-f1808b09c967"


class Vpn:
    def __init__(self):
        self.base_url = os.getenv("REMNAWAVE_BASE_URL")
        self.token = os.getenv("REMNAWAVE_TOKEN")
        self.admin_login = os.getenv('REMNAWAVE_ADMIN_LOGIN')
        self.admin_password = os.getenv('REMNAWAVE_ADMIN_PASSWORD')

    # ------------------------------------------------------------------ #
    #  Внутренние хелперы                                                  #
    # ------------------------------------------------------------------ #

    @property
    def _headers(self):
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    @staticmethod
    def _parse_iso_dt(value) -> datetime | None:
        """Парсит ISO-строку → naive datetime (локальное время). Возвращает None при ошибке."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt

    def _get_bonus_bytes(self, tg_id: int) -> int:
        """Читает накопленный бонусный трафик из БД (колонка bonus_traffic_bytes)."""
        with sq.connect('database.db') as con:
            cur = con.cursor()
            # Колонка может отсутствовать в старых БД — добавляем на лету.
            try:
                cur.execute(
                    'SELECT bonus_traffic_bytes FROM subscriptions WHERE user_id = ?', (tg_id,)
                )
                row = cur.fetchone()
                return int(row[0]) if row and row[0] else 0
            except sq.OperationalError:
                con.execute(
                    'ALTER TABLE subscriptions ADD COLUMN bonus_traffic_bytes INTEGER DEFAULT 0'
                )
                return 0

    def _set_bonus_bytes(self, tg_id: int, value: int):
        with sq.connect('database.db') as con:
            con.execute(
                'UPDATE subscriptions SET bonus_traffic_bytes = ? WHERE user_id = ?',
                (value, tg_id)
            )

    # ------------------------------------------------------------------ #
    #  Публичные методы                                                    #
    # ------------------------------------------------------------------ #

    def create_new_user(self, tg_id: int, days: int = 30) -> dict:
        body = requests.post(
            f"{self.base_url}/api/users",
            headers=self._headers,
            json={
                "username": f'user_{tg_id}',
                "trafficLimitBytes": BASE_LIMIT,
                "expireAt": (datetime.now() + timedelta(days=int(days))).isoformat(),
                "createdAt": datetime.now().isoformat(),
                "telegramId": tg_id,
                "trafficLimitStrategy": "MONTH_ROLLING",
                "hwidDeviceLimit": 3,
                "activeInternalSquads": [MAIN_SQUAD],
            }
        )
        print(body.json())
        return body.json()

    def deliver_trial_vpn(self, tg_id: int) -> dict:
        body = requests.post(
            f"{self.base_url}/api/users",
            headers=self._headers,
            json={
                "username": f'user_{tg_id}',
                "expireAt": (datetime.now() + timedelta(days=3)).isoformat(),
                "createdAt": datetime.now().isoformat(),
                "telegramId": tg_id,
                "hwidDeviceLimit": 3,
                "trafficLimitBytes": TRIAL_LIMIT,
                "trafficLimitStrategy": "MONTH_ROLLING",
                "activeInternalSquads": [TRIAL_SQUAD, MAIN_SQUAD],
            }
        )
        return body.json()

    def renew_subscription(self, tg_id: int, days: int) -> dict:
        now = datetime.now()

        # --- 1. Один запрос к панели, используем результат везде ---
        panel_data = self.get_user_by_tg_id(tg_id)
        panel_user = None
        panel_expire = None

        try:
            response_field = panel_data.get('response')
            if isinstance(response_field, list) and response_field:
                panel_user = response_field[0]
            elif isinstance(response_field, dict):
                panel_user = response_field
            if panel_user:
                panel_expire = self._parse_iso_dt(panel_user.get('expireAt'))
        except Exception:
            pass

        # --- 2. Дата продления: максимум из БД / панели / сейчас ---
        db_expire = None
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute(
                'SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?', (tg_id,)
            )
            row = cur.fetchone()
            if row:
                db_expire = self._parse_iso_dt(row[0])

        base_expire = max(
            dt for dt in (db_expire, panel_expire, now) if dt is not None
        )
        new_expire = base_expire + timedelta(days=days)

        # --- 3. Считаем leftover от БАЗОВОГО лимита (не от текущего раздутого) ---
        leftover = 0
        if panel_user:
            used = panel_user.get('userTraffic', {}).get('usedTrafficBytes', 0)
            leftover = max(0, BASE_LIMIT - used)

        # --- 4. Учитываем накопленный бонусный трафик ---
        bonus = self._get_bonus_bytes(tg_id)
        new_limit = BASE_LIMIT + leftover + bonus

        # --- 5. Обновляем пользователя в панели ---
        response = requests.patch(
            f"{self.base_url}/api/users",
            headers=self._headers,
            json={
                "username": f'user_{tg_id}',
                "trafficLimitBytes": new_limit,
                "expireAt": new_expire.isoformat(),
                "telegramId": tg_id,
                "hwidDeviceLimit": 3,
                "trafficLimitStrategy": "MONTH_ROLLING",
                "activeInternalSquads": [MAIN_SQUAD],
            }
        )

        try:
            body = response.json()
        except Exception:
            body = {}

        is_success = response.ok and not (isinstance(body, dict) and body.get('errorCode'))
        if is_success:
            upsert_subscription_days(tg_id, expires_at=new_expire.isoformat())
            # Бонус учтён в новом лимите — обнуляем
            self._set_bonus_bytes(tg_id, 0)

        print(body)
        return body

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def get_user_by_tg_id(self, tg_id: int) -> dict:
        body = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return body.json()

    def get_user_traffic_by_tg_id(self, tg_id: int) -> int:
        """Возвращает использованный трафик в байтах (int, без trailing comma)."""
        body = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return body.json()['response'][0]['userTraffic']['usedTrafficBytes']

    def get_hwid_devices(self, tg_id: int) -> list:
        user = self.get_user_by_tg_id(tg_id)
        user_uuid = user['response'][0]['uuid']  # не затираем встроенный uuid
        body = requests.get(
            f"{self.base_url}/api/hwid/devices/{user_uuid}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return body.json()['response']['devices']

    def delete_hwid_device(self, tg_id: int, hwid: str) -> bool | Exception:
        user = self.get_user_by_tg_id(tg_id)
        user_uuid = user['response'][0]['uuid']
        try:
            requests.post(
                f"{self.base_url}/api/hwid/devices/delete",
                headers=self._headers,
                json={"userUuid": user_uuid, "hwid": hwid},
            )
            return True
        except Exception as e:
            return e

    def give_lte_gbs(self, tg_id: int, gb_amount: float):
        """
        Добавляет бонусный трафик пользователю.
        Бонус сохраняется в БД и учитывается при следующем продлении,
        поэтому ГБ не сгорают при renew_subscription.
        """
        bytes_amount = int(gb_amount * 1073741824)
        print(f"[DEBUG] give_lte_gbs | tg_id={tg_id} | добавляем {bytes_amount} байт ({gb_amount} ГБ)")

        # Текущий лимит в панели + бонус
        data = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        user = data.json()['response'][0]
        current_limit = user['trafficLimitBytes']
        new_limit = current_limit + bytes_amount

        body = requests.patch(
            f"{self.base_url}/api/users",
            headers=self._headers,
            json={
                "username": f'user_{tg_id}',
                "trafficLimitBytes": new_limit,
                "activeInternalSquads": [MAIN_SQUAD],
            }
        )

        if body.ok:
            # Сохраняем бонус в БД — при следующем renew он не потеряется
            current_bonus = self._get_bonus_bytes(tg_id)
            self._set_bonus_bytes(tg_id, current_bonus + bytes_amount)

        return body