import os
import dotenv
import asyncio
import requests
import sqlite3 as sq
from databases import *
from datetime import datetime, time, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import uuid
import secrets
import string
from remnawave import RemnawaveSDK  # Updated import for new package
from remnawave.models import (  # Updated import path
    UsersResponseDto,
    UserResponseDto,
    GetAllConfigProfilesResponseDto,
    CreateInternalSquadRequestDto
)

from sync_remna_expire_from_keys_once import get_user_by_tg_id

dotenv.load_dotenv()

class Vpn:
    def __init__(self):
        self.base_url = os.getenv("REMNAWAVE_BASE_URL")
        self.token = os.getenv("REMNAWAVE_TOKEN")

        self.admin_login = os.getenv('REMNAWAVE_ADMIN_LOGIN')
        self.admin_password = os.getenv('REMNAWAVE_ADMIN_PASSWORD')

    def create_new_user(self, tg_id, days=30):
        body = requests.post(f"{self.base_url}/api/users",
                      headers={
                          "Content-Type": "application/json",
                          "Authorization": f"Bearer {self.token}",
                      },
                      json={
                          "username": f'user_{tg_id}',
                          "trafficLimitBytes": 26843545600,
                          "expireAt": (datetime.now() + timedelta(days=int(days))).isoformat(),
                          "createdAt": datetime.now().isoformat(),
                          "telegramId": tg_id,
                          'trafficLimitStrategy': 'MONTH_ROLLING',
                          "hwidDeviceLimit": 3,
                          "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
                      }
                      )
        print(body.json())
        return body.json()

    def renew_subscription(self, tg_id, days):
        def _parse_iso_dt(value):
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            text = str(value).strip()
            if not text:
                return None
            # fromisoformat не всегда принимает суффикс Z.
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            try:
                dt = datetime.fromisoformat(text)
            except Exception:
                return None
            # Приводим timezone-aware к naive в локальном времени процесса.
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            return dt

        now = datetime.now()
        db_expire = None
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?', (tg_id,))
            row = cur.fetchone()
            if row:
                db_expire = _parse_iso_dt(row[0])

        panel_expire = None
        try:
            panel_user = self.get_user_by_tg_id(tg_id)
            if isinstance(panel_user, dict):
                response = panel_user.get('response')
                if isinstance(response, list) and response:
                    panel_expire = _parse_iso_dt(response[0].get('expireAt'))
                elif isinstance(response, dict):
                    panel_expire = _parse_iso_dt(response.get('expireAt'))
        except Exception:
            panel_expire = None

        # Ключевой фикс: продлеваем от максимальной актуальной даты, а не только от "сейчас".
        base_expire = max((dt for dt in (db_expire, panel_expire, now) if dt is not None))

        new_expire = base_expire + timedelta(days=days)

        user_data = self.get_user_by_tg_id(tg_id)['response'][0] # перенос трафика на след месяц
        current_limit = user_data['trafficLimitBytes']
        used = user_data['userTraffic']['usedTrafficBytes']
        leftover = max(0, current_limit - used)

        # Сохраняем остаток в БД
        with sq.connect('database.db') as con:
            con.execute('UPDATE subscriptions SET traffic_leftover_bytes = ? WHERE user_id = ?',
                        (leftover, tg_id))

        response = requests.patch(
            f"{self.base_url}/api/users",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
            json={
                "username": f'user_{tg_id}',
                "trafficLimitBytes": 26843545600 + leftover,
                "expireAt": new_expire.isoformat(),
                "telegramId": tg_id,
                "hwidDeviceLimit": 3,
                "trafficLimitStrategy": "MONTH_ROLLING",
                "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
            }
        )
        try:
            body = response.json()
        except Exception:
            body = {}

        is_success = response.ok and not (isinstance(body, dict) and body.get('errorCode'))
        if is_success:
            upsert_subscription_days(tg_id, expires_at=new_expire.isoformat())
            # Обнуляем остаток — он уже учтён в новом лимите
            with sq.connect('database.db') as con:
                con.execute('UPDATE subscriptions SET traffic_leftover_bytes = 0 WHERE user_id = ?', (tg_id,))

        print(body)
        return body

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def get_user_by_tg_id(self, tg_id):
        body = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        return body.json()

    def get_user_traffic_by_tg_id(self, tg_id):
        body = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        data = body.json()

        return data['response'][0]['userTraffic']['usedTrafficBytes'],

    def deliver_trial_vpn(self, tg_id ):
        body = requests.post(f"{self.base_url}/api/users",
             headers={
                 "Content-Type": "application/json",
                 "Authorization": f"Bearer {self.token}",
             },
             json={
                 "username": f'user_{tg_id}',
                 "expireAt": (datetime.now() + timedelta(days=3)).isoformat(),
                 "createdAt": datetime.now().isoformat(),
                 "telegramId": tg_id,
                 "hwidDeviceLimit": 3,
                 "trafficLimitBytes": 3221225472,
                 "trafficLimitStrategy": "MONTH_ROLLING",
                 "activeInternalSquads": ["ffa0ca48-bb6e-447b-a404-f1808b09c967", "6f11955f-6b95-4f96-bba4-3d866de8ce83"],

             }
                 )
        return body.json()



    def get_hwid_devices(self, tg_id):
        user = self.get_user_by_tg_id(tg_id)
        uuid = user['response'][0]['uuid']
        body = requests.get(
            f"{self.base_url}/api/hwid/devices/{uuid}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        devices = body.json()['response']['devices']
        return devices

    def delete_hwid_device(self,tg_id, hwid):
        user = self.get_user_by_tg_id(tg_id)
        uuid = user['response'][0]['uuid']
        try:
            body = requests.post(f"{self.base_url}/api/hwid/devices/delete",
            headers={
                  "Content-Type": "application/json",
                  "Authorization": f"Bearer {self.token}"
                },
                json={
                  "userUuid": uuid,
                  "hwid": hwid,
                }
            )
            return True
        except Exception as e:
            return e

    def get_leftover_bytes(self, tg_id):
        data = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        user = data.json()['response'][0]
        print(f"[DEBUG] get_leftover_bytes raw response: {data.status_code} {data.text}")
        traffic_limit = user['trafficLimitBytes']
        used_traffic = user['userTraffic']['usedTrafficBytes']

        return traffic_limit, used_traffic

    def give_lte_gbs(self, tg_id, gb_amount):
        bytes_amount = int(gb_amount * 1073741824)
        print(f"[DEBUG] give_lte_gbs | tg_id={tg_id} | добавляем={bytes_amount} байт ({gb_amount} ГБ)")
        traffic_limit, used_traffic = self.get_leftover_bytes(tg_id)
        new_limit = traffic_limit + bytes_amount

        body = requests.patch(f"{self.base_url}/api/users",
                              headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
                              json={
                                  "username": f'user_{tg_id}',
                                  "trafficLimitBytes": new_limit,
                                  "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
                              })

        # Обнуляем только если успешно
        if body.ok:
            with sq.connect('database.db') as con:
                con.execute('UPDATE subscriptions SET traffic_leftover_bytes = 0 WHERE user_id = ?', (tg_id,))

        return body

# print(Vpn().get_user_by_tg_id(1979477416))
# Vpn().give_lte_gbs(1979477416, 2 )
# print(Vpn().get_user_by_tg_id(1979477416))

# user_data = Vpn().get_user_by_tg_id(1979477416)['response'][0]
# current_limit = user_data['trafficLimitBytes']
# used = user_data['userTraffic']['usedTrafficBytes']
# remaining_gb = (current_limit - used) / 1073741824
#
# print(current_limit, used, remaining_gb)
print(Vpn().get_leftover_bytes(1979477416))