import os
import dotenv
import asyncio
import requests
import sqlite3 as sq
from databases import *
from datetime import datetime, time, timedelta
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
                          "trafficLimitBytes": 322122547200,
                          "expireAt": (datetime.now() + timedelta(days=int(days))).isoformat(),
                          "createdAt": datetime.now().isoformat(),
                          "telegramId": tg_id,
                          'trafficLimitStrategy': 'MONTH',
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

        response = requests.patch(
            f"{self.base_url}/api/users",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
            json={
                "username": f'user_{tg_id}',
                "trafficLimitBytes": 0,
                "expireAt": new_expire.isoformat(),
                "telegramId": tg_id,
                "hwidDeviceLimit": 3,
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

        print(body)
        return body

    def get_user_by_tg_id(self, tg_id):
        body = requests.get(
            f"{self.base_url}/api/users/by-telegram-id/{tg_id}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        return body.json()

    def deliver_trial_vpn(self, tg_id ):
        body = requests.post(f"{self.base_url}/api/users",
             headers={
                 "Content-Type": "application/json",
                 "Authorization": f"Bearer {self.token}",
             },
             json={
                 "username": f'user_{tg_id}',
                 "trafficLimitBytes": 0,
                 "expireAt": (datetime.now() + timedelta(days=3)).isoformat(),
                 "createdAt": datetime.now().isoformat(),
                 "telegramId": tg_id,
                 "hwidDeviceLimit": 3,
                 "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],

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

# ok