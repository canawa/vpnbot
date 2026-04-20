import os
import dotenv
import asyncio
import requests
import sqlite3 as sq
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
dotenv.load_dotenv()

class Vpn:
    def __init__(self):
        self.base_url = os.getenv("REMNAWAVE_BASE_URL")
        self.token = os.getenv("REMNAWAVE_TOKEN")

        self.admin_login = os.getenv('REMNAWAVE_ADMIN_LOGIN')
        self.admin_password = os.getenv('REMNAWAVE_ADMIN_PASSWORD')

    def create_new_user(self, tg_id):
        body = requests.post(f"{self.base_url}/api/users",
                      headers={
                          "Content-Type": "application/json",
                          "Authorization": f"Bearer {self.token}",
                      },
                      json={
                          "username": f'user_{tg_id}',
                          "trafficLimitBytes": 0,
                          "expireAt": (datetime.now() + timedelta(days=30)).isoformat(),
                          "createdAt": datetime.now().isoformat(),
                          "telegramId": tg_id,
                          "hwidDeviceLimit": 3,
                          "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
                      }
                      )
        print(body.json())
        return body.json()

    def renew_subscription(self, tg_id, days):
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?', (tg_id,))
            expire_at = cur.fetchone()[0]
            expire_at = datetime.fromisoformat(expire_at)
            expire_at = max(expire_at, datetime.now())

        body = requests.patch(f"{self.base_url}/api/users",
           headers={
               "Content-Type": "application/json",
               "Authorization": f"Bearer {self.token}"
           },
           json={
                        "username": f'user_{tg_id}',
                          "trafficLimitBytes": 0,
                          "expireAt": (expire_at + timedelta(days=days)).isoformat(),
                          "telegramId": tg_id,
                          "hwidDeviceLimit": 3,
                          "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
           }
           )
        print(body.json())
        return body.json()

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

