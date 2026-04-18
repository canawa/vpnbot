import os
import dotenv
import asyncio
import requests
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
                          # "status": "ACTIVE",
                          # "shortUuid": str(uuid.uuid4()),
                          # # "trojanPassword": "012345678",
                          # "vlessUuid": str(uuid.uuid4()),
                          # "ssPassword": "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12)),
                          "trafficLimitBytes": 300000000000,
                          # "trafficLimitStrategy": "NO_RESET",
                          "expireAt": (datetime.now() + timedelta(days=30)).isoformat(),
                          "createdAt": datetime.now().isoformat(),
                          # "lastTrafficResetAt": None,
                          # "description": "",
                          # "tag": f"USER_{tg_id}",
                          "telegramId": tg_id,
                          # "email": None,
                          # "hwidDeviceLimit": 3,
                          "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],
                          # "uuid": str(uuid.uuid4()),
                          # "externalSquadUuid": None
                      }
                      )
        return body.json()

    def get_by_username(self, username):
        sub = requests.get(f"{self.base_url}/api/subscriptions/by-username/{username}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        return sub.json()['response']['subscriptionUrl']


    def deliver_trial_vpn(self, tg_id ):
        body = requests.post(f"{self.base_url}/api/users",
             headers={
                 "Content-Type": "application/json",
                 "Authorization": f"Bearer {self.token}",
             },
             json={
                 "username": f'user_{tg_id}',
                 "trafficLimitBytes": 300000000000,
                 "expireAt": (datetime.now() + timedelta(days=30)).isoformat(),
                 "createdAt": datetime.now().isoformat(),
                 "telegramId": tg_id,
                 "activeInternalSquads": ["6f11955f-6b95-4f96-bba4-3d866de8ce83"],

             }
                 )
        return body.json()

vpn = Vpn()
res = vpn.create_new_user(1979427406)
print(res)