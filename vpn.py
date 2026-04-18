import os
import dotenv
import asyncio
import requests
from datetime import datetime, time, timedelta
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
        requests.post(f"{self.base_url}/api/users",
                      headers={
                          "Content-Type": "application/json",
                          "Authorization": f"Bearer {self.token}",
                      },
                      json={
                          "username": f'user_{tg_id}',
                          "status": "ACTIVE",
                          "shortUuid": "",
                          "trojanPassword": "",
                          "vlessUuid": "",
                          "ssPassword": "",
                          "trafficLimitBytes": 300000000000,
                          "trafficLimitStrategy": "NO_RESET",
                          "expireAt": (datetime.now() + timedelta(days=30)).isoformat(),
                          "createdAt": datetime.now().isoformat(),
                          "lastTrafficResetAt": "",
                          "description": "",
                          "tag": "",
                          "telegramId": tg_id,
                          "email": None,
                          "hwidDeviceLimit": 3,
                          "activeInternalSquads": [
                              "VLESS REALITY",
                          ],
                          "uuid": "",
                          "externalSquadUuid": None
                      }
                      )

    def get_by_username(self, username):
        sub = requests.get(f"{self.base_url}/api/subscriptions/by-username/{username}",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        return sub.json()['response']['subscriptionUrl']


def deliver_trial_vpn(user_id, ):
    print('тут типа ключ генерится')