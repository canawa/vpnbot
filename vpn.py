import os
import secrets
from datetime import datetime, timedelta

import aiohttp
import dotenv
from marzban import MarzbanAPI
from marzban.models import UserCreate, ProxySettings

dotenv.load_dotenv()

async def deliver_trial_vpn(user_id, ):
    print('тут типа ключ генерится')