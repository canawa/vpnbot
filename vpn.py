import os
import secrets
from datetime import datetime, timedelta
import aiohttp
import dotenv
from marzban import MarzbanAPI
from marzban.models import UserCreate, ProxySettings

dotenv.load_dotenv()

MARZABAN_URL = os.getenv('MARZBAN_URL')
MARZABAN_USERNAME = os.getenv('MARZBAN_USERNAME')
MARZABAN_PASSWORD = os.getenv('MARZBAN_PASSWORD')

marzban_api = MarzbanAPI(base_url=MARZABAN_URL)
marzban_token = None

async def get_marzban_token():
    """Получить токен администратора Marzaban (async)."""
    global marzban_token
    try:
        token = await marzban_api.get_token(username=MARZABAN_USERNAME, password=MARZABAN_PASSWORD)
        marzban_token = token.access_token
        return marzban_token
    except Exception as e:
        print(f"Ошибка получения токена Marzaban: {e}")
        return None

async def generate_vpn_key(user_id: int, duration_days: int) -> str:
    """
    Создаёт пользователя в Marzban и возвращает ссылку подключения (vless://... или др.).
    Возвращает строку с ссылкой или None в случае ошибки.
    """
    global marzban_token
    if not marzban_token:
        token = await get_marzban_token()
        if not token:
            return None

    username = f"user_{user_id}_{secrets.token_hex(8)}"
    expire_ts = int((datetime.now() + timedelta(days=duration_days)).timestamp())

    try:
        new_user = UserCreate(
            username=username,
            proxies={"vless": ProxySettings(flow="xtls-rprx-vision"),
            "shadowsocks": ProxySettings(),
            },
            expire=expire_ts,
            data_limit=0
        )

        # Создаём пользователя через клиент Marzban
        added_user = await marzban_api.add_user(user=new_user, token=marzban_token)

        # Получаем информацию о пользователе (в ней может быть поле links)
        user_info = await marzban_api.get_user(username=username, token=marzban_token)

        # Попытки извлечь прямые ссылки (links) из разных мест
        links = None
        if hasattr(user_info, 'links') and user_info.links:
            links = user_info.links
        elif hasattr(user_info, 'proxies') and user_info.proxies:
            # Если links нет — пробуем endpoint subscription (иногда возвращает прямые ссылки)
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {"Authorization": f"Bearer {marzban_token}"}
                    async with session.get(
                        f"{MARZABAN_URL}/api/v1/user/{username}/subscription",
                        headers=headers,
                        params={"client_type": "v2ray"},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            if text.startswith(("vless://", "vmess://", "ss://")):
                                return text
            except Exception as e:
                print(f"Ошибка получения subscription: {e}")

        # Если links получили — отдаём предпочтительно vless
        if links:
            if isinstance(links, list) and len(links) > 0:
                for link in links:
                    if isinstance(link, str) and link.startswith("vless://"):
                        return link
                return links[0]
            elif isinstance(links, str):
                return links
# Иначе пробуем endpoint /links
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {marzban_token}"}
                async with session.get(
                    f"{MARZABAN_URL}/api/v1/user/{username}/links",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            for link in data:
                                if isinstance(link, str) and link.startswith("vless://"):
                                    return link
                            return data[0]
                        elif isinstance(data, str):
                            return data
        except Exception as e:
            print(f"Ошибка получения links через API: {e}")

        print(f"Не удалось получить links для пользователя {username}")
        return None

    except Exception as e:
        print(f"Ошибка создания пользователя в Marzaban: {e}")
        # Попытка обновить токен при ошибке авторизации и повторить (одна попытка)
        if "401" in str(e) or "Unauthorized" in str(e) or "token" in str(e).lower():
            token = await get_marzban_token()
            if token:
                try:
                    added_user = await marzban_api.add_user(user=new_user, token=marzban_token)
                    user_info = await marzban_api.get_user(username=username, token=marzban_token)
                    if hasattr(user_info, 'links') and user_info.links:
                        if isinstance(user_info.links, list):
                            return user_info.links[0]
                        return user_info.links
                except Exception as e2:
                    print(f"Ошибка повторной попытки: {e2}")
        return None