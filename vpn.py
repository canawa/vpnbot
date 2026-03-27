import os
import secrets
from datetime import datetime, timedelta

import aiohttp
import dotenv
from marzban import MarzbanAPI
from marzban.models import UserCreate, ProxySettings

dotenv.load_dotenv()

COUNTRIES = {
    "germany": {
        "url": os.getenv("MARZABAN_URL_GERMANY"),
        "username": os.getenv("MARZABAN_USERNAME_GERMANY"),
        "password": os.getenv("MARZABAN_PASSWORD_GERMANY"),
    },
    "finland": {
        "url": os.getenv("MARZBAN_URL_FINLAND"),
        "username": os.getenv("MARZABAN_USERNAME_FINLAND"),
        "password": os.getenv("MARZABAN_PASSWORD_FINLAND"),
    },
    "austria": {
        "url": os.getenv("MARZABAN_URL_AUSTRIA"),
        "username": os.getenv("MARZABAN_USERNAME_AUSTRIA"),
        "password": os.getenv("MARZABAN_PASSWORD_AUSTRIA"),
    },
    "france": {
        "url": os.getenv("MARZABAN_URL_FRANCE"),
        "username": os.getenv("MARZABAN_USERNAME_FRANCE"),
        "password": os.getenv("MARZABAN_PASSWORD_FRANCE"),
    },
}

TOKENS = {}


def get_api(country: str):
    cfg = COUNTRIES[country]
    api = MarzbanAPI(base_url=cfg["url"])
    return api, cfg


async def get_marzban_token(country: str):
    api, cfg = get_api(country)

    token = await api.get_token(
        username=cfg["username"],
        password=cfg["password"]
    )

    TOKENS[country] = token.access_token
    return TOKENS[country]


async def generate_vpn_key(user_id: int, duration_days: int, country: str) -> str:
    api, cfg = get_api(country)

    token = TOKENS.get(country)
    if not token:
        token = await get_marzban_token(country)
        if not token:
            return None

    username = f"user_{user_id}_{secrets.token_hex(8)}"

    expire_ts = 0 if duration_days <= 0 else int(
        (datetime.now() + timedelta(days=duration_days)).timestamp()
    )

    try:
        new_user = UserCreate(
            username=username,
            proxies={
                "vless": ProxySettings(flow="xtls-rprx-vision"),
                "shadowsocks": ProxySettings(),
            },
            expire=expire_ts,
            data_limit=0
        )

        await api.add_user(user=new_user, token=token)
        user_info = await api.get_user(username=username, token=token)

        links = None
        if hasattr(user_info, 'links') and user_info.links:
            links = user_info.links

        if links:
            if isinstance(links, list) and len(links) > 0:
                for link in links:
                    if isinstance(link, str) and link.startswith("vless://"):
                        return link
                return links[0]
            elif isinstance(links, str):
                return links

        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {token}"}
                async with session.get(
                    f"{cfg['url']}/api/v1/user/{username}/subscription",
                    headers=headers,
                    params={"client_type": "v2ray"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if text.startswith(("vless://", "vmess://", "ss://")):
                            return text
        except Exception as e:
            print(f"subscription error: {e}")

        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {token}"}
                async with session.get(
                    f"{cfg['url']}/api/v1/user/{username}/links",
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
            print(f"links error: {e}")

        return None

    except Exception as e:
        print(f"create user error: {e}")

        if "401" in str(e) or "Unauthorized" in str(e):
            token = await get_marzban_token(country)
            try:
                await api.add_user(user=new_user, token=token)
                user_info = await api.get_user(username=username, token=token)

                if hasattr(user_info, 'links') and user_info.links:
                    if isinstance(user_info.links, list):
                        return user_info.links[0]
                    return user_info.links

            except Exception as e2:
                print(f"retry error: {e2}")

        return None