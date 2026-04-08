import os
import secrets
import asyncio
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
        "url": os.getenv("MARZABAN_URL_FINLAND"),
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


def _extract_links(payload) -> list[str]:
    if not payload:
        return []
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, str) and item]
    if isinstance(payload, dict):
        collected: list[str] = []
        for key in ("links", "subscriptions", "subscription", "results"):
            value = payload.get(key)
            if isinstance(value, str):
                collected.append(value)
            elif isinstance(value, list):
                collected.extend([item for item in value if isinstance(item, str) and item])
        return collected
    return []


def _prefer_vless(links: list[str]) -> list[str]:
    vless_links = [link for link in links if isinstance(link, str) and link.startswith("vless://")]
    return vless_links if vless_links else links


def _unique_keep_order(items: list[str]) -> list[str]:
    seen = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


async def _fetch_links_endpoint(cfg: dict, token: str, username: str) -> list[str]:
    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": f"Bearer {token}"}
        async with session.get(
            f"{cfg['url']}/api/v1/user/{username}/links",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return _prefer_vless(_extract_links(data))


async def _poll_user_vless_links(api: MarzbanAPI, cfg: dict, token: str, username: str, tries: int = 6, delay_sec: float = 1.0) -> list[str]:
    """Marzban иногда отдает ссылки не сразу после add_user — ждём несколько попыток."""
    last_links: list[str] = []
    for attempt in range(tries):
        merged: list[str] = []
        try:
            user_info = await api.get_user(username=username, token=token)
            merged.extend(_prefer_vless(_extract_links(getattr(user_info, 'links', None))))
        except Exception:
            pass
        try:
            merged.extend(await _fetch_links_endpoint(cfg, token, username))
        except Exception:
            pass
        links = _unique_keep_order(_prefer_vless(merged))
        if len(links) >= 2:
            return links
        if links:
            # Даже если пока 1 ключ, дадим шанс серверу досинхрониться.
            last_links = links
        if attempt < tries - 1:
            await asyncio.sleep(delay_sec)
    return last_links


async def generate_vpn_keys(user_id: int, duration_days: int, country: str) -> list[str]:
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
            },
            expire=expire_ts,
            data_limit=0
        )

        await api.add_user(user=new_user, token=token)
        links = await _poll_user_vless_links(api, cfg, token, username)
        if links:
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
                            return [text]
        except Exception as e:
            print(f"subscription error: {e}")

        try:
            links = await _fetch_links_endpoint(cfg, token, username)
            if links:
                return links
        except Exception as e:
            print(f"links error: {e}")

        return []

    except Exception as e:
        print(f"create user error: {e}")

        if "401" in str(e) or "Unauthorized" in str(e):
            token = await get_marzban_token(country)
            try:
                await api.add_user(user=new_user, token=token)
                links = await _poll_user_vless_links(api, cfg, token, username)
                if links:
                    return links

            except Exception as e2:
                print(f"retry error: {e2}")

        return []


async def generate_vpn_key(user_id: int, duration_days: int, country: str) -> str:
    links = await generate_vpn_keys(user_id, duration_days, country)
    if not links:
        return None
    return links[0]