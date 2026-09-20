"""Remnawave Panel API v3 client.

Ключевые отличия от v2:
- пользователи идентифицируются числовым `id` (поле `uuid` удалено);
- поиск по Telegram ID: GET /api/users/stream?telegramId=...
- HWID: /api/hwid/devices/{userId}, в теле — userId (не userUuid);
- PATCH /api/users — по `id` или `username`;
- продление: POST /api/users/{userId}/actions/extend.
"""
import logging
import os
import sqlite3 as sq
from datetime import datetime, timedelta

import dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from databases import upsert_subscription_days

dotenv.load_dotenv()

BASE_LIMIT = 26843545600  # 25 ГБ
WEEK_TRAFFIC_LIMIT = 7516192768  # ~7 ГБ
TRIAL_TRAFFIC_LIMIT = 3221225472  # 3 ГБ
BONUS_2_DAYS_TRAFFIC_BYTES = 2 * 1073741824  # +2 ГБ к текущему расходу
BONUS_INACTIVE_DAYS = 3

PAID_SQUAD = '6f11955f-6b95-4f96-bba4-3d866de8ce83'
TRIAL_SQUAD = 'ffa0ca48-bb6e-447b-a404-f1808b09c967'

REQUEST_TIMEOUT = float(os.getenv('REMNAWAVE_TIMEOUT_SEC', '30'))


def panel_user_record(payload) -> dict | None:
    """Достаёт объект пользователя из ответа API (list / dict / stream)."""
    if not isinstance(payload, dict):
        return None
    response = payload.get('response')
    if isinstance(response, list):
        return response[0] if response else None
    if isinstance(response, dict):
        users = response.get('users')
        if isinstance(users, list):
            return users[0] if users else None
        # одиночный user-объект (create/patch/get by id)
        if 'id' in response or 'username' in response or 'expireAt' in response:
            return response
    return None


def _parse_iso_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
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


def _error_body(code: str, message: str) -> dict:
    return {'errorCode': code, 'message': message}


def _extract_stream_page(body) -> tuple[list, bool, object]:
    if isinstance(body, list):
        return body, False, None
    if not isinstance(body, dict):
        return [], False, None
    resp = body.get('response')
    if isinstance(resp, list):
        return resp, False, None
    if isinstance(resp, dict):
        users = resp.get('users') or []
        return users, bool(resp.get('hasMore')), resp.get('nextCursor')
    users = body.get('users')
    if isinstance(users, list):
        return users, bool(body.get('hasMore')), body.get('nextCursor')
    return [], False, None


def _panel_telegram_id(user: dict) -> int | None:
    raw = user.get('telegramId')
    if raw in (None, ''):
        raw = user.get('telegram_id')
    if raw in (None, '', 0, '0'):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _is_inactive_panel_user(user: dict, now: datetime | None = None) -> bool:
    status = str(user.get('status') or '').upper()
    if status in ('EXPIRED', 'DISABLED', 'LIMITED'):
        return True
    expire = _parse_iso_dt(user.get('expireAt') or user.get('expire_at'))
    now = now or datetime.now()
    if expire is not None and expire < now:
        return True
    return status not in ('', 'ACTIVE')


def _safe_json(response: requests.Response):
    if response.status_code in (204, 202) or not (response.content or b'').strip():
        return {}
    try:
        return response.json()
    except Exception:
        return None


class Vpn:
    def __init__(self):
        self.base_url = (os.getenv('REMNAWAVE_BASE_URL') or '').rstrip('/')
        self.token = os.getenv('REMNAWAVE_TOKEN')
        self.admin_login = os.getenv('REMNAWAVE_ADMIN_LOGIN')
        self.admin_password = os.getenv('REMNAWAVE_ADMIN_PASSWORD')

    def _headers(self, *, json_body: bool = False) -> dict:
        headers = {'Authorization': f'Bearer {self.token}'}
        if json_body:
            headers['Content-Type'] = 'application/json'
        return headers

    def _request(self, method: str, path: str, *, json=None, params=None) -> requests.Response:
        return requests.request(
            method,
            f'{self.base_url}{path}',
            headers=self._headers(json_body=json is not None),
            json=json,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

    @staticmethod
    def _user_id(user: dict | None) -> int | None:
        if not user:
            return None
        raw = user.get('id')
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def get_user_record_by_tg_id(self, tg_id) -> dict | None:
        """Первый пользователь с данным telegramId (API v3 stream)."""
        try:
            tg_id = int(tg_id)
        except (TypeError, ValueError):
            return None

        params: dict = {'telegramId': tg_id, 'size': 1}
        response = self._request('GET', '/api/users/stream', params=params)
        body = _safe_json(response)
        if not response.ok:
            return None
        if not isinstance(body, dict):
            return None
        return panel_user_record(body)

    def get_user_record_by_username(self, username: str) -> dict | None:
        response = self._request('GET', f'/api/users/by-username/{username}')
        body = _safe_json(response)
        if response.status_code == 404:
            return None
        if not response.ok or not isinstance(body, dict):
            return None
        return panel_user_record(body)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def get_user_by_tg_id(self, tg_id):
        """Совместимый ответ: {"response": [user]} или {"response": []}."""
        user = self.get_user_record_by_tg_id(tg_id)
        if user:
            return {'response': [user]}
        # fallback: username user_{tg_id}
        try:
            tg_id_int = int(tg_id)
        except (TypeError, ValueError):
            return {'response': []}
        user = self.get_user_record_by_username(f'user_{tg_id_int}')
        return {'response': [user]} if user else {'response': []}

    def create_new_user(self, tg_id, days=30):
        response = self._request(
            'POST',
            '/api/users',
            json={
                'username': f'user_{tg_id}',
                'trafficLimitBytes': BASE_LIMIT,
                'expireAt': (datetime.now() + timedelta(days=int(days))).isoformat(),
                'telegramId': int(tg_id),
                'trafficLimitStrategy': 'MONTH_ROLLING',
                'hwidDeviceLimit': 3,
                'activeInternalSquads': [PAID_SQUAD],
            },
        )
        body = _safe_json(response)
        if body is None:
            return _error_body('HTTP_ERROR', response.text or 'Invalid JSON')
        if not response.ok and isinstance(body, dict) and not body.get('errorCode'):
            body = {**body, 'errorCode': 'HTTP_ERROR', 'message': response.text or 'POST failed'}
        print(body)
        return body if isinstance(body, dict) else _error_body('HTTP_ERROR', str(body))

    def renew_subscription(self, tg_id, days):
        now = datetime.now()
        db_expire = None
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute(
                'SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?',
                (tg_id,),
            )
            row = cur.fetchone()
            if row:
                db_expire = _parse_iso_dt(row[0])

        user_data = self.get_user_record_by_tg_id(tg_id)
        if not user_data:
            return _error_body('USER_NOT_FOUND', 'User not found in panel')

        user_id = self._user_id(user_data)
        if user_id is None:
            return _error_body('USER_ID_MISSING', 'Panel user has no numeric id')

        panel_expire = _parse_iso_dt(user_data.get('expireAt'))
        status = str(user_data.get('status') or '').upper()
        # Если подписка уже неактивна / срок в прошлом — продлеваем от сейчас,
        # а не от старого expireAt (иначе июнь + 30 дней = июль при оплате в сентябре).
        still_active = (
            status in ('', 'ACTIVE')
            and any(dt is not None and dt > now for dt in (panel_expire, db_expire))
        )
        if still_active:
            future = [dt for dt in (panel_expire, db_expire) if dt is not None and dt > now]
            base_expire = max(future)
        else:
            base_expire = now
        new_expire = base_expire + timedelta(days=int(days))
        logging.info(
            'renew tg_id=%s days=%s status=%s panel_expire=%s db_expire=%s '
            'base=%s new=%s',
            tg_id, days, status, panel_expire, db_expire, base_expire, new_expire,
        )

        traffic = user_data.get('userTraffic') or {}
        used = int(traffic.get('usedTrafficBytes') or 0)
        leftover = max(0, BASE_LIMIT - used)
        traffic_limit = (
            WEEK_TRAFFIC_LIMIT + leftover if int(days) == 7 else BASE_LIMIT + leftover
        )

        response = self._request(
            'PATCH',
            '/api/users',
            json={
                'id': user_id,
                'status': 'ACTIVE',
                'trafficLimitBytes': traffic_limit,
                'expireAt': new_expire.isoformat(),
                'telegramId': int(tg_id),
                'hwidDeviceLimit': 3,
                'trafficLimitStrategy': 'MONTH_ROLLING',
                'activeInternalSquads': [PAID_SQUAD],
            },
        )
        body = _safe_json(response)
        if body is None:
            body = {}

        is_success = response.ok and not (
            isinstance(body, dict) and body.get('errorCode')
        )
        if is_success:
            upsert_subscription_days(tg_id, expires_at=new_expire.isoformat())
        print(body)
        if not is_success:
            if isinstance(body, dict) and body.get('errorCode'):
                return body
            return _error_body('HTTP_ERROR', response.text or 'PATCH failed')
        return body if isinstance(body, dict) else {'response': body}

    def get_user_traffic_by_tg_id(self, tg_id):
        user = self.get_user_record_by_tg_id(tg_id)
        if not user:
            raise RuntimeError(f'User not found for tg_id={tg_id}')
        traffic = user.get('userTraffic') or {}
        return (int(traffic.get('usedTrafficBytes') or 0),)

    def deliver_trial_vpn(self, tg_id):
        response = self._request(
            'POST',
            '/api/users',
            json={
                'username': f'user_{tg_id}',
                'expireAt': (datetime.now() + timedelta(days=3)).isoformat(),
                'telegramId': int(tg_id),
                'hwidDeviceLimit': 3,
                'trafficLimitBytes': TRIAL_TRAFFIC_LIMIT,
                'trafficLimitStrategy': 'MONTH_ROLLING',
                'activeInternalSquads': [TRIAL_SQUAD, PAID_SQUAD],
            },
        )
        body = _safe_json(response)
        if body is None:
            return _error_body('HTTP_ERROR', response.text or 'Invalid JSON')
        if not response.ok and isinstance(body, dict) and not body.get('errorCode'):
            body = {**body, 'errorCode': 'HTTP_ERROR', 'message': response.text or 'POST failed'}
        return body if isinstance(body, dict) else _error_body('HTTP_ERROR', str(body))

    def get_hwid_devices(self, tg_id):
        user = self.get_user_record_by_tg_id(tg_id)
        user_id = self._user_id(user)
        if user_id is None:
            return []
        response = self._request('GET', f'/api/hwid/devices/{user_id}')
        body = _safe_json(response)
        if not response.ok or not isinstance(body, dict):
            return []
        resp = body.get('response')
        if isinstance(resp, dict):
            return resp.get('devices') or []
        if isinstance(resp, list):
            return resp
        return []

    def delete_hwid_device(self, tg_id, hwid):
        user = self.get_user_record_by_tg_id(tg_id)
        user_id = self._user_id(user)
        if user_id is None:
            return _error_body('USER_NOT_FOUND', 'User not found in panel')
        try:
            response = self._request(
                'POST',
                '/api/hwid/devices/delete',
                json={'userId': user_id, 'hwid': hwid},
            )
            # v3: 200/204 без обязательного тела
            if response.ok or response.status_code in (200, 204):
                return True
            body = _safe_json(response)
            if isinstance(body, dict) and body.get('errorCode'):
                return body
            return _error_body('HTTP_ERROR', response.text or 'delete failed')
        except Exception as e:
            return e

    def get_leftover_bytes(self, tg_id):
        user = self.get_user_record_by_tg_id(tg_id)
        if not user:
            raise RuntimeError(f'User not found for tg_id={tg_id}')
        traffic_limit = int(user.get('trafficLimitBytes') or 0)
        used_traffic = int((user.get('userTraffic') or {}).get('usedTrafficBytes') or 0)
        print(
            f'[DEBUG] get_leftover_bytes tg_id={tg_id} '
            f'limit={traffic_limit} used={used_traffic}'
        )
        return traffic_limit, used_traffic

    def give_lte_gbs(self, tg_id, gb_amount):
        bytes_amount = int(gb_amount * 1073741824)
        print(
            f'[DEBUG] give_lte_gbs | tg_id={tg_id} | '
            f'добавляем={bytes_amount} байт ({gb_amount} ГБ)'
        )
        user = self.get_user_record_by_tg_id(tg_id)
        user_id = self._user_id(user)
        if user_id is None:
            return _error_body('USER_NOT_FOUND', 'User not found in panel')

        traffic_limit, _used = self.get_leftover_bytes(tg_id)
        new_limit = traffic_limit + bytes_amount

        response = self._request(
            'PATCH',
            '/api/users',
            json={
                'id': user_id,
                'trafficLimitBytes': new_limit,
                'activeInternalSquads': [PAID_SQUAD],
            },
        )
        if response.ok:
            with sq.connect('database.db') as con:
                con.execute(
                    'UPDATE subscriptions SET traffic_leftover_bytes = 0 WHERE user_id = ?',
                    (tg_id,),
                )
                con.commit()
        return response

    def _stream_users(self, *, status: str | None = None, size: int = 1000) -> list[dict]:
        """Пользователи через GET /api/users/stream (cursor)."""
        all_users: list[dict] = []
        cursor = None
        page_size = min(max(int(size), 1), 1000)

        while True:
            params: dict = {'size': page_size}
            if cursor is not None:
                params['cursor'] = cursor
            if status:
                params['status'] = status
            response = self._request('GET', '/api/users/stream', params=params)
            body = _safe_json(response)
            if not response.ok:
                logging.error(
                    'users stream HTTP %s status=%s: %s',
                    response.status_code, status, (response.text or '')[:400],
                )
                raise RuntimeError(f'panel users stream HTTP {response.status_code}')
            users, has_more, next_cursor = _extract_stream_page(body)
            all_users.extend(users)
            if not has_more or next_cursor is None:
                break
            cursor = next_cursor

        return all_users

    def get_all_users(self, size: int = 1000) -> list[dict]:
        """Все пользователи через cursor stream (v3)."""
        return self._stream_users(size=size)

    def get_unconnected_trial_users_tg_id(self) -> list:
        """Trial-пользователи без firstConnectedAt."""
        all_users = self.get_all_users()
        result = []
        for user in all_users:
            traffic = user.get('userTraffic') or {}
            if traffic.get('firstConnectedAt') is not None:
                continue
            squads = user.get('activeInternalSquads') or []
            if any(
                (isinstance(s, dict) and s.get('name') == 'trial')
                or s == TRIAL_SQUAD
                for s in squads
            ):
                tg = _panel_telegram_id(user)
                if tg is not None:
                    result.append(tg)
        return result

    def get_unactive_users(self) -> list[int]:
        return self.get_unactive_users_report()['recipients']

    def get_unactive_users_report(self) -> dict:
        """Инактив: EXPIRED / DISABLED / LIMITED или expireAt уже в прошлом."""
        all_users = self.get_all_users()
        now = datetime.now()
        recipients: list[int] = []
        seen: set[int] = set()
        active = 0
        no_tg = 0
        for user in all_users:
            tg = _panel_telegram_id(user)
            inactive = _is_inactive_panel_user(user, now)
            if not inactive:
                active += 1
                continue
            if tg is None:
                no_tg += 1
                continue
            if tg in seen:
                continue
            seen.add(tg)
            recipients.append(tg)
        return {
            'total': len(all_users),
            'active': active,
            'no_telegram_id': no_tg,
            'recipients': recipients,
        }

    def get_users_without_active_subscription(self) -> list[int]:
        """
        Кому слать промо «без подписки»:
        - на панели не ACTIVE / срок уже вышел;
        - плюс юзеры только из бота (на панели ещё не заводились).
        ACTIVE с живым expireAt — исключаются.
        """
        all_users = self.get_all_users()
        now = datetime.now()
        panel_tg: set[int] = set()
        active_tg: set[int] = set()
        recipients: list[int] = []
        seen: set[int] = set()

        for user in all_users:
            tg = _panel_telegram_id(user)
            if tg is None:
                continue
            panel_tg.add(tg)
            if _is_inactive_panel_user(user, now):
                if tg not in seen:
                    seen.add(tg)
                    recipients.append(tg)
            else:
                active_tg.add(tg)

        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id FROM users
                WHERE COALESCE(bot_blocked, 0) = 0
                """
            )
            bot_ids = [int(row[0]) for row in cur.fetchall()]

        for uid in bot_ids:
            if uid in active_tg:
                continue
            if uid in panel_tg:
                # уже учтён как inactive с панели
                continue
            if uid in seen:
                continue
            seen.add(uid)
            recipients.append(uid)

        return [uid for uid in recipients if uid not in active_tg]

    def give_2_days_bonus(self, tg_id):
        try:
            tg_id = int(tg_id)
        except (TypeError, ValueError):
            return _error_body('INVALID_USER_ID', 'Invalid telegram id')

        now = datetime.now()
        try:
            user_data = self.get_user_record_by_tg_id(tg_id)
        except Exception as e:
            return _error_body('API_ERROR', str(e))

        if not user_data:
            new_expire = now + timedelta(days=BONUS_INACTIVE_DAYS)
            response = self._request(
                'POST',
                '/api/users',
                json={
                    'username': f'user_{tg_id}',
                    'status': 'ACTIVE',
                    'expireAt': new_expire.isoformat(),
                    'telegramId': tg_id,
                    'hwidDeviceLimit': 3,
                    'trafficLimitBytes': BONUS_2_DAYS_TRAFFIC_BYTES,
                    'trafficLimitStrategy': 'MONTH_ROLLING',
                    'activeInternalSquads': [PAID_SQUAD, TRIAL_SQUAD],
                },
            )
            body = _safe_json(response)
            if body is None:
                return _error_body('HTTP_ERROR', response.text or 'Invalid JSON')
            if not isinstance(body, dict):
                return _error_body('HTTP_ERROR', response.text or str(body))
            if body.get('errorCode'):
                return body
            if not response.ok:
                return _error_body('HTTP_ERROR', response.text or 'POST failed')
            try:
                upsert_subscription_days(tg_id, expires_at=new_expire.isoformat())
                with sq.connect('database.db') as con:
                    con.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (tg_id,))
                    con.commit()
            except Exception as e:
                return _error_body('DB_ERROR', str(e))
            return {**body, 'created': True}

        user_id = self._user_id(user_data)
        if user_id is None:
            return _error_body('USER_ID_MISSING', 'Panel user has no numeric id')

        panel_expire = _parse_iso_dt(user_data.get('expireAt'))
        base_expire = max(now, panel_expire) if panel_expire and panel_expire > now else now
        new_expire = base_expire + timedelta(days=BONUS_INACTIVE_DAYS)

        traffic = user_data.get('userTraffic') or {}
        used_bytes = int(traffic.get('usedTrafficBytes') or 0)
        new_traffic_limit = used_bytes + BONUS_2_DAYS_TRAFFIC_BYTES

        response = self._request(
            'PATCH',
            '/api/users',
            json={
                'id': user_id,
                'status': 'ACTIVE',
                'trafficLimitBytes': new_traffic_limit,
                'expireAt': new_expire.isoformat(),
                'telegramId': tg_id,
                'hwidDeviceLimit': 3,
                'trafficLimitStrategy': 'MONTH_ROLLING',
                'activeInternalSquads': [PAID_SQUAD, TRIAL_SQUAD],
            },
        )
        body = _safe_json(response)
        if body is None:
            return _error_body('HTTP_ERROR', response.text or 'Invalid JSON')
        if not isinstance(body, dict):
            return _error_body('HTTP_ERROR', response.text or str(body))
        if body.get('errorCode'):
            return body
        if not response.ok:
            return _error_body('HTTP_ERROR', response.text or 'PATCH failed')

        try:
            upsert_subscription_days(tg_id, expires_at=new_expire.isoformat())
            with sq.connect('database.db') as con:
                con.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (tg_id,))
                con.commit()
        except Exception as e:
            return _error_body('DB_ERROR', str(e))

        return body

    def get_user_hwid_limit(self, tg_id):
        user = self.get_user_record_by_tg_id(tg_id)
        if not user:
            raise RuntimeError(f'User not found for tg_id={tg_id}')
        return int(user.get('hwidDeviceLimit') or 0)

    def add_hwid_devices(self, amount, tg_id):
        user = self.get_user_record_by_tg_id(tg_id)
        user_id = self._user_id(user)
        if user_id is None:
            return _error_body('USER_NOT_FOUND', 'User not found in panel')

        current = int(user.get('hwidDeviceLimit') or 0)
        response = self._request(
            'PATCH',
            '/api/users',
            json={
                'id': user_id,
                'hwidDeviceLimit': current + int(amount),
            },
        )
        body = _safe_json(response)
        if response.ok:
            return True
        if isinstance(body, dict) and body.get('errorCode'):
            return body
        return _error_body('HTTP_ERROR', response.text or 'PATCH failed')
