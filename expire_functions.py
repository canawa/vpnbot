import logging
import os
import sqlite3 as sq
from datetime import datetime, date
from datetime import timedelta
import asyncio

from aiogram.types import FSInputFile
from texts import PING_CAPTION
from vpn import Vpn, panel_user_record
from ikbs import *
from bot_delivery import is_telegram_unreachable, mark_user_bot_blocked
from renewal_funnel import renewal_funnel_handles_notifications

TRIAL_UNCONNECTED_SLEEP_SEC = int(os.getenv('TRIAL_UNCONNECTED_SLEEP_SEC', '86400'))
GBS_NOTIFY_USER_DELAY_SEC = float(os.getenv('GBS_NOTIFY_USER_DELAY_SEC', '0.15'))
DAY_BEFORE_PHOTO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'day_before.jpg')
PING_UNCONNECTED_PHOTO_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'photos', 'ping_unconnected.jpg',
)

EXPIRING_TOMORROW_CAPTION = (
    'Эй, завтра твой VPN отключится 👀\n\n'
    'И ты снова останешься без:\n\n'
    '<tg-emoji emoji-id="5332541698616629306">🔴</tg-emoji>Доступа к соцсетям\n'
    '<tg-emoji emoji-id="5332356160324409089">⌛️</tg-emoji> Интернета на улице\n'
    '<tg-emoji emoji-id="5420323339723881652">⚠️</tg-emoji> Видосиков на Ютубе\n\n'
    'Мы знаем - это бесит. Поэтому и сделали так, чтобы у нас такого не было.\n\n'
    'Продли подписку и забудь о проблемах, просто КАЙФУЙ'
)


def _fetch_user_ids(sql: str, params: tuple = ()) -> list[int]:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


async def check_expired_subscriptions_table(bot):
    """Таблица subscriptions: уведомление в день окончания subscription_expires_at."""
    await asyncio.sleep(15)
    while True:
        try:
            today_str = date.today().isoformat()
            rows = await asyncio.to_thread(
                _fetch_user_ids,
                """
                SELECT s.user_id FROM subscriptions s
                INNER JOIN users u ON u.id = s.user_id
                WHERE date(s.subscription_expires_at) = date(?)
                  AND (s.runout_notified IS NULL OR s.runout_notified = 0)
                  AND COALESCE(u.bot_blocked, 0) = 0
                """,
                (today_str,),
            )

            for user_id in rows:
                if await asyncio.to_thread(renewal_funnel_handles_notifications, user_id):
                    await asyncio.to_thread(
                        _mark_flag, 'runout_notified', user_id,
                    )
                    continue
                try:
                    await bot.send_message(
                        user_id,
                        (
                            '⏰ <b>Срок подписки истёк</b>\n\n'
                            'Чтобы продолжить пользоваться сервисом, продлите подписку.\n\n'
                        ),
                        parse_mode='HTML',
                        reply_markup=create_ikb_renew(),
                    )
                    await asyncio.to_thread(_mark_flag, 'runout_notified', user_id)
                    logging.info('%s: subscriptions expired today notified', user_id)
                except Exception as e:
                    if is_telegram_unreachable(e):
                        await asyncio.to_thread(mark_user_bot_blocked, user_id)
                    else:
                        logging.warning('subscriptions expired notify %s: %s', user_id, e)
                await asyncio.sleep(0.1)

        except Exception as e:
            logging.warning('check_expired_subscriptions_table: %s', e)

        await asyncio.sleep(3600)


def _mark_flag(column: str, user_id: int) -> None:
    allowed = {'runout_notified', 'expiring_tomorrow_notified'}
    if column not in allowed:
        return
    with sq.connect('database.db') as con:
        con.execute(
            f'UPDATE subscriptions SET {column} = 1 WHERE user_id = ?',
            (user_id,),
        )
        con.commit()


async def check_expiring_tomorrow_subscriptions_table(bot):
    """Таблица subscriptions: напоминание накануне дня из subscription_expires_at."""
    await asyncio.sleep(20)
    while True:
        try:
            tomorrow_str = (date.today() + timedelta(days=1)).isoformat()
            rows = await asyncio.to_thread(
                _fetch_user_ids,
                """
                SELECT s.user_id FROM subscriptions s
                INNER JOIN users u ON u.id = s.user_id
                WHERE date(s.subscription_expires_at) = date(?)
                  AND (s.expiring_tomorrow_notified IS NULL OR s.expiring_tomorrow_notified = 0)
                  AND COALESCE(u.bot_blocked, 0) = 0
                """,
                (tomorrow_str,),
            )

            for user_id in rows:
                if await asyncio.to_thread(renewal_funnel_handles_notifications, user_id):
                    await asyncio.to_thread(
                        _mark_flag, 'expiring_tomorrow_notified', user_id,
                    )
                    continue
                try:
                    markup = create_ikb_renew()
                    if os.path.isfile(DAY_BEFORE_PHOTO_PATH):
                        await bot.send_photo(
                            user_id,
                            FSInputFile(DAY_BEFORE_PHOTO_PATH),
                            caption=EXPIRING_TOMORROW_CAPTION,
                            parse_mode='HTML',
                            reply_markup=markup,
                        )
                    else:
                        await bot.send_message(
                            user_id,
                            EXPIRING_TOMORROW_CAPTION,
                            parse_mode='HTML',
                            reply_markup=markup,
                        )
                    await asyncio.to_thread(
                        _mark_flag, 'expiring_tomorrow_notified', user_id,
                    )
                    logging.info('%s: subscriptions expiring tomorrow notified', user_id)
                except Exception as e:
                    if is_telegram_unreachable(e):
                        await asyncio.to_thread(mark_user_bot_blocked, user_id)
                    else:
                        logging.warning('subscriptions tomorrow notify %s: %s', user_id, e)
                await asyncio.sleep(0.1)

        except Exception as e:
            logging.warning('check_expiring_tomorrow_subscriptions_table: %s', e)

        await asyncio.sleep(3600)


async def reset_runout_notified_daily():
    """Сбрасывает флаг runout_notified в 00:01 каждый день"""
    while True:
        try:
            now = datetime.now()
            next_reset = now.replace(hour=0, minute=1, second=0, microsecond=0)
            if now >= next_reset:
                next_reset += timedelta(days=1)

            seconds_until_reset = (next_reset - now).total_seconds()
            logging.info(
                'Next runout_notified reset will be at %s',
                next_reset.strftime('%Y-%m-%d %H:%M:%S'),
            )
            await asyncio.sleep(seconds_until_reset)

            def _reset():
                with sq.connect('database.db') as con:
                    cur = con.cursor()
                    cur.execute('UPDATE users SET runout_notified = 0 WHERE runout_notified = 1')
                    cur.execute(
                        'UPDATE users SET expiring_tomorrow_notified = 0 '
                        'WHERE expiring_tomorrow_notified = 1'
                    )
                    cur.execute(
                        'UPDATE subscriptions SET runout_notified = 0 WHERE runout_notified = 1'
                    )
                    cur.execute(
                        'UPDATE subscriptions SET expiring_tomorrow_notified = 0 '
                        'WHERE expiring_tomorrow_notified = 1'
                    )
                    con.commit()

            await asyncio.to_thread(_reset)
            logging.info(
                'runout_notified flags reset at %s',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            )
        except Exception as e:
            logging.warning('reset_runout_notified_daily: %s', e)
            await asyncio.sleep(3600)


def _gbs_check_one(tg_id: int) -> tuple[str, float | None]:
    """
    Returns action: 'skip' | 'notify' | 'reset', remaining_gb.
    """
    payload = Vpn().get_user_by_tg_id(tg_id)
    user_data = panel_user_record(payload)
    if not user_data:
        return 'skip', None

    current_limit = int(user_data.get('trafficLimitBytes') or 0)
    if current_limit <= 0:
        return 'skip', None

    traffic = user_data.get('userTraffic') or {}
    used = int(traffic.get('usedTrafficBytes') or 0)
    remaining_gb = (current_limit - used) / 1073741824

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT notified_low_traffic FROM subscriptions WHERE user_id = ?',
            (tg_id,),
        )
        row = cur.fetchone()
        if row is None:
            return 'skip', None
        notified = row[0]

    if remaining_gb <= 1 and not notified:
        return 'notify', remaining_gb
    if remaining_gb > 1 and notified:
        return 'reset', remaining_gb
    return 'skip', remaining_gb


def _set_notified_low_traffic(tg_id: int, value: int) -> None:
    with sq.connect('database.db') as con:
        con.execute(
            'UPDATE subscriptions SET notified_low_traffic = ? WHERE user_id = ?',
            (value, tg_id),
        )
        con.commit()


async def notify_gbs_ending(bot):
    """Не блокирует polling: VPN API в to_thread + пауза между юзерами."""
    await asyncio.sleep(45)
    while True:
        try:
            users = await asyncio.to_thread(
                _fetch_user_ids,
                """
                SELECT s.user_id FROM subscriptions s
                INNER JOIN users u ON u.id = s.user_id
                WHERE COALESCE(u.bot_blocked, 0) = 0
                """,
            )
            logging.info('[gbs_notify] batch start, users=%s', len(users))

            for tg_id in users:
                try:
                    action, remaining_gb = await asyncio.to_thread(_gbs_check_one, tg_id)
                    if action == 'notify' and remaining_gb is not None:
                        await bot.send_message(
                            tg_id,
                            (
                                f"\n<tg-emoji emoji-id='5274099962655816924'>❗️</tg-emoji>"
                                f"<b>У вас осталось всего {remaining_gb:.1f} ГБ</b>\n\n"
                                "Пополните трафик, чтобы не прерывать доступ:"
                            ),
                            parse_mode='HTML',
                            reply_markup=ikb_gbs_reminder_buy_option,
                        )
                        await asyncio.to_thread(_set_notified_low_traffic, tg_id, 1)
                    elif action == 'reset':
                        await asyncio.to_thread(_set_notified_low_traffic, tg_id, 0)
                except Exception as e:
                    if is_telegram_unreachable(e):
                        await asyncio.to_thread(mark_user_bot_blocked, tg_id)
                    # иначе молча — панель/сеть, не спамим лог на тысячах юзеров
                await asyncio.sleep(GBS_NOTIFY_USER_DELAY_SEC)

            logging.info('[gbs_notify] batch done')
        except Exception as e:
            logging.warning('notify_gbs_ending: %s', e)

        await asyncio.sleep(3600)


async def notify_inactive_trial_users(bot):
    """get_all_users() синхронный и тяжёлый — только в to_thread + старт с задержкой."""
    await asyncio.sleep(60)
    while True:
        try:
            all_users = await asyncio.to_thread(
                Vpn().get_unconnected_trial_users_tg_id,
            )
            logging.info('[trial_notify] start batch, users=%s', len(all_users))

            photo = FSInputFile(PING_UNCONNECTED_PHOTO_PATH)
            for user_id in all_users:
                try:
                    await bot.send_photo(
                        chat_id=user_id,
                        photo=photo,
                        caption=PING_CAPTION,
                        parse_mode='HTML',
                        reply_markup=ikb_my_sub,
                    )
                except Exception as e:
                    if is_telegram_unreachable(e):
                        await asyncio.to_thread(mark_user_bot_blocked, user_id)
                await asyncio.sleep(0.1)

        except Exception as e:
            logging.warning('[trial_notify] batch error: %s', e)

        await asyncio.sleep(TRIAL_UNCONNECTED_SLEEP_SEC)
