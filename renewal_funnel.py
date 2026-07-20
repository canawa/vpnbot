"""
Воронка на продление подписки (платные пользователи).
Цены фиксированные: 149 / 399 / 899 / 50₽ неделя — без таймеров и реальных скидок.
В тексте может быть «скидка 60%», кнопка года ведёт на 899₽.
"""
import asyncio
import logging
import os
import sqlite3 as sq
from datetime import datetime, date, timedelta

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from prices import SUBSCRIPTION_PLAN, WEEK_PLAN_DAYS, WEEK_PLAN_PRICE
from bot_delivery import is_telegram_unreachable, is_user_bot_blocked, mark_user_bot_blocked
from databases import db_connect, db_retry
from emojis import get_emoji

logger = logging.getLogger(__name__)

SUPPORT_URL = 'https://t.me/coffeemaniasup2'
RENEWAL_SLEEP_SEC = int(os.getenv('RENEWAL_SLEEP_SEC', '300'))
RENEWAL_USER_DELAY_SEC = float(os.getenv('RENEWAL_USER_DELAY_SEC', '0.35'))

P30 = SUBSCRIPTION_PLAN.get(30, 149)
P90 = SUBSCRIPTION_PLAN.get(90, 399)
P360 = SUBSCRIPTION_PLAN.get(360, 899)

_renewal_db_lock = asyncio.Lock()


def _connect():
    return db_connect()


def _log_event_in_tx(cur, user_id: int, event_type: str) -> None:
    cur.execute(
        'INSERT INTO funnel_events (user_id, event_type, meta, created_at) VALUES (?, ?, ?, ?)',
        (user_id, event_type, None, datetime.now().isoformat()),
    )


def _ensure_row(user_id: int, expires_at: str) -> None:
    now = datetime.now().isoformat()

    def _write():
        with _connect() as con:
            cur = con.cursor()
            cur.execute('SELECT 1 FROM user_renewal_funnel WHERE user_id = ?', (user_id,))
            if cur.fetchone():
                cur.execute(
                    'UPDATE user_renewal_funnel SET subscription_expires_at = ? WHERE user_id = ?',
                    (expires_at, user_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO user_renewal_funnel (user_id, subscription_expires_at, entered_at)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, expires_at, now),
                )
                _log_event_in_tx(cur, user_id, 'renewal_entered')
            con.commit()

    db_retry(_write)


def renewal_on_paid(user_id: int) -> None:
    now = datetime.now().isoformat()

    def _write():
        with _connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE user_renewal_funnel
                SET stopped = 1, stopped_at = ?
                WHERE user_id = ?
                """,
                (now, user_id),
            )
            if cur.rowcount:
                _log_event_in_tx(cur, user_id, 'renewal_stopped_paid')
            con.commit()

    db_retry(_write)


def _clear_cycle_after_renewal(user_id: int, expires_at: str) -> None:
    """После оплаты подписка снова далеко в будущем — сброс флагов для следующего цикла."""

    def _write():
        with _connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE user_renewal_funnel
                SET stopped = 0, stopped_at = NULL,
                    rn_m7 = 0, rn_m3 = 0, rn_d0 = 0,
                    rn_p1d = 0, rn_p3d = 0, rn_p7d = 0, rn_p30d = 0,
                    subscription_expires_at = ?
                WHERE user_id = ? AND stopped = 1
                """,
                (expires_at, user_id),
            )
            if cur.rowcount:
                _log_event_in_tx(cur, user_id, 'renewal_cycle_reset')
                con.commit()

    db_retry(_write)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if 'T' in text:
            return datetime.fromisoformat(text.replace('Z', '+00:00')).date()
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _fetch_paid_subscriber_ids() -> set[int]:
    with _connect() as con:
        cur = con.cursor()
        ids: set[int] = set()
        cur.execute(
            """
            SELECT DISTINCT user_id FROM transactions
            WHERE type IN ('yookassa', 'CryptoBot')
            """
        )
        ids.update(row[0] for row in cur.fetchall())
        cur.execute(
            'SELECT user_id FROM user_funnel WHERE last_paid_at IS NOT NULL'
        )
        ids.update(row[0] for row in cur.fetchall())
    return ids


def is_paid_subscriber(user_id: int) -> bool:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT 1 FROM transactions
            WHERE user_id = ? AND type IN ('yookassa', 'CryptoBot')
            LIMIT 1
            """,
            (user_id,),
        )
        if cur.fetchone():
            return True
        cur.execute(
            'SELECT 1 FROM user_funnel WHERE user_id = ? AND last_paid_at IS NOT NULL',
            (user_id,),
        )
        return cur.fetchone() is not None


def renewal_funnel_handles_notifications(user_id: int) -> bool:
    """Платникам напоминания об окончании шлёт только воронка продления."""
    return is_paid_subscriber(user_id)


def _load_flags(user_id: int) -> tuple[int, ...] | None:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT COALESCE(stopped, 0), COALESCE(rn_m7, 0), COALESCE(rn_m3, 0),
                   COALESCE(rn_d0, 0), COALESCE(rn_p1d, 0), COALESCE(rn_p3d, 0),
                   COALESCE(rn_p7d, 0), COALESCE(rn_p30d, 0)
            FROM user_renewal_funnel WHERE user_id = ?
            """,
            (user_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row


def _mark_flag(user_id: int, column: str) -> None:
    allowed = {'rn_m7', 'rn_m3', 'rn_d0', 'rn_p1d', 'rn_p3d', 'rn_p7d', 'rn_p30d'}
    if column not in allowed:
        return

    def _write():
        with _connect() as con:
            cur = con.cursor()
            cur.execute(
                f'UPDATE user_renewal_funnel SET {column} = 1 WHERE user_id = ?',
                (user_id,),
            )
            _log_event_in_tx(cur, user_id, f'renewal_{column}')
            con.commit()

    db_retry(_write)


async def _run_db(fn, *args):
    """Сериализуем обращения воронки продления к SQLite."""
    async with _renewal_db_lock:
        return await asyncio.to_thread(fn, *args)


def ikb_renew_buy() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text='Продлить подписку',
            callback_data='buy_vpn',
            icon_custom_emoji_id=get_emoji('plus'),
            style='success',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_restore() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Восстановить', callback_data='buy_vpn', style='success')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_enough() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Хватит терпеть', callback_data='buy_vpn', style='success')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_renew_plans() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f'1 месяц · {P30}₽',
            callback_data=f'deposit_{P30}_30_card',
        )],
        [InlineKeyboardButton(
            text=f'3 месяца · {P90}₽',
            callback_data=f'deposit_{P90}_90_card',
        )],
        [InlineKeyboardButton(
            text=f'1 год · {P360}₽',
            callback_data=f'deposit_{P360}_360_card',
            style='success',
        )],
        [InlineKeyboardButton(
            text=f'Неделя · {WEEK_PLAN_PRICE}₽',
            callback_data=f'deposit_{WEEK_PLAN_PRICE}_{WEEK_PLAN_DAYS}_card',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_year_60_marketing() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f'🔥 Забрать год за {P360}₽ (−60%)',
            callback_data=f'deposit_{P360}_360_card',
            style='success',
        )],
        [InlineKeyboardButton(text='Написать в поддержку', url=SUPPORT_URL)],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


MSG_M7 = (
    '🛡 <b>Подписка закончится через 7 дней</b>\n\n'
    'Ещё есть время — но лучше продлить сейчас, пока не забыл 👇'
)

MSG_M3 = (
    '⚡️ <b>3 дня до конца подписки</b>\n\n'
    'Закройте глаза… вспомните, как приятно было пользоваться интернетом без ограничений.\n\n'
    'Скоро доступ может пропасть — продлите заранее, и всё продолжит работать 👇'
)

MSG_D0 = (
    'Стоп! ✋ Твоя подписка заканчивается <b>СЕГОДНЯ!</b>\n\n'
    'Через несколько часов ты останешься без:\n\n'
    '<tg-emoji emoji-id="5350672507919674722">😫</tg-emoji> Доступа к соцсетям\n'
    '<tg-emoji emoji-id="5328108441963604719">🫣</tg-emoji> Интернета на улице\n'
    '<tg-emoji emoji-id="5235696010067460335">🫨</tg-emoji> Видосиков на Ютубе\n\n'
    'Мы знаем — это бесит. Поэтому у нас такого нет.\n\n'
    'Успей продлить пока ещё есть доступ и просто КАЙФУЙ'
)

MSG_P1D = (
    '🔴 <b>Подписка закончилась</b>\n\n'
    'Интернет всё ещё глушат, а мы помогаем с обходом ограничений.\n\n'
    'Вернуть одной кнопкой 👇'
)

MSG_P3D = (
    'Всё ещё без VPN? Держи актуальные тарифы:\n\n'
    f'• 1 месяц — <b>{P30} ₽</b>\n'
    f'• 3 месяца — <b>{P90} ₽</b>\n'
    f'• <b>1 год — {P360} ₽</b> (≈75 ₽/мес)\n'
    f'• Неделя — <b>{WEEK_PLAN_PRICE} ₽</b>\n\n'
    'Чем дольше — тем спокойнее. Подключи снова 👇'
)

MSG_P7D = (
    'Без VPN это сразу чувствуется: то не грузит, то не открывается.\n\n'
    'Наши пользователи решили так не жить — вернулись. Ты следующий? 👇'
)

MSG_P30D = (
    'Прошёл месяц. Мы соскучились 👋\n\n'
    'Не знаем, что пошло не так — может, цена, может просто не дошли руки.\n\n'
    'Если что-то остановило — напишите в поддержку, разберёмся.\n\n'
    f'А если просто отложил — наш лучший вариант: <b>год со скидкой 60%</b> '
    f'всего за <b>{P360} ₽</b> (обычно дороже, сейчас фиксированная цена на год).'
)


async def _safe_send(bot: Bot, user_id: int, text: str, markup) -> bool:
    if is_user_bot_blocked(user_id):
        return False
    try:
        await bot.send_message(user_id, text, parse_mode='HTML', reply_markup=markup)
        logger.info('renewal funnel sent to user_id=%s', user_id)
        return True
    except Exception as e:
        if is_telegram_unreachable(e):
            mark_user_bot_blocked(user_id)
            logger.info(
                'renewal funnel skip user_id=%s (blocked bot or deleted account)', user_id,
            )
        else:
            logger.warning('renewal send failed user_id=%s: %s', user_id, e)
        return False


async def _process_user(bot: Bot, user_id: int, expires_at: str) -> None:
    if await _run_db(is_user_bot_blocked, user_id):
        return

    exp_d = _parse_date(expires_at)
    if not exp_d:
        return

    today = date.today()
    days_until = (exp_d - today).days

    if days_until > 7:
        await _run_db(_clear_cycle_after_renewal, user_id, expires_at)
        return

    await _run_db(_ensure_row, user_id, expires_at)
    loaded = await _run_db(_load_flags, user_id)
    if not loaded:
        return
    stopped, rn_m7, rn_m3, rn_d0, rn_p1d, rn_p3d, rn_p7d, rn_p30d = loaded
    if stopped:
        return

    flag_to_mark: str | None = None
    msg: str | None = None
    markup = None

    if days_until >= 0:
        if not rn_m7 and days_until <= 7:
            msg, markup, flag_to_mark = MSG_M7, ikb_renew_buy(), 'rn_m7'
        elif not rn_m3 and days_until <= 3:
            msg, markup, flag_to_mark = MSG_M3, ikb_renew_buy(), 'rn_m3'
        elif not rn_d0 and days_until == 0:
            msg, markup, flag_to_mark = MSG_D0, ikb_renew_buy(), 'rn_d0'
    else:
        days_past = -days_until
        if not rn_p1d and days_past >= 1:
            msg, markup, flag_to_mark = MSG_P1D, ikb_restore(), 'rn_p1d'
        elif not rn_p3d and days_past >= 3:
            msg, markup, flag_to_mark = MSG_P3D, ikb_renew_plans(), 'rn_p3d'
        elif not rn_p7d and days_past >= 7:
            msg, markup, flag_to_mark = MSG_P7D, ikb_enough(), 'rn_p7d'
        elif not rn_p30d and days_past >= 30:
            msg, markup, flag_to_mark = MSG_P30D, ikb_year_60_marketing(), 'rn_p30d'

    if not msg or not flag_to_mark:
        return

    if await _safe_send(bot, user_id, msg, markup):
        await _run_db(_mark_flag, user_id, flag_to_mark)


async def run_renewal_funnel_worker(bot: Bot) -> None:
    logger.info(
        'renewal funnel worker started, sleep=%ss, user_delay=%ss',
        RENEWAL_SLEEP_SEC,
        RENEWAL_USER_DELAY_SEC,
    )
    await asyncio.sleep(40)
    while True:
        try:
            rows = await _run_db(_load_subscription_rows)
            paid_ids = await _run_db(_fetch_paid_subscriber_ids)

            for uid, exp in rows:
                if uid not in paid_ids:
                    continue
                try:
                    await _process_user(bot, uid, exp)
                except sq.OperationalError as e:
                    logger.warning('renewal funnel db locked user_id=%s: %s', uid, e)
                except Exception as e:
                    logger.exception('renewal funnel user_id=%s: %s', uid, e)
                await asyncio.sleep(RENEWAL_USER_DELAY_SEC)
        except Exception as e:
            logger.exception('renewal funnel worker error: %s', e)
        await asyncio.sleep(RENEWAL_SLEEP_SEC)


def _load_subscription_rows() -> list[tuple[int, str]]:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT s.user_id, s.subscription_expires_at
            FROM subscriptions s
            INNER JOIN users u ON u.id = s.user_id
            WHERE s.subscription_expires_at IS NOT NULL
              AND TRIM(s.subscription_expires_at) != ''
              AND COALESCE(u.bot_blocked, 0) = 0
            """
        )
        return cur.fetchall()


def fetch_renewal_stats() -> tuple[str, list[dict]]:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                r.user_id, u.username, r.subscription_expires_at, r.stopped,
                r.rn_m7, r.rn_m3, r.rn_d0, r.rn_p1d, r.rn_p3d, r.rn_p7d, r.rn_p30d,
                r.entered_at, r.stopped_at
            FROM user_renewal_funnel r
            LEFT JOIN users u ON u.id = r.user_id
            ORDER BY r.entered_at DESC
            """
        )
        rows = cur.fetchall()

    items = []
    stopped_n = 0
    for r in rows:
        if r[3]:
            stopped_n += 1
        items.append({
            'user_id': r[0],
            'username': r[1] or '',
            'subscription_expires_at': r[2] or '',
            'stopped': r[3] or 0,
            'rn_m7': r[4], 'rn_m3': r[5], 'rn_d0': r[6],
            'rn_p1d': r[7], 'rn_p3d': r[8], 'rn_p7d': r[9], 'rn_p30d': r[10],
            'entered_at': r[11] or '',
            'stopped_at': r[12] or '',
        })

    summary = (
        '<b>📊 Воронка продления</b>\n\n'
        f'В трекере: <b>{len(items)}</b>\n'
        f'Остановлено (продлили): <b>{stopped_n}</b>\n\n'
        '<i>Детали — лист «Продление» в выгрузке воронки или отдельный отчёт позже.</i>'
    )
    return summary, items
