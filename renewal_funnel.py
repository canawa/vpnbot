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
from funnel import log_funnel_event
from emojis import get_emoji

logger = logging.getLogger(__name__)

SUPPORT_URL = 'https://t.me/coffeemaniasup2'
RENEWAL_SLEEP_SEC = int(os.getenv('RENEWAL_SLEEP_SEC', '300'))

P30 = SUBSCRIPTION_PLAN.get(30, 149)
P90 = SUBSCRIPTION_PLAN.get(90, 399)
P360 = SUBSCRIPTION_PLAN.get(360, 899)


def _connect():
    return sq.connect('database.db')


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


def _ensure_row(user_id: int, expires_at: str) -> None:
    now = datetime.now().isoformat()
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
            log_funnel_event(user_id, 'renewal_entered')
        con.commit()


def renewal_on_paid(user_id: int) -> None:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE user_renewal_funnel
            SET stopped = 1, stopped_at = ?
            WHERE user_id = ?
            """,
            (datetime.now().isoformat(), user_id),
        )
        con.commit()
    log_funnel_event(user_id, 'renewal_stopped_paid')


def _clear_cycle_after_renewal(user_id: int, expires_at: str) -> None:
    """После оплаты подписка снова далеко в будущем — сброс флагов для следующего цикла."""
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
            con.commit()
            log_funnel_event(user_id, 'renewal_cycle_reset')


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
    with _connect() as con:
        con.execute(f'UPDATE user_renewal_funnel SET {column} = 1 WHERE user_id = ?', (user_id,))
        con.commit()
    log_funnel_event(user_id, f'renewal_{column}')


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
    try:
        await bot.send_message(user_id, text, parse_mode='HTML', reply_markup=markup)
        logger.info('renewal funnel sent to user_id=%s', user_id)
        return True
    except Exception as e:
        logger.warning('renewal send failed user_id=%s: %s', user_id, e)
        return False


async def _process_user(bot: Bot, user_id: int, expires_at: str) -> None:
    if not is_paid_subscriber(user_id):
        return

    exp_d = _parse_date(expires_at)
    if not exp_d:
        return

    today = date.today()
    days_until = (exp_d - today).days

    if days_until > 7:
        _clear_cycle_after_renewal(user_id, expires_at)
        return

    _ensure_row(user_id, expires_at)
    loaded = _load_flags(user_id)
    if not loaded:
        return
    stopped, rn_m7, rn_m3, rn_d0, rn_p1d, rn_p3d, rn_p7d, rn_p30d = loaded
    if stopped:
        return

    # Одно письмо за проход; catch-up если воркер пропустил точный день.
    if days_until >= 0:
        if not rn_m7 and days_until <= 7:
            if await _safe_send(bot, user_id, MSG_M7, ikb_renew_buy()):
                _mark_flag(user_id, 'rn_m7')
        elif not rn_m3 and days_until <= 3:
            if await _safe_send(bot, user_id, MSG_M3, ikb_renew_buy()):
                _mark_flag(user_id, 'rn_m3')
        elif not rn_d0 and days_until == 0:
            if await _safe_send(bot, user_id, MSG_D0, ikb_renew_buy()):
                _mark_flag(user_id, 'rn_d0')
    else:
        days_past = -days_until
        if not rn_p1d and days_past >= 1:
            if await _safe_send(bot, user_id, MSG_P1D, ikb_restore()):
                _mark_flag(user_id, 'rn_p1d')
        elif not rn_p3d and days_past >= 3:
            if await _safe_send(bot, user_id, MSG_P3D, ikb_renew_plans()):
                _mark_flag(user_id, 'rn_p3d')
        elif not rn_p7d and days_past >= 7:
            if await _safe_send(bot, user_id, MSG_P7D, ikb_enough()):
                _mark_flag(user_id, 'rn_p7d')
        elif not rn_p30d and days_past >= 30:
            if await _safe_send(bot, user_id, MSG_P30D, ikb_year_60_marketing()):
                _mark_flag(user_id, 'rn_p30d')


async def run_renewal_funnel_worker(bot: Bot) -> None:
    logger.info('renewal funnel worker started, sleep=%ss', RENEWAL_SLEEP_SEC)
    await asyncio.sleep(8)
    while True:
        try:
            with _connect() as con:
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT s.user_id, s.subscription_expires_at
                    FROM subscriptions s
                    WHERE s.subscription_expires_at IS NOT NULL
                      AND TRIM(s.subscription_expires_at) != ''
                    """
                )
                rows = cur.fetchall()

            for row in rows:
                uid, exp = row[0], row[1]
                if not is_paid_subscriber(uid):
                    continue
                await _process_user(bot, uid, exp)
                await asyncio.sleep(0.05)
        except Exception as e:
            logger.exception('renewal funnel worker error: %s', e)
        await asyncio.sleep(RENEWAL_SLEEP_SEC)


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
