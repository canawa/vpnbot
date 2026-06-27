"""
Воронка на покупку (без таймерных скидок).
Тарифы: 50₽/неделя, 899₽/год + стандартная линейка.
"""
import asyncio
import logging
import os
import sqlite3 as sq
from datetime import datetime, date, timedelta

from aiogram import F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from prices import WEEK_PLAN_DAYS, WEEK_PLAN_PRICE, SUBSCRIPTION_PLAN
from bot_delivery import is_telegram_unreachable, mark_user_bot_blocked
from databases import upsert_subscription_days
from emojis import get_emoji

logger = logging.getLogger(__name__)

SUPPORT_URL = 'https://t.me/coffeemaniasup2'

# Интервалы воронки
TD_30M = timedelta(minutes=30)
TD_24H = timedelta(hours=24)
TD_48H = timedelta(hours=48)
TD_72H = timedelta(hours=72)
TD_1H = timedelta(hours=1)
TD_3D = timedelta(days=3)
TD_7D = timedelta(days=7)




# Минимальный шаг воронки — 30 мин (TD_30M). Тик 15 мин: письмо уйдёт в окне +0…+15 мин.
# Для теста с TD_* в минутах: FUNNEL_SLEEP_SEC=30 в .env
FUNNEL_SLEEP_SEC = int(os.getenv('FUNNEL_SLEEP_SEC', '900'))

_FLAG_EVENT = {
    'nt_30m': 'msg_nt_30m',
    'nt_24h': 'msg_nt_24h',
    'nt_48h': 'msg_nt_48h',
    'nt_72h': 'msg_nt_72h',
    'pt_1h': 'msg_pt_1h',
    'pt_24h': 'msg_pt_24h',
    'pt_3d': 'msg_pt_3d',
    'pt_7d': 'msg_pt_7d',
}



def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _connect():
    return sq.connect('database.db')


def log_funnel_event(user_id: int, event_type: str, meta: str | None = None) -> None:
    with _connect() as con:
        con.execute(
            'INSERT INTO funnel_events (user_id, event_type, meta, created_at) VALUES (?, ?, ?, ?)',
            (user_id, event_type, meta, datetime.now().isoformat()),
        )
        con.commit()


EVENT_MSG_OPEN_INVOICE_3M = 'msg_open_invoice_3m'
EVENT_CLICK_OPEN_INVOICE_CHECK = 'click_open_invoice_check'
EVENT_PAID_OPEN_INVOICE_3M = 'paid_open_invoice_3m'


def _open_invoice_meta(tx_type: str, payment_id: str) -> str:
    return f'{(tx_type or "yookassa").strip()}:{str(payment_id).strip()}'


def log_open_invoice_reminder_sent(
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
) -> None:
    """Видео/текст через 3 мин после неоплаченного счёта ЮKassa."""
    log_funnel_event(user_id, EVENT_MSG_OPEN_INVOICE_3M, _open_invoice_meta(tx_type, payment_id))


def _open_invoice_reminder_was_sent(payment_id: str, tx_type: str) -> bool:
    meta = _open_invoice_meta(tx_type, payment_id)
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT 1 FROM funnel_events WHERE event_type = ? AND meta = ? LIMIT 1',
            (EVENT_MSG_OPEN_INVOICE_3M, meta),
        )
        return cur.fetchone() is not None


def try_log_open_invoice_check_click(
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
) -> None:
    """«Проверить» после напоминания (тот же callback, что у «Я оплатил»)."""
    if not _open_invoice_reminder_was_sent(payment_id, tx_type):
        return
    meta = _open_invoice_meta(tx_type, payment_id)
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT 1 FROM funnel_events WHERE event_type = ? AND meta = ? LIMIT 1',
            (EVENT_CLICK_OPEN_INVOICE_CHECK, meta),
        )
        if cur.fetchone():
            return
    log_funnel_event(user_id, EVENT_CLICK_OPEN_INVOICE_CHECK, meta)


def try_log_open_invoice_reminder_paid(
    user_id: int,
    payment_id: str,
    amount: int,
    tx_type: str = 'yookassa',
) -> None:
    if not _open_invoice_reminder_was_sent(payment_id, tx_type):
        return
    base_meta = _open_invoice_meta(tx_type, payment_id)
    paid_meta = f'{base_meta}:{int(amount)}'
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT 1 FROM funnel_events WHERE event_type = ? AND meta LIKE ? LIMIT 1',
            (EVENT_PAID_OPEN_INVOICE_3M, f'{base_meta}:%'),
        )
        if cur.fetchone():
            return
    log_funnel_event(user_id, EVENT_PAID_OPEN_INVOICE_3M, paid_meta)


def _open_invoice_reminder_stats() -> dict:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT COUNT(*) FROM funnel_events WHERE event_type = ?',
            (EVENT_MSG_OPEN_INVOICE_3M,),
        )
        sent = cur.fetchone()[0] or 0
        cur.execute(
            'SELECT COUNT(*) FROM funnel_events WHERE event_type = ?',
            (EVENT_CLICK_OPEN_INVOICE_CHECK,),
        )
        clicks = cur.fetchone()[0] or 0
        cur.execute(
            'SELECT meta FROM funnel_events WHERE event_type = ?',
            (EVENT_PAID_OPEN_INVOICE_3M,),
        )
        paid_rows = cur.fetchall()
    paid_count = len(paid_rows)
    paid_sum = 0
    for (meta,) in paid_rows:
        if not meta:
            continue
        try:
            paid_sum += int(str(meta).rsplit(':', 1)[-1])
        except (ValueError, IndexError):
            pass
    return {
        'sent': sent,
        'clicks': clicks,
        'paid_count': paid_count,
        'paid_sum': paid_sum,
    }


def _set_survey_answer(user_id: int, answer: str) -> None:
    _ensure_row(user_id)
    with _connect() as con:
        con.execute(
            'UPDATE user_funnel SET survey_answer = ? WHERE user_id = ?',
            (answer, user_id),
        )
        con.commit()
    log_funnel_event(user_id, f'click_survey_{answer}')


def _ensure_row(user_id: int) -> None:
    now = datetime.now().isoformat()
    with _connect() as con:
        cur = con.cursor()
        cur.execute('SELECT 1 FROM user_funnel WHERE user_id = ?', (user_id,))
        if not cur.fetchone():
            cur.execute(
                'INSERT INTO user_funnel (user_id, branch, first_seen_at) VALUES (?, ?, ?)',
                (user_id, 'no_trial', now),
            )
            con.commit()


def funnel_on_first_seen(user_id: int) -> None:
    existed = False
    with _connect() as con:
        cur = con.cursor()
        cur.execute('SELECT 1 FROM user_funnel WHERE user_id = ?', (user_id,))
        existed = cur.fetchone() is not None
    _ensure_row(user_id)
    if not existed:
        log_funnel_event(user_id, 'funnel_entered')
        log_funnel_event(user_id, 'branch_no_trial')


def funnel_on_trial_started(user_id: int) -> None:
    _ensure_row(user_id)
    now = datetime.now().isoformat()
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE user_funnel
            SET branch = 'trial_active',
                trial_started_at = COALESCE(trial_started_at, ?),
                trial_ended_at = NULL
            WHERE user_id = ?
            """,
            (now, user_id),
        )
        con.commit()
    log_funnel_event(user_id, 'trial_started')
    log_funnel_event(user_id, 'branch_trial_active')


def funnel_on_paid(user_id: int) -> None:
    _ensure_row(user_id)
    now = datetime.now().isoformat()
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE user_funnel
            SET branch = 'paid',
                last_paid_at = ?
            WHERE user_id = ?
            """,
            (now, user_id),
        )
        con.commit()
    log_funnel_event(user_id, 'paid')
    log_funnel_event(user_id, 'branch_paid')


def _had_trial(user_id: int) -> bool:
    with _connect() as con:
        cur = con.cursor()
        cur.execute('SELECT had_trial FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        return bool(row and row[0] == 1)


def _subscription_active(user_id: int) -> bool:
    today_str = date.today().isoformat()
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT 1 FROM subscriptions
            WHERE user_id = ? AND date(subscription_expires_at) >= date(?)
            """,
            (user_id, today_str),
        )
        return cur.fetchone() is not None


def _get_subscription_expires(user_id: int) -> datetime | None:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?',
            (user_id,),
        )
        row = cur.fetchone()
    return _parse_dt(row[0]) if row else None


def _mark_flag(user_id: int, column: str) -> None:
    allowed = {
        'nt_30m', 'nt_24h', 'nt_48h', 'nt_72h',
        'pt_1h', 'pt_24h', 'pt_3d', 'pt_7d',
    }
    if column not in allowed:
        return
    with _connect() as con:
        con.execute(f'UPDATE user_funnel SET {column} = 1 WHERE user_id = ?', (user_id,))
        con.commit()
    ev = _FLAG_EVENT.get(column)
    if ev:
        log_funnel_event(user_id, ev)


def _set_branch(user_id: int, branch: str, trial_ended_at: str | None = None) -> None:
    with _connect() as con:
        cur = con.cursor()
        if trial_ended_at is not None:
            cur.execute(
                'UPDATE user_funnel SET branch = ?, trial_ended_at = ? WHERE user_id = ?',
                (branch, trial_ended_at, user_id),
            )
        else:
            cur.execute(
                'UPDATE user_funnel SET branch = ? WHERE user_id = ?',
                (branch, user_id),
            )
        con.commit()
    log_funnel_event(user_id, f'branch_{branch}')


def _fetch_funnel_rows() -> list[tuple]:
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT f.user_id, f.branch, f.first_seen_at, f.trial_started_at, f.trial_ended_at,
                   f.nt_30m, f.nt_24h, f.nt_48h, f.nt_72h,
                   f.pt_1h, f.pt_24h, f.pt_3d, f.pt_7d, f.extra_trial_once
            FROM user_funnel f
            INNER JOIN users u ON u.id = f.user_id
            WHERE COALESCE(u.bot_blocked, 0) = 0
              AND (
                f.branch IN ('trial_active', 'paid')
                OR (
                  f.branch = 'no_trial'
                  AND (f.nt_30m = 0 OR f.nt_24h = 0 OR f.nt_48h = 0 OR f.nt_72h = 0)
                )
                OR (
                  f.branch = 'post_trial'
                  AND (f.pt_1h = 0 OR f.pt_24h = 0 OR f.pt_3d = 0 OR f.pt_7d = 0)
                )
              )
            """
        )
        return cur.fetchall()


def fetch_funnel_stats() -> tuple[str, list[dict], list[dict]]:
    """Сводка HTML, строки пользователей и агрегат событий для Excel."""
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                uf.user_id,
                u.username,
                uf.branch,
                uf.first_seen_at,
                uf.trial_started_at,
                uf.trial_ended_at,
                uf.last_paid_at,
                uf.survey_answer,
                uf.nt_30m, uf.nt_24h, uf.nt_48h, uf.nt_72h,
                uf.pt_1h, uf.pt_24h, uf.pt_3d, uf.pt_7d,
                uf.extra_trial_once,
                (SELECT COUNT(*) FROM funnel_events fe WHERE fe.user_id = uf.user_id) AS events_count
            FROM user_funnel uf
            LEFT JOIN users u ON u.id = uf.user_id
            ORDER BY uf.first_seen_at DESC
            """
        )
        user_rows = cur.fetchall()

        cur.execute(
            """
            SELECT event_type, COUNT(*) AS cnt
            FROM funnel_events
            GROUP BY event_type
            ORDER BY cnt DESC
            """
        )
        event_agg = [{'Событие': r[0], 'Количество': r[1]} for r in cur.fetchall()]

    total = len(user_rows)
    by_branch: dict[str, int] = {}
    trial_started = 0
    paid_count = 0
    paid_after_trial_end = 0
    survey_counts: dict[str, int] = {}
    msg_sent = {k: 0 for k in _FLAG_EVENT.values()}

    users_excel: list[dict] = []
    for r in user_rows:
        (
            uid, username, branch, first_seen, trial_start, trial_end, last_paid,
            survey_answer,
            nt_30m, nt_24h, nt_48h, nt_72h,
            pt_1h, pt_24h, pt_3d, pt_7d,
            extra_once, events_count,
        ) = r
        branch = branch or 'no_trial'
        by_branch[branch] = by_branch.get(branch, 0) + 1
        if trial_start:
            trial_started += 1
        if last_paid:
            paid_count += 1
            if trial_end and last_paid > trial_end:
                paid_after_trial_end += 1
        if survey_answer:
            survey_counts[survey_answer] = survey_counts.get(survey_answer, 0) + 1

        flags = {
            'nt_30m': nt_30m, 'nt_24h': nt_24h, 'nt_48h': nt_48h, 'nt_72h': nt_72h,
            'pt_1h': pt_1h, 'pt_24h': pt_24h, 'pt_3d': pt_3d, 'pt_7d': pt_7d,
        }
        for col, ev in _FLAG_EVENT.items():
            if flags.get(col):
                msg_sent[ev] = msg_sent.get(ev, 0) + 1

        users_excel.append({
            'user_id': uid,
            'username': username or '',
            'branch': branch,
            'first_seen_at': first_seen or '',
            'trial_started_at': trial_start or '',
            'trial_ended_at': trial_end or '',
            'last_paid_at': last_paid or '',
            'survey_answer': survey_answer or '',
            'nt_30m': nt_30m or 0,
            'nt_24h': nt_24h or 0,
            'nt_48h': nt_48h or 0,
            'nt_72h': nt_72h or 0,
            'pt_1h': pt_1h or 0,
            'pt_24h': pt_24h or 0,
            'pt_3d': pt_3d or 0,
            'pt_7d': pt_7d or 0,
            'extra_trial_once': extra_once or 0,
            'events_count': events_count or 0,
        })

    def pct(part: int, whole: int) -> str:
        if not whole:
            return '0%'
        return f'{round(part * 100 / whole, 1)}%'

    survey_lines = '\n'.join(
        f'• {k}: <b>{v}</b>' for k, v in sorted(survey_counts.items())
    ) or '• пока нет'

    msg_lines = '\n'.join(
        f'• {k}: <b>{v}</b>' for k, v in sorted(msg_sent.items()) if v
    ) or '• пока нет'

    branch_lines = '\n'.join(
        f'• {k}: <b>{v}</b>' for k, v in sorted(by_branch.items())
    )

    oir = _open_invoice_reminder_stats()
    oir_sent = oir['sent']
    oir_clicks = oir['clicks']
    oir_paid = oir['paid_count']
    oir_sum = oir['paid_sum']
    open_invoice_block = (
        f'<b>Напоминание об оплате (3 мин, zhirik):</b>\n'
        f'• Отправлено: <b>{oir_sent}</b>\n'
        f'• Нажали «Проверить»: <b>{oir_clicks}</b> ({pct(oir_clicks, oir_sent)})\n'
        f'• Оплатили после напоминания: <b>{oir_paid}</b> ({pct(oir_paid, oir_sent)})\n'
        f'• Сумма оплат с напоминания: <b>{oir_sum} ₽</b>'
    )

    summary = (
        '<b>📊 Статистика воронки</b>\n\n'
        f'Всего в воронке: <b>{total}</b>\n\n'
        f'<b>Ветки:</b>\n{branch_lines}\n\n'
        f'<b>Воронка:</b>\n'
        f'• Взяли триал: <b>{trial_started}</b> ({pct(trial_started, total)})\n'
        f'• Оплатили (last_paid_at): <b>{paid_count}</b> ({pct(paid_count, total)})\n'
        f'• Оплата после конца триала: <b>{paid_after_trial_end}</b>\n'
        f'• Бонус +1 день: <b>{sum(1 for u in users_excel if u["extra_trial_once"])}</b>\n\n'
        f'{open_invoice_block}\n\n'
        f'<b>Ответы опроса:</b>\n{survey_lines}\n\n'
        f'<b>Отправлено писем (флаги):</b>\n{msg_lines}\n\n'
        '<i>Подробности — в файле (листы «Покупка», «События», «Продление»).</i>'
    )
    return summary, users_excel, event_agg


# --- Клавиатуры ---

def ikb_funnel_trial() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text='🎁 Активировать 3 дня бесплатно',
            callback_data='funnel_trial',
            style='success',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_funnel_buy() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text='Подключить VPN',
            callback_data='buy_vpn',
            icon_custom_emoji_id=get_emoji('plus'),
            style='success',
        )],
        [InlineKeyboardButton(
            text='🎁 Попробовать бесплатно',
            callback_data='funnel_trial',
            style='success',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_funnel_post_1h() -> InlineKeyboardMarkup:
    y360 = SUBSCRIPTION_PLAN.get(360, 899)
    y7 = WEEK_PLAN_PRICE
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f'12 месяцев · {y360}₽',
            callback_data=f'deposit_{y360}_360_card',
            style='success',
        )],
        [InlineKeyboardButton(
            text=f'1 месяц · {SUBSCRIPTION_PLAN.get(30, 149)}₽',
            callback_data=f'deposit_{SUBSCRIPTION_PLAN.get(30, 149)}_30_card',
        )],
        [InlineKeyboardButton(
            text=f'Тариф на неделю · {y7}₽',
            callback_data=f'deposit_{y7}_{WEEK_PLAN_DAYS}_card',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_funnel_survey() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Слишком дорого', callback_data='funnel_survey_expensive')],
        [InlineKeyboardButton(text='Не успел потестить', callback_data='funnel_survey_no_time')],
        [InlineKeyboardButton(text='Не понял, как пользоваться', callback_data='funnel_survey_confused')],
        [InlineKeyboardButton(text='Подключить VPN', callback_data='buy_vpn', style='success')],
    ])


def ikb_funnel_micro_week() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f'Неделя за {WEEK_PLAN_PRICE}₽',
            callback_data=f'deposit_{WEEK_PLAN_PRICE}_{WEEK_PLAN_DAYS}_card',
            style='success',
        )],
        [InlineKeyboardButton(text='Все тарифы', callback_data='buy_vpn')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])


def ikb_funnel_extend_trial() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text='🔥 Активировать продление',
            callback_data='funnel_extend_trial',
            style='success',
        )],
        [InlineKeyboardButton(text='Моя подписка', callback_data='my_subscription')],
    ])


# --- Тексты ---

MSG_NT_30M = (
    'Ты заглянул в бота, но так и не активировал бесплатный доступ на <b>3 дня</b>.\n\n'
    'Все функции уже готовы к работе, подписка ни к чему тебя не обязывает — '
    '<b>карта не нужна</b>.'
)

MSG_NT_24H = (
    '⚡ <b>Твой VPN уже готов</b>\n\n'
    'После одной кнопки:\n'
    '• Экономия времени — сервер выбирается автоматически\n'
    '• Работает в РФ\n'
    '• Подключение за 1 минуту\n\n'
    'Попробуй сам — это бесплатно 👇'
)

MSG_NT_48H = (
    'Вижу, ты всё ещё сомневаешься. Коротко — ответы на главные вопросы:\n\n'
    '1️⃣ <b>Это точно бесплатно?</b> Да, на 3 дня.\n'
    '2️⃣ <b>Нужна карта?</b> Нет. Доступ просто закроется, если не захочешь продлевать.\n'
    '3️⃣ <b>Не разберусь?</b> Всё в 2 клика. Поддержка: '
    f'<a href="{SUPPORT_URL}">написать</a>.\n\n'
    'Активируй триал, пока он доступен 👇'
)

MSG_NT_72H = (
    'Твой приветственный триал скоро станет недоступен для активации.\n\n'
    'Успей протестировать бесплатно — без карты и скрытых подписок 👇'
)

MSG_PT_1H = (
    'Твой бесплатный доступ завершён 🔒\n\n'
    'Чтобы снова пользоваться VPN, выбери подписку:\n'
    f'• 1 месяц — <b>{SUBSCRIPTION_PLAN.get(30, 149)} ₽</b>\n'
    f'• 3 месяца — <b>{SUBSCRIPTION_PLAN.get(90, 399)} ₽</b>\n'
    f'• <b>1 год — {SUBSCRIPTION_PLAN.get(360, 899)} ₽</b> (≈75 ₽/мес)\n'
    f'• Неделя — <b>{WEEK_PLAN_PRICE} ₽</b>\n\n'
    'Подключи сейчас 👇'
)

MSG_PT_24H = (
    'Нам важно стать лучше.\n'
    'Подскажи, почему не продлил подписку? Выбери вариант 👇'
)

MSG_PT_3D = (
    'Тяжело найти стабильный VPN? Пока подписка отключена, снова тратишь нервы на блокировки.\n\n'
    'Месяц стоит как две чашки кофе — <b>бот работает на тебя 30 дней</b>.\n'
    f'Год — <b>{SUBSCRIPTION_PLAN.get(360, 899)} ₽</b>.\n\n'
    'Верни себе комфорт 👇'
)

MSG_PT_7D = (
    '📦 Профиль без активной подписки.\n\n'
    'Если хочешь продолжить — подключи VPN. '
    f'Есть тариф на неделю всего за <b>{WEEK_PLAN_PRICE} ₽</b> или год за '
    f'<b>{SUBSCRIPTION_PLAN.get(360, 899)} ₽</b> 👇'
)

MSG_SURVEY_EXPENSIVE = (
    'Понимаем: стабильные серверы стоят денег, но сервис должен быть доступным.\n\n'
    f'<b>Тариф на неделю — {WEEK_PLAN_PRICE} ₽</b>. '
    'Без автосписаний: неделя полной скорости, чтобы оценить наш сервис в деле.'
)

MSG_SURVEY_NO_TIME = (
    'Знакомая история: запустил бота, отвлекся — и время ушло.\n\n'
    'Мы можем один раз продлить тест. Нажми кнопку ниже — '
    '<b>+1 день</b> доступа, чтобы успеть проверить.'
)

MSG_SURVEY_CONFUSED = (
    'Всё проще, чем кажется:\n\n'
    '1. Открой ссылку из «Моя подписка»\n'
    '2. Выбери приложение — подписка подставится сама\n'
    '3. Нажми «Подключиться»\n\n'
    'Один раз можем дать <b>+1 день</b> на тест. '
    f'Если не получится — <a href="{SUPPORT_URL}">поддержка</a> поможет за пару минут.'
)


async def _safe_send(bot: Bot, user_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.send_message(user_id, text, parse_mode='HTML', reply_markup=reply_markup)
        logger.info('funnel sent to user_id=%s', user_id)
        return True
    except Exception as e:
        if is_telegram_unreachable(e):
            mark_user_bot_blocked(user_id)
            logger.info('funnel skip user_id=%s (blocked bot or deleted account)', user_id)
        else:
            logger.warning('funnel send failed user_id=%s: %s', user_id, e)
        return False


async def _maybe_start_post_trial(user_id: int) -> bool:
    """Перевод trial_active/paid без активной подписки → post_trial."""
    if _subscription_active(user_id):
        return False
    exp = _get_subscription_expires(user_id)
    ended_at = (exp or datetime.now()).isoformat()
    _set_branch(user_id, 'post_trial', trial_ended_at=ended_at)
    return True


async def _process_one_user(bot: Bot, row: tuple) -> None:
    (
        user_id, branch, first_seen_at, trial_started_at, trial_ended_at,
        nt_30m, nt_24h, nt_48h, nt_72h,
        pt_1h, pt_24h, pt_3d, pt_7d, extra_trial_once,
    ) = row
    now = datetime.now()

    if branch == 'no_trial':
        if _had_trial(user_id):
            funnel_on_trial_started(user_id)
            return
        fs = _parse_dt(first_seen_at)
        if not fs:
            return
        # По одному сообщению за проход (иначе при опоздании воркера уйдёт пачка).
        if not nt_30m and now >= fs + TD_30M:
            if await _safe_send(bot, user_id, MSG_NT_30M, ikb_funnel_trial()):
                _mark_flag(user_id, 'nt_30m')
        elif not nt_24h and now >= fs + TD_24H:
            if await _safe_send(bot, user_id, MSG_NT_24H, ikb_funnel_trial()):
                _mark_flag(user_id, 'nt_24h')
        elif not nt_48h and now >= fs + TD_48H:
            if await _safe_send(bot, user_id, MSG_NT_48H, ikb_funnel_trial()):
                _mark_flag(user_id, 'nt_48h')
        elif not nt_72h and now >= fs + TD_72H:
            if await _safe_send(bot, user_id, MSG_NT_72H, ikb_funnel_trial()):
                _mark_flag(user_id, 'nt_72h')
        return

    if branch == 'trial_active':
        if _subscription_active(user_id):
            return
        if _had_trial(user_id):
            await _maybe_start_post_trial(user_id)
        return

    if branch == 'paid':
        if _subscription_active(user_id):
            return
        await _maybe_start_post_trial(user_id)
        return

    if branch == 'post_trial':
        if _subscription_active(user_id):
            _set_branch(user_id, 'paid')
            return
        te = _parse_dt(trial_ended_at) or _parse_dt(trial_started_at) or now
        if not pt_1h and now >= te + TD_1H:
            if await _safe_send(bot, user_id, MSG_PT_1H, ikb_funnel_post_1h()):
                _mark_flag(user_id, 'pt_1h')
        elif not pt_24h and now >= te + TD_24H:
            if await _safe_send(bot, user_id, MSG_PT_24H, ikb_funnel_survey()):
                _mark_flag(user_id, 'pt_24h')
        elif not pt_3d and now >= te + TD_3D:
            if await _safe_send(bot, user_id, MSG_PT_3D, ikb_funnel_buy()):
                _mark_flag(user_id, 'pt_3d')
        elif not pt_7d and now >= te + TD_7D:
            if await _safe_send(bot, user_id, MSG_PT_7D, ikb_funnel_buy()):
                _mark_flag(user_id, 'pt_7d')


def reset_funnel_for_test(user_id: int) -> str:
    """Сброс воронки для теста: ветка no_trial, таймеры с нуля."""
    now = datetime.now().isoformat()
    _ensure_row(user_id)
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE user_funnel SET
                branch = 'no_trial',
                first_seen_at = ?,
                trial_started_at = NULL,
                trial_ended_at = NULL,
                last_paid_at = NULL,
                nt_30m = 0, nt_24h = 0, nt_48h = 0, nt_72h = 0,
                pt_1h = 0, pt_24h = 0, pt_3d = 0, pt_7d = 0,
                extra_trial_once = 0,
                survey_answer = NULL
            WHERE user_id = ?
            """,
            (now, user_id),
        )
        cur.execute('UPDATE users SET had_trial = 0 WHERE id = ?', (user_id,))
        con.commit()
    return (
        f'OK: воронка сброшена для {user_id}\n'
        f'branch=no_trial, first_seen_at={now}\n'
        f'Первое сообщение (~{TD_30M}) после следующего тика воркера (каждые {FUNNEL_SLEEP_SEC} с).'
    )


async def run_funnel_worker(bot: Bot) -> None:
    logger.info('funnel worker started, sleep=%ss', FUNNEL_SLEEP_SEC)
    await asyncio.sleep(5)
    while True:
        try:
            rows = _fetch_funnel_rows()
            logger.info('funnel tick, users=%s', len(rows))
            for row in rows:
                await _process_one_user(bot, row)
                await asyncio.sleep(0.05)
        except Exception as e:
            logger.exception('funnel worker error: %s', e)
        await asyncio.sleep(FUNNEL_SLEEP_SEC)


async def grant_extra_trial_day(vpn, user_id: int) -> tuple[bool, str]:
    """+1 день VPN (один раз на пользователя)."""
    _ensure_row(user_id)
    with _connect() as con:
        cur = con.cursor()
        cur.execute('SELECT extra_trial_once FROM user_funnel WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        if row and row[0]:
            return False, 'Продление уже использовано.'
    log_funnel_event(user_id, 'click_extend_trial')
    try:
        panel_user = await asyncio.to_thread(vpn.get_user_by_tg_id, user_id)
        has_panel_user = (
            isinstance(panel_user, dict)
            and panel_user.get('response')
            and len(panel_user['response']) > 0
        )
        if has_panel_user:
            body = await asyncio.to_thread(vpn.renew_subscription, user_id, 1)
            if isinstance(body, dict) and body.get('errorCode'):
                raise RuntimeError(body.get('message', 'renew failed'))
        else:
            trial_body = await asyncio.to_thread(vpn.deliver_trial_vpn, user_id)
            if isinstance(trial_body, dict) and trial_body.get('errorCode'):
                raise RuntimeError(trial_body.get('message', 'trial create failed'))
            upsert_subscription_days(user_id, 1)
            with _connect() as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (user_id,))
                cur.execute(
                    """
                    UPDATE user_funnel
                    SET extra_trial_once = 1, branch = 'trial_active', trial_ended_at = NULL
                    WHERE user_id = ?
                    """,
                    (user_id,),
                )
                con.commit()
            funnel_on_trial_started(user_id)
            log_funnel_event(user_id, 'extend_trial_ok')
            return True, '✅ Добавили +1 день доступа. Открой «Моя подписка».'
        upsert_subscription_days(user_id, 1)
    except Exception as e:
        logger.exception('grant_extra_trial_day %s: %s', user_id, e)
        log_funnel_event(user_id, 'extend_trial_fail', str(e)[:200])
        return False, 'Не удалось продлить доступ. Напишите в поддержку.'
    with _connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            UPDATE user_funnel
            SET extra_trial_once = 1, branch = 'trial_active', trial_ended_at = NULL
            WHERE user_id = ?
            """,
            (user_id,),
        )
        con.commit()
    log_funnel_event(user_id, 'extend_trial_ok')
    return True, '✅ Добавили +1 день доступа. Открой «Моя подписка».'


def setup_funnel(dp, bot, vpn, *, trial_flow_cb):
    """
    trial_flow_cb: async (CallbackQuery) -> None — выдача триала (канал + ключ).
    """

    @dp.callback_query(F.data == 'funnel_trial')
    async def funnel_trial_callback(callback: CallbackQuery):
        await callback.answer()
        log_funnel_event(callback.from_user.id, 'click_funnel_trial')
        try:
            await callback.message.delete()
        except Exception:
            pass
        await trial_flow_cb(callback)

    @dp.callback_query(F.data == 'funnel_extend_trial')
    async def funnel_extend_trial_callback(callback: CallbackQuery):
        await callback.answer()
        ok, text = await grant_extra_trial_day(vpn, callback.from_user.id)
        await callback.message.answer(text, parse_mode='HTML', reply_markup=ikb_back_funnel())

    @dp.callback_query(F.data == 'funnel_survey_expensive')
    async def funnel_survey_expensive(callback: CallbackQuery):
        _set_survey_answer(callback.from_user.id, 'expensive')
        await callback.message.delete()
        await callback.message.answer(
            MSG_SURVEY_EXPENSIVE,
            parse_mode='HTML',
            reply_markup=ikb_funnel_micro_week(),
        )

    @dp.callback_query(F.data == 'funnel_survey_no_time')
    async def funnel_survey_no_time(callback: CallbackQuery):
        _set_survey_answer(callback.from_user.id, 'no_time')
        await callback.message.delete()
        await callback.message.answer(
            MSG_SURVEY_NO_TIME,
            parse_mode='HTML',
            reply_markup=ikb_funnel_extend_trial(),
        )

    @dp.callback_query(F.data == 'funnel_survey_confused')
    async def funnel_survey_confused(callback: CallbackQuery):
        _set_survey_answer(callback.from_user.id, 'confused')
        await callback.message.delete()
        with _connect() as con:
            cur = con.cursor()
            cur.execute('SELECT extra_trial_once FROM user_funnel WHERE user_id = ?', (callback.from_user.id,))
            row = cur.fetchone()
            used = bool(row and row[0])
        markup = ikb_funnel_buy() if used else ikb_funnel_extend_trial()
        await callback.message.answer(MSG_SURVEY_CONFUSED, parse_mode='HTML', reply_markup=markup)


def ikb_back_funnel():
    from ikbs import ikb_back
    return ikb_back
