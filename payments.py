import asyncio
import os
import logging
import sqlite3 as sq
from datetime import date, datetime

import dotenv
import requests
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
from yookassa import Payment, Configuration

logger = logging.getLogger(__name__)

dotenv.load_dotenv()
Configuration.account_id = os.getenv('YOOKASSA_ACCOUNT_ID')
Configuration.secret_key = os.getenv('YOOKASSA_SECRET_KEY')

def get_rate():
    r = requests.get('https://v6.exchangerate-api.com/v6/d8e4beb763d54112c6a63999/latest/USD')
    return r.json()['conversion_rates']['RUB']

rub_to_usdt = get_rate()
API_TOKEN = os.getenv('CRYPTO_BOT_API_TOKEN')

def get_pay_link(amount):
    headers = {"Crypto-Pay-API-Token": API_TOKEN}
    data = {"asset": "USDT", "amount": amount}
    response = requests.post('https://pay.crypt.bot/api/createInvoice', headers=headers, json=data)
    return response.json()

def check_payment_status(invoice_id):
    headers = {
        "Crypto-Pay-API-Token": API_TOKEN,
        "Content-Type": "application/json"
    }
    response = requests.post(
        'https://pay.crypt.bot/api/getInvoices',
        headers=headers,
        json={"invoice_ids": [invoice_id]}
    )
    response = response.json()
    if response.get('ok') and response.get('result', {}).get('items'):
        inv = response['result']['items'][0]
        if inv['invoice_id'] == invoice_id:
            return inv['status'], float(inv['amount']) * rub_to_usdt
    return None, None

def check_payment_yookassa_status(amount, payment_id, user_id, tx_type='yookassa'):
    """
    Возвращает:
        'paid'             — оплачено, транзакция записана
        'already_processed'— уже обрабатывали этот payment_id
        'not_paid'         — ещё не оплачено
        'timeout'          — YooKassa не ответила (попробовать позже)
        'error'            — другая ошибка
    """
    try:
        payment = Payment.find_one(str(payment_id).strip())
        print(payment.json())
    except ReadTimeout:
        logger.warning(
            f"YooKassa timeout | payment_id={payment_id} | "
            f"user_id={user_id} | amount={amount}₽"
        )
        return 'timeout'
    except RequestsConnectionError as e:
        logger.error(f"YooKassa connection error | payment_id={payment_id}: {e}")
        return 'timeout'
    except AttributeError as e:
        # Баг в yookassa SDK: e.response is None при таймауте
        logger.error(
            f"YooKassa SDK bug (NoneType) | payment_id={payment_id} | "
            f"user_id={user_id} | amount={amount}₽ | {e}"
        )
        return 'timeout'
    except Exception as e:
        logger.exception(f"YooKassa неизвестная ошибка | payment_id={payment_id}: {e}")
        return 'error'

    if payment.status == 'succeeded':
        try:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                tx_type = (tx_type or 'yookassa').strip()
                cur.execute(
                    'SELECT 1 FROM transactions WHERE type = ? AND external_payment_id = ?',
                    (tx_type, str(payment_id).strip()),
                )
                if cur.fetchone():
                    return 'already_processed'
                cur.execute(
                    'INSERT INTO transactions (user_id, amount, type, date, external_payment_id) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (user_id, amount, tx_type,
                     datetime.now().isoformat(), str(payment_id).strip())
                )
                con.commit()
        except Exception as e:
            logger.exception(f"Ошибка записи транзакции | payment_id={payment_id}: {e}")
            return 'error'
        mark_open_invoice_reminder_paid(str(payment_id).strip())
        return 'paid'

    return 'not_paid'


OPEN_INVOICE_REMINDER_DELAY_SEC = 180
ZHIRIK_VIDEO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zhirik.MP4')

OPEN_INVOICE_REMINDER_TEXT = (
    '<tg-emoji emoji-id="5253742260054409879">✉️</tg-emoji> У тебя есть открытый счет на оплату!\n\n'
    '<tg-emoji emoji-id="5415841262177626085">😳</tg-emoji> Сейчас трудные времена и очень легко '
    'забыть о важных вещах! Понимаем, поэтому хотим облегчить тебе жизнь и убрать пару головняков '
    'из твой жизни.\n\n'
    'Подключайся быстрее к подписке и можешь забыть о проблемах с интернетом! '
    '<tg-emoji emoji-id="5458782963678322179">🥳</tg-emoji>'
)


def _fetch_yookassa_confirmation_url(payment_id: str) -> str | None:
    try:
        payment = Payment.find_one(str(payment_id).strip())
        conf = getattr(payment, 'confirmation', None)
        url = getattr(conf, 'confirmation_url', None) if conf else None
        return url.strip() if url else None
    except Exception as e:
        logger.warning('fetch confirmation_url %s: %s', payment_id, e)
        return None


OINV_CALLBACK_PREFIX = 'oinv_'


def _oinv_callback_data(payment_id: str) -> str:
    return f'{OINV_CALLBACK_PREFIX}{str(payment_id).strip()}'


def build_open_invoice_reminder_keyboard(payment_id: str):
    """Callback «Оплатить» — клик учитывается, затем кнопка меняется на ссылку YooKassa."""
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    pid = str(payment_id).strip()
    if not pid:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Оплатить', callback_data=_oinv_callback_data(pid), style='success')],
    ])


def build_open_invoice_pay_url_keyboard(confirmation_url: str):
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    url = (confirmation_url or '').strip()
    if not url:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Оплатить', url=url, style='success')],
    ])


def record_open_invoice_reminder_sent(
    user_id: int,
    payment_id: str,
    tx_type: str,
    confirmation_url: str | None,
    amount: int | None = None,
) -> None:
    from funnel import log_funnel_event

    pid = str(payment_id).strip()
    now = datetime.now().isoformat()
    with sq.connect('database.db') as con:
        con.execute(
            """
            INSERT OR REPLACE INTO open_invoice_reminders (
                payment_id, user_id, tx_type, amount, confirmation_url,
                reminder_sent_at, pay_clicked_at, paid_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (pid, user_id, (tx_type or 'yookassa').strip(), amount, confirmation_url, now),
        )
        con.commit()
    log_funnel_event(user_id, 'open_invoice_reminder_sent', pid)


def record_open_invoice_reminder_click(payment_id: str, user_id: int) -> str | None:
    """Фиксирует клик «Оплатить». Возвращает confirmation_url."""
    from funnel import log_funnel_event

    pid = str(payment_id).strip()
    now = datetime.now().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT user_id, confirmation_url, pay_clicked_at FROM open_invoice_reminders '
            'WHERE payment_id = ?',
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            return None
        owner_id, url, clicked = row[0], row[1], row[2]
        if owner_id != user_id:
            return None
        if not clicked:
            cur.execute(
                'UPDATE open_invoice_reminders SET pay_clicked_at = ? WHERE payment_id = ?',
                (now, pid),
            )
            con.commit()
            log_funnel_event(user_id, 'open_invoice_reminder_pay_click', pid)
        return url


def mark_open_invoice_reminder_paid(payment_id: str) -> None:
    from funnel import log_funnel_event

    pid = str(payment_id).strip()
    now = datetime.now().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT user_id FROM open_invoice_reminders WHERE payment_id = ? AND paid_at IS NULL',
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            return
        cur.execute(
            'UPDATE open_invoice_reminders SET paid_at = ? WHERE payment_id = ?',
            (now, pid),
        )
        con.commit()
        log_funnel_event(row[0], 'open_invoice_reminder_paid', pid)


def fetch_open_invoice_reminder_stats() -> tuple[str, list[dict]]:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT COUNT(*) FROM open_invoice_reminders')
        sent = int((cur.fetchone() or (0,))[0])
        cur.execute(
            'SELECT COUNT(*) FROM open_invoice_reminders WHERE pay_clicked_at IS NOT NULL',
        )
        clicked = int((cur.fetchone() or (0,))[0])
        cur.execute(
            'SELECT COUNT(*) FROM open_invoice_reminders WHERE paid_at IS NOT NULL',
        )
        paid = int((cur.fetchone() or (0,))[0])
        cur.execute(
            """
            SELECT COUNT(*) FROM open_invoice_reminders
            WHERE paid_at IS NOT NULL AND pay_clicked_at IS NOT NULL
            """,
        )
        paid_after_click = int((cur.fetchone() or (0,))[0])
        cur.execute(
            """
            SELECT
                r.payment_id, r.user_id, u.username, r.tx_type, r.amount,
                r.reminder_sent_at, r.pay_clicked_at, r.paid_at
            FROM open_invoice_reminders r
            LEFT JOIN users u ON u.id = r.user_id
            ORDER BY r.reminder_sent_at DESC
            LIMIT 500
            """,
        )
        rows = [
            {
                'payment_id': r[0],
                'user_id': r[1],
                'username': r[2] or '',
                'tx_type': r[3],
                'amount': r[4],
                'reminder_sent_at': r[5],
                'pay_clicked_at': r[6] or '',
                'paid_at': r[7] or '',
            }
            for r in cur.fetchall()
        ]

    def pct(part: int, whole: int) -> str:
        if not whole:
            return '0%'
        return f'{round(part * 100 / whole, 1)}%'

    summary = (
        '<b>📬 Напоминание об открытом счёте (zhirik)</b>\n'
        f'Отправлено: <b>{sent}</b>\n'
        f'Клик «Оплатить»: <b>{clicked}</b> ({pct(clicked, sent)} от отправок)\n'
        f'Оплатили: <b>{paid}</b> ({pct(paid, sent)} от отправок)\n'
        f'Оплата после клика: <b>{paid_after_click}</b> ({pct(paid_after_click, clicked)} от кликов)\n'
    )
    return summary, rows


def is_yookassa_payment_settled(payment_id: str, tx_type: str = 'yookassa') -> bool:
    """Оплачен в YooKassa или уже записан в transactions."""
    pid = str(payment_id).strip()
    tx_type = (tx_type or 'yookassa').strip()
    try:
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute(
                'SELECT 1 FROM transactions WHERE type = ? AND external_payment_id = ?',
                (tx_type, pid),
            )
            if cur.fetchone():
                return True
    except Exception as e:
        logger.warning('is_yookassa_payment_settled db %s: %s', pid, e)

    try:
        payment = Payment.find_one(pid)
        return payment.status == 'succeeded'
    except Exception as e:
        logger.warning('is_yookassa_payment_settled api %s: %s', pid, e)
        return False


async def _send_open_invoice_reminder(
    bot,
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
    confirmation_url: str | None = None,
    amount: int | None = None,
) -> None:
    from aiogram.types import FSInputFile

    pay_url = (confirmation_url or '').strip() or _fetch_yookassa_confirmation_url(payment_id)
    reply_markup = build_open_invoice_reminder_keyboard(payment_id)

    if not os.path.isfile(ZHIRIK_VIDEO_PATH):
        logger.error('open invoice reminder: video not found: %s', ZHIRIK_VIDEO_PATH)
        await bot.send_message(
            user_id,
            OPEN_INVOICE_REMINDER_TEXT,
            parse_mode='HTML',
            reply_markup=reply_markup,
        )
    else:
        await bot.send_video(
            user_id,
            FSInputFile(ZHIRIK_VIDEO_PATH),
            caption=OPEN_INVOICE_REMINDER_TEXT,
            parse_mode='HTML',
            reply_markup=reply_markup,
        )
    record_open_invoice_reminder_sent(user_id, payment_id, tx_type, pay_url, amount)


async def _open_invoice_reminder_worker(
    bot,
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
    confirmation_url: str | None = None,
    amount: int | None = None,
) -> None:
    await asyncio.sleep(OPEN_INVOICE_REMINDER_DELAY_SEC)
    if is_yookassa_payment_settled(payment_id, tx_type):
        return
    try:
        await _send_open_invoice_reminder(
            bot, user_id, payment_id, tx_type, confirmation_url, amount,
        )
        logger.info(
            'open invoice reminder sent user_id=%s payment_id=%s',
            user_id, payment_id,
        )
    except Exception as e:
        logger.warning(
            'open invoice reminder failed user_id=%s payment_id=%s: %s',
            user_id, payment_id, e,
        )


def schedule_open_invoice_payment_reminder(
    bot,
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
    confirmation_url: str | None = None,
    amount: int | None = None,
) -> None:
    """Через 3 минуты — видео + текст + «Оплатить», если счёт ещё не оплачен."""
    asyncio.create_task(
        _open_invoice_reminder_worker(
            bot,
            user_id,
            str(payment_id).strip(),
            tx_type,
            confirmation_url,
            amount,
        ),
    )


def setup_open_invoice_reminder_handlers(dp) -> None:
    from aiogram import F
    from aiogram.types import CallbackQuery

    @dp.callback_query(F.data.startswith(OINV_CALLBACK_PREFIX))
    async def open_invoice_pay_callback(callback: CallbackQuery):
        payment_id = callback.data[len(OINV_CALLBACK_PREFIX):]
        url = record_open_invoice_reminder_click(payment_id, callback.from_user.id)
        if not url:
            url = _fetch_yookassa_confirmation_url(payment_id)
        if not url:
            await callback.answer('Ссылка на оплату недоступна. Создайте счёт заново.', show_alert=True)
            return
        await callback.answer()
        markup = build_open_invoice_pay_url_keyboard(url)
        if markup:
            try:
                await callback.message.edit_reply_markup(reply_markup=markup)
            except Exception:
                await callback.message.answer(
                    '👉 Перейдите к оплате:',
                    reply_markup=markup,
                )
        else:
            await callback.message.answer(f'<a href="{url}">Оплатить</a>', parse_mode='HTML')