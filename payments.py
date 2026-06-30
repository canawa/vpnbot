import asyncio
import os
import logging
import sqlite3 as sq
import time
from datetime import date, datetime

import dotenv
import requests
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
from yookassa import Payment, Configuration
from funnel import log_open_invoice_reminder_sent

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

PAYMENT_CHECK_COOLDOWN_SEC = float(os.getenv('PAYMENT_CHECK_COOLDOWN_SEC', '8'))
_payment_check_last: dict[tuple[int, str], float] = {}
_payment_check_lock = asyncio.Lock()


async def answer_if_payment_check_rate_limited(callback, payment_id: str) -> bool:
    """
    Защита от спама кнопки «Я оплатил» / «Проверить».
    Возвращает True, если нажатие нужно проигнорировать.
    """
    pid = str(payment_id).strip()
    if not pid:
        return False
    user_id = callback.from_user.id
    now = time.monotonic()
    wait_secs: int | None = None
    async with _payment_check_lock:
        key = (user_id, pid)
        last = _payment_check_last.get(key)
        if last is not None:
            wait = PAYMENT_CHECK_COOLDOWN_SEC - (now - last)
            if wait > 0:
                wait_secs = max(1, int(wait + 0.99))
            else:
                _payment_check_last[key] = now
        else:
            _payment_check_last[key] = now
        if wait_secs is None and len(_payment_check_last) > 5000:
            cutoff = now - PAYMENT_CHECK_COOLDOWN_SEC * 2
            for k, t in list(_payment_check_last.items()):
                if t < cutoff:
                    del _payment_check_last[k]
    if wait_secs is not None:
        await callback.answer(
            f'Подождите {wait_secs} сек. перед повторной проверкой.',
            show_alert=True,
        )
        return True
    return False


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
        return 'paid'

    return 'not_paid'


OPEN_INVOICE_REMINDER_DELAY_SEC = 180
ZHIRIK_VIDEO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zhirik.MP4')

OPEN_INVOICE_REMINDER_TEXT = (
    '<tg-emoji emoji-id="5253742260054409879">✉️</tg-emoji> У тебя есть открытый счет на оплату!\n\n'
    '<tg-emoji emoji-id="5415841262177626085">😳</tg-emoji> Сейчас трудные времена и очень легко '
    'забыть о важных вещах! Понимаем, поэтому хотим облегчить тебе жизнь и убрать пару головняков\n\n'
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


def build_check_payment_callback_data(
    payment_id: str,
    tx_type: str,
    amount: int,
    extra: int,
) -> str:
    """extra: paid_days для подписки, gb_amount для ГБ."""
    pid = str(payment_id).strip()
    if (tx_type or '').strip() == 'yookassa_gb':
        return f'gb_yookassa_{pid}_{int(extra)}_{int(amount)}'
    return f'yookassa_{int(amount)}_{int(extra)}_{pid}'


def build_open_invoice_reminder_keyboard(
    confirmation_url: str | None,
    check_callback_data: str | None = None,
):
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    rows = []
    url = (confirmation_url or '').strip()
    if url:
        rows.append([InlineKeyboardButton(text='Оплатить', url=url, style='success')])
    check_cb = (check_callback_data or '').strip()
    if check_cb:
        rows.append([
            InlineKeyboardButton(text='Проверить', callback_data=check_cb),
        ])
    if not rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    check_callback_data: str | None = None,
) -> None:
    from aiogram.types import FSInputFile

    pay_url = (confirmation_url or '').strip() or _fetch_yookassa_confirmation_url(payment_id)
    reply_markup = build_open_invoice_reminder_keyboard(pay_url, check_callback_data)

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
    log_open_invoice_reminder_sent(user_id, payment_id, tx_type)


async def _open_invoice_reminder_worker(
    bot,
    user_id: int,
    payment_id: str,
    tx_type: str = 'yookassa',
    confirmation_url: str | None = None,
    check_callback_data: str | None = None,
) -> None:
    from bot_delivery import is_telegram_unreachable, is_user_bot_blocked, mark_user_bot_blocked

    await asyncio.sleep(OPEN_INVOICE_REMINDER_DELAY_SEC)
    if is_user_bot_blocked(user_id):
        return
    if is_yookassa_payment_settled(payment_id, tx_type):
        return
    try:
        await _send_open_invoice_reminder(
            bot,
            user_id,
            payment_id,
            tx_type,
            confirmation_url,
            check_callback_data,
        )
        logger.info(
            'open invoice reminder sent user_id=%s payment_id=%s',
            user_id, payment_id,
        )
    except Exception as e:
        if is_telegram_unreachable(e):
            mark_user_bot_blocked(user_id)
            logger.info(
                'open invoice reminder skip user_id=%s (blocked bot or deleted account)',
                user_id,
            )
        else:
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
    check_extra: int | None = None,
) -> None:
    """Через 3 минуты — видео + «Оплатить» + «Проверить», если счёт ещё не оплачен."""
    check_cb = None
    if amount is not None and check_extra is not None:
        check_cb = build_check_payment_callback_data(
            payment_id, tx_type, amount, check_extra,
        )
    asyncio.create_task(
        _open_invoice_reminder_worker(
            bot,
            user_id,
            str(payment_id).strip(),
            tx_type,
            confirmation_url,
            check_cb,
        ),
    )