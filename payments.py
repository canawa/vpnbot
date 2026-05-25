import requests
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
from yookassa import Payment, Configuration
from datetime import date, datetime
import dotenv
import os
import sqlite3 as sq
import logging

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
        return 'paid'

    return 'not_paid'