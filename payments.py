import requests
from yookassa import Payment, Configuration
from datetime import date, datetime
import dotenv
import os
import sqlite3 as sq
dotenv.load_dotenv() # загружаем переменные окружения
Configuration.account_id = os.getenv('YOOKASSA_ACCOUNT_ID')
Configuration.secret_key = os.getenv('YOOKASSA_SECRET_KEY')

def get_rate():
    r = requests.get('https://v6.exchangerate-api.com/v6/d8e4beb763d54112c6a63999/latest/USD')
    return r.json()['conversion_rates']['RUB']

rub_to_usdt = get_rate()

API_TOKEN = os.getenv('CRYPTO_BOT_API_TOKEN')  # это криптобот

# PAY FUNCTIONS
def get_pay_link(amount):
    headers = {"Crypto-Pay-API-Token": API_TOKEN}
    data = {"asset": "USDT", "amount": amount}
    response = requests.post('https://pay.crypt.bot/api/createInvoice', headers=headers, json=data)
    response = response.json()
    return response


def check_payment_status(invoice_id):
    headers = {"Crypto-Pay-API-Token": API_TOKEN,
               "Content-Type": "application/json"
               }
    # Получаем только нужный инвойс по ID, а не все инвойсы
    response = requests.post('https://pay.crypt.bot/api/getInvoices', headers=headers,
                             json={"invoice_ids": [invoice_id]})
    response = response.json()
    if response.get('ok') and response.get('result', {}).get('items'):
        inv = response['result']['items'][0]
        if inv['invoice_id'] == invoice_id:
            return inv['status'], float(inv['amount']) * rub_to_usdt  # возвращаем статус оплаты и сумму в рублях

    return None, None

def check_payment_yookassa_status(amount, payment_id, user_id): # функция для проверки статуса оплаты через Юкассу
    payment = Payment.find_one(str(payment_id).strip())
    if payment.status == 'succeeded':
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (user_id, amount, 'yookassa', datetime.now().isoformat() ))
            con.commit()
        return True
    else:
        return False