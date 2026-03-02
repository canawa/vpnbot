from datetime import date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, invoice, LabeledPrice, FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
import asyncio # для работы с асинхронными функциями
import sqlite3 as sq
import requests
import dotenv
import os
from yookassa import Configuration, Payment # для работы с Юкассой
import uuid
from vpn import generate_vpn_key, get_marzban_token
import pandas as pd
import openpyxl
from datetime import datetime
from check_subscription import is_subscribed
from translations import get_lang, set_lang, t, TEXTS
import locale 
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
print('BOT STARTED!!!')


### РАБОТА С ФОТКАМИ:
try:
    WELCOME_PHOTO = FSInputFile("photos/welcome.png")
    BUY_VPN_PHOTO = FSInputFile("photos/buy_vpn.png")
    PROFILE_PHOTO = FSInputFile("photos/profile.png")
    DOCUMENTS_PHOTO = FSInputFile("photos/documents.jpg")
    INVITE_FRIEND_PHOTO = FSInputFile("photos/invite_friend.png")
    MY_KEYS_PHOTO = FSInputFile("photos/my_keys.png")
    DEPOSIT_PHOTO = FSInputFile("photos/deposit.png")
except FileNotFoundError:
    print("Photo files not found")
    exit()


dotenv.load_dotenv() # загружаем переменные окружения
Configuration.account_id = os.getenv('YOOKASSA_ACCOUNT_ID')
Configuration.secret_key = os.getenv('YOOKASSA_SECRET_KEY')


bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота
API_TOKEN = os.getenv('CRYPTO_BOT_API_TOKEN') # это криптобот

def get_rate():
    r = requests.get('https://v6.exchangerate-api.com/v6/d8e4beb763d54112c6a63999/latest/USD')
    return r.json()['conversion_rates']['RUB']

rub_to_usdt = get_rate()

dp = Dispatcher() # объект диспетчера


with sq.connect('database.db') as con:
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT, balance INTEGER, ref_balance INTEGER DEFAULT 0, ref_amount INTEGER DEFAULT 0, keys TEXT, role TEXT DEFAULT NULL, had_trial INTEGER DEFAULT 0, runout_notified INTEGER DEFAULT 0, has_active_keys INTEGER DEFAULT 0)")
    cur.execute('CREATE TABLE IF NOT EXISTS referal_users (id INTEGER PRIMARY KEY, referral_id INTEGER UNIQUE, ref_master_id INTEGER, registration_date TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER, type TEXT, date TEXT)')
    # Добавляем поле role, если его еще нет
    try:
        cur.execute('ALTER TABLE users ADD COLUMN role TEXT DEFAULT NULL')
    except:
        pass  # Поле уже существует
    # Добавляем поле runout_notified, если его еще нет
    try:
        cur.execute('ALTER TABLE users ADD COLUMN runout_notified INTEGER DEFAULT 0')
    except:
        pass  # Поле уже существует
    # Добавляем поле had_trial, если его еще нет
    try:
        cur.execute('ALTER TABLE users ADD COLUMN had_trial INTEGER DEFAULT 0')
    except:
        pass  # Поле уже существует
    # Добавляем поле has_active_keys, если его еще нет
    try:
        cur.execute('ALTER TABLE users ADD COLUMN has_active_keys INTEGER DEFAULT 0')
    except:
        pass  # Поле уже существует
    # Добавляем поле expiring_tomorrow_notified, если его еще нет
    try:
        cur.execute('ALTER TABLE users ADD COLUMN expiring_tomorrow_notified INTEGER DEFAULT 0')
    except:
        pass  # Поле уже существует
    # Добавляем поле registration_date в таблицу referal_users, если его еще нет
    try:
        cur.execute('ALTER TABLE referal_users ADD COLUMN registration_date TEXT')
    except:
        pass  # Поле уже существует
    # Язык интерфейса: ru / fa (персидский)
    try:
        cur.execute('ALTER TABLE users ADD COLUMN lang TEXT DEFAULT "ru"')
    except:
        pass  # Поле уже существует
    


@dp.message(CommandStart())
async def start_command(message):
    
    try:
        ref = message.text.split()[1] # получить реферальный код
        ref = int(ref)
    except:
        ref = None
    if ref:
        with sq.connect('database.db') as con:
            cur = con.cursor()
            # Проверяем, что referral_id != ref_master_id перед вставкой
                if message.from_user.id != ref:
                    cur.execute("SELECT * FROM referal_users WHERE referral_id = ?", (message.from_user.id,))
                    result = cur.fetchone()
                    if not result:
                        ref_lang = get_lang(ref)
                        await bot.send_message(ref, t(ref_lang, 'ref_new', username=message.from_user.username or ''), parse_mode='HTML')
                    registration_date = date.today().isoformat()
                    cur.execute(
                        "INSERT OR IGNORE INTO referal_users (referral_id, ref_master_id, registration_date) VALUES (?, ?, ?)", (message.from_user.id, ref, registration_date)
                    )
                    cur.execute("UPDATE users SET balance = balance + 50 WHERE id = ?", (ref,))
                    cur.execute('UPDATE users SET ref_amount = ref_amount + 1 WHERE id = ?', (ref,))
                con.commit()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (message.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0

    lang = get_lang(message.from_user.id)
    await message.answer_photo(FSInputFile("photos/welcome.png"), caption=t(lang, 'welcome', balance=balance), parse_mode='HTML', reply_markup=generate_ikb_main(message.from_user.id))
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO users (id, username, balance, had_trial) VALUES (?, ?, ?, ?)", (message.from_user.id, message.from_user.username, 0, 0))
    
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
    response = requests.post('https://pay.crypt.bot/api/getInvoices', headers=headers, json={"invoice_ids": [invoice_id]})
    response = response.json()
    if response.get('ok') and response.get('result', {}).get('items'):
        inv = response['result']['items'][0]
        if inv['invoice_id'] == invoice_id:
            return inv['status'], float(inv['amount'])*rub_to_usdt # возвращаем статус оплаты и сумму в рублях
    
    return None, None

def get_ikb_subscribe(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_subscribe'), url='https://t.me/coffemaniavpn')],
        [InlineKeyboardButton(text=t(lang, 'btn_subscribed'), callback_data='subscribe_confirmed')],
    ])

def get_ikb_back(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_plans(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'plan_week'), callback_data='plan_week')],
        [InlineKeyboardButton(text=t(lang, 'plan_month'), callback_data='plan_month')],
        [InlineKeyboardButton(text=t(lang, 'plan_halfyear'), callback_data='plan_halfyear')],
        [InlineKeyboardButton(text=t(lang, 'plan_year'), callback_data='plan_year')],
        [InlineKeyboardButton(text=t(lang, 'plan_lifetime'), callback_data='plan_lifetime')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_lifetime_agreement(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_agree'), callback_data='lifetime_agreement_confirmed')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_deposit(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_deposit'), callback_data='deposit')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_deposit_methods(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_crypto'), callback_data='deposit_crypto')],
        [InlineKeyboardButton(text=t(lang, 'btn_card'), callback_data='deposit_card')],
        [InlineKeyboardButton(text=t(lang, 'btn_stars'), callback_data='deposit_stars')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_referral(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_referral_reminder(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_referral'), callback_data='referral')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_documents(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'doc_user_agreement'), url='https://telegra.ph/Polzovatelskoe-soglashenie-12-22-25')],
        [InlineKeyboardButton(text=t(lang, 'doc_privacy'), url='https://telegra.ph/Politika-konfidencialnosti-12-22-25')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def get_ikb_support(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_support'), url='https://t.me/CoffemaniaSupport')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

def deposit_keyboard(lang, method):
    amount = [50, 100, 200, 500, 2900]
    ikb = InlineKeyboardMarkup(inline_keyboard=[])
    for sum in amount:
        ikb.inline_keyboard.append([InlineKeyboardButton(text=f'🟣 {sum}₽', callback_data=f'deposit_{sum}_{method}')])
    ikb.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')])
    return ikb

def yookassa_payment_keyboard(lang, amount, confirmation_url, payment_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'pay_go', amount=amount), url=confirmation_url)],
        [InlineKeyboardButton(text=t(lang, 'pay_done'), callback_data=f'check_{amount}_{payment_id}')],
        [InlineKeyboardButton(text=t(lang, 'pay_cancel'), callback_data='back')],
    ])

# Старые клавиатуры оставлены для обратной совместимости (админка и т.д.), пользовательские строятся через get_* с lang
ikb_subscribe = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔗 Подписаться на канал', url='https://t.me/coffemaniavpn')],
    [InlineKeyboardButton(text='✅ Я подписался', callback_data='subscribe_confirmed')],
])

def generate_ikb_main(user_id):
    lang = get_lang(user_id)
    ikb_main = InlineKeyboardMarkup(inline_keyboard=[])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT had_trial FROM users WHERE id = ?', (user_id,))
        result = cur.fetchone()
        had_trial = result[0] if result else 0
        if had_trial != 1:
            ikb_main.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_trial'), callback_data='trial')])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_buy_vpn'), callback_data='buy_vpn')])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_profile'), callback_data='profile')])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_referral'), callback_data='referral')])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_documents'), callback_data='documents')])
    ikb_main.inline_keyboard.append([
        InlineKeyboardButton(text=t(lang, 'btn_lang_fa'), callback_data='lang_fa'),
        InlineKeyboardButton(text=t(lang, 'btn_lang_ru'), callback_data='lang_ru'),
    ])
    return ikb_main

# ikb_profile создаётся динамически в зависимости от роли и языка
ikb_referral_reminder = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🤝 Получить 50₽ на баланс', callback_data='referral')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])


ikb_admin = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='👤 Пользователи', callback_data='admin_users')],
    [InlineKeyboardButton(text='🔄 Оплаты', callback_data='admin_payments')],
    [InlineKeyboardButton(text='🔑 Ключи', callback_data='admin_keys')],
    [InlineKeyboardButton(text='👉🏼 Рефералы', callback_data='admin_referrals')],
    [InlineKeyboardButton(text='👑 Роли', callback_data='admin_roles')],
    [InlineKeyboardButton(text='🔊 Напомнить юзерам о бесплатном тестовом периоде', callback_data='admin_notify_trial')],
    # [InlineKeyboardButton(text='⏰ Уведомить о завершении пробной подписки', callback_data='admin_notify_expired')],
    [InlineKeyboardButton(text='🤝 Напомнить о рефке', callback_data='admin_notify_referral')],

])

ikb_admin_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔙 Назад', callback_data='admin_back')],
])


@dp.callback_query(lambda c: c.data.startswith('check_payment_'))
async def check_payment_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'pay_done'))
    parts = callback.data.split('_')
    if len(parts) < 3:
        await callback.message.answer(t(lang, 'err_format'), parse_mode='HTML')
        return
    invoice_id = int(parts[2])
    status, amount = check_payment_status(invoice_id)
    try:
        if status == 'paid':
            await callback.message.answer(t(lang, 'paid_ok', amount=int(amount)), parse_mode='HTML', reply_markup=get_ikb_back(lang))
            await callback.message.delete()
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount, callback.from_user.id))
                cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (callback.from_user.id, amount, 'CryptoBot', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                # Проверяем реферала и его роль
                cur.execute('SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?', (callback.from_user.id,))
                ref_master = cur.fetchone()
                if ref_master:
                    ref_master_id = ref_master[0]
                    registration_date_str = ref_master[1]
                    if registration_date_str:
                        registration_date = date.fromisoformat(registration_date_str)
                        three_months_later = registration_date + timedelta(days=90)
                        if date.today() <= three_months_later:
                            cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                            ref_master_role = cur.fetchone()
                            if ref_master_role and ref_master_role[0] == 'refmaster':
                                cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (int(amount)/2, ref_master_id))
                con.commit()
        else:
            await callback.message.answer(t(lang, 'wait_pay'), parse_mode='HTML')
    except Exception as e:
        await callback.message.answer(f'❌ Ошибка: {e}', parse_mode='HTML')
        raise e


@dp.callback_query(lambda c: c.data.startswith('check_'))
async def check_payment_yookassa_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'pay_done'))
    _ , amount , payment_id = callback.data.split('_')
    # Убрали лишний print для экономии памяти
    if check_payment_yookassa_status(int(amount), payment_id, callback.from_user.id):
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount, callback.from_user.id))
            cur.execute('SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?', (callback.from_user.id,))
            ref_master = cur.fetchone() 
            if ref_master: # если есть рефовод то:
                ref_master_id = ref_master[0]
                registration_date_str = ref_master[1]
                if registration_date_str:
                    registration_date = date.fromisoformat(registration_date_str)
                    three_months_later = registration_date + timedelta(days=90)
                    if date.today() <= three_months_later:
                        # Проверяем роль рефмастера
                        cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                        ref_master_role = cur.fetchone()
                        if ref_master_role and ref_master_role[0] == 'refmaster':
                            cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (int(amount)/2, ref_master_id)) # начислить 50% реферального бонуса рефоводу
            con.commit()
        await callback.message.answer(t(lang, 'paid_ok', amount=amount), parse_mode='HTML', reply_markup=get_ikb_back(lang))
        await callback.message.delete()

    else:
        await callback.message.answer(t(lang, 'wait_pay'), parse_mode='HTML', reply_markup=get_ikb_back(lang))


def check_payment_yookassa_status(amount, payment_id, user_id): # функция для проверки статуса оплаты через Юкассу
    payment = Payment.find_one(payment_id)
    if payment.status == 'succeeded':
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (user_id, amount, 'yookassa', datetime.now().isoformat() ))
            con.commit()
        return True
    else:
        return False

# ОБРАБОТЧИКИ КОЛЛБЭКОВ
@dp.callback_query(lambda c: c.data == 'buy_vpn')
async def buy_vpn_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.message.delete()
    await callback.answer(t(lang, 'btn_buy_vpn'))
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(FSInputFile("photos/buy_vpn.png"), caption=t(lang, 'buy_vpn_title', balance=balance), parse_mode='HTML', reply_markup=get_ikb_plans(lang))

@dp.callback_query(lambda c: c.data == 'profile')
async def profile_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_profile'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance, ref_balance, role FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        ref_balance = result[1] if result and len(result) > 1 else 0
        role = result[2] if result and len(result) > 2 else None

    ikb_profile = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_my_keys'), callback_data='my_keys')],
        [InlineKeyboardButton(text=t(lang, 'btn_deposit'), callback_data='deposit')],
    ])
    if role == 'refmaster':
        ikb_profile.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_ref_withdraw'), callback_data='ref_withdraw')])
    ikb_profile.inline_keyboard.append([InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')])

    await callback.message.answer_photo(PROFILE_PHOTO, caption=t(lang, 'profile_title', balance=balance, ref_balance=ref_balance, user_id=callback.from_user.id), parse_mode='HTML', reply_markup=ikb_profile)

@dp.callback_query(lambda c: c.data == 'documents')
async def documents_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_documents'))
    await callback.message.delete()
    await callback.message.answer_photo(DOCUMENTS_PHOTO, caption=t(lang, 'documents_title'), parse_mode='HTML', reply_markup=get_ikb_documents(lang))

# ДЛЯ ДОКУМЕНТОВ КОЛБЕК НЕ НУЖЕН, ОНИ ОТКРЫВАЮТСЯ КАК СТАТЬЯ

@dp.callback_query(lambda c: c.data == 'referral')
async def referral_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_referral'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT ref_amount FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        ref_amount = result[0] if result else 0
    ref_link = f"https://t.me/coffemaniaVPNbot?start={callback.from_user.id}"
    await callback.message.answer_photo(INVITE_FRIEND_PHOTO, caption=t(lang, 'referral_title', ref_link=ref_link, earned=ref_amount*50), parse_mode='HTML', reply_markup=get_ikb_referral(lang))


@dp.callback_query(lambda c: c.data == 'support')
async def support_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer("ℹ️ " + t(lang, 'support_toast'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'support_title'), parse_mode='HTML', reply_markup=get_ikb_support(lang))


@dp.callback_query(lambda c: c.data == 'back')
async def back_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_back'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(WELCOME_PHOTO, caption=t(lang, 'welcome', balance=balance), parse_mode='HTML', reply_markup=generate_ikb_main(callback.from_user.id))

@dp.callback_query(lambda c: c.data == 'lang_fa')
async def lang_fa_callback(callback: CallbackQuery):
    set_lang(callback.from_user.id, 'fa')
    lang = 'fa'
    await callback.answer(t(lang, 'lang_switched_fa'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(WELCOME_PHOTO, caption=t(lang, 'welcome', balance=balance), parse_mode='HTML', reply_markup=generate_ikb_main(callback.from_user.id))

@dp.callback_query(lambda c: c.data == 'lang_ru')
async def lang_ru_callback(callback: CallbackQuery):
    set_lang(callback.from_user.id, 'ru')
    lang = 'ru'
    await callback.answer(t(lang, 'lang_switched_ru'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(WELCOME_PHOTO, caption=t(lang, 'welcome', balance=balance), parse_mode='HTML', reply_markup=generate_ikb_main(callback.from_user.id))

@dp.callback_query(lambda c: c.data == 'trial')
async def plan_trial(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        try:
            vpn_key = await generate_vpn_key(callback.from_user.id, 3)
        except Exception as e:
            await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
            raise e

        if vpn_key:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                expire_date = date.today() + timedelta(days=3)
                expire_date_str = expire_date.isoformat()
                buy_date_str = date.today().isoformat()
                cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, username, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?, ?)', (vpn_key, 3, 0, callback.from_user.id, callback.from_user.username, buy_date_str, expire_date_str))
                cur.execute('SELECT key FROM keys WHERE duration = 3 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                con.commit()
                result = cur.fetchone()
                cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (callback.from_user.id,))
            await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_3')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
    else:
        await callback.message.answer(t(lang, 'not_subscribed'), parse_mode='HTML', reply_markup=get_ikb_subscribe(lang))


@dp.callback_query(lambda c: c.data == 'subscribe_confirmed')
async def subscribe_confirmed_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_subscribed'))
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        try:
            vpn_key = await generate_vpn_key(callback.from_user.id, 3)
        except Exception as e:
            await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
            raise e

        if vpn_key:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                expire_date = date.today() + timedelta(days=3)
                expire_date_str = expire_date.isoformat()
                buy_date_str = date.today().isoformat()
                cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, username, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?, ?)', (vpn_key, 3, 0, callback.from_user.id, callback.from_user.username, buy_date_str, expire_date_str))
                cur.execute('SELECT key FROM keys WHERE duration = 3 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                con.commit()
                result = cur.fetchone()
                cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (callback.from_user.id,))
            await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_3')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
    else:
        await callback.message.answer(t(lang, 'not_subscribed'), parse_mode='HTML', reply_markup=get_ikb_subscribe(lang))

@dp.callback_query(lambda c: c.data == 'plan_lifetime')
async def plan_lifetime_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'plan_lifetime'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'lifetime_agreement'), parse_mode='HTML', reply_markup=get_ikb_lifetime_agreement(lang))
    
@dp.callback_query(lambda c: c.data == 'lifetime_agreement_confirmed')
async def lifetime_agreement_confirmed_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_agree'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        if balance >= 2900:
            try:
                vpn_key = await generate_vpn_key(callback.from_user.id, 0)
            except Exception as e:
                await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                raise e

            if vpn_key:
                with sq.connect('database.db') as con:
                    cur = con.cursor()
                    expire_date = date.today() + timedelta(days=10000)
                    expire_date_str = expire_date.isoformat()
                    buy_date_str = date.today().isoformat()
                    cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, username, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?, ?)', (vpn_key, 10000, 0, callback.from_user.id, callback.from_user.username, buy_date_str, expire_date_str))
                    con.commit()

                    cur.execute('SELECT key FROM keys WHERE duration = 10000 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                    result = cur.fetchone()
                    if result:
                        cur.execute('UPDATE users SET balance = balance - 2900 WHERE id = ? AND balance >= 2900', (callback.from_user.id,))
                        con.commit()
                        await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_inf')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
                        cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],))
                        cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0]))
                        cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (callback.from_user.id,))
                        con.commit()
                    else:
                        await callback.message.answer(t(lang, 'no_keys'), parse_mode='HTML', reply_markup=get_ikb_support(lang))
        else:
            await callback.message.answer(t(lang, 'insufficient'), parse_mode='HTML', reply_markup=get_ikb_deposit(lang))

@dp.callback_query(lambda c: c.data == 'plan_week')
async def plan_week_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'plan_week'))
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        if balance >= 50:
            with sq.connect('database.db') as con:
                try:
                    vpn_key = await generate_vpn_key(callback.from_user.id, 7)
                except Exception as e:
                    await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                    raise e

                if vpn_key:
                    with sq.connect('database.db') as con:
                        cur = con.cursor()
                        expire_date = date.today() + timedelta(days=7)
                        expire_date_str = expire_date.isoformat()
                        buy_date_str = date.today().isoformat()
                        cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?)', (vpn_key, 7, 0, callback.from_user.id, buy_date_str, expire_date_str))
                        con.commit()

                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 7 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                result = cur.fetchone()
                if result:
                    cur.execute('UPDATE users SET balance = balance - 50 WHERE id = ? AND balance >= 50' , (callback.from_user.id,))
                    con.commit()
                    await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_7')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],))
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0]))
                else:
                    await callback.message.answer(t(lang, 'no_keys'), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                con.commit()
        else:
            await callback.message.answer(t(lang, 'insufficient'), parse_mode='HTML', reply_markup=get_ikb_deposit(lang))
          
@dp.callback_query(lambda c: c.data == 'plan_month')
async def plan_month_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'plan_month'))
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        if balance >= 100:
            with sq.connect('database.db') as con:
                try:
                    vpn_key = await generate_vpn_key(callback.from_user.id, 30)
                except Exception as e:
                    await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                    raise e

                if vpn_key:
                    with sq.connect('database.db') as con:
                        cur = con.cursor()
                        expire_date = date.today() + timedelta(days=30)
                        expire_date_str = expire_date.isoformat()
                        buy_date_str = date.today().isoformat()
                        cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?)', (vpn_key, 30, 0, callback.from_user.id, buy_date_str, expire_date_str))
                        con.commit()

                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 30 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                result = cur.fetchone()
                if result:
                    cur.execute('UPDATE users SET balance = balance - 100 WHERE id = ? AND balance >= 100' , (callback.from_user.id,))
                    con.commit()
                    await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_30')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],))
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0]))
                else:
                    await callback.message.answer(t(lang, 'no_keys'), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                con.commit()
        else:
            await callback.message.answer(t(lang, 'insufficient'), parse_mode='HTML', reply_markup=get_ikb_deposit(lang))

@dp.callback_query(lambda c: c.data == 'plan_halfyear')
async def plan_halfyear_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'plan_halfyear'))
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        if balance >= 500:
            with sq.connect('database.db') as con:
                try:
                    vpn_key = await generate_vpn_key(callback.from_user.id, 180)
                except Exception as e:
                    await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                    raise e

                if vpn_key:
                    with sq.connect('database.db') as con:
                        cur = con.cursor()
                        expire_date = date.today() + timedelta(days=180)
                        expire_date_str = expire_date.isoformat()
                        buy_date_str = date.today().isoformat()
                        cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?)', (vpn_key, 180, 0, callback.from_user.id, buy_date_str, expire_date_str))
                        con.commit()

                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 180 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                result = cur.fetchone()
                if result:
                    cur.execute('UPDATE users SET balance = balance - 500 WHERE id = ? AND balance >= 500' , (callback.from_user.id,))
                    con.commit()
                    await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_180')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],))
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0]))
                else:
                    await callback.message.answer(t(lang, 'no_keys'), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                con.commit()
        else:
            await callback.message.answer(t(lang, 'insufficient'), parse_mode='HTML', reply_markup=get_ikb_deposit(lang))

@dp.callback_query(lambda c: c.data == 'plan_year')
async def plan_year_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'plan_year'))
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
        if balance >= 800:
            with sq.connect('database.db') as con:
                try:
                    vpn_key = await generate_vpn_key(callback.from_user.id, 365)
                except Exception as e:
                    await callback.message.answer(t(lang, 'key_error', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                    raise e

                if vpn_key:
                    with sq.connect('database.db') as con:
                        cur = con.cursor()
                        expire_date = date.today() + timedelta(days=365)
                        expire_date_str = expire_date.isoformat()
                        buy_date_str = date.today().isoformat()
                        cur.execute('INSERT INTO keys (key, duration, SOLD, buyer_id, buy_date, expiration_date) VALUES (?, ?, ?, ?, ?, ?)', (vpn_key, 365, 0, callback.from_user.id, buy_date_str, expire_date_str))
                        con.commit()

                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 365 AND SOLD = 0 ORDER BY rowid DESC LIMIT 1')
                result = cur.fetchone()
                if result:
                    cur.execute('UPDATE users SET balance = balance - 800 WHERE id = ? AND balance >= 800' , (callback.from_user.id,))
                    con.commit()
                    await callback.message.answer(t(lang, 'key_your', key=result[0], days=t(lang, 'key_days_365')), parse_mode='HTML', reply_markup=get_ikb_back(lang))
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],))
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0]))
                else:
                    await callback.message.answer(t(lang, 'no_keys'), parse_mode='HTML', reply_markup=get_ikb_support(lang))
                con.commit()
        else:
            await callback.message.answer(t(lang, 'insufficient'), parse_mode='HTML', reply_markup=get_ikb_deposit(lang))
            
@dp.callback_query(lambda c: c.data == 'my_keys')
async def my_keys_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_my_keys'))
    await callback.message.delete()
    ikb_my_keys = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = date.today()
        today_str = today.isoformat()
        cur.execute('SELECT key, expiration_date FROM keys WHERE buyer_id = ? AND expiration_date >= ? ', (callback.from_user.id, today_str))
        result = cur.fetchall()

        for key_id, key in enumerate(result):
            ikb_my_keys.inline_keyboard.append([InlineKeyboardButton(text=f'🔑 {key_id + 1}', callback_data=f'use_key_{key_id}')])
        if result:
            await callback.message.answer_photo(MY_KEYS_PHOTO, caption=t(lang, 'my_keys_title'), parse_mode='HTML', reply_markup=ikb_my_keys)
        else:
            cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
            result = cur.fetchone()
            balance = result[0] if result else 0
            await callback.message.answer_photo(MY_KEYS_PHOTO, caption=t(lang, 'my_keys_empty', balance=balance), parse_mode='HTML', reply_markup=get_ikb_plans(lang))
            con.commit()

@dp.callback_query(lambda c: c.data.startswith('use_key_'))
async def use_key_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_my_keys'))
    await callback.message.delete()
    with sq.connect('database.db') as con:
        today = date.today()
        today_str = today.isoformat()
        cur = con.cursor()
        cur.execute('SELECT key, expiration_date FROM keys WHERE buyer_id = ? AND expiration_date >= ? ORDER BY expiration_date LIMIT 1 OFFSET ? ' , (callback.from_user.id, today_str, callback.data.split('_')[2]))
        result = cur.fetchone()
        key = result[0]
        expiration_date = result[1]
        expiration_date = datetime.strptime(expiration_date, '%Y-%m-%d').date()
        if expiration_date >= today + timedelta(days=5000):
            human_date = '∞'
        else:
            human_date = expiration_date.strftime('%d.%m.%Y')
    await callback.message.answer(t(lang, 'use_key_title', key=key, date=human_date), parse_mode='HTML', reply_markup=get_ikb_back(lang))


@dp.callback_query(lambda c: c.data == 'deposit')
async def deposit_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_deposit'))
    await callback.message.delete()
    await callback.message.answer_photo(DEPOSIT_PHOTO, caption=t(lang, 'deposit_choose'), parse_mode='HTML', reply_markup=get_ikb_deposit_methods(lang))

@dp.callback_query(lambda c: c.data == 'deposit_crypto')
async def deposit_crypto_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_crypto'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'deposit_sum'), parse_mode='HTML', reply_markup=deposit_keyboard(lang, 'CryptoBot'))

@dp.callback_query(lambda c: c.data == ('deposit_card'))
async def deposit_card_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_card'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'deposit_sum'), parse_mode='HTML', reply_markup=deposit_keyboard(lang, 'card'))

@dp.callback_query(lambda c: c.data == ('deposit_stars'))
async def deposit_stars_callback(callback : CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_stars'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'deposit_sum_stars'), parse_mode='HTML', reply_markup=deposit_keyboard(lang, 'stars')) 



@dp.callback_query(lambda c: c.data.startswith('deposit_'))
async def process_deposit(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    _ , sum , method = callback.data.split('_')
    amount = int(sum)
    await callback.message.delete()
    if method == 'card':
        try:
            payment = Payment.create({
                "amount": {
                    "value": amount,
                    "currency": "RUB"
                },
                "description": "Пополнение баланса",
                'capture': True,
                'confirmation': {
                    'type': 'redirect',
                    'return_url': 'https://t.me/coffemaniaVPNbot',
                },
                "metadata": {
                    "user_id": callback.from_user.id,
                }
            }, uuid.uuid4())
            payment_id = payment.id
            confirmation_url = payment.confirmation.confirmation_url
            await callback.message.answer(t(lang, 'payment_created'), parse_mode='HTML', reply_markup=yookassa_payment_keyboard(lang, amount, confirmation_url, payment_id))
        except Exception as e:
            await callback.message.answer(t(lang, 'payment_fail_err', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_deposit_methods(lang))
            raise e

    if method == 'stars':
        stars_rate = 1.50
        amount_stars = amount * stars_rate
        amount_stars = int(amount_stars)
        try:
            await bot.send_invoice(
                chat_id=callback.from_user.id,
                title=f"Пополнение баланса на {amount} ₽",
                description=t(lang, 'payment_created'),
                payload=f"deposit_{amount}_{callback.from_user.id}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label=f"Пополнение на {amount} ₽", amount=amount_stars),],
            )
        except Exception as e:
            await callback.message.answer(t(lang, 'payment_fail_err', e=str(e)), parse_mode='HTML', reply_markup=get_ikb_deposit_methods(lang))
            raise e

    if method == 'CryptoBot':
        response = get_pay_link(amount/rub_to_usdt)
        ok = response['ok']
        result = response['result']
        pay_url = result['pay_url']
        invoice_id = result['invoice_id']

        ikb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, 'pay_go', amount=amount), url=pay_url)],
            [InlineKeyboardButton(text=t(lang, 'pay_done'), callback_data=f'check_payment_{invoice_id}')],
            [InlineKeyboardButton(text=t(lang, 'pay_cancel'), callback_data='back')],
        ])

        if ok:
            await callback.message.answer(t(lang, 'payment_created'), parse_mode='HTML', reply_markup=ikb)
        else:
            await callback.message.answer(t(lang, 'payment_fail'), parse_mode='HTML', reply_markup=get_ikb_deposit_methods(lang))


@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout):
    lang = get_lang(pre_checkout.from_user.id)
    parts = pre_checkout.invoice_payload.split('_')
    if len(parts) >= 3 and parts[0] == 'deposit':
        amount = int(parts[1])
        user_id = int(parts[2])
        if pre_checkout.invoice_payload == f"deposit_{amount}_{user_id}":
            await pre_checkout.answer(ok=True)
        else:
            await pre_checkout.answer(ok=False, error_message=t(lang, 'pre_checkout_err'))
    else:
        await pre_checkout.answer(ok=False, error_message=t(lang, 'pre_checkout_err'))
                


@dp.message(lambda m: m.successful_payment is not None)
async def handle_successful_payment(message: Message):
    lang = get_lang(message.from_user.id)
    payment = message.successful_payment
    payload = payment.invoice_payload
    parts = payload.split('_')
    if len(parts) >= 3 and parts[0] == 'deposit':
        amount_rub = int(parts[1])
        user_id = int(parts[2])
        if message.from_user.id != user_id:
            await message.answer(t(lang, 'err_user'))
            return
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount_rub, user_id))
            cur.execute('SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?', (user_id,))
            ref_master = cur.fetchone()
            if ref_master:
                ref_master_id = ref_master[0]
                registration_date_str = ref_master[1]
                if registration_date_str:
                    registration_date = date.fromisoformat(registration_date_str)
                    three_months_later = registration_date + timedelta(days=90)
                    if date.today() <= three_months_later:
                        cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                        ref_master_role = cur.fetchone()
                        if ref_master_role and ref_master_role[0] == 'refmaster':
                            cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (int(amount_rub)/2, ref_master_id))
            con.commit()
        await message.answer(t(lang, 'paid_ok', amount=amount_rub), parse_mode='HTML', reply_markup=get_ikb_back(lang))

@dp.callback_query(lambda c: c.data == 'bug_report')
async def bug_report_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer("⚠️ Баг репорт")
    await callback.message.delete()
    await callback.message.answer("⚠️ <b>Баг репорт</b>\n\nhttps://forms.gle/Pwdm8uzAgtu9T2296!", parse_mode='HTML', reply_markup=get_ikb_back(lang))

@dp.callback_query(lambda c: c.data == 'admin_back')
async def admin_back_callback(callback: CallbackQuery):
    await callback.answer("🔙 Назад") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("👤 Админ панель", parse_mode='HTML', reply_markup=ikb_admin)

@dp.message(F.text.startswith('shout'), (F.from_user.id.in_([1979477416, 7562967579])))
async def shout_message(message: Message):
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users;')
        result = cur.fetchall()
        for user in result:
            try:
                await bot.send_message(user[0], message.text[6:], parse_mode='HTML')
            except:
                pass
    await message.answer("🔊 Сообщение отправлено всем пользователям", parse_mode='HTML', reply_markup=get_ikb_back('ru'))


@dp.message(F.text == 'admin' , (F.from_user.id.in_([1979477416, 7562967579])))
async def admin_message (message: Message):
    await message.answer("👤 Админ панель", parse_mode='HTML', reply_markup=ikb_admin)

@dp.callback_query(lambda c: c.data == 'admin_users')
async def admin_users_callback(callback: CallbackQuery):
    await callback.answer("👤 Пользователи") # на пол экрана хуйня высветится
    await callback.message.delete() # удаляем соо на котором нажали на кнопку
    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = date.today()
        today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
        # Оптимизация: обновляем has_active_keys одним запросом вместо цикла
        # Сначала устанавливаем всем 0
        cur.execute("UPDATE users SET has_active_keys = 0")
        # Затем устанавливаем 1 тем, у кого есть активные ключи
        cur.execute('''
            UPDATE users 
            SET has_active_keys = 1 
            WHERE id IN (
                SELECT DISTINCT buyer_id 
                FROM keys 
                WHERE expiration_date >= ? AND buyer_id IS NOT NULL
            )
        ''', (today_str,))
        con.commit()
        cur.execute('SELECT id, username, balance, ref_amount, role, had_trial, has_active_keys FROM users')
        result = cur.fetchall()
        # используя пандас содаем xlsx файл
        df = pd.DataFrame(result, columns=['ID', 'Username', 'Balance', 'Ref_amount', 'Role', 'Had_trial', 'Has_active_keys'])
        
        # Вычисляем статистику
        total_users = len(df)
        had_trial_count = len(df[df['Had_trial'] == 1])
        has_active_keys_count = len(df[df['Has_active_keys'] == 1])
        
        had_trial_percent = (had_trial_count / total_users * 100) if total_users > 0 else 0
        has_active_keys_percent = (has_active_keys_count / total_users * 100) if total_users > 0 else 0
        
        # Добавляем колонки со статистикой
        df['Had_trial_%'] = round(had_trial_percent, 2)
        df['Has_active_keys_%'] = round(has_active_keys_percent, 2)
        
        df.to_excel('users.xlsx', index=False)
        try:
            await callback.message.answer_document(document=FSInputFile('users.xlsx'), reply_markup=ikb_admin_back)
        finally:
            # Удаляем файл после отправки, чтобы не засорять диск
            try:
                os.remove('users.xlsx')
            except:
                pass
        
    #     message_text = "Список пользователей:\n\n" + "\n".join(
    # f'👤 {user[0]} - {user[1]} - {user[2]} Р - {user[3]} рефов' for user in result)
    #     message_text = message_text + f'\n\n ВСЕГО ПОЛЬЗОВАТЕЛЕЙ: {len(result)}'
    # await callback.message.answer(f"{message_text}", parse_mode='HTML', reply_markup=ikb_admin_back)

@dp.callback_query(lambda c: c.data == 'admin_payments')
async def admin_payments_callback(callback: CallbackQuery):
    await callback.answer("🔄 Оплаты") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT transactions.id, users.username, transactions.amount, transactions.type, transactions.date FROM transactions JOIN users ON transactions.user_id = users.id')
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['ID', 'Username', 'Amount', 'Type', 'Date'])
        df.to_excel('payments.xlsx', index=False)
        try:
            await callback.message.answer_document(document=FSInputFile('payments.xlsx'), reply_markup=ikb_admin_back)
        finally:
            # Удаляем файл после отправки, чтобы не засорять диск
            try:
                os.remove('payments.xlsx')
            except:
                pass

@dp.callback_query(lambda c: c.data == 'admin_keys')
async def admin_keys_callback(callback: CallbackQuery):
    await callback.answer("🔑 Ключи") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT key, duration, buyer_id, username, buy_date, expiration_date FROM keys')
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['Key', 'Duration', 'Buyer_id', 'username', 'buy_date', 'expires_at'])
        df.to_excel('keys.xlsx', index=False)
        try:
            await callback.message.answer_document(document=FSInputFile('keys.xlsx'), reply_markup=ikb_admin_back)
        finally:
            # Удаляем файл после отправки, чтобы не засорять диск
            try:
                os.remove('keys.xlsx')
            except:
                pass

@dp.callback_query(lambda c: c.data == 'admin_notify_trial')
async def admin_notify_trial_callback(callback: CallbackQuery):
    await callback.answer("🔊 Напомнить юзерам о бесплатном тестовом периоде") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users WHERE had_trial != 1 AND has_active_keys = 0')
        result = cur.fetchall()
        success = 0
        fail = 0
        for user in result:
            try:
                await bot.send_message(user[0], "🎁 <b>У вас есть бесплатный тестовый период VPN на 3 дня!</b>\n\nВы можете использовать его, чтобы протестировать наш сервис.\n\n Пишите /start чтобы получить бесплатный тестовый период!", parse_mode='HTML')
                success+=1
            except:
                fail+=1
                pass
    await callback.message.answer(f"Итого: \n\n ✅ {success} \n\n ❌ {fail} ", parse_mode='HTML', reply_markup=ikb_admin_back)

@dp.callback_query(lambda c: c.data == 'admin_notify_expired')
async def admin_notify_expired_callback(callback: CallbackQuery):
    await callback.answer("⏰ Уведомить о завершении пробной подписки") # на пол экрана хуйня высветится
    await callback.message.delete()
    today = date.today()
    today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
    
    with sq.connect('database.db') as con:
        cur = con.cursor()
        # Находим всех пользователей, у которых нет активных ключей
        # (либо вообще нет ключей, либо все ключи истекли)
        cur.execute('''
            SELECT DISTINCT users.id 
            FROM users 
            WHERE users.id NOT IN (
                SELECT DISTINCT buyer_id 
                FROM keys 
                WHERE buyer_id IS NOT NULL 
                AND expiration_date >= ?
            )
        ''', (today_str,))
        users_without_active_keys = cur.fetchall()
        
        sent_count = 0
        failed_count = 0
        
        for user in users_without_active_keys:
            try:
                cur.execute('SELECT balance FROM users WHERE id = ?', (user[0],))
                result = cur.fetchone() # получить результат из базы данных
                balance = result[0] if result else 0 # если результат не пустой, то вытащить баланс, иначе 0
                ulang = get_lang(user[0])
                await bot.send_message(
                    user[0],
                    t(ulang, 'notify_expired', balance=balance),
                    parse_mode='HTML',
                    reply_markup=get_ikb_plans(ulang)
                )
                sent_count += 1
            except Exception as e:
                failed_count += 1
                print(f"Error sending message to user {user[0]}: {e}")
        
        await callback.message.answer(
            f"✅ Уведомления отправлены!\n\n"
            f"📤 Отправлено: {sent_count}\n"
            f"❌ Ошибок: {failed_count}",
            parse_mode='HTML',
            reply_markup=ikb_admin_back
        )

@dp.callback_query(lambda c: c.data == 'admin_notify_referral')
async def admin_notify_referral_callback(callback: CallbackQuery):
    await callback.answer("🤝 Напомнить о рефке") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users')
        result = cur.fetchall()
        sent_count = 0
        failed_count = 0
        for user in result:
            try:
                await bot.send_message(user[0], 'Кстати, небольшой бонус: если пригласить друга по реферальной ссылке, можно получить +50₽ 🙂', reply_markup=ikb_referral_reminder)
                sent_count += 1
            except:
                failed_count += 1
                pass
    await callback.message.answer(
        f"✅ Уведомления отправлены!\n\n"
        f"📤 Отправлено: {sent_count}\n"
        f"❌ Ошибок: {failed_count}",
        parse_mode='HTML',
        reply_markup=ikb_admin_back
    )

@dp.callback_query(lambda c: c.data == 'admin_apologize')
async def admin_apologize_callback(callback: CallbackQuery):
    await callback.answer("🙏 Извините") # на пол экрана хуйня высветится
    await callback.message.delete()
    
    with sq.connect('database.db') as con:
        cur = con.cursor()
        # Находим всех пользователей с активными ключами
        cur.execute('SELECT id FROM users WHERE has_active_keys = 1')
        users_with_active_keys = cur.fetchall()
        
        sent_count = 0
        failed_count = 0
        
        for user_tuple in users_with_active_keys:
            user_id = user_tuple[0]
            try:
                # Получаем текущий баланс пользователя для отображения
                cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                balance_result = cur.fetchone()
                current_balance = balance_result[0] if balance_result else 0
                
                # Формируем сообщение с жирным шрифтом для основных тезисов
                message_text = (
                    "Сегодня произошла печальная ситуация, которая вас никак не должна волновать.\n"
                    "<b>Все ключи были сброшены.</b> Нам очень жаль, что VPN был недоступен на протяжении 8 часов.\n\n"
                    "<b>ПОЭТОМУ:</b>\n\n"
                    "👉 <b>Мы выдали всем вам 100р компенсации.</b>\n"
                    "👉 <b>Вернули деньги на баланс, которые вы депозитнули.</b>\n\n"
                    "<b>Просьба, зайти и купить ключ заново.</b>\n\n"
                    f"👉🏼 <b>Баланс: {current_balance}₽</b>"
                )
                
                ulang = get_lang(user_id)
                await bot.send_message(
                    user_id,
                    message_text,
                    parse_mode='HTML',
                    reply_markup=get_ikb_plans(ulang)
                )
                sent_count += 1
            except Exception as e:
                failed_count += 1
                print(f"Error sending apologize message to user {user_id}: {e}")
        
        await callback.message.answer(
            f"✅ Сообщения отправлены!\n\n"
            f"📤 Отправлено: {sent_count}\n"
            f"❌ Ошибок: {failed_count}",
            parse_mode='HTML',
            reply_markup=ikb_admin_back
        )

@dp.callback_query(lambda c: c.data == 'admin_roles')
async def admin_roles_callback(callback: CallbackQuery):
    await callback.answer("👑 Роли") # на пол экрана хуйня высветится
    await callback.message.delete()
    ikb_admin_roles = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='👑 Выдать роль Refmaster', callback_data='admin_give_refmaster')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data='admin_back')],
    ])
    await callback.message.answer("👑 <b>Управление ролями</b>\n\nВыберите действие:", parse_mode='HTML', reply_markup=ikb_admin_roles)

@dp.callback_query(lambda c: c.data == 'admin_give_refmaster')
async def admin_give_refmaster_callback(callback: CallbackQuery):
    await callback.answer("👑 Выдать роль Refmaster") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("👑 <b>Выдача роли Refmaster</b>\n\nОтправьте ID пользователя, которому нужно выдать роль Refmaster:", parse_mode='HTML', reply_markup=ikb_admin_back)

@dp.message(F.text.isdigit(), (F.from_user.id.in_([1979477416, 7562967579])))
async def admin_set_role_message(message: Message):
    # Обработчик для выдачи роли Refmaster по ID пользователя
    user_id = int(message.text)
    with sq.connect('database.db') as con:
        cur = con.cursor()
        # Проверяем, существует ли пользователь
        cur.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
        user = cur.fetchone()
        if user:
            # Выдаем роль Refmaster
            cur.execute('UPDATE users SET role = ? WHERE id = ?', ('refmaster', user_id))
            con.commit()
            await message.answer(f"✅ Роль Refmaster успешно выдана пользователю:\n\n🆔 ID: {user_id}\n👤 Username: {user[1] if user[1] else 'Не указан'}", parse_mode='HTML', reply_markup=ikb_admin_back)
        else:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.", parse_mode='HTML', reply_markup=ikb_admin_back)

def get_ikb_withdraw(lang):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💰 200 ₽', callback_data='withdraw_200')],
        [InlineKeyboardButton(text='💰 300 ₽', callback_data='withdraw_300')],
        [InlineKeyboardButton(text='💰 500 ₽', callback_data='withdraw_500')],
        [InlineKeyboardButton(text='💰 1000 ₽', callback_data='withdraw_1000')],
        [InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data='back')],
    ])

@dp.callback_query(lambda c: c.data == 'ref_withdraw')
async def ref_withdraw_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.answer(t(lang, 'btn_ref_withdraw'))
    await callback.message.delete()
    await callback.message.answer(t(lang, 'ref_withdraw_text'), parse_mode='HTML', reply_markup=get_ikb_withdraw(lang))


@dp.callback_query(lambda c: c.data == 'admin_referrals')
async def admin_referrals_callback(callback: CallbackQuery):
    await callback.answer("👉🏼 Рефералы") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("""SELECT u.username, u2.username, r.ref_master_id,  r.referral_id FROM referal_users as r 
                    JOIN users as u ON r.ref_master_id = u.id
                    JOIN users as u2 ON r.referral_id = u2.id """)
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['Рефовод Юзернейм', 'Реферал Юзернейм', 'Рефмастер Айди' , 'Реферал айди'])
        df.to_excel('referals.xlsx')
        await callback.message.answer_document(FSInputFile('referals.xlsx'))

@dp.callback_query(lambda c: c.data.startswith('withdraw_'))
async def withdraw_callback(callback: CallbackQuery):
    lang = get_lang(callback.from_user.id)
    await callback.message.delete()
    _ , sum = callback.data.split('_')
    amount = int(sum)
    if amount < 200:
        await callback.message.answer(t(lang, 'withdraw_min'), parse_mode='HTML', reply_markup=get_ikb_withdraw(lang))
        return
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT ref_balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        ref_balance = result[0] if result else 0
    if amount > ref_balance:
        await callback.message.answer(t(lang, 'withdraw_insufficient'), parse_mode='HTML', reply_markup=get_ikb_withdraw(lang))
        return

    await callback.message.answer(t(lang, 'withdraw_contact'), parse_mode='HTML')


    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('UPDATE users SET ref_balance = ref_balance - ? WHERE id = ?', (amount, callback.from_user.id))
        cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (callback.from_user.id, amount, 'Выплата по реферальному балансу', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        con.commit()



async def check_expired_subscriptions():
    """Проверяет истекшие подписки и отправляет уведомления пользователям"""
    while True:
        try:
            today = date.today()
            today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
            with sq.connect('database.db') as con:
                cur = con.cursor()
                # Находим всех пользователей, у которых сегодня истекает подписка и которым еще не отправляли уведомление
                cur.execute('''
                    SELECT DISTINCT keys.buyer_id FROM keys 
                    INNER JOIN users ON keys.buyer_id = users.id
                    WHERE keys.expiration_date = ? AND keys.buyer_id IS NOT NULL AND (users.runout_notified IS NULL OR users.runout_notified = 0)
                ''', (today_str,))
                expired_users = cur.fetchall()
                
                for user_tuple in expired_users:
                    user_id = user_tuple[0]
                    try:
                        # Проверяем, есть ли у пользователя другие активные ключи
                        cur.execute('''
                            SELECT COUNT(*) 
                            FROM keys 
                            WHERE buyer_id = ? AND expiration_date > ?
                        ''', (user_id, today_str))
                        active_keys_count = cur.fetchone()[0]
                        
                        # Отправляем сообщение только если нет других активных ключей
                        if active_keys_count == 0:
                            cur.execute('UPDATE users SET runout_notified = 1 WHERE id = ?', (user_id,))
                            cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                            result = cur.fetchone()
                            balance = result[0] if result else 0
                            con.commit()
                            ulang = get_lang(user_id)
                            await bot.send_message(
                                user_id,
                                t(ulang, 'notify_expired', balance=balance),
                                parse_mode='HTML', reply_markup=get_ikb_plans(ulang))
                            print(f'{user_id} was notified about his subscription ending!')
                    except Exception as e:
                        print(f"Error {user_id}: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error checking expired subscriptions: {e}")
        
        # Проверяем раз в час (3600 секунд = 1 час)
        await asyncio.sleep(3600)

async def check_expiring_tomorrow_subscriptions():
    """Проверяет подписки, истекающие завтра, и отправляет уведомления пользователям"""
    while True:
        try:
            today = date.today()
            tomorrow = today + timedelta(days=1)
            tomorrow_str = tomorrow.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD
            with sq.connect('database.db') as con:
                cur = con.cursor()
                # Находим всех пользователей, у которых завтра истекает подписка и которым еще не отправляли уведомление
                cur.execute('''
                    SELECT DISTINCT keys.buyer_id FROM keys 
                    INNER JOIN users ON keys.buyer_id = users.id
                    WHERE keys.expiration_date = ? AND keys.buyer_id IS NOT NULL 
                    AND (users.expiring_tomorrow_notified IS NULL OR users.expiring_tomorrow_notified = 0)
                ''', (tomorrow_str,))
                expiring_users = cur.fetchall()
                
                for user_tuple in expiring_users:
                    user_id = user_tuple[0]
                    try:
                        cur.execute('UPDATE users SET expiring_tomorrow_notified = 1 WHERE id = ?', (user_id,))
                        cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                        result = cur.fetchone()
                        balance = result[0] if result else 0
                        con.commit()
                        ulang = get_lang(user_id)
                        await bot.send_message(
                            user_id,
                            t(ulang, 'notify_expiring_tomorrow', balance=balance),
                            parse_mode='HTML', reply_markup=get_ikb_plans(ulang))
                        print(f'{user_id} was notified about his subscription expiring tomorrow!')
                    except Exception as e:
                        print(f"Error {user_id}: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error checking expiring tomorrow subscriptions: {e}")
        
        # Проверяем раз в час (3600 секунд = 1 час)
        await asyncio.sleep(3600)

async def reset_runout_notified_daily(): # НЕ ЕБУ КАК РАБОТАЕТ!
    """Сбрасывает флаг runout_notified в 00:01 каждый день"""
    while True:
        try:
            now = datetime.now()
            # Вычисляем время до следующего 00:01
            next_reset = now.replace(hour=0, minute=1, second=0, microsecond=0)
            # Если уже прошло 00:01 сегодня, то следующий сброс будет завтра
            if now >= next_reset:
                next_reset += timedelta(days=1)
            
            # Вычисляем количество секунд до следующего 00:01
            seconds_until_reset = (next_reset - now).total_seconds()
            
            print(f"Next runout_notified reset will be at {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
            await asyncio.sleep(seconds_until_reset)
            
            # Сбрасываем флаги для всех пользователей
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET runout_notified = 0 WHERE runout_notified = 1')
                cur.execute('UPDATE users SET expiring_tomorrow_notified = 0 WHERE expiring_tomorrow_notified = 1')
                con.commit()
                print(f"runout_notified and expiring_tomorrow_notified flags reset for all users at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"Error resetting runout_notified: {e}")
            # В случае ошибки ждем час перед следующей попыткой
            await asyncio.sleep(3600)

async def main():
    # Запускаем фоновую задачу для проверки подписок
    asyncio.create_task(check_expired_subscriptions()) # бесокнечная задача параллельно, если не через create_task то не будет работать
    # Запускаем фоновую задачу для проверки подписок, истекающих завтра
    asyncio.create_task(check_expiring_tomorrow_subscriptions())
    # Запускаем фоновую задачу для сброса флага runout_notified в 00:01 каждый день
    asyncio.create_task(reset_runout_notified_daily())
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())