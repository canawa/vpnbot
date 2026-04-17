from datetime import date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, invoice, LabeledPrice, FSInputFile, MessageEntity
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
import asyncio # для работы с асинхронными функциями
import sqlite3 as sq
import requests
import dotenv
import os
import random
from yookassa import Configuration, Payment # для работы с Юкассой
import uuid
from vpn import generate_vpn_user, get_marzban_token
import pandas as pd
import openpyxl
from datetime import datetime
from check_subscription import is_subscribed
import locale 
from emojis import get_emoji
from databases import create_tables
from payments import get_pay_link, check_payment_status, check_payment_yookassa_status
from expire_functions import check_expired_subscriptions, check_expiring_tomorrow_subscriptions, reset_runout_notified_daily
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
print('BOT STARTED!!!')


### РАБОТА С ФОТКАМИ:
try:
    WELCOME_PHOTO = FSInputFile("photos/welcome.png")
    BUY_VPN_PHOTO = FSInputFile("photos/buy_vpn.png")
    # PROFILE_PHOTO = FSInputFile("photos/profile.png")  # раздел профиль скрыт
    DOCUMENTS_PHOTO = FSInputFile("photos/documents.png")
    INVITE_FRIEND_PHOTO = FSInputFile("photos/invite_friend.png")
    MY_KEYS_PHOTO = FSInputFile("photos/my_keys.png")
    DEPOSIT_PHOTO = FSInputFile("photos/deposit.png")
except FileNotFoundError:
    print("Photo files not found")
    exit()




bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота
API_TOKEN = os.getenv('CRYPTO_BOT_API_TOKEN') # это криптобот

create_tables()


dp = Dispatcher() # объект диспетчера

MONTH_PRICE = 150

def get_vpn_pay_keyboard(balance: int) -> InlineKeyboardMarkup:
    rows = []
    if balance >= MONTH_PRICE:
        rows.append([InlineKeyboardButton(text=f'Оплатить с баланса ({MONTH_PRICE} ₽)', callback_data='vpn_pay_balance')])
    rows.extend([
        [InlineKeyboardButton(text='СБП (или картой)', callback_data='vpnpay_card', icon_custom_emoji_id=get_emoji('sbp'))],
        [InlineKeyboardButton(text='Криптобот', callback_data='vpnpay_crypto', icon_custom_emoji_id=get_emoji('crypto_bot'))],
        [InlineKeyboardButton(text='Звёзды', callback_data='vpnpay_stars', icon_custom_emoji_id=get_emoji('stars'))],
        [InlineKeyboardButton(text='Назад', callback_data='vpn_pay_back', icon_custom_emoji_id=get_emoji('exit'))],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
                cur.execute("SELECT 1 FROM users WHERE id = ?", (message.from_user.id,))
                already_used_bot = cur.fetchone() is not None
                # Бонус только за нового пользователя; возвращаться по реф-ссылке после первого /start — без выплаты
                if not already_used_bot:
                    cur.execute("SELECT * FROM referal_users WHERE referral_id = ?", (message.from_user.id,))
                    result = cur.fetchone()
                    if not result:
                        try:
                            await bot.send_message(ref, f' <b>🎉 У вас новый реферал - {message.from_user.username}! </b>', parse_mode='HTML')
                        except:
                            pass

                        registration_date = date.today().isoformat()
                        cur.execute("SELECT username FROM users WHERE id = ?", (ref,))
                        ref_master_username_row = cur.fetchone()
                        ref_master_username = ref_master_username_row[0] if ref_master_username_row else None
                        cur.execute(
                            "INSERT OR IGNORE INTO referal_users (referral_id, ref_master_id, registration_date, referral_username, ref_master_username) VALUES (?, ?, ?, ?, ?)",
                            (message.from_user.id, ref, registration_date, message.from_user.username, ref_master_username),
                        )
                        con.commit()
                        cur.execute('SELECT role FROM users WHERE id = ?', (ref,))
                        ref_role_row = cur.fetchone()
                        ref_role = (ref_role_row[0] if ref_role_row else None) or ''
                        # Для refmaster отключен старый фиксированный бонус +50 за нового друга.
                        if ref_role.lower() != 'refmaster':
                            cur.execute("UPDATE users SET balance = balance + 50 WHERE id = ?", (ref,))
                        cur.execute('UPDATE users SET ref_amount = ref_amount + 1 WHERE id = ?', (ref,))
                con.commit()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (message.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0

    text = welcome_back_caption(balance)
    await message.answer_photo(
        WELCOME_PHOTO,
        caption=text,
        reply_markup=generate_ikb_main(message.from_user.id)
    )
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO users (id, username, balance, had_trial) VALUES (?, ?, ?, ?)", (message.from_user.id, message.from_user.username, 0, 0))
    

ikb_subscribe = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔗 Подписаться на канал', url='https://t.me/coffemaniavpn')],
    [InlineKeyboardButton(text='✅ Я подписался', callback_data='subscribe_confirmed')],
])

def generate_ikb_main(user_id):
    # запиши это через append
    ikb_main = InlineKeyboardMarkup(inline_keyboard=[])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT had_trial FROM users WHERE id = ?', (user_id,))
        result = cur.fetchone()
        had_trial = result[0] if result else 0
        if had_trial != 1:
            ikb_main.inline_keyboard.append([InlineKeyboardButton(text='🎁 Попробовать бесплатно', callback_data='trial', style = 'success')])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Подключить VPN', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('plus'))])
    ikb_main.inline_keyboard.append([
        InlineKeyboardButton(text='Реферальная программа', callback_data='referral', icon_custom_emoji_id=get_emoji('add_user')),
        InlineKeyboardButton(text='Мои ключи', callback_data='my_keys', icon_custom_emoji_id=get_emoji('keys')),
    ])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Документы', callback_data='documents', icon_custom_emoji_id=get_emoji('documents'))])
    return ikb_main

ikb_back = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])

ikb_referral_reminder = InlineKeyboardMarkup(inline_keyboard=[ # клава которая вылезит людям
    [InlineKeyboardButton(text='🤝 Получить 50₽ на баланс', callback_data='referral', icon_custom_emoji_id=get_emoji('game'), style = 'success')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_documents = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Пользовательское соглашение', url='https://telegra.ph/Polzovatelskoe-soglashenie-12-22-25', icon_custom_emoji_id=get_emoji('documents'))],
    [InlineKeyboardButton(text='Политика конфиденциальности', url='https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-03-29-41', icon_custom_emoji_id=get_emoji('lock'))],
    [InlineKeyboardButton(text='Написать в поддержку', url='https://t.me/CoffemaniaSupport', icon_custom_emoji_id=get_emoji('telegram'))],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_referral = InlineKeyboardMarkup(inline_keyboard=[
    # [InlineKeyboardButton(text='💸 Вывести реферальный баланс', callback_data='ref_withdraw')], ПОКА ЧТО УБРАЛ 
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_support = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💬 Написать в поддержку', url='https://t.me/CoffemaniaSupport')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])


def get_ikb_lifetime_agreement(country: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='✅ Я согласен', callback_data=f'lifetime_confirm_{country}')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])

ikb_deposit = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💰 Пополнить', callback_data='deposit')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_deposit_methods = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='СБП (или картой)', callback_data='deposit_card', icon_custom_emoji_id=get_emoji('sbp'))],
    [InlineKeyboardButton(text='Криптобот', callback_data='deposit_crypto', icon_custom_emoji_id=get_emoji('crypto_bot'))],
    [InlineKeyboardButton(text='Звёзды', callback_data='deposit_stars', icon_custom_emoji_id=get_emoji('stars'))],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

 
def yookassa_payment_keyboard(amount, confirmation_url, payment_id): # функция для создания клавиатуры для оплаты через Юкассу
    ikb_yookassa = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'👉 Перейти к оплате {amount} ₽', url=confirmation_url)],
        [InlineKeyboardButton(text='Я оплатил', callback_data=f'check_{amount}_{payment_id}', style = 'success')],
        [InlineKeyboardButton(text='Отменить платеж!', callback_data='back', style = 'danger')],
    ])
    return ikb_yookassa

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
    [InlineKeyboardButton(text=' Назад', callback_data='admin_back', icon_custom_emoji_id=get_emoji('exit'))],
])


@dp.callback_query(lambda c: c.data.startswith('check_payment_'))
async def check_payment_callback(callback: CallbackQuery):
    await callback.answer("✅️ Я оплатил") # на пол экрана хуйня высветится
    # Убрали лишний print для экономии памяти
    parts = callback.data.split('_')
    if len(parts) < 3:
        await callback.message.answer('❌ Ошибка: неверный формат данных', parse_mode='HTML')
        return
    invoice_id = int(parts[2])
    status, amount = check_payment_status(invoice_id)
    try:
        # Убрали лишний print для экономии памяти
        if status == 'paid':
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
            am_rub = int(float(amount))
            handled_vpn = await _maybe_complete_vpn_after_topup(callback.from_user.id, am_rub, callback.message)
            if handled_vpn:
                await callback.message.delete()
                return
            await callback.message.answer(f'🤑 Оплачено! \n\n ➕ Начислено {amount} ₽ на баланс', parse_mode='HTML', reply_markup=ikb_back)
            await callback.message.delete()
        else:
            await callback.message.answer('👀 Ожидаем оплату, оплатите и попробуйте снова!', parse_mode='HTML')
    except Exception as e:
        await callback.message.answer(f'❌ Ошибка: {e}', parse_mode='HTML')
        raise e



# ОБРАБОТЧИКИ КОЛЛБЭКОВ
@dp.callback_query(lambda c: c.data == 'buy_vpn')
async def buy_vpn_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("🛒 Раздел покупки VPN") # на пол экрана хуйня высветится
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(FSInputFile("photos/buy_vpn.png"), parse_mode='HTML', reply_markup=ikb_deposit_methods)

@dp.callback_query(lambda c: c.data == 'documents')
async def documents_callback(callback: CallbackQuery):
    await callback.answer("📄 Документы") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer_photo(DOCUMENTS_PHOTO, parse_mode='HTML', reply_markup=ikb_documents)

# ДЛЯ ДОКУМЕНТОВ КОЛБЕК НЕ НУЖЕН, ОНИ ОТКРЫВАЮТСЯ КАК СТАТЬЯ

@dp.callback_query(lambda c: c.data == 'referral')
async def referral_callback(callback: CallbackQuery):
    await callback.answer("🤝 Реферальная программа") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT ref_amount, role FROM users WHERE id = ?", (callback.from_user.id,)) # вытащить реферальное количество из базы данных текущего пользователя
        result = cur.fetchone() # получить результат из базы данных
        ref_amount = result[0] if result else 0 # если результат не пустой, то вытащить реферальное количество, иначе 0
        role = (result[1] if result and len(result) > 1 else None) or ''
        cur.execute('SELECT ref_balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        ref_balance = result[0] if result else 0
        if role.lower() == 'refmaster':
            cur.execute('SELECT COUNT(*) FROM referal_users WHERE ref_master_id = ?', (callback.from_user.id,))
            refs_total = (cur.fetchone() or (0,))[0]
            cur.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(CAST(t.amount AS INTEGER)), 0)
                FROM transactions t
                JOIN referal_users r ON r.referral_id = t.user_id
                WHERE r.ref_master_id = ?
                  AND t.type IN ('CryptoBot', 'yookassa')
                """,
                (callback.from_user.id,),
            )
            dep_stats = cur.fetchone() or (0, 0)
            deposits_count = dep_stats[0] or 0
            deposits_total = int(dep_stats[1] or 0)
            # "Ваша доля" считаем строго как 50% от депозитов рефералов.
            # Фиксированные бонусы 50₽ за приглашения сюда не входят.
            ref_share = int(deposits_total * 0.5)
            await callback.message.answer_photo(
                INVITE_FRIEND_PHOTO,
                caption=(
                    "🤝 <b>Реферальная программа</b>\n\n"
                    "Ваша реферальная ссылка:\n"
                    f"<code>https://t.me/coffemaniaVPNbot?start={callback.from_user.id}</code>\n\n"
                    f"👥 Количество рефералов: {refs_total}\n"
                    f"💳 Количество депозитов: {deposits_count}\n"
                    f"💰 Общая сумма депозитов: {deposits_total} ₽\n"
                    f"🧮 Ваша доля (50%, без бонусов 50₽): {ref_share} ₽\n"
                    f"🏦 Реферальный баланс: {int(ref_balance)} ₽"
                ),
                parse_mode='HTML',
                reply_markup=ikb_referral,
            )
            return
    await callback.message.answer_photo(INVITE_FRIEND_PHOTO, caption=f"🤝 <b>Пригласить друга</b>\n\nВаша реферальная ссылка:\n<code>https://t.me/coffemaniaVPNbot?start={callback.from_user.id}</code>\n\n👁️ Всего заработано на баланс VPN: {ref_amount*50} ₽\nВсего приведедено друзей: {ref_amount}\n\n🤔 <b>За каждого приглашенного друга вы получите 50 ₽ на баланс!</b>", parse_mode='HTML', reply_markup=ikb_referral)


@dp.callback_query(lambda c: c.data == 'support')
async def support_callback(callback: CallbackQuery):
    await callback.answer("ℹ️ Поддержка") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("ℹ️ <b>Поддержка</b>\n\nЕсли у вас возникли вопросы, напишите нам в поддержку!", parse_mode='HTML', reply_markup=ikb_support)


def welcome_back_caption(balance: int):
    text = (
        "👋 Добро пожаловать в Кофеманию\n"
        "\n"
        f'Подписка: СТАТУС ПОДПИСКИ\n' 
        f" 👉🏼 Баланс : {balance} ₽\n"
        "Купить ключи можно так же на сайте coffeemaniavpn.ru"
    )
    return text



@dp.callback_query(lambda c: c.data == 'back')
async def back_callback(callback: CallbackQuery):
    await callback.answer("Назад") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    text = welcome_back_caption(balance)
    await callback.message.answer_photo(
        WELCOME_PHOTO,
        caption=text,
        reply_markup=generate_ikb_main(callback.from_user.id),
    )

@dp.callback_query(lambda c: c.data == 'trial')
async def plan_trial(callback: CallbackQuery):
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        await deliver_trial_vpn(callback.from_user.id, callback.message)
    else:
        await callback.message.answer('❌ Вы не подписаны на канал! Подпишитесь на канал, чтобы получить бесплатный тестовый период!', parse_mode='HTML', reply_markup=ikb_subscribe)


@dp.callback_query(lambda c: c.data == 'subscribe_confirmed')
async def subscribe_confirmed_callback(callback: CallbackQuery):
    await callback.answer("✅ Я подписался") # на пол экрана хуйня высветится
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        await deliver_trial_vpn(callback.from_user.id, callback.message)
    else:
        await callback.message.answer('❌ Вы не подписаны на канал! Подпишитесь на канал, чтобы получить бесплатный тестовый период!', parse_mode='HTML', reply_markup=ikb_subscribe)

@dp.callback_query(lambda c: c.data.startswith('plan_lifetime'))
async def plan_lifetime_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)

@dp.callback_query(lambda c: c.data == 'vpn_pay_back')
async def vpn_pay_back_callback(callback: CallbackQuery):
    await callback.answer("Назад")
    _vpn_pending_clear(callback.from_user.id)
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        balance = result[0] if result else 0
    await callback.message.answer_photo(BUY_VPN_PHOTO, caption=f"<b>Выберите локацию: </b>\n\n👉🏼 <b>Баланс: {balance}₽</b>", parse_mode='HTML', reply_markup=ikb_locations)

@dp.callback_query(lambda c: c.data.startswith('check_'))
async def check_payment_yookassa_callback(callback: CallbackQuery): # сюды
    await callback.answer("🔄 Проверка статуса оплаты") # на пол экрана хуйня высветится
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
        handled_vpn = await _maybe_complete_vpn_after_topup(callback.from_user.id, int(amount), callback.message)
        if handled_vpn:
            await callback.message.delete()
            return
        await callback.message.answer(f'🤑 Оплачено! \n\n ➕ Начислено {amount} ₽ на баланс', parse_mode='HTML', reply_markup=ikb_back)
        await callback.message.delete()

    else:
        await callback.message.answer(f'👀 Ожидаем оплату, оплатите и попробуйте снова!', parse_mode='HTML', reply_markup=ikb_back)



@dp.callback_query(lambda c: c.data == 'vpn_pay_balance')
async def vpn_pay_balance_callback(callback: CallbackQuery):
    await callback.answer()
    country = _vpn_pending_get(callback.from_user.id)
    if not country:
        await callback.message.answer('❌ Сначала выберите страну в разделе «Подключить VPN».', reply_markup=ikb_back)
        return
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        balance = (cur.fetchone() or (0,))[0]
    if balance < MONTH_PRICE:
        await callback.answer('Недостаточно средств на балансе', show_alert=True)
        return
    await callback.message.delete()
    _vpn_pending_clear(callback.from_user.id)
    await _deliver_month_vpn(callback.from_user.id, country, callback.message)

@dp.callback_query(lambda c: c.data == 'vpnpay_crypto')
async def vpnpay_crypto_callback(callback: CallbackQuery):
    await callback.answer("Криптобот")
    country = _vpn_pending_get(callback.from_user.id)
    if not country:
        await callback.message.answer('❌ Сначала выберите страну: «Подключить VPN» → локация.', reply_markup=ikb_back)
        return
    await callback.message.delete()
    amount = MONTH_PRICE
    response = get_pay_link(amount / rub_to_usdt)
    ok = response['ok']
    result = response['result']
    pay_url = result['pay_url']
    invoice_id = result['invoice_id']
    ikb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'👉 Перейти к оплате {amount} ₽', url=pay_url)],
        [InlineKeyboardButton(text='✅️ Я оплатил', callback_data=f'check_payment_{invoice_id}')],
        [InlineKeyboardButton(text='❌ Отменить', callback_data='vpn_reopen_payment', icon_custom_emoji_id=get_emoji('exit'))],
    ])
    if ok:
        await callback.message.answer('👉 Создали заявку на оплату, перейдите по ссылке.\n\n <b>❗ После оплаты нажмите «Я оплатил»</b>', parse_mode='HTML', reply_markup=ikb)
    else:
        await callback.message.answer('❌ Не удалось создать заявку. Попробуйте позже.', reply_markup=get_vpn_pay_keyboard(0))


@dp.callback_query(lambda c: c.data == 'vpnpay_stars')
async def vpnpay_stars_callback(callback: CallbackQuery):
    await callback.answer("Звёзды")
    country = _vpn_pending_get(callback.from_user.id)
    if not country:
        await callback.message.answer('❌ Сначала выберите страну: «Подключить VPN» → локация.', reply_markup=ikb_back)
        return
    await callback.message.delete()
    amount = MONTH_PRICE
    stars_rate = 1.50
    amount_stars = int(amount * stars_rate)
    payload = f"vpnmonth_{amount}_{callback.from_user.id}_{country}"
    try:
        await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"VPN на месяц ({country})",
            description=f"Оплата {MONTH_PRICE} ₽",
            payload=payload,
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"VPN {MONTH_PRICE} ₽", amount=amount_stars)],
        )
    except Exception as e:
        await callback.message.answer(f'❌ Не удалось создать счёт: {e}', reply_markup=get_vpn_pay_keyboard(0))

@dp.callback_query(lambda c: c.data.startswith('plan_week_'))
async def plan_week_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)


@dp.callback_query(lambda c: c.data.startswith('plan_month_'))
async def plan_month_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)


@dp.callback_query(lambda c: c.data.startswith('plan_halfyear'))
async def plan_halfyear_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)


@dp.callback_query(lambda c: c.data.startswith('plan_year'))
async def plan_year_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)


@dp.callback_query(lambda c: c.data == 'my_keys')
async def my_keys_callback(callback: CallbackQuery):
    await callback.answer("🔗 Мои ключи") # на пол экрана хуйня высветится
    await callback.message.delete()
    ikb_my_keys = InlineKeyboardMarkup(inline_keyboard=[])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = date.today()
        today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
        cur.execute(
            'SELECT key, expiration_date, location FROM keys WHERE buyer_id = ? AND expiration_date >= ? ORDER BY expiration_date, rowid',
            (callback.from_user.id, today_str),
        )
        result = cur.fetchall()  # кортежи (key, expiration_date, location)

        buttons_row = []
        for key_id, row in enumerate(result):
            _key_str, exp_raw, loc = row[0], row[1], row[2]
            try:
                icon_id = get_emoji(loc) if loc else get_emoji('germany')
            except KeyError:
                icon_id = get_emoji('germany')
            btn = InlineKeyboardButton(
                text=f'До {exp_raw}',
                callback_data=f'use_key_{key_id}',
                icon_custom_emoji_id=icon_id,
            )
            buttons_row.append(btn)
            if len(buttons_row) == 2:
                ikb_my_keys.inline_keyboard.append(buttons_row)
                buttons_row = []
        if buttons_row:
            ikb_my_keys.inline_keyboard.append(buttons_row)

        ikb_my_keys.inline_keyboard.append([InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))])
        if result:
            await callback.message.answer_photo(MY_KEYS_PHOTO, reply_markup=ikb_my_keys)
        else:
            cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
            result = cur.fetchone() # получить результат из базы данных
            balance = result[0] if result else 0 # если результат не пустой, то вытащить баланс, иначе 0
            await callback.message.answer_photo(MY_KEYS_PHOTO, caption=f"🔗 У вас нет ключей. Купите ключ и используйте его. \n\n👉🏼 <b>Баланс: {balance}₽</b>", parse_mode='HTML', reply_markup=ikb_locations)
            con.commit() # сохранить изменения в базе данных

@dp.callback_query(lambda c: c.data.startswith('use_key_')) # ЭТО ПОСМОТРЕТЬ КЛЮЧИ
async def use_key_callback(callback: CallbackQuery):
    await callback.answer(f"🔑 Использовать ключ {callback.data.split('_')[2]}") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        today = date.today()
        today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
        cur = con.cursor()
        offset = int(callback.data.split('_')[2])
        cur.execute(
            'SELECT key, expiration_date, location FROM keys WHERE buyer_id = ? AND expiration_date >= ? ORDER BY expiration_date, rowid LIMIT 1 OFFSET ?',
            (callback.from_user.id, today_str, offset),
        )
        result = cur.fetchone() # получить результат из базы данных
        if not result:
            await callback.message.answer('❌ Ключ не найден. Обновите раздел «Мои ключи».', reply_markup=ikb_back)
            return
        key = result[0]
        expiration_date = result[1]
        location = result[2] if len(result) > 2 else None
        expiration_date = datetime.strptime(expiration_date, '%Y-%m-%d').date() # преобразуем дату в объект datetime
        if expiration_date >= today + timedelta(days=5000): # это ублюдская затычка но ладно (типо если не дотягивает до бесконечности)
            human_date = '∞'
        else:
            human_date = expiration_date.strftime('%d.%m.%Y') # преобразуем дату в строку формата дд.мм.гггг
        action_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Заменить конфиг', callback_data=f'replace_config_{offset}')],
            [InlineKeyboardButton(text='Назад', callback_data='my_keys', icon_custom_emoji_id=get_emoji('exit'))],
        ])
    t, ent = _format_key_message(key, f"Срок действия до: {human_date}")
    action_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Заменить конфиг', callback_data=f'replace_config_{offset}')],
        [InlineKeyboardButton(text='Назад', callback_data='my_keys', icon_custom_emoji_id=get_emoji('exit'))],
    ])
    await callback.message.answer(t, entities=ent, reply_markup=action_kb)


@dp.callback_query(lambda c: c.data.startswith('replace_config_'))
async def replace_config_prompt_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    offset = int(callback.data.split('_')[2])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = date.today().isoformat()
        cur.execute(
            'SELECT location FROM keys WHERE buyer_id = ? AND expiration_date >= ? ORDER BY expiration_date, rowid LIMIT 1 OFFSET ?',
            (callback.from_user.id, today, offset),
        )
        row = cur.fetchone()
        if not row:
            await callback.message.answer('❌ Ключ не найден.', reply_markup=ikb_back)
            return
        current_location = row[0] or 'germany'
    await callback.message.answer(
        'Выберите новую локацию. Старый конфиг будет удалён, новый выдан с оставшимся сроком.',
        reply_markup=_replace_config_keyboard(offset, current_location),
    )


@dp.callback_query(lambda c: c.data.startswith('replace_to_'))
async def replace_config_execute_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    payload = callback.data.removeprefix('replace_to_')
    new_location, offset_str = payload.rsplit('_', 1)
    offset = int(offset_str)
    uid = callback.from_user.id
    today = date.today()
    today_str = today.isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT rowid, key, expiration_date, location, marzban_username FROM keys WHERE buyer_id = ? AND expiration_date >= ? ORDER BY expiration_date, rowid LIMIT 1 OFFSET ?',
            (uid, today_str, offset),
        )
        row = cur.fetchone()
        if not row:
            await callback.message.answer('❌ Ключ не найден.', reply_markup=ikb_back)
            return
        rowid, _key, expiration_raw, old_location, old_mz_username = row
        expiration_date = datetime.strptime(expiration_raw, '%Y-%m-%d').date()
        days_left = max((expiration_date - today).days, 1)

        group_rowids = [rowid]

    if old_mz_username:
        deleted_ok = await _delete_key_from_marzban(old_mz_username, old_location or 'germany')
        if not deleted_ok:
            await callback.message.answer('❌ Не удалось удалить старый ключ. Попробуйте позже или обратитесь в поддержку.', reply_markup=ikb_support)
            return
    else:
        pass

    try:
        new_username, new_keys = await generate_vpn_user(uid, days_left, new_location)
    except Exception as e:
        await callback.message.answer(f'❌ Не удалось создать новый ключ: {e}', reply_markup=ikb_support)
        return
    new_keys = [k for k in (new_keys or []) if k]
    if not new_keys:
        await callback.message.answer('❌ Не удалось получить новый ключ. Напишите в поддержку.', reply_markup=ikb_support)
        return

    new_expiration = expiration_date.isoformat()
    buy_date_str = date.today().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.executemany('DELETE FROM keys WHERE rowid = ? AND buyer_id = ?', [(rid, uid) for rid in group_rowids])
        for key in new_keys:
            cur.execute(
                'INSERT INTO keys (key, duration, SOLD, buyer_id, buy_date, expiration_date, location, marzban_username) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (key, 30, 1, uid, buy_date_str, new_expiration, new_location, new_username),
            )
        con.commit()

    human_date = expiration_date.strftime('%d.%m.%Y')
    t, ent = _format_key_message(new_keys[0], f"Срок действия до: {human_date}")
    await callback.message.answer('✅ Конфиг заменён.', reply_markup=ikb_back)
    await callback.message.answer(t, entities=ent, reply_markup=ikb_back)


@dp.callback_query(lambda c: c.data == 'vpn_reopen_payment')
async def vpn_reopen_payment_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    uid = callback.from_user.id
    if not _vpn_pending_get(uid):
        await callback.message.answer('Выберите снова: «Подключить VPN» → локация.', reply_markup=ikb_back)
        return
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (uid,))
        balance = (cur.fetchone() or (0,))[0]
    await callback.message.answer_photo(
        BUY_VPN_PHOTO,
        caption=f'<b>VPN на месяц — {MONTH_PRICE} ₽</b>\n\nВыберите способ оплаты:',
        parse_mode='HTML',
        reply_markup=get_vpn_pay_keyboard(balance),
    )

@dp.callback_query(lambda c: c.data.startswith('deposit_'))
async def process_deposit(callback: CallbackQuery):
    # Убрали лишний print для экономии памяти
    _ , sum , method = callback.data.split('_')
    
    amount = int(sum)
    # await callback.message.answer(f"💰 Пополнение на {amount} ₽\n\n<b>💳 Способ пополнения: {method}</b> \n\n Создаем заявку...", parse_mode='HTML')
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
            # Убрали pprint для экономии памяти
            payment_id = payment.id
            confirmation_url = payment.confirmation.confirmation_url
            await callback.message.answer(f'👉 Создали заявку на оплату, переходите по ссылке и оплатите.\n\n <b>❗ После оплаты нажмите на кнопку "Я оплатил"</b>', parse_mode='HTML', reply_markup=yookassa_payment_keyboard(amount, confirmation_url, payment_id))
        except Exception as e:
            await callback.message.answer(f'❌ Не удалось создать заявку: {e}. Напишите в техподдержку, мы обязательно поможем!', reply_markup=ikb_deposit_methods)
            raise e

    if method == 'stars':
        stars_rate = 1.50 # 1 звезда = 1.50 рубля
        amount_stars = amount * stars_rate
        amount_stars = int(amount_stars)
        try:
            await bot.send_invoice(
                chat_id=callback.from_user.id, # куда отправится инвойс
                title=f"Пополнение баланса на {amount} ₽", # заголовок инвойса
                description=f"👉 Создали заявку на оплату, переходите по ссылке и оплатите",
                payload=f"deposit_{amount}_{callback.from_user.id}", # то что получит бот после оплаты (это для обработки успешности)
                provider_token="", # для звезд не нужен provider_token
                currency="XTR", # валюта звезд
                prices=[LabeledPrice(label=f"Пополнение на {amount} ₽", amount=amount_stars),],
            )
        except Exception as e:
            await callback.message.answer(f'❌ Не удалось создать заявку: {e}',  reply_markup=ikb_deposit_methods)
            raise e

        
    if method == 'CryptoBot': # рассматриваем оплату криптой
        response = get_pay_link(amount/rub_to_usdt) # переводим рубли в доллары от руки пока что пох
        ok = response['ok'] # тру фолс
        result = response['result'] # содержит инфу о результате запроса
        pay_url = result['pay_url'] # ссылка на оплату
        bot_invoice_url = result['bot_invoice_url'] # ссылка на оплату в боте
        invoice_id = result['invoice_id'] # id заявки
        # print(pay_url, bot_invoice_url, ok)

        ikb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f'👉 Перейти к оплате {amount} ₽', url=pay_url)],
            [InlineKeyboardButton(text='✅️ Я оплатил', callback_data=f'check_payment_{invoice_id}')],
            [InlineKeyboardButton(text='❌ Отменить платеж!', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
        ])
        

        if ok:
            await callback.message.answer('👉 Создали заявку на оплату, переходите по ссылке и оплатите.\n\n <b>❗ После оплаты нажмите на кнопку "Я оплатил"</b>', parse_mode='HTML', reply_markup=ikb)
        else:
            await callback.message.answer('❌ Не удалось создать заявку. Попробуйте позже.',  reply_markup=ikb_deposit_methods)


@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout): # обработчик подтверждения платежа (я так понял типо это надо чтобы payload совпал с фактическим)
    parts = pre_checkout.invoice_payload.split('_', 3)
    if len(parts) >= 3 and parts[0] == 'deposit':
        amount = int(parts[1])
        user_id = int(parts[2])
        if pre_checkout.invoice_payload == f"deposit_{amount}_{user_id}":
            await pre_checkout.answer(ok=True)
        else:
            await pre_checkout.answer(ok=False, error_message="❌ Неверный payload (Напиши в поддержку)")
    elif len(parts) >= 4 and parts[0] == 'vpnmonth':
        amount = int(parts[1])
        user_id = int(parts[2])
        country = parts[3]
        if (
            pre_checkout.invoice_payload == f"vpnmonth_{amount}_{user_id}_{country}"
            and pre_checkout.from_user.id == user_id
        ):
            await pre_checkout.answer(ok=True)
        else:
            await pre_checkout.answer(ok=False, error_message="❌ Неверный payload (Напиши в поддержку)")
    else:
        await pre_checkout.answer(ok=False, error_message="❌ Неверный payload (Напиши в поддержку)")
                


@dp.message(lambda m: m.successful_payment is not None) # обработчик успешного платежа
async def handle_successful_payment(message: Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    parts = payload.split('_', 3)
    if len(parts) >= 4 and parts[0] == 'vpnmonth':
        amount_rub = int(parts[1])
        user_id = int(parts[2])
        country = parts[3]
        if message.from_user.id != user_id:
            await message.answer("❌ Ошибка: несоответствие пользователя")
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
                            cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (int(amount_rub) / 2, ref_master_id))
            con.commit()
        _vpn_pending_clear(user_id)
        await _deliver_month_vpn(user_id, country, message)
        return
    if len(parts) >= 3 and parts[0] == 'deposit':
        amount_rub = int(parts[1])
        user_id = int(parts[2])
        if message.from_user.id != user_id:
            await message.answer("❌ Ошибка: несоответствие пользователя")
            return 
        with sq.connect('database.db') as con:
            cur = con.cursor()

            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount_rub, user_id))
            # Проверяем реферала и его роль
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
        await message.answer(f'🤑 Оплачено! \n\n ➕ Начислено {amount_rub} ₽ на баланс', parse_mode='HTML', reply_markup=ikb_back)
        await _maybe_complete_vpn_after_topup(user_id, amount_rub, message)

@dp.callback_query(lambda c: c.data == 'bug_report')
async def bug_report_callback(callback: CallbackQuery):
    await callback.answer("⚠️ Баг репорт") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("⚠️ <b>Баг репорт</b>\n\nhttps://forms.gle/Pwdm8uzAgtu9T2296!", parse_mode='HTML', reply_markup=ikb_back)

@dp.callback_query(lambda c: c.data == 'admin_back')
async def admin_back_callback(callback: CallbackQuery):
    await callback.answer("Назад") # на пол экрана хуйня высветится
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
                await bot.send_message(user[0], message.text[6:], parse_mode='Markdown')
            except Exception as e:
                await bot.send_message.answer(1979477416, f'Ошибка {e}')
                await bot.send_message.answer(7562967579, f'Ошибка {e}')
    await message.answer("🔊 Сообщение отправлено всем пользователям", parse_mode='Markdown', reply_markup=ikb_back)


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
        cur.execute('SELECT key, duration, buyer_id, username, buy_date, expiration_date, location FROM keys')
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['Key', 'Duration', 'Buyer_id', 'username', 'buy_date', 'expires_at', 'location'])
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
                await bot.send_message(
                    user[0], 
                    f"⏰ <b>Ваша пробная подписка закончилась</b>\n\nВаш тестовый период VPN истек. Для продолжения использования сервиса, пожалуйста, приобретите новый ключ.\n\n<b>Баланс: {balance}₽</b>",
                    parse_mode='HTML',
                    reply_markup=ikb_locations
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
                await bot.send_message(user[0], 'Кстати! Если позвать друга, то получишь 50₽ на баланс!', reply_markup=ikb_referral_reminder)
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


@dp.callback_query(lambda c: c.data == 'admin_roles')
async def admin_roles_callback(callback: CallbackQuery):
    await callback.answer("👑 Роли") # на пол экрана хуйня высветится
    await callback.message.delete()
    ikb_admin_roles = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='👑 Выдать роль Refmaster', callback_data='admin_give_refmaster')],
        [InlineKeyboardButton(text='Назад', callback_data='admin_back')],
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

@dp.callback_query(lambda c: c.data == 'ref_withdraw')
async def ref_withdraw_callback(callback: CallbackQuery):
    await callback.answer("💸 Вывести реферальный баланс") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("<b> 🤝 Чтобы вывести реферальный баланс, на реферальном балансе должно быть минимум 200 ₽. \n\n 🟢 Выберите сумму для вывода:</b>", parse_mode='HTML', reply_markup=ikb_withdraw)


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
        await callback.message.answer_document(FSInputFile('referals.xlsx'), reply_markup=ikb_admin_back)

@dp.callback_query(lambda c: c.data.startswith('withdraw_'))
async def withdraw_callback(callback: CallbackQuery):
    await callback.message.delete()
    _ , sum = callback.data.split('_')  
    amount = int(sum)
    if amount < 200:
        await callback.message.answer("❌ Минимальная сумма для вывода 200 ₽", parse_mode='HTML', reply_markup=ikb_withdraw)
        return
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT ref_balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        ref_balance = result[0] if result else 0
    if amount > ref_balance:
        await callback.message.answer("❌ Недостаточно средств на реферальном балансе", parse_mode='HTML', reply_markup=ikb_withdraw)
        return

    await callback.message.answer("💸 <b>Теперь напишите @CoffemaniaSupport, в сообщении укажите реквизиты для вывода: (например, СБП +7978334455 Тбанк ИЛИ 2200 4500 1111 1111 СБЕР)</b>", parse_mode='HTML')


    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('UPDATE users SET ref_balance = ref_balance - ? WHERE id = ?', (amount, callback.from_user.id))
        cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (callback.from_user.id, amount, 'Выплата по реферальному балансу', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        con.commit()



async def main():
    # Запускаем фоновую задачу для проверки подписок
    asyncio.create_task(check_expired_subscriptions(bot)) # бесокнечная задача параллельно, если не через create_task то не будет работать
    # Запускаем фоновую задачу для проверки подписок, истекающих завтра
    asyncio.create_task(check_expiring_tomorrow_subscriptions(bot))
    # Запускаем фоновую задачу для сброса флага runout_notified в 00:01 каждый день
    asyncio.create_task(reset_runout_notified_daily())
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())