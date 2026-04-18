from datetime import date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, invoice, LabeledPrice, FSInputFile, MessageEntity
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
import asyncio # для работы с асинхронными функциями
import html
import sqlite3 as sq
import requests
import dotenv
import os
import random

from traitlets import Bool
from yookassa import Configuration, Payment # для работы с Юкассой
import uuid
import pandas as pd
import openpyxl
from datetime import datetime
from check_subscription import is_subscribed
import locale 
from emojis import get_emoji
from databases import create_tables, upsert_subscription_days
from payments import get_pay_link, check_payment_status, check_payment_yookassa_status, rub_to_usdt
from vpn import Vpn
from ikbs import *
from expire_functions import *
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
print('BOT STARTED!!!')

vpn = Vpn()


_SUBSCRIPTION_URL_KEYS = ( # не уверен
    'subscriptionUrl',
    'subscription_url',
    'subscriptionLink',
    'subscription_link',
)


def _subscription_url_from_dict(d):
    if not isinstance(d, dict):
        return None
    for k in _SUBSCRIPTION_URL_KEYS:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _vpn_response_subscription_url(payload):
    """Достаёт ссылку подписки из JSON ответа панели (разная вложенность у GET/POST). ЭТО ПИСАЛ GPT ХУЙ ЕГО ЗНАЕТ ЧТО ЭТО"""
    if not isinstance(payload, dict):
        return None
    seen_ids = set()

    def walk(obj, depth):
        if depth > 14:
            return None
        oid = id(obj)
        if oid in seen_ids:
            return None
        if isinstance(obj, dict):
            seen_ids.add(oid)
            u = _subscription_url_from_dict(obj)
            if u:
                return u
            for v in obj.values():
                r = walk(v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            seen_ids.add(oid)
            for it in obj[:40]:
                r = walk(it, depth + 1)
                if r:
                    return r
        return None

    return walk(payload, 0)


def _vpn_response_user_already_exists(payload):
    if not isinstance(payload, dict):
        return False
    msg = str(payload.get('message', '') or '')
    if msg == 'User username already exists':
        return True
    return 'already exists' in msg.lower()


def fetch_vpn_subscription_url_after_purchase(tg_id: int):
    created = vpn.create_new_user(tg_id)
    url = _vpn_response_subscription_url(created)
    if url:
        return url
    if _vpn_response_user_already_exists(created):
        renewed = vpn.renew_subscription(tg_id)
        return _vpn_response_subscription_url(renewed)
    return None

def vpn_subscription_message_html(url: str) -> str:
    return (
        "🔑 <b>Твоя подписка КОФЕМАНИЯ VPN</b>\n"
        "\n"
        "☕️ Мы автоматически установим ключ в приложении HAPP\n"
        "\n"
        "🚀 Нажми кнопку ниже — и всё настроится за тебя\n"
        "\n"
        "Если хочешь воспользоваться другим клиентом, то копируй ссылку:\n"
        "\n"
        f"<pre>{url}</pre>"
    )


### РАБОТА С ФОТКАМИ:
try:
    WELCOME_PHOTO = FSInputFile("photos/welcome.png")
    BUY_VPN_PHOTO = FSInputFile("photos/buy_vpn.png")
    DOCUMENTS_PHOTO = FSInputFile("photos/documents.png")
    INVITE_FRIEND_PHOTO = FSInputFile("photos/invite_friend.png")
    MY_KEYS_PHOTO = FSInputFile("photos/my_keys.png")
    DEPOSIT_PHOTO = FSInputFile("photos/deposit.png")
except FileNotFoundError:
    print("Photo files not found")
    exit()

bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота

create_tables()

dp = Dispatcher() # объект диспетчера

MONTH_PRICE = 149
# Сроки как в vpn.py: create/renew +30 дн., trial POST +3 дн.
VPN_SUBSCRIPTION_DAYS_PAID = 30
VPN_SUBSCRIPTION_DAYS_TRIAL = 3


def get_vpn_pay_keyboard() -> InlineKeyboardMarkup:
    rows = []
    rows.extend([
        [InlineKeyboardButton(text='СБП (или картой)', callback_data=f'deposit_{MONTH_PRICE}_card', icon_custom_emoji_id=get_emoji('sbp'))],
        # [InlineKeyboardButton(text='Криптобот', callback_data=f'deposit_{MONTH_PRICE}_crypto', icon_custom_emoji_id=get_emoji('crypto_bot'))],
        # [InlineKeyboardButton(text='Звёзды', callback_data=f'deposit_{MONTH_PRICE}_stars', icon_custom_emoji_id=get_emoji('stars'))],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
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

    today_str = date.today().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT subscription_expires_at FROM subscriptions
            WHERE user_id = ?
              AND date(subscription_expires_at) >= date(?)
            LIMIT 1
            """,
            (message.from_user.id, today_str),
        )
        sub_row = cur.fetchone()
        has_active_subscription = sub_row is not None
        subscription_expires_at = sub_row[0] if sub_row else None

    text = welcome_back_caption(has_active_subscription, subscription_expires_at)
    await message.answer_photo(
        WELCOME_PHOTO,
        caption=text,
        reply_markup=generate_ikb_main(message.from_user.id),
        parse_mode='HTML'
    )
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO users (id, username, balance, had_trial) VALUES (?, ?, ?, ?)", (message.from_user.id, message.from_user.username, 0, 0))

    generate_ikb_main(message.from_user.id)
 


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
            await callback.message.answer(f'Спасибо за покупку, ваша подписка: ТУТ ГАЙД', parse_mode='HTML', reply_markup=ikb_back)
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
    await callback.message.answer_photo(FSInputFile("photos/buy_vpn.png"), parse_mode='HTML', reply_markup=get_vpn_pay_keyboard())

@dp.callback_query(lambda c: c.data == 'my_subscription')
async def my_sub_callback(callback: CallbackQuery):
    await callback.answer('Моя подписка')
    await callback.message.delete()
    uid = callback.from_user.id
    try:
        result = vpn.get_user_by_tg_id(uid)
    except Exception as e:
        print(f'get_user_by_tg_id({uid}): {e}')
        result = None
    url = _vpn_response_subscription_url(result) if result else None
    if url:
        await callback.message.answer_photo(
            MY_KEYS_PHOTO,
            caption=vpn_subscription_message_html(url),
            parse_mode='HTML',
            reply_markup=create_ikb_sub_after_buy(url),
        )
        return
    today_str = date.today().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT subscription_expires_at FROM subscriptions
            WHERE user_id = ? AND date(subscription_expires_at) >= date(?)
            """,
            (uid, today_str),
        )
        sub_row = cur.fetchone()
    if sub_row:
        exp_safe = html.escape(str(sub_row[0]), quote=True)
        retry_url = fetch_vpn_subscription_url_after_purchase(uid)
        sub_markup = create_ikb_sub_after_buy(retry_url) if retry_url else ikb_back
        await callback.message.answer_photo(
            MY_KEYS_PHOTO,
            caption=(
                '🔑 <b>Подписка активна</b>\n\n'
                f'По данным бота доступ до: <b>{exp_safe}</b>\n\n'
                + (
                    'Нажми «Подключиться» ниже — откроется ссылка для приложения.'
                    if retry_url
                    else (
                        'Ссылку для приложения панель сейчас не вернула в ответе API. '
                        'Если ключ уже выдавался — открой прошлое сообщение с ключом или нажми «Подключить VPN» в главном меню; '
                        'иначе напиши в поддержку.'
                    )
                )
            ),
            parse_mode='HTML',
            reply_markup=sub_markup,
        )
        return
    await callback.message.answer_photo(
        MY_KEYS_PHOTO,
        caption='<b>У тебя еще нет подписки!</b>',
        parse_mode='HTML',
        reply_markup=get_vpn_pay_keyboard(),
    )

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


def welcome_back_caption(has_active: bool, subscription_expires_at=None) -> str:
    if has_active and subscription_expires_at:
        exp_safe = html.escape(str(subscription_expires_at).strip(), quote=True)
        sub_line = f"🟢 Активна до <b>{exp_safe}</b>"
    elif has_active:
        sub_line = '🟢 Активна'
    else:
        sub_line = '🔴 Отсутствует'
    return ("""👋 Добро пожаловать в Кофеманию\n"
        "\n",
        "📦 Информация о подписке\n",
        "├ Действует до\n",
        "├ 4 региона + обход LTE\n",
        "└ 300GB трафика"\n,
        f"Подписка: {sub_line}\n""")

@dp.callback_query(lambda c: c.data == 'back')
async def back_callback(callback: CallbackQuery):
    await callback.answer("Назад") # на пол экрана хуйня высветится
    await callback.message.delete()
    today_str = date.today().isoformat()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT subscription_expires_at FROM subscriptions
            WHERE user_id = ? AND date(subscription_expires_at) >= date(?)
            LIMIT 1
            """,
            (callback.from_user.id, today_str),
        )
        sub_row = cur.fetchone()
        has_active_subscription = sub_row is not None
        subscription_expires_at = sub_row[0] if sub_row else None
        text = welcome_back_caption(has_active_subscription, subscription_expires_at)
    await callback.message.answer_photo(
        WELCOME_PHOTO,
        caption=text,
        parse_mode='HTML',
        reply_markup=generate_ikb_main(callback.from_user.id),
    )

@dp.callback_query(lambda c: c.data == 'trial')
async def plan_trial(callback: CallbackQuery):
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        url = None
        try:
            url = fetch_vpn_subscription_url_after_purchase(callback.from_user.id)
        except Exception as e:
            print(f'Ошибка при выдаче подписки (trial): {e}')
        if url:
            upsert_subscription_days(callback.from_user.id, VPN_SUBSCRIPTION_DAYS_PAID)
            try:
                await callback.message.answer_photo(
                    MY_KEYS_PHOTO,
                    caption=vpn_subscription_message_html(url),
                    parse_mode='HTML',
                    reply_markup=create_ikb_sub_after_buy(url),
                )
            except Exception as e:
                print(f'Ошибка отправки сообщения с ключом (trial): {e}')
        await callback.message.delete()
    else:
        await callback.message.answer('❌ Вы не подписаны на канал! Подпишитесь на канал, чтобы получить бесплатный тестовый период!', parse_mode='HTML', reply_markup=ikb_subscribe)


@dp.callback_query(lambda c: c.data == 'subscribe_confirmed')
async def subscribe_confirmed_callback(callback: CallbackQuery):
    await callback.answer("✅ Я подписался") # на пол экрана хуйня высветится
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        result = vpn.deliver_trial_vpn(callback.from_user.id)
        trial_url = _vpn_response_subscription_url(result) if isinstance(result, dict) else None
        if trial_url:
            upsert_subscription_days(callback.from_user.id, VPN_SUBSCRIPTION_DAYS_TRIAL)
            try:
                await callback.message.answer_photo(
                    MY_KEYS_PHOTO,
                    caption=vpn_subscription_message_html(trial_url)
                    + '\n\n✅ <b>Бесплатный тестовый период выдан!</b>',
                    parse_mode='HTML',
                    reply_markup=create_ikb_sub_after_buy(trial_url),
                )
            except Exception as e:
                print(f'Ошибка отправки trial-ключа: {e}')
        else:
            await callback.message.answer(
                '✅ Подписка на канал подтверждена. Если ключ не пришёл — нажми «Попробовать бесплатно» ещё раз или напиши в поддержку.',
                parse_mode='HTML',
                reply_markup=ikb_back,
            )
    else:
        await callback.message.answer('❌ Вы не подписаны на канал! Подпишитесь на канал, чтобы получить бесплатный тестовый период!', parse_mode='HTML', reply_markup=ikb_subscribe)

@dp.callback_query(lambda c: c.data.startswith('plan_lifetime'))
async def plan_lifetime_callback(callback: CallbackQuery):
    await callback.answer('Сейчас доступна только подписка на месяц. «Подключить VPN» → страна → оплата.', show_alert=True)


@dp.callback_query(
    lambda c: c.data.startswith('yookassa_')
    or (
        c.data.startswith('check_')
        and not c.data.startswith('check_payment_')
    )
)
async def check_payment_yookassa_callback(callback: CallbackQuery): # сюды
    await callback.answer("🔄 Проверка статуса оплаты") # на пол экрана хуйня высветится
    raw = callback.data
    # Два первых разбиения: префикс (check|yookassa), сумма, остаток — id платежа Юкассы (UUID с дефисами)
    parts = raw.split('_', 2)
    if len(parts) != 3:
        await callback.answer('❌ Устарела кнопка оплаты. Создайте платёж заново.', show_alert=True)
        return
    _, amount_str, payment_id = parts
    try:
        amount_rub = int(amount_str)
    except ValueError:
        await callback.answer('❌ Неверная сумма в данных кнопки.', show_alert=True)
        return
    # Убрали лишний print для экономии памяти
    if check_payment_yookassa_status(amount_rub, payment_id, callback.from_user.id):
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount_rub, callback.from_user.id))
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
                            cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (amount_rub / 2, ref_master_id)) # начислить 50% реферального бонуса рефоводу
            con.commit()
        url = None
        try:
            url = fetch_vpn_subscription_url_after_purchase(callback.from_user.id)
        except Exception as e:
            print(f'Ошибка при выдаче подписки после оплаты: {e}')
        if url:
            upsert_subscription_days(callback.from_user.id, VPN_SUBSCRIPTION_DAYS_PAID)
            try:
                await callback.message.answer_photo(
                    MY_KEYS_PHOTO,
                    caption=vpn_subscription_message_html(url),
                    parse_mode='HTML',
                    reply_markup=create_ikb_sub_after_buy(url),
                )
            except Exception as e:
                print(f'Ошибка отправки сообщения с ключом после оплаты: {e}')
        await callback.message.delete()

    else:
        await callback.message.answer(f'👀 Ожидаем оплату, оплатите и попробуйте снова!', parse_mode='HTML', reply_markup=ikb_back)

@dp.callback_query(lambda c: c.data.startswith('deposit_'))
async def process_deposit(callback: CallbackQuery):
    # Убрали лишний print для экономии памяти
    print(callback.data)
    _ , price , method = callback.data.split('_')
    
    amount = int(price)
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
            await callback.message.answer(f'👉 Создали заявку на оплату, переходите по ссылке и оплатите.\n\n <b>❗ После оплаты нажмите на кнопку "Я оплатил"</b>', parse_mode='HTML', reply_markup=create_yookassa_payment_keyboard(amount, confirmation_url, payment_id))
        except Exception as e:
            await callback.message.answer(f'❌ Не удалось создать заявку: {e}. Напишите в техподдержку, мы обязательно поможем!', reply_markup=ikb_support)
            raise e

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

@dp.message(F.text.startswith('shout '), (F.from_user.id.in_([1979477416, 7562967579])))
async def shout_message(message: Message):
    text = (message.text or '')[6:].strip()
    if not text:
        await message.answer("❌ Пустой текст. Пример: <code>shout Привет!</code>", parse_mode='HTML', reply_markup=ikb_back)
        return

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users;')
        result = cur.fetchall()

    sent = 0
    failed = 0
    blocked = 0
    for (uid,) in result:
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception as e:
            err_name = type(e).__name__
            if err_name == 'TelegramRetryAfter':
                wait_s = getattr(e, 'retry_after', 5) or 5
                await asyncio.sleep(float(wait_s) + 0.5)
                try:
                    await bot.send_message(uid, text)
                    sent += 1
                    continue
                except Exception:
                    failed += 1
            elif err_name in ('TelegramForbiddenError', 'TelegramNotFound'):
                blocked += 1
            else:
                failed += 1
                print(f'shout → {uid}: {err_name}: {e}')
        await asyncio.sleep(0.05)

    summary = (
        f"🔊 Рассылка завершена\n"
        f"✅ Отправлено: {sent}\n"
        f"🚫 Заблокировали бота: {blocked}\n"
        f"⚠️ Ошибок: {failed}\n"
        f"👥 Всего в базе: {len(result)}"
    )
    try:
        await message.answer(summary, reply_markup=ikb_back)
    except Exception:
        pass


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
                    reply_markup='ПОЧИНИТЬ'
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


async def main():
    asyncio.create_task(check_expired_subscriptions_table(bot))
    asyncio.create_task(check_expiring_tomorrow_subscriptions_table(bot))
    # Запускаем фоновую задачу для сброса флага runout_notified в 00:01 каждый день
    asyncio.create_task(reset_runout_notified_daily())
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())