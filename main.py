from datetime import date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, invoice, LabeledPrice, FSInputFile, MessageEntity
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from texts import *
from aiogram.fsm.context import FSMContext
import asyncio # для работы с асинхронными функциями
import html
import sqlite3 as sq
import requests
from prices import *
import dotenv
import os
from aiogram.fsm.state import State, StatesGroup
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
from logging.handlers import RotatingFileHandler
import logging
from sync_remna_expire_from_keys_once import get_user_by_tg_id
from vpn import Vpn
from ikbs import *

import sys
from expire_functions import *
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
print('BOT STARTED!!!')



LOG_PATH = os.getenv("LOG_PATH", "logs/bot.log")  # если нет env — берёт локальный путь
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(LOG_PATH, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

vpn = Vpn()


_SUBSCRIPTION_URL_KEYS = ( # не уверен
    'subscriptionUrl',
    'subscription_url',
    'subscriptionLink',
    'subscription_link',
)

class AdvCampaign(StatesGroup):
    waiting_name = State()
    waiting_description = State()
    waiting_custom_link = State()

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


def fetch_vpn_subscription_url_after_purchase(tg_id: int, paid_days: int | None = None):
    if paid_days is None:
        paid_days = VPN_SUBSCRIPTION_DAYS_PAID
    created = vpn.create_new_user(tg_id, days=paid_days)
    if created.get('errorCode'):
        if _vpn_response_user_already_exists(created):
            renewed = vpn.renew_subscription(tg_id, paid_days)  # renew сам обновляет БД
            return _vpn_response_subscription_url(renewed)
        return None
    # Новый пользователь — обновляем БД здесь
    upsert_subscription_days(tg_id, paid_days)
    return _vpn_response_subscription_url(created)


def vpn_subscription_message_html(url: str) -> str:
    return (
        "<tg-emoji emoji-id=\"5307843983102204243\">🔑</tg-emoji> Твоя подписка КОФЕМАНИЯ VPN\n\n" +
        "- Подключите до 3 устройств одновременно\n" +
        "- Обходит белые списки и ограничения мобильного интернета\n" +
        "- Установка в два клика - никаких настроек\n\n" +
        "<tg-emoji emoji-id=\"5310144251621809870\">✅</tg-emoji> В подписке от 25 ГБ трафика\n" +
        "- расходуется только на LTE серверах\n\n" +
        "<tg-emoji emoji-id=\"5420323339723881652\">⚠️</tg-emoji> В пробной подписке - 3 ГБ на 3 дня\n\n" +
        "<tg-emoji emoji-id=\"5375381102586247966\">🔗</tg-emoji> Твоя ссылка: "
        f"<pre>{url}</pre>"

    )


### РАБОТА С ФОТКАМИ:
try:
    WELCOME_PHOTO = FSInputFile("photos/welcome.png")
    BUY_VPN_PHOTO = FSInputFile("photos/buy_vpn.jpg")
    DOCUMENTS_PHOTO = FSInputFile("photos/documents.png")
    INVITE_FRIEND_PHOTO = FSInputFile("photos/invite_friend.jpg")
    MY_KEYS_PHOTO = FSInputFile("photos/my_keys.png")
    DEPOSIT_PHOTO = FSInputFile("photos/deposit.png")
    DEVICES_PHOTO = FSInputFile('photos/devices.png')
    BUY_GBS_PHOTO = FSInputFile('photos/buy_gbs.png')
    LIMITED_OFFER_PHOTO = FSInputFile('photos/LIMITED OFFER.png')
    INVITE_FRIEND_COLORED_PHOTO = FSInputFile('photos/invite_friend_colored.png')
    PING_UNCONNECTED_PHOTO = FSInputFile('photos/ping_unconnected.jpg')
    PING_UNACTIVE_PHOTO=FSInputFile('photos/ping_unactive_photo.jpg')
except FileNotFoundError:
    print("Photo files not found")
    exit()

bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота

create_tables()

dp = Dispatcher() # объект диспетчера


def format_date_ru(dt) -> str:
    return f"{dt.day} {MONTHS_RU[dt.month]} {dt.year}"




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
                            pass
                        cur.execute('UPDATE users SET ref_amount = ref_amount + 1 WHERE id = ?', (ref,))
                con.commit()


    # print(user)
    try:
        user = vpn.get_user_by_tg_id(message.from_user.id)
        expire_at_str = user['response'][0]['expireAt']
    except:
        expire_at_str = None
    if expire_at_str:
        expire_at = datetime.fromisoformat(expire_at_str)
        has_active_subscription = expire_at.date() > date.today()
        subscription_expires_at = format_date_ru(expire_at)
    else:
        has_active_subscription = False
        subscription_expires_at = None

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

# ОБРАБОТЧИКИ КОЛЛБЭКОВ
@dp.callback_query(lambda c: c.data == 'buy_vpn')
async def buy_vpn_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("🛒 Раздел покупки VPN") # на пол экрана хуйня высветится
    await callback.message.answer_photo(FSInputFile("photos/buy_vpn.png"), caption= (
        '<b>В подписку входит:</b>\n  \n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> Неограниченная скорость</i> \n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> Обход белых списков</i> \n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> Множество локаций </i>\n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> До 3 устройств </i>\n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> Безотказная работа </i>\n'
        '<i><tg-emoji emoji-id="5233346147560465779">🟢</tg-emoji> Отзывчивая техподдержка </i>\n'


    ), parse_mode='HTML', reply_markup=generate_ikb_duration_choose(callback.from_user.id))


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
        reply_markup=generate_ikb_duration_choose(callback.from_user.id),
    )

@dp.callback_query(F.data == 'buy_lte_gigabytes')
async def lte_gigabytes(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer_photo(BUY_GBS_PHOTO, caption=buy_gbs_text, parse_mode='HTML', reply_markup=ikb_gbs_variants)

@dp.callback_query(F.data.startswith('gbs_'))
async def buy_gbs(callback: CallbackQuery):
    await callback.message.delete()
    gb_amount = int(callback.data.replace('gbs_', ''))
    price = GBS_PRICES.get(gb_amount)
    if price is None:
        await callback.message.answer("❌ Неверный тариф")
        return
    try:
        payment = await asyncio.to_thread(Payment.create, {
            "amount": {
                "value": f"{price}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": "https://t.me/coffemaniaVPNbot"
            },
            "capture": True,
            "description": f"Покупка дополнительных {gb_amount} ГБ id={callback.from_user.id} username = {callback.from_user.username}",
            "metadata": {
            "user_id": callback.from_user.id,
        }
        }, uuid.uuid4())

        payment_id = payment.id
        confirmation_url = payment.confirmation.confirmation_url
        await callback.message.answer(
            f'👉 Создали заявку на оплату, переходите по ссылке и оплатите.\n\n <b>❗ После оплаты нажмите на кнопку "Я оплатил"</b>',
            parse_mode='HTML',
            reply_markup=create_yookassa_gb_payment(payment_id, gb_amount, confirmation_url, price))
    except Exception as e:
        await callback.message.answer(
            '❌ Не удалось создать заявку. Напишите в техподдержку, мы обязательно поможем!',
            reply_markup=ikb_support,
        )
        print(f'process_deposit error: {type(e).__name__}: {e}')

@dp.callback_query(F.data.startswith('gb_yookassa_'))
async def process_gb_addition(callback: CallbackQuery):

    try:

        data = callback.data.replace('gb_yookassa_', '').split('_')

        pid = data[0]
        gb_amount = int(data[1])
        price = int(data[2])

        status = await asyncio.to_thread(
            check_payment_yookassa_status,
            price,
            pid,
            callback.from_user.id
        )

        if status == 'paid':

            body = await asyncio.to_thread(
                Vpn().give_lte_gbs,
                callback.from_user.id,
                gb_amount
            )

            print(body)

            try:
                await callback.message.delete()
            except:
                pass

            await callback.message.answer(
                text=f'Успешно добавили вам +{gb_amount} ГБ к LTE трафику!',
                parse_mode='HTML',
                reply_markup=ikb_back
            )

        else:

            await callback.message.answer(
                text='Ваша оплата не прошла. Попробуйте еще раз!',
                parse_mode='HTML',
                reply_markup=ikb_back
            )

    except Exception as e:

        print(f'process_gb_addition error: {type(e).__name__}: {e}')

        await callback.message.answer(
            '❌ Ошибка проверки оплаты',
            reply_markup=ikb_back
        )

@dp.callback_query(F.data == 'device_list')
async def devices_list_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer_photo(DEVICES_PHOTO, caption=get_devices_list_text(callback.from_user.id), parse_mode='HTML', reply_markup = create_ikb_devices(callback.from_user.id))

@dp.callback_query(F.data.startswith('delete_device_'))
async def delete_device(callback: CallbackQuery):
    hwid = callback.data.replace('delete_device_', '')
    await callback.message.delete()
    try:
        Vpn().delete_hwid_device(callback.from_user.id, hwid)
        await callback.message.answer(text=hwid_deleted_text, parse_mode = 'HTML', reply_markup=ikb_back)
    except Exception as e:
        await callback.message.answer(text=f'Ошибка удаления устройства: \n {e}\n\n Перешлите это сообщение в поддержку.', reply_markup=ikb_support)

@dp.callback_query(lambda c: c.data == 'documents')
async def documents_callback(callback: CallbackQuery):
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
        cur.execute('SELECT ref_withdraw FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone()
        ref_withdraw = result[0]
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
                    f"🧮 Ваша доля: {ref_share} ₽\n"
                    f"🏦 Выведено: {int(ref_withdraw)} ₽"
                    '\nДля вывода обращаться @yatogotsirka'
                ),
                parse_mode='HTML',
                reply_markup=ikb_referral,
            )
            return
    await callback.message.answer_photo(INVITE_FRIEND_PHOTO, caption=f"🤝 <b>Пригласить друга</b>\n\nВаша реферальная ссылка:\n<code>https://t.me/coffemaniaVPNbot?start={callback.from_user.id}</code>\n\nВсего приведено друзей: {ref_amount}\n\n<tg-emoji emoji-id='5407064977544583568'>👌</tg-emoji> <b>За каждого приглашенного друга, который пополнит баланс вы получаете 7 дней подписки!</b>", parse_mode='HTML', reply_markup=ikb_referral)


@dp.callback_query(lambda c: c.data == 'support')
async def support_callback(callback: CallbackQuery):
    await callback.answer("ℹ️ Поддержка") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("ℹ️ <b>Поддержка</b>\n\nЕсли у вас возникли вопросы, напишите нам в поддержку!", parse_mode='HTML', reply_markup=ikb_support)


def welcome_back_caption(has_active: bool, subscription_expires_at=None) -> str:
    if has_active and subscription_expires_at:
        sub_line = f"🟢 Активна до <b>{subscription_expires_at}</b>"
    elif has_active:
        sub_line = '🟢 Активна'
    else:
        sub_line = '🔴 Отсутствует'
    return (
        "👋 Добро пожаловать в Кофеманию\n"
        "\n"
        "📦 Информация о подписке\n"
        "├ Множество серверов + обход LTE\n" 
        "├ До 3-х устройств\n"
        f"└ Подписка: {sub_line}\n" # sub_line - это строка с информацией о подписке
        
    )

@dp.callback_query(lambda c: c.data == 'back')
async def back_callback(callback: CallbackQuery):
    await callback.answer("Назад") # на пол экрана хуйня высветится
    await callback.message.delete()

    # print(user)
    try:
        user = vpn.get_user_by_tg_id(callback.from_user.id)
        expire_at_str = user['response'][0]['expireAt']
    except:
        expire_at_str = None
    if expire_at_str:
        expire_at = datetime.fromisoformat(expire_at_str)
        has_active_subscription = expire_at.date() > date.today()
        subscription_expires_at = format_date_ru(expire_at)
    else:
        has_active_subscription = False
        subscription_expires_at = None

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
        result = vpn.deliver_trial_vpn(callback.from_user.id)
        url = _vpn_response_subscription_url(result) if isinstance(result, dict) else None
        if url:
            upsert_subscription_days(callback.from_user.id, VPN_SUBSCRIPTION_DAYS_TRIAL)  # 3 дня
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (callback.from_user.id,))
            try:
                await callback.message.answer_photo(
                    MY_KEYS_PHOTO,
                    caption=vpn_subscription_message_html(url),
                    parse_mode='HTML',
                    reply_markup=create_ikb_sub_after_buy(url),
                )
            except Exception as e:
                print(f'Ошибка отправки сообщения с ключом (trial): {e}')
    else:
        await callback.message.answer('❌ Вы не подписаны на канал!', parse_mode='HTML', reply_markup=ikb_subscribe)

@dp.callback_query(lambda c: c.data == 'subscribe_confirmed')
async def subscribe_confirmed_callback(callback: CallbackQuery):
    await callback.answer("✅ Я подписался") # на пол экрана хуйня высветится
    await callback.message.delete()
    if await is_subscribed(bot, callback.from_user.id):
        result = vpn.deliver_trial_vpn(callback.from_user.id)
        trial_url = _vpn_response_subscription_url(result) if isinstance(result, dict) else None
        if trial_url:
            upsert_subscription_days(callback.from_user.id, VPN_SUBSCRIPTION_DAYS_TRIAL)
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (callback.from_user.id,))
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
async def check_payment_yookassa_callback(callback: CallbackQuery):
    raw = callback.data
    parts = raw.split('_', 3)

    # теперь принимаем только новый формат
    if len(parts) != 4:
        await callback.answer('❌ Устарела кнопка оплаты. Создайте платёж заново.', show_alert=True)
        return

    _, amount_str, days_str, payment_id = parts

    try:
        amount_rub = int(amount_str)
        paid_days = int(days_str)
    except ValueError:
        await callback.answer('❌ Неверные данные в кнопке оплаты.', show_alert=True)
        return

    # expected_amount = SUBSCRIPTION_PLAN_PRICES.get(paid_days)
    # if expected_amount is None or expected_amount != amount_rub:
    #     await callback.answer('❌ Сумма не соответствует тарифу. Создайте платёж заново.', show_alert=True)
    #     return

    payment_state = await asyncio.to_thread(check_payment_yookassa_status,amount_rub, payment_id, callback.from_user.id )

    if payment_state == 'paid':
        # Реферальный блок (оставлен без изменений)
        try:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute(
                    'SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?',
                    (callback.from_user.id,),
                )
                ref_master = cur.fetchone()

                if ref_master:
                    ref_master_id = ref_master[0]
                    registration_date_str = ref_master[1]

                    if registration_date_str:
                        registration_date = date.fromisoformat(registration_date_str)
                        three_months_later = registration_date + timedelta(days=90)

                        if date.today() <= three_months_later:
                            cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                            ref_master_role_row = cur.fetchone()
                            ref_master_role = ref_master_role_row[0] if ref_master_role_row else None

                            if ref_master_role == 'refmaster':
                                cur.execute(
                                    'UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?',
                                    (amount_rub // 2, ref_master_id),
                                )
                            else:
                                cur.execute(
                                    'SELECT * FROM users WHERE received_bonus = 0 AND id = ?',
                                    (callback.from_user.id,),
                                )
                                result = cur.fetchone()
                                if result is not None:
                                    cur.execute(
                                        'UPDATE users SET received_bonus = 1 WHERE id = ?',
                                        (callback.from_user.id,),
                                    )
                                    try:
                                        await asyncio.to_thread(vpn.renew_subscription, ref_master_id, 7)
                                        await bot.send_message(
                                            ref_master_id,
                                            '<tg-emoji emoji-id="5416117059207572332">➡️</tg-emoji> Ваш реферал совершил депозит, вы получили бонусом 7 дней подписки!',
                                            parse_mode='HTML',
                                            reply_markup=ikb_my_sub,
                                        )
                                    except Exception as e:
                                        print(f'Ошибка выдачи реф-бонуса для {ref_master_id}: {e}')

                con.commit()

        except Exception as e:
            print(f'Ошибка реферального блока для {callback.from_user.id}: {e}')

        # Выдача подписки
        url = None
        try:
            url = fetch_vpn_subscription_url_after_purchase(callback.from_user.id, paid_days=paid_days)
        except Exception as e:
            print(f'Ошибка при выдаче подписки после оплаты для {callback.from_user.id}: {e}')

        try:
            await callback.message.delete()
        except Exception:
            pass

        if url:
            try:
                await callback.message.answer_photo(
                    MY_KEYS_PHOTO,
                    caption=vpn_subscription_message_html(url),
                    parse_mode='HTML',
                    reply_markup=create_ikb_sub_after_buy(url),
                )
            except Exception as e:
                print(f'Ошибка отправки сообщения с ключом для {callback.from_user.id}: {e}')
        else:
            await callback.message.answer(
                '✅ Оплата прошла успешно!\n\n'
                '⚠️ Не удалось автоматически выдать ключ. '
                'Откройте «Моя подписка» — ключ там уже должен быть. '
                'Если нет — напишите в поддержку.',
                parse_mode='HTML',
                reply_markup=ikb_my_sub,
            )

    elif payment_state == 'already_processed':
        await callback.message.answer(
            '✅ Этот платёж уже обработан ранее. Если доступ не появился, откройте «Моя подписка».',
            parse_mode='HTML',
            reply_markup=ikb_my_sub,
        )
    elif payment_state in ('timeout', 'error'):  # 👈 вот сюда, в конец цепочки
        await callback.answer(
            "⏳ Сервис оплаты не отвечает. Подождите минуту и нажмите «Я оплатил» снова.",
            show_alert=True,
        )
    else:
        await callback.message.answer(
            '👀 Ожидаем оплату, оплатите и попробуйте снова!',
            parse_mode='HTML',
        )


@dp.callback_query(lambda c: c.data.startswith('deposit_'))
async def process_deposit(callback: CallbackQuery):
    parts = callback.data.split('_')

    if len(parts) != 4:
        await callback.message.answer(
            '❌ Неверные данные платежа. Выберите тариф заново.',
            reply_markup=ikb_back
        )
        return

    _, price, days_str, method = parts

    try:
        amount = int(price)
        paid_days = int(days_str)
    except ValueError:
        await callback.message.answer(
            '❌ Неверные данные суммы/тарифа. Выберите тариф заново.',
            reply_markup=ikb_back
        )
        return

    try:
        await callback.message.delete()
    except:
        pass

    if method == 'card':
        try:
            payment = await asyncio.to_thread(Payment.create,{
                "amount": {
                    "value": amount,
                    "currency": "RUB"
                },
                "description": f"Покупка подписки на {paid_days} дней id={callback.from_user.id} username={callback.from_user.username} ",
                "capture": True,
                "confirmation": {
                    "type": "redirect",
                    "return_url": "https://t.me/coffemaniaVPNbot",
                },
                "metadata": {
                    "user_id": callback.from_user.id,
                }
            }, uuid.uuid4())

            payment_id = payment.id
            confirmation_url = payment.confirmation.confirmation_url

            await callback.message.answer(
                '👉 Создали заявку на оплату, переходите по ссылке и оплатите.\n\n'
                '<b>❗ После оплаты нажмите на кнопку "Я оплатил"</b>',
                parse_mode='HTML',
                reply_markup=create_yookassa_payment_keyboard(
                    amount,
                    paid_days,
                    confirmation_url,
                    payment_id
                )
            )

        except Exception as e:
            await callback.message.answer(
                '❌ Не удалось создать заявку. Напишите в техподдержку, мы обязательно поможем!',
                reply_markup=ikb_support,
            )
            print(f'process_deposit error: {type(e).__name__}: {e}')
            raise

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
            await bot.send_message(uid, text, parse_mode='HTML')
            sent += 1
        except Exception as e:
            err_name = type(e).__name__
            if err_name == 'TelegramRetryAfter':
                wait_s = getattr(e, 'retry_after', 5) or 5
                await asyncio.sleep(float(wait_s) + 0.5)
                try:
                    await bot.send_message(uid, text, parse_mode = 'HTML')
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

@dp.callback_query(F.data == 'admin_users')
async def admin_users_callback(callback: CallbackQuery):
    await callback.answer("👤 Пользователи") # на пол экрана хуйня высветится
    await callback.message.delete() # удаляем соо на котором нажали на кнопку
    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = date.today()
        today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
        # Оптимизация: обновляем has_active_keys одним запросом вместо цикла
        # Сначала устанавливаем всем 0
        cur.execute("UPDATE users SET has_active_subscription = 0")
        # Затем устанавливаем 1 тем, у кого есть активные подписки
        cur.execute('''
            UPDATE users 
            SET has_active_subscription = 1 
            WHERE id IN (
                SELECT DISTINCT user_id 
                FROM subscriptions 
                WHERE subscription_expires_at >= ?
            )
        ''', (today_str,))
        cur.execute("""SELECT users.id, users.username, users.ref_balance, users.ref_amount, users.had_trial, users.ref_withdraw, users.received_bonus, subscriptions.subscription_expires_at FROM users 
                    LEFT JOIN subscriptions ON users.id = subscriptions.user_id
        """)
        result = cur.fetchall()
        # используя пандас содаем xlsx файл
        df = pd.DataFrame(result, columns=['ID', 'Username', 'РефБаланс', 'Кол-во Рефов', 'had_trial', 'Сумма вывода ', 'received_bonus', 'Дата окончания подписки'])
        
        # Вычисляем статистику
        total_users = len(df)
        had_trial_count = len(df[df['had_trial'] == 1])
        has_active_sub_count = len(df[df['Дата окончания подписки'] >= today_str])
        
        had_trial_percent = (had_trial_count / total_users * 100) if total_users > 0 else 0
        has_active_sub_percent = (has_active_sub_count / total_users * 100) if total_users > 0 else 0

        # Добавляем колонки со статистикой
        df['Had_trial_%'] = round(had_trial_percent, 2)
        df['Has_active_keys_%'] = round(has_active_sub_percent, 2)
        
        df.to_excel('users.xlsx', index=False)
        try:
            await callback.message.answer_document(document=FSInputFile('users.xlsx'), reply_markup=ikb_admin_back)
        finally:
            # Удаляем файл после отправки, чтобы не засорять диск
            try:
                os.remove('users.xlsx')
            except:
                pass

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

@dp.callback_query(F.data == 'admin_keys')
async def admin_keys_callback(callback: CallbackQuery):
    await callback.answer("🔑 Подписки") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT user_id, subscription_expires_at, runout_notified , expiring_tomorrow_notified FROM subscriptions')
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['user_id', 'subscription_expires_at', 'runout_notified', 'expiring_tomorrow_notified'])
        df.to_excel('subscriptions.xlsx', index=False)
        try:
            await callback.message.answer_document(document=FSInputFile('subscriptions.xlsx'), reply_markup=ikb_admin_back)
        finally:
            # Удаляем файл после отправки, чтобы не засорять диск
            try:
                os.remove('subscriptions.xlsx')
            except:
                pass

@dp.callback_query(lambda c: c.data == 'admin_notify_sale')
async def admin_notify_sale(callback: CallbackQuery):
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cur.execute(
            'SELECT user_id FROM subscriptions WHERE subscription_expires_at < ?',
            (today,)
        )
        result = cur.fetchall()

    success = 0
    fail = 0

    for user in result:
        try:
            await bot.send_message(
                user[0],
                (
                    "<tg-emoji emoji-id='5436319619000313467'>🛑</tg-emoji> СКОРО ТЫ ПОТЕРЯЕШЬ ВСЁ...\n"
                    "\n"
                    "Твой доступ к Telegram, YouTube и любимым Reels под угрозой...\n"
                    "\n"
                    "Мы создали сервис, который работает по принципу «включил и забыл»:\n"
                    "\n"
                    "<tg-emoji emoji-id='5436087613456918666'>✅</tg-emoji> Сервера переключаются автоматически.\n"
                    "<tg-emoji emoji-id='5307965711065292927'>🚀</tg-emoji> Одинаково стабильно летит ДАЖЕ ПРИ ГЛУШИЛКАХ\n"
                    "<tg-emoji emoji-id='5433895041242246420'>🎙</tg-emoji> Никаких настроек - всё просто работает.\n"
                    "\n"
                    "Для тебя подписка с 'обходом' за всего 99₽\n"
                    "\n"
                    "Больше не думай о том, какой VPN сегодня заработает. Просто пользуйся НАШИМ."
                ),
                parse_mode='HTML',
                reply_markup=ikb_sale
            )

            success += 1

        except Exception as e:
            print(e)
            fail += 1

        await asyncio.sleep(0.05)  # 👈 маленький дилей

    await callback.message.answer(
        f"Итого:\n\n✅ {success}\n\n❌ {fail}",
        reply_markup=ikb_admin_back
    )


@dp.callback_query(lambda c: c.data == 'admin_notify_trial')
async def admin_notify_trial_callback(callback: CallbackQuery):
    await callback.answer("🔊 Напомнить юзерам о бесплатном тестовом периоде") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users WHERE had_trial != 1 AND has_active_subscription = 0')
        result = cur.fetchall()
        success = 0
        fail = 0
        for user in result:
            try:
                await bot.send_message(user[0], (
                    "Похоже, ты не успел попробовать VPN 👌\n\n"
                    'Коротко, почему его вообще стоит включить хотя бы раз:\n'
                    '— установка и запуск &lt; 1 минуты\n'
                    '— включил и забыл (работает в фоне)\n'
                    '— ВК и ру-сервисы не ломаются\n'
                    '— быстрый обход блокировок без тормозов\n\n'
                    '👉 Можешь просто зайти и проверить — без лишних настроек\n\n'
                    '🎁 Если подключишься сегодня — получишь 3 дня бесплатной подписки\n\n'
                    'Жми /start'

                                                 ), parse_mode='HTML')
                success+=1
            except Exception as e:
                print(e)
                fail+=1
                pass
    await callback.message.answer(f"Итого: \n\n ✅ {success} \n\n ❌ {fail} ", parse_mode='HTML', reply_markup=ikb_admin_back)



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
                await bot.send_photo(
                    user[0],
                    INVITE_FRIEND_COLORED_PHOTO,
                    caption=(
                            '<tg-emoji emoji-id="5458908492687497206">🔥</tg-emoji> Приведи друга — получи <b>7 дней VPN бесплатно</b> \n\n'
                            '<b>Как это работает:</b>\n'
                            '• друг регистрируется по твоей ссылке\n'
                            '• покупает подписку\n'
                            '• ты получаешь бонус\n\n'
                            ),
                    parse_mode='HTML',
                    reply_markup=ikb_referral_reminder
                )
                sent_count += 1
            except Exception as e:
                print(e)
                failed_count += 1
                pass
            await asyncio.sleep(0.5)  # задержка 0.5 секунды

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
        cur.execute("""SELECT u.id, u.username, u2.id, u2.username, r.ref_master_id,  r.referral_id FROM referal_users as r 
                    JOIN users as u ON r.ref_master_id = u.id
                    JOIN users as u2 ON r.referral_id = u2.id """)
        result = cur.fetchall()
        df = pd.DataFrame(result, columns=['Рефовод Айди', 'Рефовод Юзернейм', 'Реферал Айди', 'Реферал Юзернейм', 'Рефмастер Айди' , 'Реферал айди'])
        df.to_excel('referals.xlsx')
        await callback.message.answer_document(FSInputFile('referals.xlsx'), reply_markup=ikb_admin_back)

@dp.callback_query((F.data == 'adv_campaigns') & (F.from_user.id.in_([7562967579, 1979477416])))
async def adv_campaigns_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(text='Выберите:', reply_markup=ikb_adv_campaigns_menu)

@dp.callback_query(F.data == 'adv_new_campaign_create')
async def create_adv_campaign(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.set_state(AdvCampaign.waiting_name)
    await callback.message.answer("Введите название кампании:")

@dp.message(AdvCampaign.waiting_name)
async def get_campaign_name(message: Message,state: FSMContext):
    name = message.text
    await state.update_data(campaign_name = name)
    await state.set_state(AdvCampaign.waiting_description)
    await message.answer("Введите описание кампании:")

@dp.message(AdvCampaign.waiting_description)
async def get_campaign_description(message: Message, state: FSMContext):
    description = message.text
    data = await state.get_data()
    name = data["campaign_name"]

    with sq.connect('database.db') as con:
        cur = con.cursor()

        cur.execute("""
            INSERT INTO adv_campaigns (campaign_name, campaign_description, campaign_link)
            VALUES (?, ?, ?)
        """, (name, description, ""))

        row_id = cur.lastrowid
        link = f"https://t.me/coffemaniaVPNbot?start={row_id}"

        cur.execute("""
            UPDATE adv_campaigns
            SET campaign_link = ?
            WHERE rowid = ?
        """, (link, row_id))

        con.commit()

    await state.clear()

    await message.answer(
        f"Кампания создана\nID: {row_id}\nСсылка: {link}",
        reply_markup=ikb_adv_back
    )

@dp.callback_query(F.data == 'adv_get_campaigns')
async def get_campaigns(callback : CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(text='Список кампаний:',reply_markup=generate_ikb_campaigns_list())


@dp.callback_query(F.data.startswith('adv_campaign_'))
async def adv_campaigns(callback: CallbackQuery):
    await callback.message.delete()
    campaign_name = callback.data.replace('adv_campaign_','')
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT campaign_name, campaign_description, campaign_link FROM adv_campaigns WHERE campaign_name == ?', (campaign_name,))
        result = cur.fetchone()
        campaign_id = result[-1].replace('https://t.me/coffemaniaVPNbot?start=', '')
        cur.execute('SELECT COUNT(*) FROM referal_users WHERE ref_master_id = ?', (campaign_id,))
        refs_total = (cur.fetchone() or (0,))[0]
        cur.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(CAST(t.amount AS INTEGER)), 0)
            FROM transactions t
            JOIN referal_users r ON r.referral_id = t.user_id
            WHERE r.ref_master_id = ?
              AND t.type IN ('CryptoBot', 'yookassa')
            """,
            (campaign_id,),
        )
        dep_stats = cur.fetchone() or (0, 0)
        deposits_count = dep_stats[0] or 0
        deposits_total = int(dep_stats[1] or 0)
        # "Ваша доля" считаем строго как 50% от депозитов рефералов.
        # Фиксированные бонусы 50₽ за приглашения сюда не входят.
    try:
        await callback.message.answer(
        "<b>Название: " + str(result[0]) + "</b>\n\n"
       "Описание: " + str(result[1]) + "\n\n"
         "Ссылка: " + str(result[2]) + "\n\n"
         "-------------------------\n"
         "👥 Количество рефералов: " + str(refs_total) + "\n"
          "💳 Количество депозитов: " + str(deposits_count) + "\n"
         "💰 Общая сумма депозитов: " + str(deposits_total) + " ₽",
            parse_mode="HTML",
            reply_markup=ikb_adv_back
        )
    except Exception as e:
        await callback.message.answer(e)

@dp.callback_query(F.data == 'ping_unactive')
async def ping_unactive_users(callback: CallbackQuery):
    users = vpn.get_unactive_users()

    for user in users:
        try:
            await bot.send_photo(
                chat_id=user,
                photo=PING_UNACTIVE_PHOTO,
                caption="""
😂 <b> Всего 6 рублей в день </b> 

Именно столько стоит свободный доступ в интернет с нами: Ютуб, Рилсы, ТикТок, Нейросети. Делает твою жизнь комфортнее, а доступ к контенту и знаниям неограниченным.

<b>Почему это выгодно именно сейчас:</b>

<tg-emoji emoji-id="5208705542226202990">☑️</tg-emoji> Авторский обход белых списков
<tg-emoji emoji-id="5208705542226202990">☑️</tg-emoji> Триал - 3 дня без платежа, без обязательств
<tg-emoji emoji-id="5208705542226202990">☑️</tg-emoji> Настроили все за тебя

Присоединяйся к нам. Всё просто.
""",
                parse_mode="HTML",
                reply_markup=ikb_unactive_ping_button,
            )

            await asyncio.sleep(0.1)

        except Exception as e:
            logging.exception(
                f"Ошибка при отправке сообщения пользователю {user}: {e}"
            )

async def main():
    asyncio.create_task(check_expired_subscriptions_table(bot))
    asyncio.create_task(check_expiring_tomorrow_subscriptions_table(bot))
    asyncio.create_task(notify_gbs_ending(bot))
    asyncio.create_task(notify_inactive_trial_users(bot))
    # Запускаем фоновую задачу для сброса флага runout_notified в 00:01 каждый день
    asyncio.create_task(reset_runout_notified_daily())
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())