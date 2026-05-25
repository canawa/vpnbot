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
from referrals import (
    REFMASTER_ROLE,
    REFMASTER_20_ROLE,
    REFMASTER_20_DEPOSIT_BONUS_RUB,
    REFMASTER_20_MIN_DEPOSIT_RUB,
    role_has_refmaster_ui,
    role_uses_deposit_share,
    role_uses_fixed_deposit_bonus,
    role_display_name,
    apply_deposit_reward_to_ref_partner,
    should_grant_subscription_referral_bonus,
    mark_subscription_referral_bonus_used,
    estimated_earnings_from_deposits,
    format_admin_campaign_stats,
)
import locale
from emojis import get_emoji
from databases import (
    create_tables,
    upsert_subscription_days,
    referral_link,
    resolve_ref_master_id,
    set_custom_ref_code,
    fetch_all_referrers_progress,
    get_adv_campaign_dashboard,
    get_ref_partner_dashboard,
    list_adv_campaigns,
)
from payments import get_pay_link, check_payment_status, check_payment_yookassa_status, rub_to_usdt
from logging.handlers import RotatingFileHandler
import logging
from sync_remna_expire_from_keys_once import get_user_by_tg_id
from vpn import Vpn
from ikbs import *

import sys
from expire_functions import *
from funnel import (
    funnel_on_first_seen,
    funnel_on_trial_started,
    funnel_on_paid,
    run_funnel_worker,
    setup_funnel,
    reset_funnel_for_test,
    FUNNEL_SLEEP_SEC,
    fetch_funnel_stats,
)
from renewal_funnel import renewal_on_paid, run_renewal_funnel_worker, fetch_renewal_stats
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

ADMIN_IDS = (1979477416, 7562967579)

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
    waiting_campaign_id = State()


class AdminRefmaster(StatesGroup):
    waiting_user_id = State()

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
    INVITE_MAX_COLORED_PHOTO=FSInputFile('photos/INVITE_MAX_COLORED.jpg')
    DECISION_PHOTO=FSInputFile('photos/decision.jpg')

except FileNotFoundError:
    print("Photo files not found")
    exit()

bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота

create_tables()

dp = Dispatcher() # объект диспетчера


def format_date_ru(dt) -> str:
    return f"{dt.day} {MONTHS_RU[dt.month]} {dt.year}"




async def _activate_trial_for_user(callback: CallbackQuery) -> bool:
    """Выдача триала после проверки канала. True — ключ отправлен."""
    uid = callback.from_user.id
    if not await is_subscribed(bot, uid):
        await callback.message.answer(
            '❌ Вы не подписаны на канал!',
            parse_mode='HTML',
            reply_markup=ikb_subscribe,
        )
        return False
    result = vpn.deliver_trial_vpn(uid)
    url = _vpn_response_subscription_url(result) if isinstance(result, dict) else None
    if not url:
        await callback.message.answer(
            'Не удалось выдать ключ. Попробуйте ещё раз или напишите в поддержку.',
            parse_mode='HTML',
            reply_markup=ikb_support,
        )
        return False
    upsert_subscription_days(uid, VPN_SUBSCRIPTION_DAYS_TRIAL)
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('UPDATE users SET had_trial = 1 WHERE id = ?', (uid,))
        con.commit()
    funnel_on_trial_started(uid)
    try:
        await callback.message.answer_photo(
            MY_KEYS_PHOTO,
            caption=vpn_subscription_message_html(url) + '\n\n✅ <b>Бесплатный тестовый период выдан!</b>',
            parse_mode='HTML',
            reply_markup=create_ikb_sub_after_buy(url),
        )
    except Exception as e:
        print(f'Ошибка отправки trial-ключа: {e}')
    return True


async def _register_referral(referral_id: int, ref_master_id: int, referral_username: str | None):
    if referral_id == ref_master_id:
        return
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT 1 FROM users WHERE id = ?', (referral_id,))
        if cur.fetchone() is not None:
            return
        cur.execute('SELECT * FROM referal_users WHERE referral_id = ?', (referral_id,))
        if cur.fetchone():
            return
        try:
            await bot.send_message(
                ref_master_id,
                f' <b>🎉 У вас новый реферал - {referral_username}!</b>',
                parse_mode='HTML',
            )
        except Exception:
            pass
        registration_date = date.today().isoformat()
        cur.execute('SELECT username FROM users WHERE id = ?', (ref_master_id,))
        ref_master_username_row = cur.fetchone()
        ref_master_username = ref_master_username_row[0] if ref_master_username_row else None
        cur.execute(
            'INSERT OR IGNORE INTO referal_users (referral_id, ref_master_id, registration_date, referral_username, ref_master_username) VALUES (?, ?, ?, ?, ?)',
            (referral_id, ref_master_id, registration_date, referral_username, ref_master_username),
        )
        con.commit()
        cur.execute('UPDATE users SET ref_amount = ref_amount + 1 WHERE id = ?', (ref_master_id,))
        con.commit()


@dp.message(CommandStart())
async def start_command(message):

    ref_master_id = None
    parts = (message.text or '').split(maxsplit=1)
    if len(parts) > 1:
        ref_master_id = resolve_ref_master_id(parts[1])
    if ref_master_id:
        await _register_referral(
            message.from_user.id,
            ref_master_id,
            message.from_user.username,
        )

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

    funnel_on_first_seen(message.from_user.id)
    generate_ikb_main(message.from_user.id)

# ОБРАБОТЧИКИ КОЛЛБЭКОВ
@dp.callback_query(lambda c: c.data == 'buy_vpn')
async def buy_vpn_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("🛒 Раздел покупки VPN") # на пол экрана хуйня высветится
    await callback.message.answer_photo(BUY_VPN_PHOTO, caption= (
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
            callback.from_user.id,
            'yookassa_gb',
        )

        if status == 'paid':
            body = await asyncio.to_thread(
                Vpn().give_lte_gbs,
                callback.from_user.id,
                gb_amount
            )
            print(body)
            success_text = f'Успешно добавили вам +{gb_amount} ГБ к LTE трафику!'
        elif status == 'already_processed':
            success_text = (
                f'✅ Этот платёж уже обработан. +{gb_amount} ГБ должны быть на аккаунте.'
            )
        else:
            await callback.message.answer(
                text='Ваша оплата не прошла. Попробуйте еще раз!',
                parse_mode='HTML',
                reply_markup=ikb_back
            )
            return

        try:
            await callback.message.delete()
        except Exception:
            pass

        await callback.message.answer(
            text=success_text,
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
        if role_has_refmaster_ui(role):
            cur.execute('SELECT ref_balance FROM users WHERE id = ?', (callback.from_user.id,))
            ref_balance_row = cur.fetchone()
            ref_balance = int(ref_balance_row[0] or 0) if ref_balance_row else 0
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
            ref_withdraw = int(ref_withdraw or 0)
            available = ref_balance - ref_withdraw

            if role_uses_deposit_share(role):
                model_line = (
                    '<tg-emoji emoji-id="5474417568053745249">🌱</tg-emoji> '
                    f'Ваша доля (50% от депозитов): {int(deposits_total * 0.5)} ₽\n'
                )
            else:
                model_line = (
                    '<tg-emoji emoji-id="5474417568053745249">🌱</tg-emoji> '
                    f'Бонус: +{REFMASTER_20_DEPOSIT_BONUS_RUB} ₽ за продление подписки '
                    f'реферала от 149 ₽ (покупка ГБ не считается)\n'
                )

            await callback.message.answer_photo(
                INVITE_FRIEND_PHOTO,
                caption=(
                    f"🤝 <b>Реферальная программа</b> ({role_display_name(role)})\n\n"
                    "Ваша реферальная ссылка:\n"
                    f"<code>{referral_link(callback.from_user.id)}</code>\n\n"
                    f"<tg-emoji emoji-id=\"5429278861932124623\">🪧</tg-emoji> Количество рефералов: {refs_total}\n"
                    f"<tg-emoji emoji-id=\"5472250091332993630\">💳</tg-emoji> Количество депозитов: {deposits_count}\n"
                    f"<tg-emoji emoji-id=\"5298614648138919107\">📈</tg-emoji> Общая сумма депозитов: {deposits_total} ₽\n"
                    f"{model_line}"
                    f"<tg-emoji emoji-id=\"5474417568053745249\">💰</tg-emoji> На реф. балансе: {ref_balance} ₽\n"
                    f"<tg-emoji emoji-id=\"5463424023734014980\">🛫</tg-emoji> Выведено: {ref_withdraw} ₽\n"
                    f'<tg-emoji emoji-id=\"5238132025323444613\">🏦</tg-emoji> Доступно к выводу: {available} ₽\n'
                    '\nДля вывода обращаться @yatogotsirka'
                ),
                parse_mode='HTML',
                reply_markup=ikb_referral,
            )
            return
    await callback.message.answer_photo(INVITE_FRIEND_PHOTO, caption=f"🤝 <b>Пригласить друга</b>\n\nВаша реферальная ссылка:\n<code>{referral_link(callback.from_user.id)}</code>\n\nВсего приведено друзей: {ref_amount}\n\n<tg-emoji emoji-id='5407064977544583568'>👌</tg-emoji> <b>За каждого приглашенного друга, который пополнит баланс вы получаете 7 дней подписки!</b>", parse_mode='HTML', reply_markup=ikb_referral)


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
    await _activate_trial_for_user(callback)

@dp.callback_query(lambda c: c.data == 'subscribe_confirmed')
async def subscribe_confirmed_callback(callback: CallbackQuery):
    await callback.answer("✅ Я подписался") # на пол экрана хуйня высветится
    await callback.message.delete()
    if not await is_subscribed(bot, callback.from_user.id):
        await callback.message.answer(
            '❌ Вы не подписаны на канал! Подпишитесь на канал, чтобы получить бесплатный тестовый период!',
            parse_mode='HTML',
            reply_markup=ikb_subscribe,
        )
        return
    ok = await _activate_trial_for_user(callback)
    if not ok:
        await callback.message.answer(
            '✅ Подписка на канал подтверждена. Если ключ не пришёл — нажми «Попробовать бесплатно» ещё раз или напиши в поддержку.',
            parse_mode='HTML',
            reply_markup=ikb_back,
        )

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
        funnel_on_paid(callback.from_user.id)
        renewal_on_paid(callback.from_user.id)
        try:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                reward = apply_deposit_reward_to_ref_partner(
                    cur, callback.from_user.id, amount_rub, 'yookassa',
                )
                if reward:
                    ref_master_id, reward_kind = reward
                    if reward_kind == 'share':
                        bonus_text = f'+{amount_rub // 2} ₽ (50% от депозита)'
                    else:
                        bonus_text = f'+{REFMASTER_20_DEPOSIT_BONUS_RUB} ₽ на реф. баланс'
                    try:
                        await bot.send_message(
                            ref_master_id,
                            f'<tg-emoji emoji-id="5416117059207572332">➡️</tg-emoji> '
                            f'Ваш реферал совершил депозит. Начислено {bonus_text}.',
                            parse_mode='HTML',
                        )
                    except Exception as e:
                        print(f'Не удалось уведомить рефовода {ref_master_id}: {e}')

                ref_master_id_sub = should_grant_subscription_referral_bonus(
                    cur, callback.from_user.id,
                )
                if ref_master_id_sub is not None:
                    mark_subscription_referral_bonus_used(cur, callback.from_user.id)
                    try:
                        await asyncio.to_thread(vpn.renew_subscription, ref_master_id_sub, 7)
                        await bot.send_message(
                            ref_master_id_sub,
                            '<tg-emoji emoji-id="5416117059207572332">➡️</tg-emoji> '
                            'Ваш реферал совершил депозит, вы получили бонусом 7 дней подписки!',
                            parse_mode='HTML',
                            reply_markup=ikb_my_sub,
                        )
                    except Exception as e:
                        print(f'Ошибка выдачи реф-бонуса для {ref_master_id_sub}: {e}')

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
async def admin_back_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Назад")
    await callback.message.delete()
    await callback.message.answer("👤 Админ панель", parse_mode='HTML', reply_markup=ikb_admin)

@dp.message(F.text == 'funnel_reset', F.from_user.id.in_(ADMIN_IDS))
async def admin_funnel_reset(message: Message):
    """Сброс своей воронки для теста no_trial."""
    text = reset_funnel_for_test(message.from_user.id)
    await message.answer(
        text + f'\n\nНе бери триал. Жди до {FUNNEL_SLEEP_SEC} с + 1 мин (TD_30M).',
        parse_mode='HTML',
    )


@dp.message(F.text.startswith('funnel_reset '), F.from_user.id.in_(ADMIN_IDS))
async def admin_funnel_reset_user(message: Message):
    try:
        uid = int(message.text.split(maxsplit=1)[1])
    except (IndexError, ValueError):
        await message.answer('Формат: <code>funnel_reset USER_ID</code>', parse_mode='HTML')
        return
    await message.answer(reset_funnel_for_test(uid), parse_mode='HTML')


@dp.message(F.text.startswith('setref '), F.from_user.id.in_(ADMIN_IDS))
async def admin_setref_command(message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            'Формат: <code>setref USER_ID код</code>\n'
            'Пример: <code>setref 123456789 author_ivan</code>\n'
            'Код: латиница, цифры, _ и -, до 64 символов.',
            parse_mode='HTML',
        )
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer('USER_ID должен быть числом.', parse_mode='HTML')
        return
    ok, text = set_custom_ref_code(user_id, parts[2])
    prefix = '✅ ' if ok else '❌ '
    await message.answer(prefix + text, parse_mode='HTML')


@dp.message(F.text.startswith('delref '), F.from_user.id.in_(ADMIN_IDS))
async def admin_delref_command(message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer('Формат: <code>delref USER_ID</code>', parse_mode='HTML')
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer('USER_ID должен быть числом.', parse_mode='HTML')
        return
    ok, text = set_custom_ref_code(user_id, None)
    prefix = '✅ ' if ok else '❌ '
    await message.answer(prefix + text, parse_mode='HTML')


@dp.message(F.text.startswith('shout '), (F.from_user.id.in_(ADMIN_IDS)))
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


@dp.message(F.text == 'admin', F.from_user.id.in_(ADMIN_IDS))
async def admin_message (message: Message):
    await message.answer("👤 Админ панель", parse_mode='HTML', reply_markup=ikb_admin)


@dp.callback_query((F.data == 'admin_funnel_stats') & F.from_user.id.in_(ADMIN_IDS))
async def admin_funnel_stats_callback(callback: CallbackQuery):
    await callback.answer('Собираем статистику воронки…')
    await callback.message.delete()

    summary, users_rows, events_rows = fetch_funnel_stats()
    renewal_summary, renewal_rows = fetch_renewal_stats()
    if not users_rows and not renewal_rows:
        await callback.message.answer(
            'В воронке пока никого нет. Пользователи попадают после /start.',
            parse_mode='HTML',
            reply_markup=ikb_admin_back,
        )
        return

    out_path = 'funnel_stats.xlsx'
    try:
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            if users_rows:
                pd.DataFrame(users_rows).to_excel(writer, sheet_name='Покупка', index=False)
            if events_rows:
                pd.DataFrame(events_rows).to_excel(writer, sheet_name='События', index=False)
            if renewal_rows:
                pd.DataFrame(renewal_rows).to_excel(writer, sheet_name='Продление', index=False)
        parts = [summary] if users_rows else []
        parts.append(renewal_summary)
        full_summary = '\n\n'.join(parts)
        await callback.message.answer(full_summary, parse_mode='HTML', reply_markup=ikb_admin_back)
        await callback.message.answer_document(
            FSInputFile(out_path),
            caption='Воронки: покупка, продление, события',
            reply_markup=ikb_admin_back,
        )
    finally:
        try:
            os.remove(out_path)
        except OSError:
            pass


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
                    INVITE_MAX_COLORED_PHOTO,
                    caption=(
                        'Один из наших пользователей — назовём его Макс — не платил за подписку <b>3 месяца</b>.\n\n'
                        'Он просто каждую неделю отправлял ссылку одному другу в Telegram. '
                        'Тому, кому это реально было нужно.\n\n'
                        '🎁 '
                        '<b>Итог:</b> 13 рефералов, купивших подписку = <b>91 день бесплатно</b>.\n\n'

                        'Ты можешь так же. Даже лучше.\n\n'

                        'Отправь ссылку прямо сейчас одному человеку — '
                        'тому, кому очень нужен VPN с обходом.\n\n'

                        'Это займёт <b>15 секунд</b>.'
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
        [InlineKeyboardButton(text='👑 Refmaster (50% с депозитов)', callback_data='admin_give_refmaster')],
        [InlineKeyboardButton(
            text='👑 Refmaster 2.0 (+50₽, подписка ≥149₽)',
            callback_data='admin_give_refmaster_20',
        )],
        [InlineKeyboardButton(text='Назад', callback_data='admin_back')],
    ])
    await callback.message.answer("👑 <b>Управление ролями</b>\n\nВыберите действие:", parse_mode='HTML', reply_markup=ikb_admin_roles)

@dp.callback_query(lambda c: c.data == 'admin_give_refmaster')
async def admin_give_refmaster_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminRefmaster.waiting_user_id)
    await state.update_data(assign_role=REFMASTER_ROLE)
    await callback.answer("👑 Refmaster")
    await callback.message.delete()
    await callback.message.answer(
        "👑 <b>Выдача роли Refmaster</b>\n"
        "Модель: <b>50% от каждого депозита реферала</b> (90 дней) на реф. баланс.\n\n"
        "Отправьте ID пользователя:",
        parse_mode='HTML',
        reply_markup=ikb_admin_back,
    )

@dp.callback_query(lambda c: c.data == 'admin_give_refmaster_20')
async def admin_give_refmaster_20_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminRefmaster.waiting_user_id)
    await state.update_data(assign_role=REFMASTER_20_ROLE)
    await callback.answer("👑 Refmaster 2.0")
    await callback.message.delete()
    await callback.message.answer(
        "👑 <b>Выдача роли Refmaster 2.0</b>\n"
        f"Модель: <b>+{REFMASTER_20_DEPOSIT_BONUS_RUB} ₽</b> за продление подписки реферала "
        f"от <b>{REFMASTER_20_MIN_DEPOSIT_RUB} ₽</b> (90 дней, без ГБ), <b>без</b> доли 50%.\n\n"
        "Отправьте ID пользователя:",
        parse_mode='HTML',
        reply_markup=ikb_admin_back,
    )

@dp.message(AdminRefmaster.waiting_user_id, F.text.isdigit(), F.from_user.id.in_(ADMIN_IDS))
async def admin_set_role_message(message: Message, state: FSMContext):
    data = await state.get_data()
    assign_role = data.get('assign_role')
    if assign_role not in (REFMASTER_ROLE, REFMASTER_20_ROLE):
        await message.answer(
            '❌ Сессия устарела. Снова выберите роль в админке → Роли.',
            reply_markup=ikb_admin_back,
        )
        await state.clear()
        return

    user_id = int(message.text)
    role_label = role_display_name(assign_role)
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
        user = cur.fetchone()
        if user:
            cur.execute('UPDATE users SET role = ? WHERE id = ?', (assign_role, user_id))
            con.commit()
            await message.answer(
                f"✅ Роль {role_label} выдана:\n\n🆔 ID: {user_id}\n"
                f"👤 Username: {user[1] if user[1] else 'Не указан'}",
                parse_mode='HTML',
                reply_markup=ikb_admin_back,
            )
        else:
            await message.answer(
                f"❌ Пользователь с ID {user_id} не найден в базе данных.",
                parse_mode='HTML',
                reply_markup=ikb_admin_back,
            )
    await state.clear()

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

@dp.callback_query((F.data == 'adv_campaigns') & F.from_user.id.in_(ADMIN_IDS))
async def adv_campaigns_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    count = len(list_adv_campaigns())
    await callback.message.answer(
        f'<b>Рекламные кампании</b>\n\nВсего в базе: <b>{count}</b>\n'
        'Поиск по ID: кампания (<code>?start=ID</code>) или Telegram ID рефовода.\n'
        'Для <b>Refmaster 2.0</b> — блок «К выплате сейчас» и список депозитов.',
        parse_mode='HTML',
        reply_markup=ikb_adv_campaigns_menu,
    )


async def _send_ref_partner_dashboard(message: Message, lookup_id: int, reply_markup):
    dashboard = get_ref_partner_dashboard(lookup_id)
    if not dashboard:
        await message.answer(
            f'❌ По ID <b>{lookup_id}</b> ничего не найдено '
            '(нет кампании, пользователя и рефералов).',
            parse_mode='HTML',
            reply_markup=reply_markup,
        )
        return
    text = format_admin_campaign_stats(dashboard)
    if len(text) > 4000:
        await message.answer(text[:4000] + '\n…', parse_mode='HTML', reply_markup=reply_markup)
        await message.answer(text[4000:], parse_mode='HTML')
    else:
        await message.answer(text, parse_mode='HTML', reply_markup=reply_markup)


async def _send_campaign_dashboard(message: Message, campaign_id: int, reply_markup):
    await _send_ref_partner_dashboard(message, campaign_id, reply_markup)

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
        f'✅ <b>Кампания создана</b>\n\n'
        f'ID: <code>{row_id}</code>\n'
        f'Ссылка: <code>{link}</code>\n\n'
        f'Отслеживание: админка → Рекламные кампании → «Найти по ID» → <code>{row_id}</code>',
        parse_mode='HTML',
        reply_markup=ikb_adv_back,
    )

@dp.callback_query(F.data == 'adv_get_campaigns')
async def get_campaigns(callback: CallbackQuery):
    await callback.message.delete()
    campaigns = list_adv_campaigns()
    if not campaigns:
        await callback.message.answer(
            'Кампаний пока нет. Создайте первую.',
            reply_markup=ikb_adv_campaigns_menu,
        )
        return
    await callback.message.answer(
        'Выберите кампанию (ID в кнопке):',
        reply_markup=generate_ikb_campaigns_list(),
    )

@dp.callback_query((F.data == 'adv_lookup_by_id') & F.from_user.id.in_(ADMIN_IDS))
async def adv_lookup_by_id_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdvCampaign.waiting_campaign_id)
    await callback.message.delete()
    await callback.message.answer(
        '🔎 <b>Поиск по ID</b>\n\n'
        'Отправьте число:\n'
        '• ID кампании из <code>?start=ID</code>\n'
        '• или Telegram ID рефовода (Refmaster / 2.0)\n\n'
        'Для Refmaster 2.0 увидите: <b>к выплате сейчас</b>, начисления +50₽/депозит, депозиты.',
        parse_mode='HTML',
        reply_markup=ikb_adv_back,
    )

@dp.message(AdvCampaign.waiting_campaign_id, F.from_user.id.in_(ADMIN_IDS))
async def adv_campaign_id_lookup_message(message: Message, state: FSMContext):
    raw = (message.text or '').strip()
    if not raw.isdigit():
        await message.answer(
            '❌ ID должен быть числом. Пример: <code>12</code>',
            parse_mode='HTML',
            reply_markup=ikb_adv_back,
        )
        return
    campaign_id = int(raw)
    await _send_campaign_dashboard(message, campaign_id, ikb_adv_back)
    await state.clear()


@dp.callback_query((F.data == 'adv_referrers_progress') & F.from_user.id.in_(ADMIN_IDS))
async def adv_referrers_progress_callback(callback: CallbackQuery):
    await callback.answer('Собираем статистику рефоводов…')
    await callback.message.delete()

    rows = fetch_all_referrers_progress()
    if not rows:
        await callback.message.answer(
            'Пока нет привязок в referal_users — рефоводов для отчёта нет.',
            reply_markup=ikb_adv_back,
        )
        return

    excel_rows = []
    total_refs = 0
    total_deposits_count = 0
    total_deposits_sum = 0

    for row in rows:
        (
            ref_master_id,
            username,
            role,
            custom_ref_code,
            ref_amount,
            ref_balance,
            ref_withdraw,
            refs_count,
            paying_refs,
            deposits_count,
            deposits_total,
            bonus_deposits_count,
        ) = row
        refs_count = int(refs_count or 0)
        paying_refs = int(paying_refs or 0)
        deposits_count = int(deposits_count or 0)
        deposits_total = int(deposits_total or 0)
        bonus_deposits_count = int(bonus_deposits_count or 0)
        total_refs += refs_count
        total_deposits_count += deposits_count
        total_deposits_sum += deposits_total

        ref_share_est = estimated_earnings_from_deposits(
            role, deposits_total, deposits_count, bonus_deposits_count,
        )
        if role_uses_deposit_share(role):
            model_note = '50% с депозитов'
            share_col = ref_share_est if ref_share_est is not None else ''
            fixed_col = ''
        elif role_uses_fixed_deposit_bonus(role):
            model_note = (
                f'+{REFMASTER_20_DEPOSIT_BONUS_RUB}₽/продление от {REFMASTER_20_MIN_DEPOSIT_RUB}₽'
            )
            share_col = ''
            fixed_col = ref_share_est if ref_share_est is not None else ''
        else:
            model_note = '7 дней за 1-й депозит'
            share_col = ''
            fixed_col = ''
        in_users = username is not None

        excel_rows.append({
            'ID рефовода': ref_master_id,
            'Username': username or '',
            'В users': 'да' if in_users else 'нет (кампания?)',
            'Роль': role_display_name(role) if role else '',
            'Модель': model_note,
            'Авторский код': custom_ref_code or '',
            'ref_amount': int(ref_amount or 0),
            'Рефералов': refs_count,
            'Рефералов с оплатой': paying_refs,
            'Депозитов': deposits_count,
            'Сумма депозитов ₽': deposits_total,
            'Оценка 50% ₽': share_col,
            f'Оценка +{REFMASTER_20_DEPOSIT_BONUS_RUB}₽×деп': fixed_col,
            'ref_balance ₽': int(ref_balance or 0),
            'Выведено ₽': int(ref_withdraw or 0),
        })

    df = pd.DataFrame(excel_rows)
    out_path = 'referrers_progress.xlsx'
    df.to_excel(out_path, index=False)

    top_lines = []
    for i, item in enumerate(excel_rows[:12], start=1):
        uname = item['Username']
        who = f"@{html.escape(uname)}" if uname else f"id {item['ID рефовода']}"
        top_lines.append(
            f"{i}. <code>{item['ID рефовода']}</code> {who} — "
            f"реф. {item['Рефералов']}, оплат {item['Рефералов с оплатой']}, "
            f"{item['Сумма депозитов ₽']} ₽"
        )

    summary = (
        f'<b>Прогресс рефоводов</b>\n\n'
        f'Рефоводов: <b>{len(excel_rows)}</b>\n'
        f'Всего привлечено: <b>{total_refs}</b>\n'
        f'Депозитов рефералов: <b>{total_deposits_count}</b> на <b>{total_deposits_sum} ₽</b>\n\n'
        f'<b>Топ-12:</b>\n' + '\n'.join(top_lines)
    )
    if len(excel_rows) > 12:
        summary += f'\n\n…ещё {len(excel_rows) - 12} в файле ниже.'

    try:
        await callback.message.answer(summary, parse_mode='HTML', reply_markup=ikb_adv_back)
        await callback.message.answer_document(
            FSInputFile(out_path),
            caption='Полная таблица по всем рефоводам',
            reply_markup=ikb_adv_back,
        )
    finally:
        try:
            os.remove(out_path)
        except OSError:
            pass

@dp.callback_query(F.data.startswith('adv_cid_'))
async def adv_campaign_by_id_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    try:
        campaign_id = int(callback.data.replace('adv_cid_', '', 1))
    except ValueError:
        await callback.message.answer('❌ Неверный ID кампании.', reply_markup=ikb_adv_back)
        return
    await _send_campaign_dashboard(callback.message, campaign_id, ikb_adv_back)

@dp.callback_query(F.data == 'ping_brokes')
async def ping_broke_users(callback: CallbackQuery): # оповестить нищеебов ебаных
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT id FROM users WHERE is_legacy == 0')
        users = cur.fetchall()

    text = (
        "<tg-emoji emoji-id='5389038097860144794'>🔥</tg-emoji> <b>СНИЖЕНИЕ ЦЕНЫ!</b>\n\n"
        "Теперь подписка стоит всего <b>149₽</b> вместо <s>199₽</s>.\n\n"
        "Успей подключиться по выгодной цене <tg-emoji emoji-id='5307965711065292927'>🚀</tg-emoji>"
    )

    success = 0

    for user in users:
        user_id = user[0]

        try:
            await bot.send_message(user_id, text, parse_mode='HTML', reply_markup=ikb_ping_brokes)
            success += 1
        except:
            pass

    await callback.message.answer(
        f"✅ Рассылка завершена.\n"
        f"Отправлено: {success}"
    )

@dp.callback_query(F.data == 'ping_unactive')
async def ping_unactive_users(callback: CallbackQuery):
    users = vpn.get_unactive_users()

    for user in users:
        try:
            await bot.send_photo(
                chat_id=user,
                photo=DECISION_PHOTO,
                caption=(
                    "<tg-emoji emoji-id=\"5467389807556579005\">🙂</tg-emoji> Платить 150₽ за кофе — норм.\n"
                    "Но 5 рублей в день за свободный интернет — «дорого»?\n\n"

                    "За эти деньги у тебя:\n\n"

                    "<tg-emoji emoji-id=\"5233346147560465779\">🟢</tg-emoji> YouTube без вечной загрузки\n"
                    "<tg-emoji emoji-id=\"5233346147560465779\">🟢</tg-emoji> TikTok, Reels и нейросети без ограничений\n"
                    "<tg-emoji emoji-id=\"5233346147560465779\">🟢</tg-emoji> Рабочий обход белых списков\n"
                    "<tg-emoji emoji-id=\"5233346147560465779\">🟢</tg-emoji> 3 дня бесплатно, чтобы проверить самому\n\n"

                    "Пока кто-то ищет «новый способ», наши пользователи просто открывают интернет и живут спокойно."
                ),
                parse_mode="HTML",
                reply_markup=ikb_unactive_ping_button,
            )

            await asyncio.sleep(0.1)

        except Exception as e:
            logging.exception(
                f"Ошибка при отправке сообщения пользователю {user}: {e}"
            )

async def main():
    setup_funnel(dp, bot, vpn, trial_flow_cb=_activate_trial_for_user)
    asyncio.create_task(check_expired_subscriptions_table(bot))
    asyncio.create_task(check_expiring_tomorrow_subscriptions_table(bot))
    asyncio.create_task(notify_gbs_ending(bot))
    asyncio.create_task(notify_inactive_trial_users(bot))
    asyncio.create_task(run_funnel_worker(bot))
    asyncio.create_task(run_renewal_funnel_worker(bot))
    # Запускаем фоновую задачу для сброса флага runout_notified в 00:01 каждый день
    asyncio.create_task(reset_runout_notified_daily())
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())