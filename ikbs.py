from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import sqlite3 as sq
from emojis import get_emoji
from datetime import datetime
from vpn import *
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
        cur.execute('SELECT subscription_expires_at FROM subscriptions WHERE user_id = ?', (user_id,))
        result = cur.fetchone()
        subscription_expires_at = result[0] if result else None
        if had_trial != 1:
            ikb_main.inline_keyboard.append([InlineKeyboardButton(text='🎁 Попробовать бесплатно', callback_data='trial', style = 'success')])
        if subscription_expires_at:
            subscription_expires_at = datetime.fromisoformat(subscription_expires_at)
            if subscription_expires_at < datetime.now():
                ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Подключить VPN', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('plus'))])
        else:
            ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Подключить VPN', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('plus'))])

    ikb_main.inline_keyboard.append([
        InlineKeyboardButton(text='Реферальная программа', callback_data='referral', icon_custom_emoji_id=get_emoji('add_user')),
        InlineKeyboardButton(text='Моя подписка', callback_data='my_subscription', icon_custom_emoji_id=get_emoji('keys')),
    ])
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Написать в поддержку', url='@coffeemaniasup2', icon_custom_emoji_id=get_emoji('telegram'))]),
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Документы', callback_data='documents', icon_custom_emoji_id=get_emoji('documents'))])

    return ikb_main

ikb_back = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])

ikb_referral_reminder = InlineKeyboardMarkup(inline_keyboard=[ # клава которая вылезит людям
    [InlineKeyboardButton(text='Получить 7 дней подписки', callback_data='referral', icon_custom_emoji_id=get_emoji('game'), style = 'success')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_documents = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Пользовательское соглашение', url='https://telegra.ph/Polzovatelskoe-soglashenie-12-22-25', icon_custom_emoji_id=get_emoji('documents'))],
    [InlineKeyboardButton(text='Политика конфиденциальности', url='https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-03-29-41', icon_custom_emoji_id=get_emoji('lock'))],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_referral = InlineKeyboardMarkup(inline_keyboard=[
    # [InlineKeyboardButton(text='💸 Вывести реферальный баланс', callback_data='ref_withdraw')], ПОКА ЧТО УБРАЛ
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_support = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💬 Написать в поддержку', url='@coffeemaniasup2')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_deposit = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💰 Пополнить', callback_data='deposit')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

# ikb_deposit_methods = InlineKeyboardMarkup(inline_keyboard=[
#     [InlineKeyboardButton(text='СБП (или картой)', callback_data='deposit_card', icon_custom_emoji_id=get_emoji('sbp'))],
#     [InlineKeyboardButton(text='Криптобот', callback_data='deposit_crypto', icon_custom_emoji_id=get_emoji('crypto_bot'))],
#     [InlineKeyboardButton(text='Звёзды', callback_data='deposit_stars', icon_custom_emoji_id=get_emoji('stars'))],
#     [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
# ])

ikb_admin = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='👤 Пользователи', callback_data='admin_users')],
    [InlineKeyboardButton(text='🔄 Оплаты', callback_data='admin_payments')],
    [InlineKeyboardButton(text='🔑 Подписки', callback_data='admin_keys')],
    [InlineKeyboardButton(text='👉🏼 Рефералы', callback_data='admin_referrals')],
    [InlineKeyboardButton(text='👑 Роли', callback_data='admin_roles')],
    [InlineKeyboardButton(text='🔊 Напомнить юзерам о бесплатном тестовом периоде', callback_data='admin_notify_trial')],
    [InlineKeyboardButton(text='⏰ Уведомить ро скидке у кого нет подписки', callback_data='admin_notify_sale')],
    [InlineKeyboardButton(text='🤝 Напомнить о рефке', callback_data='admin_notify_referral')],
    [InlineKeyboardButton(text='Тест рассылка рекламы', callback_data='admin_test_adv')],


])

ikb_admin_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=' Назад', callback_data='admin_back', icon_cuadminstom_emoji_id=get_emoji('exit'))],
])

def create_yookassa_payment_keyboard(amount, days, confirmation_url, payment_id): # функция для создания клавиатуры для оплаты через Юкассу
    # Формат callback_data: yookassa_{amount}_{days}_{payment_id}
    pid = str(payment_id).strip()
    ikb_yookassa = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'👉 Перейти к оплате {amount} ₽', url=confirmation_url)],
        [InlineKeyboardButton(text='Я оплатил', callback_data=f'yookassa_{amount}_{days}_{pid}', style = 'success')],
        [InlineKeyboardButton(text='Отменить платеж!', callback_data='back', style = 'danger')],
    ])
    return ikb_yookassa

def create_ikb_sub_after_buy(url):
    ikb_subscription_after_buy = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Подключиться', url=url, icon_custom_emoji_id=get_emoji('shield_emoji'), style='success')],
            [InlineKeyboardButton(text ='Продлить подписку', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('locate'))],
            [InlineKeyboardButton(text ='Докупить ГБ обхода LTE', callback_data='buy_lte_gigabytes', icon_custom_emoji_id=get_emoji('time'))],
            [InlineKeyboardButton(text='Список устройств', callback_data='device_list', icon_custom_emoji_id = get_emoji('device'))],
            [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
        ])
    return ikb_subscription_after_buy

def create_ikb_renew():
    ikb_renew = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Продлить подписку', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('shield_emoji'), style='success')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'), style='danger')],
    ])
    return ikb_renew

ikb_my_sub = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Моя подписка', callback_data='my_subscription', icon_custom_emoji_id=get_emoji('shield_emoji'), style='success')],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))]
    ])
ikb_sale = InlineKeyboardMarkup(inline_keyboard = [
        [InlineKeyboardButton(text = 'Оплатить', callback_data = 'deposit_99_card', icon_custom_emoji_id=get_emoji('shield_emoji'), style='success')],
])

def create_ikb_devices(tg_id):
    devices = Vpn().get_hwid_devices(tg_id)
    ikb_devices = InlineKeyboardMarkup(inline_keyboard=[])
    for device in devices:
       ikb_devices.inline_keyboard.append([InlineKeyboardButton(text = f'Удалить {device["deviceModel"]}', callback_data = f'delete_device_{device["hwid"]}' , style='danger')])
    ikb_devices.inline_keyboard.append([InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))])
    return ikb_devices

vpn_sub_duration_ikb_choose = InlineKeyboardMarkup(inline_keyboard = [
    [InlineKeyboardButton(text='30 дней · 149₽', callback_data='duration_30')],
    [InlineKeyboardButton(text='90 дней · 399₽', callback_data='duration_90')],
    [InlineKeyboardButton(text='180 дней · 599₽', callback_data='duration_180')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))]
])

def get_vpn_pay_keyboard(price, days) -> InlineKeyboardMarkup:
    rows = []
    rows.extend([
        [InlineKeyboardButton(text=f'Оплатить {price}₽', callback_data=f'deposit_{price}_{days}_card', icon_custom_emoji_id=get_emoji('pay'))],
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

ikb_gbs_variants = InlineKeyboardMarkup(inline_keyboard = [
    [InlineKeyboardButton(text='10 ГБ · 49₽', callback_data='gbs_10')],
    [InlineKeyboardButton(text='30 ГБ · 99₽', callback_data='gbs_30')],
    [InlineKeyboardButton(text='50 ГБ · 149₽', callback_data='gbs_50')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))]
    ])

def create_yookassa_gb_payment(payment_id, gb_amount, confirmation_url, price):
    pid = str(payment_id).strip()
    ikb_yookassa = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'👉 Перейти к оплате {price} ₽', url=confirmation_url)],
        [InlineKeyboardButton(text='Я оплатил', callback_data=f'gb_yookassa_{pid}_{gb_amount}_{price}', style='success')],
        [InlineKeyboardButton(text='Отменить платеж!', callback_data='back', style='danger')],
    ])
    return ikb_yookassa
