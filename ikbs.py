from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import sqlite3 as sq
from emojis import get_emoji
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
        InlineKeyboardButton(text='Моя подписка', callback_data='my_subscription', icon_custom_emoji_id=get_emoji('keys')),
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

ikb_admin = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='👤 Пользователи', callback_data='admin_users')],
    [InlineKeyboardButton(text='🔄 Оплаты', callback_data='admin_payments')],
    [InlineKeyboardButton(text='🔑 Ключи', callback_data='admin_keys')],
    [InlineKeyboardButton(text='👉🏼 Рефералы', callback_data='admin_referrals')],
    [InlineKeyboardButton(text='👑 Роли', callback_data='admin_roles')],
    [InlineKeyboardButton(text='🔊 Напомнить юзерам о бесплатном тестовом периоде', callback_data='admin_notify_trial')],
    [InlineKeyboardButton(text='⏰ Уведомить о завершении пробной подписки', callback_data='admin_notify_expired')],
    [InlineKeyboardButton(text='🤝 Напомнить о рефке', callback_data='admin_notify_referral')],

])

ikb_admin_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=' Назад', callback_data='admin_back', icon_custom_emoji_id=get_emoji('exit'))],
])
