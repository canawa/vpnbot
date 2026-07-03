from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CopyTextButton
import sqlite3 as sq
from emojis import get_emoji
from datetime import datetime

from prices import SUBSCRIPTION_PLAN, SUBSCRIPTION_PLAN_LEGACY, MONTH_PRICE, MONTH_PROMO_PRICE
from vpn import *

ikb_subscribe = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔗 Подписаться на канал', url='https://t.me/coffemaniavpn')],
    [InlineKeyboardButton(
        text='Я подписался',
        callback_data='subscribe_confirmed',
        icon_custom_emoji_id=get_emoji('check'),
    )],
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
        if had_trial != 1 :
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
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Написать в поддержку', url='t.me/coffeemaniasup2', icon_custom_emoji_id=get_emoji('telegram'))]),
    ikb_main.inline_keyboard.append([InlineKeyboardButton(text='Документы', callback_data='documents', icon_custom_emoji_id=get_emoji('documents'))])

    return ikb_main

ikb_back = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
    ])

ikb_referral_reminder = InlineKeyboardMarkup(inline_keyboard=[ # клава которая вылезит людям
    [InlineKeyboardButton(text='Получить 7 дней подписки', callback_data='referral', style = 'success', icon_custom_emoji_id='5375434377360587873')],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_documents = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Пользовательское соглашение', url='https://telegra.ph/Polzovatelskoe-soglashenie-05-21-30', icon_custom_emoji_id=get_emoji('documents'))],
    [InlineKeyboardButton(text='Политика конфиденциальности', url='https://telegra.ph/Politika-konfidencialnosti-05-21-29', icon_custom_emoji_id=get_emoji('lock'))],
    [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
])

def get_ikb_referral(link, with_settings: bool = False):
    rows = [
        [InlineKeyboardButton(
            text='Скопировать ссылку',
            copy_text=CopyTextButton(text=f'{link}'))],
        [InlineKeyboardButton(
            text='Поделиться ссылкой',
            switch_inline_query=(
                f'Привет, приглашаю тебя пользоваться хорошим ВПН сервисом '
                f'с обходом глушилок: {link}'
            ),
            style='primary',
        )],
    ]
    if with_settings:
        rows.append([
            InlineKeyboardButton(text='⚙️ Настройки', callback_data='ref_settings'),
        ])
    rows.append([
        InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit')),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_ikb_ref_settings(notify_referral: bool, notify_deposit: bool):
    ref_label = (
        '🔔 Новый реферал: вкл' if notify_referral else '🔕 Новый реферал: выкл'
    )
    dep_label = (
        '🔔 Новый депозит: вкл' if notify_deposit else '🔕 Новый депозит: выкл'
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=ref_label, callback_data='ref_toggle_notify_referral')],
        [InlineKeyboardButton(text=dep_label, callback_data='ref_toggle_notify_deposit')],
        [InlineKeyboardButton(text='Назад', callback_data='referral', icon_custom_emoji_id=get_emoji('exit'))],
    ])

ikb_support = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💬 Написать в поддержку', url='t.me/coffeemaniasup2')],
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
    [InlineKeyboardButton(text='🔗 Авторские ссылки', callback_data='admin_custom_ref')],
    [InlineKeyboardButton(text='👑 Роли', callback_data='admin_roles')],
    [InlineKeyboardButton(text='🤝 Напомнить о рефке', callback_data='admin_notify_referral')],
    [InlineKeyboardButton(text='Рекламные кампании', callback_data='adv_campaigns')],
    [InlineKeyboardButton(text='Рекламные кампании 2.0', callback_data='adv2_campaigns')],
    [InlineKeyboardButton(text='📊 Статистика воронки', callback_data='admin_funnel_stats')],
    [InlineKeyboardButton(text='Рассказать челам что 5р в день', callback_data='ping_unactive')],
    [InlineKeyboardButton(text='Рассылка скидка 99₽ (без подписки)', callback_data='ping_funnel_sale')],
    # [InlineKeyboardButton(text='оповесть бомжей о снижении', callback_data='ping_brokes')]
    [InlineKeyboardButton(text='Рассказать что ищем рефоводов', callback_data='we_need_refmasters')],
    [InlineKeyboardButton(text='Выдать 2 дня подписки инактив юзерам', callback_data='admin_give_2_days_bonus')],


])

def get_ikb_connect_via_app(link):
    token = link.rstrip("/").split("/")[-1]
    app_link = f"https://sub.coffemaniavpn.online/app/add?token={token}&connect=1"
    ikb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Подключиться', url=app_link)]
    ])
    return ikb

ikb_adv_manager_panel = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Рекламные кампании 2.0', callback_data='adv2_campaigns')],
])


def build_ikb_adv_legacy_menu() -> InlineKeyboardMarkup:
    """Рефоводы, выплаты, старый поиск по ID."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text='👑 Refmaster / 2.0 — выплаты',
            callback_data='adv_refmasters',
        )],
        [InlineKeyboardButton(text='Смотреть по ID', callback_data='adv_lookup_by_id')],
        [InlineKeyboardButton(
            text='Прогресс всех рефоводов',
            callback_data='adv_referrers_progress',
        )],
        [InlineKeyboardButton(
            text=' Назад',
            callback_data='admin_back',
            icon_custom_emoji_id=get_emoji('exit'),
        )],
    ])


def build_ikb_adv2_menu() -> InlineKeyboardMarkup:
    """Кампании с несколькими ссылками."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Создать новую кампанию', callback_data='adv_new_campaign_create')],
        [InlineKeyboardButton(text='Список кампаний', callback_data='adv_get_campaigns')],
        [InlineKeyboardButton(text='Смотреть по ID', callback_data='adv2_lookup_by_id')],
        [InlineKeyboardButton(
            text=' Назад',
            callback_data='admin_back',
            icon_custom_emoji_id=get_emoji('exit'),
        )],
    ])


def generate_ikb_refmaster_partners(partners: list) -> InlineKeyboardMarkup:
    """Кнопки по каждому refmaster / refmaster_20."""
    from referrals import role_uses_fixed_deposit_bonus, partner_pending_payout

    keyboard = []
    for p in partners[:25]:
        pending = partner_pending_payout(p['ref_balance'], p['ref_withdraw'])
        tag = '2.0' if role_uses_fixed_deposit_bonus(p.get('role')) else '50%'
        keyboard.append([
            InlineKeyboardButton(
                text=f'{p["id"]} · {pending}₽ · {tag}',
                callback_data=f'adv_rm_{p["id"]}',
            )
        ])
    if len(partners) > 25:
        keyboard.append([
            InlineKeyboardButton(
                text=f'…ещё {len(partners) - 25} — смотри Excel',
                callback_data='adv_refmasters_excel',
            )
        ])
    keyboard.append([
        InlineKeyboardButton(text='📥 Excel по выплатам', callback_data='adv_refmasters_excel'),
    ])
    keyboard.append([
        InlineKeyboardButton(
            text=' Назад',
            callback_data='adv_campaigns',
            icon_custom_emoji_id=get_emoji('exit'),
        ),
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


ikb_adv_refmaster_detail_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='◀️ К списку Refmaster', callback_data='adv_refmasters')],
    [InlineKeyboardButton(text=' Назад в кампании', callback_data='adv_campaigns', icon_custom_emoji_id=get_emoji('exit'))],
])

def generate_ikb_campaigns_list(campaigns: list | None = None):
    if campaigns is None:
        from databases import list_adv_campaigns
        campaigns = list_adv_campaigns(admin_sees_all=True)

    keyboard = []
    for rowid, name, _desc in campaigns:
        label = name if len(name) <= 28 else f'{name[:25]}…'
        keyboard.append([
            InlineKeyboardButton(
                text=f'#{rowid} · {label}',
                callback_data=f'adv_cid_{rowid}',
            )
        ])
    if not campaigns:
        keyboard.append([
            InlineKeyboardButton(text='(кампаний пока нет)', callback_data='adv2_campaigns'),
        ])
    keyboard.append([
        InlineKeyboardButton(
            text='Найти по ID',
            callback_data='adv2_lookup_by_id',
        ),
    ])
    keyboard.append([
        InlineKeyboardButton(
            text='Назад',
            callback_data='adv2_campaigns',
            icon_custom_emoji_id=get_emoji('exit'),
        ),
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def generate_ikb_campaign_detail(campaign_id: int, links: list) -> InlineKeyboardMarkup:
    keyboard = []
    for link in links[:20]:
        lid = link['id']
        name = link.get('link_name') or f'#{lid}'
        short = name if len(name) <= 22 else f'{name[:19]}…'
        refs = int(link.get('refs_total') or 0)
        keyboard.append([
            InlineKeyboardButton(
                text=f'🔗 {lid} · {short} · {refs} реф.',
                callback_data=f'adv_lid_{lid}',
            )
        ])
    keyboard.append([
        InlineKeyboardButton(
            text='➕ Добавить ссылку',
            callback_data=f'adv_add_link_{campaign_id}',
        ),
    ])
    keyboard.append([
        InlineKeyboardButton(
            text='◀️ К списку кампаний',
            callback_data='adv_get_campaigns',
            icon_custom_emoji_id=get_emoji('exit'),
        ),
    ])
    keyboard.append([
        InlineKeyboardButton(
            text=' Назад в меню',
            callback_data='adv2_campaigns',
            icon_custom_emoji_id=get_emoji('exit'),
        ),
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def generate_ikb_link_detail_back(campaign_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='◀️ К кампании', callback_data=f'adv_cid_{campaign_id}')],
        [InlineKeyboardButton(
            text=' Назад в меню',
            callback_data='adv2_campaigns',
            icon_custom_emoji_id=get_emoji('exit'),
        )],
    ])


ikb_admin_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=' Назад', callback_data='admin_back', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_adv_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=' Назад', callback_data='adv_campaigns', icon_custom_emoji_id=get_emoji('exit'))],
])

ikb_adv2_back = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=' Назад', callback_data='adv2_campaigns', icon_custom_emoji_id=get_emoji('exit'))],
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
            [InlineKeyboardButton(text='Устройства', callback_data='device_list', icon_custom_emoji_id = get_emoji('device'))],
            [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
        ])
    return ikb_subscription_after_buy

ikb_gbs_reminder_buy_option = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text ='Докупить ГБ обхода LTE', callback_data='buy_lte_gigabytes', icon_custom_emoji_id=get_emoji('time'))],
])

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
    for i, device in enumerate(devices):
        # callback_data ≤ 64 байта; полный hwid часто длиннее
        ikb_devices.inline_keyboard.append([
            InlineKeyboardButton(
                text=f'Удалить {device["deviceModel"]}',
                callback_data=f'delete_device_{i}',
                style='danger',
            )
        ])
    ikb_devices.inline_keyboard.append(
        [InlineKeyboardButton(text='Купить +1 устройство (30 рублей)', callback_data='buy_device', icon_custom_emoji_id=get_emoji('tv_emoji'))])
    ikb_devices.inline_keyboard.append([InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))])
    return ikb_devices

ikb_unactive_ping_button = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Подключить VPN', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('plus'), style='success')],
])

ikb_funnel_summer_sale = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(
        text=f'Купить на месяц · {MONTH_PRICE}₽ → {MONTH_PROMO_PRICE}₽',
        callback_data=f'deposit_{MONTH_PROMO_PRICE}_30_card',
        style='success',
    )],
])

ikb_ping_brokes = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Подключиться сейчас (-25%)', callback_data='buy_vpn', icon_custom_emoji_id=get_emoji('plus'), style='success')],
])

def generate_ikb_duration_choose(tg_id):
    with sq.connect('database.db') as con:
        cur = con.cursor()

        # Проверяем существование колонки
        cur.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cur.fetchall()]

        is_legacy = 0
        if 'is_legacy' in columns:
            cur.execute('SELECT is_legacy FROM users WHERE id = ?', (tg_id,))
            result = cur.fetchone()
            is_legacy = result[0] if result else 0  # None если юзера нет в БД

        plan = SUBSCRIPTION_PLAN_LEGACY if is_legacy == 1 else SUBSCRIPTION_PLAN

        # Неделя 50₽ — только в воронках (funnel / renewal), не в обычном buy_vpn
        rows = [
            [InlineKeyboardButton(text=f'1 месяц · {plan.get(30)}₽ ', callback_data=f'deposit_{plan.get(30)}_30_card')],
            [InlineKeyboardButton(text=f'3 месяца · {plan.get(90)}₽ | Скидка 15%', callback_data=f'deposit_{plan.get(90)}_90_card', style='primary')],
            [InlineKeyboardButton(text=f'12 месяцев · {plan.get(360)}₽ | Скидка 50%', callback_data=f'deposit_{plan.get(360)}_360_card')],
            [InlineKeyboardButton(text='Назад', callback_data='back', icon_custom_emoji_id=get_emoji('exit'))],
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

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

def get_ikb_2_days_bonus(tg_id):
    ikb_2_days_bonus = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Получить 2 дня подписки', callback_data=f'2_days_bonus_{tg_id}', style='success')],
    ])
    return ikb_2_days_bonus

def get_ikb_device_payment(payment_id, confirmation_url, price):
    pid = str(payment_id).strip()
    ikb_yookassa = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'👉 Перейти к оплате {price} ₽', url=confirmation_url)],
        [InlineKeyboardButton(text='Я оплатил', callback_data=f'device_yookassa_{pid}',style='success')],
        [InlineKeyboardButton(text='Отменить платеж!', callback_data='back', style='danger')],

    ])
    return ikb_yookassa