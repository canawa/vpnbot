MONTH_PRICE = 149
MONTH_PROMO_PRICE = 99
MONTH_PROMO_HOURS = 24
# Сроки как в vpn.py: create/renew +30 дн., trial POST +3 дн.
VPN_SUBSCRIPTION_DAYS_PAID = 30
VPN_SUBSCRIPTION_DAYS_TRIAL = 3
SUBSCRIPTION_PLAN = {
    7: 50,
    30: 169,
    90: 449,
    360: 1099,
}

SUBSCRIPTION_PLAN_LEGACY = {
    7: 50,
    30: 169,
    90: 449,
    360: 1099,

}

WEEK_PLAN_DAYS = 7
WEEK_PLAN_PRICE = 50

# Акция «выборы VPN» — только после кнопки из рассылки
ELECTIONS_PROMO_HOURS = 48
ELECTIONS_PROMO_PLAN = {
    30: 149,
    90: 399,
    360: 999,
}

GBS_PRICES = {
    10: 49,
    30: 99,
    50: 149,
}


def days_for_subscription_amount(amount: int) -> int | None:
    """Дни тарифа по реально оплаченной сумме. Неизвестная сумма — None."""
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return None
    if amount == MONTH_PROMO_PRICE:
        return VPN_SUBSCRIPTION_DAYS_PAID
    for days, price in ELECTIONS_PROMO_PLAN.items():
        if int(price) == amount:
            return int(days)
    for days, price in SUBSCRIPTION_PLAN.items():
        if int(price) == amount:
            return int(days)
    if amount == WEEK_PLAN_PRICE:
        return WEEK_PLAN_DAYS
    return None


def is_elections_promo_plan(amount: int, days: int) -> bool:
    try:
        return ELECTIONS_PROMO_PLAN.get(int(days)) == int(amount)
    except (TypeError, ValueError):
        return False


def is_listed_subscription_plan(amount: int, days: int) -> bool:
    """Обычный каталог + акция 99₽. Цены «выборов» сюда не входят."""
    try:
        amount = int(amount)
        days = int(days)
    except (TypeError, ValueError):
        return False
    if days in SUBSCRIPTION_PLAN and int(SUBSCRIPTION_PLAN[days]) == amount:
        return True
    if amount == WEEK_PLAN_PRICE and days == WEEK_PLAN_DAYS:
        return True
    if amount == MONTH_PROMO_PRICE and days == VPN_SUBSCRIPTION_DAYS_PAID:
        return True
    return False


def gb_amount_for_paid_price(amount: int) -> int | None:
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return None
    for gb, price in GBS_PRICES.items():
        if int(price) == amount:
            return int(gb)
    return None


MONTHS_RU = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
    5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
    9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
}
