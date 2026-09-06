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
    for days, price in SUBSCRIPTION_PLAN.items():
        if int(price) == amount:
            return int(days)
    if amount == WEEK_PLAN_PRICE:
        return WEEK_PLAN_DAYS
    return None


def is_listed_subscription_plan(amount: int, days: int) -> bool:
    return days_for_subscription_amount(amount) == int(days)


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
