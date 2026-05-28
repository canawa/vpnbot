# -*- coding: utf-8 -*-
"""Роли и начисления реферальной программы."""

from datetime import date, timedelta

REFMASTER_ROLE = 'refmaster'
REFMASTER_20_ROLE = 'refmaster_20'
REFERRAL_COMMISSION_WINDOW_DAYS = 90
REFMASTER_20_DEPOSIT_BONUS_RUB = 50
REFMASTER_20_MIN_DEPOSIT_RUB = 149

TX_TYPE_YOOKASSA_SUBSCRIPTION = 'yookassa'
TX_TYPE_YOOKASSA_GB = 'yookassa_gb'
TX_TYPE_CRYPTOBOT = 'CryptoBot'
REFERRAL_DEPOSIT_TX_TYPES = frozenset({
    TX_TYPE_YOOKASSA_SUBSCRIPTION,
    TX_TYPE_CRYPTOBOT,
})

REF_PARTNER_ROLES = frozenset({REFMASTER_ROLE, REFMASTER_20_ROLE})


def is_subscription_referral_deposit(amount_rub: int, tx_type: str | None) -> bool:
    """Учитываются только продления/подписка, не покупка ГБ."""
    return (tx_type or '').strip() in REFERRAL_DEPOSIT_TX_TYPES


def is_refmaster_20_qualifying_deposit(amount_rub: int, tx_type: str | None) -> bool:
    """+50 ₽: подписка/продление, сумма от 149 ₽."""
    return (
        is_subscription_referral_deposit(amount_rub, tx_type)
        and int(amount_rub) >= REFMASTER_20_MIN_DEPOSIT_RUB
    )


def normalize_role(role: str | None) -> str:
    return (role or '').strip().lower()


def role_uses_deposit_share(role: str | None) -> bool:
    return normalize_role(role) == REFMASTER_ROLE


def role_uses_fixed_deposit_bonus(role: str | None) -> bool:
    return normalize_role(role) == REFMASTER_20_ROLE


def role_has_refmaster_ui(role: str | None) -> bool:
    return normalize_role(role) in REF_PARTNER_ROLES


def role_display_name(role: str | None) -> str:
    r = normalize_role(role)
    if r == REFMASTER_ROLE:
        return 'Refmaster'
    if r == REFMASTER_20_ROLE:
        return 'Refmaster 2.0'
    return role or '—'


def referral_commission_active(registration_date_str: str | None) -> bool:
    if not registration_date_str:
        return False
    try:
        registration_date = date.fromisoformat(registration_date_str)
    except ValueError:
        return False
    return date.today() <= registration_date + timedelta(days=REFERRAL_COMMISSION_WINDOW_DAYS)


def fetch_ref_master_for_referral(cur, referral_id: int) -> tuple[int, str] | None:
    cur.execute(
        'SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?',
        (referral_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    ref_master_id, registration_date_str = row[0], row[1]
    if not referral_commission_active(registration_date_str):
        return None
    return ref_master_id, registration_date_str


def fetch_ref_master_role(cur, ref_master_id: int) -> str:
    cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
    row = cur.fetchone()
    return normalize_role(row[0] if row else None)


def apply_deposit_reward_to_ref_partner(
    cur,
    referral_id: int,
    amount_rub: int,
    tx_type: str = TX_TYPE_YOOKASSA_SUBSCRIPTION,
) -> tuple[int, str] | None:
    """
    Начисляет вознаграждение рефоводу за депозит реферала.

    refmaster: +50% суммы депозита на ref_balance (только подписка, не ГБ).
    refmaster_20: +50 ₽ при продлении/подписке от 149 ₽.

    Returns:
        (ref_master_id, 'share' | 'fixed50') при успешном начислении, иначе None.
    """
    if not is_subscription_referral_deposit(amount_rub, tx_type):
        return None

    ref_info = fetch_ref_master_for_referral(cur, referral_id)
    if not ref_info:
        return None

    ref_master_id, _ = ref_info
    role = fetch_ref_master_role(cur, ref_master_id)

    if role_uses_deposit_share(role):
        bonus = max(int(amount_rub) // 2, 0)
        if bonus <= 0:
            return None
        cur.execute(
            'UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?',
            (bonus, ref_master_id),
        )
        return ref_master_id, 'share'

    if role_uses_fixed_deposit_bonus(role):
        if not is_refmaster_20_qualifying_deposit(amount_rub, tx_type):
            return None
        cur.execute(
            'UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?',
            (REFMASTER_20_DEPOSIT_BONUS_RUB, ref_master_id),
        )
        return ref_master_id, 'fixed50'

    return None


def should_grant_subscription_referral_bonus(cur, referral_id: int) -> int | None:
    """
    Обычный рефовод (не refmaster / refmaster_20): один раз 7 дней подписки за первый депозит реферала.
    Returns ref_master_id или None.
    """
    ref_info = fetch_ref_master_for_referral(cur, referral_id)
    if not ref_info:
        return None

    ref_master_id, _ = ref_info
    role = fetch_ref_master_role(cur, ref_master_id)
    if role in REF_PARTNER_ROLES:
        return None

    cur.execute(
        'SELECT 1 FROM users WHERE received_bonus = 0 AND id = ?',
        (referral_id,),
    )
    if cur.fetchone() is None:
        return None
    return ref_master_id


def mark_subscription_referral_bonus_used(cur, referral_id: int) -> None:
    cur.execute('UPDATE users SET received_bonus = 1 WHERE id = ?', (referral_id,))


def estimated_earnings_from_deposits(
    role: str | None,
    deposits_total: int,
    deposits_count: int,
    bonus_deposits_count: int = 0,
) -> int | None:
    """Оценка «заработано» для отчётов (refmaster — 50%, refmaster_20 — 50₽ × квалиф. депозиты)."""
    if role_uses_deposit_share(role):
        return int(deposits_total) // 2
    if role_uses_fixed_deposit_bonus(role):
        return int(bonus_deposits_count) * REFMASTER_20_DEPOSIT_BONUS_RUB
    return None


def _format_deposit_rows_block(deposit_rows: list, role: str | None) -> str:
    if not deposit_rows:
        return '\n<i>Депозитов рефералов пока нет.</i>'
    lines = ['\n<b>Последние депозиты рефералов:</b>']
    for i, d in enumerate(deposit_rows[:15], 1):
        uname = d.get('referral_username') or ''
        who = f'@{uname}' if uname else f"id {d['referral_id']}"
        window = '✅' if d.get('in_window') else '⛔ вне 90д'
        bonus_note = ''
        if (
            role_uses_fixed_deposit_bonus(role)
            and d.get('in_window')
            and is_refmaster_20_qualifying_deposit(d['amount'], d.get('pay_type'))
        ):
            bonus_note = f' → +{REFMASTER_20_DEPOSIT_BONUS_RUB}₽'
        elif role_uses_deposit_share(role) and d.get('in_window'):
            bonus_note = f' → +{d["amount"] // 2}₽'
        lines.append(
            f'{i}. {who} — <b>{d["amount"]}₽</b> ({d.get("pay_type", "")}) '
            f'{d.get("date", "")[:10]} {window}{bonus_note}'
        )
    if len(deposit_rows) > 15:
        lines.append(f'<i>…ещё {len(deposit_rows) - 15} в истории</i>')
    return '\n'.join(lines)


def partner_pending_payout(ref_balance: int, ref_withdraw: int) -> int:
    return max(int(ref_balance or 0) - int(ref_withdraw or 0), 0)


def filter_refmaster_partners_with_pending(partners: list[dict]) -> list[dict]:
    """Только партнёры с ненулевым долгом (ref_balance − ref_withdraw > 0)."""
    return [
        p for p in partners
        if partner_pending_payout(p.get('ref_balance'), p.get('ref_withdraw')) > 0
    ]


def format_refmasters_overview(partners: list[dict], *, all_roles_count: int = 0) -> str:
    """Сводный список Refmaster / 2.0 с ненулевым долгом."""
    if not partners:
        if all_roles_count > 0:
            return (
                '<b>👑 Refmaster / 2.0</b>\n\n'
                f'Партнёров с ролью: <b>{all_roles_count}</b>, '
                'но <b>к выплате никому ничего нет</b> (долг = 0).\n'
                '<i>В списке показываются только те, кому ещё должны.</i>'
            )
        return (
            '<b>👑 Refmaster / 2.0</b>\n\n'
            'Нет пользователей с ролью Refmaster или Refmaster 2.0.\n'
            '<i>Выдайте роль: Админка → Роли.</i>'
        )

    total_pending = sum(
        partner_pending_payout(p['ref_balance'], p['ref_withdraw']) for p in partners
    )
    total_balance = sum(int(p.get('ref_balance') or 0) for p in partners)
    total_withdrawn = sum(int(p.get('ref_withdraw') or 0) for p in partners)
    count_10 = sum(1 for p in partners if role_uses_deposit_share(p.get('role')))
    count_20 = sum(1 for p in partners if role_uses_fixed_deposit_bonus(p.get('role')))

    lines = [
        '<b>👑 Refmaster / 2.0 — выплаты</b>',
        f'Партнёров: <b>{len(partners)}</b> '
        f'(1.0: {count_10}, 2.0: {count_20})',
        f'<b>🔴 К выплате всего: {total_pending} ₽</b>',
        f'На ref_balance: {total_balance} ₽ | выведено: {total_withdrawn} ₽',
        '',
        '<b>По партнёрам</b> (сверху — больше долг):',
    ]

    for i, p in enumerate(partners, 1):
        pending = partner_pending_payout(p['ref_balance'], p['ref_withdraw'])
        role = role_display_name(p.get('role'))
        uname = p.get('username')
        who = f'@{uname}' if uname else f'id {p["id"]}'
        code = p.get('custom_ref_code')
        code_bit = f' · <code>{code}</code>' if code else ''

        if role_uses_fixed_deposit_bonus(p.get('role')):
            accrual_note = (
                f'начисл. 90д: {p["qualified_deposits_count"]}×'
                f'{REFMASTER_20_DEPOSIT_BONUS_RUB}₽'
            )
        elif role_uses_deposit_share(p.get('role')):
            accrual_note = f'~50% от деп: {int(p.get("deposits_total") or 0) // 2}₽'
        else:
            accrual_note = '—'

        lines.append(
            f'\n{i}. <code>{p["id"]}</code> {who}{code_bit}\n'
            f'   <b>{role}</b> · 🔴 <b>{pending} ₽</b> к выплате\n'
            f'   баланс {p["ref_balance"]} | выведено {p["ref_withdraw"]} | {accrual_note}\n'
            f'   рефералов {p["refs_total"]} (оплатили {p["paying_refs"]}) · '
            f'деп {p["deposits_count"]} / {p["deposits_total"]}₽'
        )

    lines.append('\n<i>Кнопки ниже — полная карточка партнёра.</i>')
    return '\n'.join(lines)


def format_refmaster_20_payout_block(dashboard: dict) -> str:
    ref_balance = int(dashboard.get('ref_balance') or 0)
    ref_withdraw = int(dashboard.get('ref_withdraw') or 0)
    pending = max(ref_balance - ref_withdraw, 0)
    bonus_count = int(dashboard.get('bonus_deposits_count') or 0)
    qualified_count = int(dashboard.get('qualified_deposits_count') or 0)
    expected_accrual = qualified_count * REFMASTER_20_DEPOSIT_BONUS_RUB
    expected_all_time = bonus_count * REFMASTER_20_DEPOSIT_BONUS_RUB
    delta = ref_balance - expected_accrual
    delta_sign = '+' if delta >= 0 else ''

    return (
        f'\n<b>💸 Выплаты Refmaster 2.0</b>\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'На реф. балансе (ref_balance): <b>{ref_balance} ₽</b>\n'
        f'Уже выплачено (ref_withdraw): <b>{ref_withdraw} ₽</b>\n'
        f'<b>🔴 К выплате сейчас: {pending} ₽</b>\n\n'
        f'<b>Начисления (+{REFMASTER_20_DEPOSIT_BONUS_RUB}₽ за продление от '
        f'{REFMASTER_20_MIN_DEPOSIT_RUB}₽, без ГБ):</b>\n'
        f'• Квалиф. депозитов всего: <b>{bonus_count}</b> '
        f'(оценка ×{REFMASTER_20_DEPOSIT_BONUS_RUB}₽ = <b>{expected_all_time} ₽</b>)\n'
        f'• В окне 90 дней (бот реально начисляет): <b>{qualified_count}</b> '
        f'→ <b>{expected_accrual} ₽</b>\n'
        f'• Сверка ref_balance с окном 90д: <b>{delta_sign}{delta} ₽</b>\n'
        f'  <i>(+ — на балансе больше ожидания; − — часть ещё не доначислена)</i>\n'
    )


def format_admin_campaign_stats(dashboard: dict) -> str:
    """HTML-карточка по ID (кампания / рефовод) для админки."""
    master_id = dashboard.get('ref_master_id') or dashboard.get('campaign_id')
    role = dashboard.get('role')
    if role_has_refmaster_ui(role):
        role_label = role_display_name(role)
    elif role:
        role_label = str(role)
    else:
        role_label = 'не задана'

    role_hint = ''
    if not role_has_refmaster_ui(role) and int(dashboard.get('deposits_count') or 0) > 0:
        role_hint = (
            f'\n⚠️ Назначьте роль Refmaster / 2.0 пользователю '
            f'<code>{master_id}</code> (Админка → Роли), иначе бонусы не начисляются.\n'
        )

    refs_total = int(dashboard.get('refs_total') or 0)
    paying_refs = int(dashboard.get('paying_refs') or 0)
    deposits_count = int(dashboard.get('deposits_count') or 0)
    deposits_total = int(dashboard.get('deposits_total') or 0)
    qualified_count = int(dashboard.get('qualified_deposits_count') or 0)
    ref_balance = int(dashboard.get('ref_balance') or 0)
    ref_withdraw = int(dashboard.get('ref_withdraw') or 0)
    ref_amount = int(dashboard.get('ref_amount') or 0)
    username = dashboard.get('username')
    user_line = f'@{username}' if username else 'нет в users'
    kind = 'Рекламная кампания' if dashboard.get('is_campaign') else 'Рефовод (Telegram ID)'
    custom_code = dashboard.get('custom_ref_code')
    code_line = f'Авторский код: <code>{custom_code}</code>\n' if custom_code else ''

    payout_block = ''
    stats_tail = ''

    if role_uses_fixed_deposit_bonus(role):
        payout_block = format_refmaster_20_payout_block(dashboard)
        stats_tail = _format_deposit_rows_block(dashboard.get('deposit_rows') or [], role)
    elif role_uses_deposit_share(role):
        earned_est = int(deposits_total * 0.5)
        pending = max(int(deposits_total * 0.5) - ref_withdraw, 0)
        payout_block = (
            f'\n<b>💸 Выплаты Refmaster 1.0</b>\n'
            f'Оценка 50% от депозитов: <b>{earned_est} ₽</b>\n'
            f'ref_balance: <b>{ref_balance} ₽</b> | выведено: <b>{ref_withdraw} ₽</b>\n'
            f'<b>🔴 К выплате (оценка): {pending} ₽</b>\n'
        )
        stats_tail = _format_deposit_rows_block(dashboard.get('deposit_rows') or [], role)
    else:
        payout_block = (
            f'\nМодель: <b>7 дней</b> подписки за первый депозит (без денежного ref_balance).\n'
            f'ref_balance: <b>{ref_balance} ₽</b>\n'
        )

    desc = dashboard.get('campaign_description') or '—'
    if dashboard.get('is_campaign'):
        desc_block = f'{desc}\n\n'
    else:
        desc_block = ''

    return (
        f'<b>{kind} · ID {master_id}</b>\n'
        f'<b>{dashboard["campaign_name"]}</b>\n\n'
        f'{desc_block}'
        f'Ссылка: <code>{dashboard["campaign_link"]}</code>\n'
        f'{code_line}'
        f'Профиль users: {user_line}\n'
        f'Роль: <b>{role_label}</b>{role_hint}\n'
        f'Приглашено (ref_amount): <b>{ref_amount}</b>\n\n'
        f'<b>📊 Рефералы</b>\n'
        f'Всего: <b>{refs_total}</b> | с оплатой: <b>{paying_refs}</b> | '
        f'без оплаты: <b>{refs_total - paying_refs}</b>\n'
        f'Депозитов: <b>{deposits_count}</b> на <b>{deposits_total} ₽</b>\n'
        f'Депозитов в окне 90д: <b>{qualified_count}</b>'
        + (
            f' (квалиф. +{REFMASTER_20_DEPOSIT_BONUS_RUB}₽: от {REFMASTER_20_MIN_DEPOSIT_RUB}₽, подписка)'
            if role_uses_fixed_deposit_bonus(role)
            else ''
        )
        + '\n'
        f'{payout_block}'
        f'{stats_tail}'
    )
