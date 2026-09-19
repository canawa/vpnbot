"""Статистика и события акции «выборы VPN»."""
from __future__ import annotations

import sqlite3 as sq
from datetime import datetime

from prices import ELECTIONS_PROMO_PLAN, is_elections_promo_plan

EV_SENT = 'el_sent'
EV_CLICK_CHOOSE = 'el_click_choose'
EV_CLICK_PLAN = 'el_click_plan'
EV_PAID = 'el_paid'


def _connect():
    return sq.connect('database.db')


def log_elections_event(user_id: int, event_type: str, meta: str | None = None) -> None:
    with _connect() as con:
        con.execute(
            'INSERT INTO funnel_events (user_id, event_type, meta, created_at) VALUES (?, ?, ?, ?)',
            (int(user_id), event_type, meta, datetime.now().isoformat()),
        )
        con.commit()


def log_elections_sent(user_id: int) -> None:
    log_elections_event(user_id, EV_SENT)


def log_elections_click_choose(user_id: int) -> None:
    log_elections_event(user_id, EV_CLICK_CHOOSE)


def log_elections_click_plan(user_id: int, amount: int, days: int) -> None:
    log_elections_event(user_id, EV_CLICK_PLAN, f'days={int(days)};amount={int(amount)}')


def log_elections_paid(user_id: int, amount: int, days: int, payment_id: str | None = None) -> None:
    meta = f'days={int(days)};amount={int(amount)}'
    if payment_id:
        meta = f'{meta};pid={str(payment_id).strip()}'
    # один раз на payment_id
    if payment_id:
        with _connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT 1 FROM funnel_events
                WHERE event_type = ? AND meta LIKE ?
                LIMIT 1
                """,
                (EV_PAID, f'%pid={str(payment_id).strip()}%'),
            )
            if cur.fetchone():
                return
    log_elections_event(user_id, EV_PAID, meta)


def _count(cur, event_type: str) -> tuple[int, int]:
    cur.execute(
        'SELECT COUNT(*), COUNT(DISTINCT user_id) FROM funnel_events WHERE event_type = ?',
        (event_type,),
    )
    total, uniq = cur.fetchone() or (0, 0)
    return int(total or 0), int(uniq or 0)


def _pct(part: int, whole: int) -> str:
    if not whole:
        return '—'
    return f'{part / whole * 100:.1f}%'


def fetch_elections_promo_stats() -> str:
    with _connect() as con:
        cur = con.cursor()
        sent_n, sent_u = _count(cur, EV_SENT)
        choose_n, choose_u = _count(cur, EV_CLICK_CHOOSE)
        plan_n, plan_u = _count(cur, EV_CLICK_PLAN)
        paid_n, paid_u = _count(cur, EV_PAID)

        plan_by_days: dict[int, int] = {30: 0, 90: 0, 360: 0}
        cur.execute(
            'SELECT meta FROM funnel_events WHERE event_type = ?',
            (EV_CLICK_PLAN,),
        )
        for (meta,) in cur.fetchall():
            days = _meta_int(meta, 'days')
            if days in plan_by_days:
                plan_by_days[days] += 1

        paid_by_days: dict[int, tuple[int, int]] = {
            30: (0, 0), 90: (0, 0), 360: (0, 0),
        }  # count, revenue
        cur.execute(
            'SELECT meta FROM funnel_events WHERE event_type = ?',
            (EV_PAID,),
        )
        for (meta,) in cur.fetchall():
            days = _meta_int(meta, 'days')
            amount = _meta_int(meta, 'amount')
            if days in paid_by_days and amount is not None:
                cnt, rev = paid_by_days[days]
                paid_by_days[days] = (cnt + 1, rev + amount)

        cur.execute(
            """
            SELECT COUNT(*) FROM users
            WHERE elections_promo_until IS NOT NULL
              AND elections_promo_until > ?
            """,
            (datetime.now().isoformat(),),
        )
        promo_active = int((cur.fetchone() or (0,))[0] or 0)

    revenue = sum(rev for _, rev in paid_by_days.values())
    lines = [
        '🗳 <b>Статистика акции «выборы VPN»</b>\n',
        f'📨 Доставлено: <b>{sent_n}</b> (уник. {sent_u})',
        f'🟢 Нажали «СДЕЛАТЬ СВОЙ ВЫБОР»: <b>{choose_u}</b> '
        f'({_pct(choose_u, sent_u)} от доставленных)',
        f'💳 Выбрали тариф: <b>{plan_u}</b> '
        f'({_pct(plan_u, choose_u)} от клика)',
        f'✅ Оплатили: <b>{paid_u}</b> '
        f'({_pct(paid_u, choose_u)} от клика, {_pct(paid_u, sent_u)} от рассылки)',
        f'💰 Выручка: <b>{revenue}₽</b>',
        f'⏳ Акция активна сейчас: <b>{promo_active}</b>',
        '',
        '<b>Клики по тарифам</b>',
    ]
    for days in (30, 90, 360):
        price = ELECTIONS_PROMO_PLAN[days]
        label = {30: '1 мес', 90: '3 мес', 360: '12 мес'}[days]
        lines.append(f'• {label} ({price}₽): {plan_by_days.get(days, 0)}')

    lines.append('')
    lines.append('<b>Оплаты по тарифам</b>')
    for days in (30, 90, 360):
        price = ELECTIONS_PROMO_PLAN[days]
        label = {30: '1 мес', 90: '3 мес', 360: '12 мес'}[days]
        cnt, rev = paid_by_days[days]
        lines.append(f'• {label} ({price}₽): {cnt} шт. / {rev}₽')

    return '\n'.join(lines)


def _meta_int(meta: str | None, key: str) -> int | None:
    if not meta:
        return None
    prefix = f'{key}='
    for part in str(meta).split(';'):
        part = part.strip()
        if part.startswith(prefix):
            try:
                return int(part[len(prefix):])
            except ValueError:
                return None
    return None


def maybe_log_elections_paid(user_id: int, amount: int, days: int, payment_id: str | None = None) -> None:
    if is_elections_promo_plan(amount, days):
        log_elections_paid(user_id, amount, days, payment_id)
