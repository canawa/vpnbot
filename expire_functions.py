import logging
import sqlite3 as sq
from datetime import datetime, date
from datetime import timedelta
import asyncio

from main import PING_UNCONNECTED_PHOTO
from texts import PING_CAPTION
from vpn import Vpn
from ikbs import *
from aiogram.exceptions import TelegramForbiddenError
async def check_expired_subscriptions_table(bot):
    """Таблица subscriptions: уведомление в день окончания subscription_expires_at (TEXT, сравнение через date())."""
    while True:
        try:
            today_str = date.today().isoformat()
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT s.user_id FROM subscriptions s
                    INNER JOIN users u ON u.id = s.user_id
                    WHERE date(s.subscription_expires_at) = date(?)
                      AND (s.runout_notified IS NULL OR s.runout_notified = 0)
                    """,
                    (today_str,),
                )
                rows = cur.fetchall()

                for (user_id,) in rows:
                    try:

                        await bot.send_message(
                            user_id,
                            (
                                '⏰ <b>Срок подписки истёк</b>\n\n'
                                'Чтобы продолжить пользоваться сервисом, продлите подписку.\n\n'
                            ),
                            parse_mode='HTML',
                            reply_markup=create_ikb_renew(),
                        )
                        cur.execute(
                            'UPDATE subscriptions SET runout_notified = 1 WHERE user_id = ?',
                            (user_id,),
                        )
                        con.commit()
                        print(f'{user_id}: subscriptions table — expired today notified')
                    except Exception as e:
                        print(f"subscriptions expired notify {user_id}: {e}")
                        continue

        except Exception as e:
            print(f'Error check_expired_subscriptions_table: {e}')

        await asyncio.sleep(3600)


async def check_expiring_tomorrow_subscriptions_table(bot):
    """Таблица subscriptions: напоминание накануне дня из subscription_expires_at."""
    while True:
        try:
            tomorrow_str = (date.today() + timedelta(days=1)).isoformat()
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT s.user_id FROM subscriptions s
                    INNER JOIN users u ON u.id = s.user_id
                    WHERE date(s.subscription_expires_at) = date(?)
                      AND (s.expiring_tomorrow_notified IS NULL OR s.expiring_tomorrow_notified = 0)
                    """,
                    (tomorrow_str,),
                )
                rows = cur.fetchall()

                for (user_id,) in rows:
                    try:
                        await bot.send_message(
                            user_id,
                            (
                                '⏰ <b>Подписка заканчивается завтра</b>\n\n'
                                'Продлите заранее, чтобы не прерывать доступ.\n\n'
                            ),
                            parse_mode='HTML',
                            reply_markup=create_ikb_renew(),
                        )
                        cur.execute(
                            'UPDATE subscriptions SET expiring_tomorrow_notified = 1 WHERE user_id = ?',
                            (user_id,),
                        )
                        con.commit()
                        print(f'{user_id}: subscriptions table — expiring tomorrow notified')
                    except Exception as e:
                        print(f"subscriptions tomorrow notify {user_id}: {e}")
                        continue

        except Exception as e:
            print(f'Error check_expiring_tomorrow_subscriptions_table: {e}')

        await asyncio.sleep(3600)


async def reset_runout_notified_daily(): # НЕ ЕБУ КАК РАБОТАЕТ!
    """Сбрасывает флаг runout_notified в 00:01 каждый день"""
    while True:
        try:
            now = datetime.now()
            # Вычисляем время до следующего 00:01
            next_reset = now.replace(hour=0, minute=1, second=0, microsecond=0)
            # Если уже прошло 00:01 сегодня, то следующий сброс будет завтра
            if now >= next_reset:
                next_reset += timedelta(days=1)

            # Вычисляем количество секунд до следующего 00:01
            seconds_until_reset = (next_reset - now).total_seconds()

            print(f"Next runout_notified reset will be at {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
            await asyncio.sleep(seconds_until_reset)

            # Сбрасываем флаги для всех пользователей
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET runout_notified = 0 WHERE runout_notified = 1')
                cur.execute('UPDATE users SET expiring_tomorrow_notified = 0 WHERE expiring_tomorrow_notified = 1')
                cur.execute('UPDATE subscriptions SET runout_notified = 0 WHERE runout_notified = 1')
                cur.execute('UPDATE subscriptions SET expiring_tomorrow_notified = 0 WHERE expiring_tomorrow_notified = 1')
                con.commit()
                print(
                    f"runout_notified and expiring_tomorrow_notified flags reset for all users at "
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
        except Exception as e:
            print(f"Error resetting runout_notified: {e}")
            # В случае ошибки ждем час перед следующей попыткой
            await asyncio.sleep(3600)

async def notify_gbs_ending(bot):
    while True:
        try:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('SELECT user_id FROM subscriptions')
                users = cur.fetchall()

            for (tg_id,) in users:
                # print('РАссылаем об окончании трафика')
                # print(tg_id)
                try:

                    user_data = Vpn().get_user_by_tg_id(tg_id)['response'][0]
                    current_limit = user_data['trafficLimitBytes']
                    used = user_data['userTraffic']['usedTrafficBytes']
                    remaining_gb = (current_limit - used) / 1073741824

                    with sq.connect('database.db') as con:
                        cur = con.cursor()
                        cur.execute('SELECT notified_low_traffic FROM subscriptions WHERE user_id = ?', (tg_id,))
                        notified = cur.fetchone()[0]

                    if remaining_gb <= 1 and not notified:
                        await bot.send_message(
                            tg_id,
                            f"\n<tg-emoji emoji-id='5274099962655816924'>❗️</tg-emoji><b>У вас осталось всего {remaining_gb:.1f} ГБ</b>\n\nПополните трафик, чтобы не прерывать доступ:",
                            parse_mode='HTML',
                            reply_markup=ikb_gbs_reminder_buy_option,
                        )
                        with sq.connect('database.db') as con:
                            con.execute('UPDATE subscriptions SET notified_low_traffic = 1 WHERE user_id = ?', (tg_id,))

                    # Сбрасываем флаг когда трафик обновился (купил ещё или продлил)
                    elif remaining_gb > 1 and notified:
                        with sq.connect('database.db') as con:
                            con.execute('UPDATE subscriptions SET notified_low_traffic = 0 WHERE user_id = ?', (tg_id,))

                except Exception as e:
                    print(f'Ошибка для {tg_id}: {e}')

                    continue

        except Exception as e:

            print(e)

        await asyncio.sleep(3600)


async def notify_inactive_trial_users(bot):
    while True:
        try:
            all_users = Vpn().get_unconnected_trial_users_tg_id()

            logging.info(f"[trial_notify] start batch, users={len(all_users)}")

            for user_id in all_users:
                try:
                    logging.info(f"[trial_notify] sending to user_id={user_id}")

                    await bot.send_photo(
                        chat_id=user_id,
                        photo=PING_UNCONNECTED_PHOTO,
                        caption=PING_CAPTION,
                        parse_mode='HTML',
                        reply_markup=ikb_my_sub
                    )

                    logging.info(f"[trial_notify] sent ok user_id={user_id}")

                except Exception as e:
                    logging.exception(f"[trial_notify] failed user_id={user_id}: {e}")

                await asyncio.sleep(0.05)

        except Exception as e:
            logging.exception(f"[trial_notify] batch error: {e}")

        logging.info("[trial_notify] sleep 24000s")
        await asyncio.sleep(24000)