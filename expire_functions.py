import sqlite3 as sq
from datetime import datetime, date
from datetime import timedelta
import asyncio

from ikbs import generate_ikb_main
async def check_expired_subscriptions(bot):
    """Проверяет истекшие подписки и отправляет уведомления пользователям"""
    while True:
        try:
            today = date.today()
            today_str = today.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD для корректного сравнения
            with sq.connect('database.db') as con:
                cur = con.cursor()
                # Находим всех пользователей, у которых сегодня истекает подписка и которым еще не отправляли уведомление
                cur.execute('''
                    SELECT DISTINCT keys.buyer_id FROM keys 
                    INNER JOIN users ON keys.buyer_id = users.id
                    WHERE keys.expiration_date = ? AND keys.buyer_id IS NOT NULL AND (users.runout_notified IS NULL OR users.runout_notified = 0)
                ''', (today_str,))
                expired_users = cur.fetchall()

                for user_tuple in expired_users:
                    user_id = user_tuple[0]
                    try:
                        # Проверяем, есть ли у пользователя другие активные ключи
                        cur.execute('''
                            SELECT COUNT(*) 
                            FROM keys 
                            WHERE buyer_id = ? AND expiration_date > ?
                        ''', (user_id, today_str))
                        active_keys_count = cur.fetchone()[0]

                        # Отправляем сообщение только если нет других активных ключей
                        if active_keys_count == 0:
                            cur.execute('UPDATE users SET runout_notified = 1 WHERE id = ?', (user_id,))
                            cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                            result = cur.fetchone()
                            balance = result[0] if result else 0
                            con.commit()
                            await bot.send_message(
                                user_id,
                                f"⏰ <b>У вас закончилась подписка</b>\n\n"
                                f"Ваша подписка VPN истекла сегодня. Для продолжения использования сервиса, пожалуйста, приобретите новый ключ.\n\n👉🏼 <b>Баланс: {balance}₽</b>",
                                parse_mode='HTML', reply_markup=generate_ikb_main(user_id))
                            print(f'{user_id} was notified about his subscription ending!')
                    except Exception as e:
                        print(f"Error {user_id}: {e}")
                        continue

        except Exception as e:
            print(f"Error checking expired subscriptions: {e}")

        # Проверяем раз в час (3600 секунд = 1 час)
        await asyncio.sleep(3600)

async def check_expiring_tomorrow_subscriptions(bot):
    """Проверяет подписки, истекающие завтра, и отправляет уведомления пользователям"""
    while True:
        try:
            today = date.today()
            tomorrow = today + timedelta(days=1)
            tomorrow_str = tomorrow.isoformat()  # Преобразуем дату в строку формата YYYY-MM-DD
            with sq.connect('database.db') as con:
                cur = con.cursor()
                # Находим всех пользователей, у которых завтра истекает подписка и которым еще не отправляли уведомление
                cur.execute('''
                    SELECT DISTINCT keys.buyer_id FROM keys 
                    INNER JOIN users ON keys.buyer_id = users.id
                    WHERE keys.expiration_date = ? AND keys.buyer_id IS NOT NULL 
                    AND (users.expiring_tomorrow_notified IS NULL OR users.expiring_tomorrow_notified = 0)
                ''', (tomorrow_str,))
                expiring_users = cur.fetchall()

                for user_tuple in expiring_users:
                    user_id = user_tuple[0]
                    try:
                        cur.execute('UPDATE users SET expiring_tomorrow_notified = 1 WHERE id = ?', (user_id,))
                        cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                        result = cur.fetchone()
                        balance = result[0] if result else 0
                        con.commit()
                        await bot.send_message(
                            user_id,
                            f"⏰ <b>Ваша подписка истекает завтра</b>\n\n"
                            f"Ваша подписка VPN истечет завтра. Чтобы не прерывать использование сервиса, пожалуйста, приобретите новый ключ заранее.\n\n👉🏼 <b>Баланс: {balance}₽</b>",
                            parse_mode='HTML', reply_markup=generate_ikb_main(user_id))
                        print(f'{user_id} was notified about his subscription expiring tomorrow!')
                    except Exception as e:
                        print(f"Error {user_id}: {e}")
                        continue

        except Exception as e:
            print(f"Error checking expiring tomorrow subscriptions: {e}")

        # Проверяем раз в час (3600 секунд = 1 час)
        await asyncio.sleep(3600)


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
                        cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                        result = cur.fetchone()
                        balance = result[0] if result else 0
                        await bot.send_message(
                            user_id,
                            '⏰ <b>Срок подписки истёк</b>\n\n'
                            'Дата окончания по вашей записи в системе подписок наступила сегодня. '
                            'Чтобы продолжить пользоваться сервисом, продлите подписку.\n\n'
                            f'👉🏼 <b>Баланс: {balance}₽</b>',
                            parse_mode='HTML',
                            reply_markup=generate_ikb_main(user_id),
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
                        cur.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
                        result = cur.fetchone()
                        balance = result[0] if result else 0
                        await bot.send_message(
                            user_id,
                            '⏰ <b>Подписка заканчивается завтра</b>\n\n'
                            'По записи в системе подписок срок действия истекает завтра. '
                            'Продлите заранее, чтобы не прерывать доступ.\n\n'
                            f'👉🏼 <b>Баланс: {balance}₽</b>',
                            parse_mode='HTML',
                            reply_markup=generate_ikb_main(user_id),
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
                print \
                    (f"runout_notified and expiring_tomorrow_notified flags reset for all users at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"Error resetting runout_notified: {e}")
            # В случае ошибки ждем час перед следующей попыткой
            await asyncio.sleep(3600)
