"""Доставка в Telegram: пропуск пользователей, заблокировавших бота или удаливших аккаунт."""
import sqlite3 as sq

from aiogram.exceptions import TelegramForbiddenError, TelegramNotFound


def is_telegram_unreachable(exc: BaseException) -> bool:
    return isinstance(exc, (TelegramForbiddenError, TelegramNotFound))


def is_user_bot_blocked(user_id: int) -> bool:
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute(
            'SELECT COALESCE(bot_blocked, 0) FROM users WHERE id = ?',
            (user_id,),
        )
        row = cur.fetchone()
    return bool(row and row[0])


def mark_user_bot_blocked(user_id: int) -> None:
    with sq.connect('database.db') as con:
        con.execute(
            'UPDATE users SET bot_blocked = 1 WHERE id = ?',
            (user_id,),
        )
        con.commit()


def clear_user_bot_blocked(user_id: int) -> None:
    with sq.connect('database.db') as con:
        con.execute(
            'UPDATE users SET bot_blocked = 0 WHERE id = ?',
            (user_id,),
        )
        con.commit()
