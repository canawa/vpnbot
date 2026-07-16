"""Доставка в Telegram: пропуск пользователей, заблокировавших бота или удаливших аккаунт."""
from aiogram.exceptions import TelegramForbiddenError, TelegramNotFound

from databases import db_connect, db_retry


def is_telegram_unreachable(exc: BaseException) -> bool:
    return isinstance(exc, (TelegramForbiddenError, TelegramNotFound))


def is_user_bot_blocked(user_id: int) -> bool:
    with db_connect() as con:
        cur = con.cursor()
        cur.execute(
            'SELECT COALESCE(bot_blocked, 0) FROM users WHERE id = ?',
            (user_id,),
        )
        row = cur.fetchone()
    return bool(row and row[0])


def mark_user_bot_blocked(user_id: int) -> None:
    def _write():
        with db_connect() as con:
            con.execute(
                'UPDATE users SET bot_blocked = 1 WHERE id = ?',
                (user_id,),
            )
            con.commit()

    db_retry(_write)


def clear_user_bot_blocked(user_id: int) -> None:
    def _write():
        with db_connect() as con:
            con.execute(
                'UPDATE users SET bot_blocked = 0 WHERE id = ?',
                (user_id,),
            )
            con.commit()

    db_retry(_write)
