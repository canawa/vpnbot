

from aiogram.types import ChatMember

CHANNEL_ID = '@coffemaniavpn'

async def is_subscribed(bot, user_id):
    try:
        member: ChatMember = await bot.get_chat_member(
            chat_id = CHANNEL_ID,
            user_id=user_id
        )
        return member.status not in ('left', 'kicked') # если не лефт и не кикед то вернет true
    except Exception as e:
        print(e)
        return False

