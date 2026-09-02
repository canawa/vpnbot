"""Админ-композер массовой рассылки: текст, фото, кнопки, превью, отправка."""
from __future__ import annotations

import asyncio
import logging
import os
import re
from html import unescape

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot_delivery import (
    is_telegram_unreachable,
    is_user_bot_blocked,
    mark_user_bot_blocked,
    safe_delete_message,
)
from emojis import CHECK_EMOJI_HTML, CROSS_EMOJI_HTML
from ikbs import ikb_admin, ikb_admin_back, ikb_broadcast_composer, ikb_broadcast_confirm

PLACEHOLDER = (
    'Здесь появится текст поста. Нажмите «✏️ Текст» или «🖼️ Фото».'
)
HINT_TEXT = '✏️ Пришлите текст поста (можно с HTML и эмодзи). «-» — очистить текст.'
HINT_PHOTO = '🖼️ Пришлите фото. «-» — убрать фото.'
HINT_BUTTONS = (
    '⚪ Кнопки поста — по одной на строку:\n'
    '<code>Текст | https://t.me/...</code>\n'
    '<code>Купить | callback:buy_vpn</code>\n'
    '<code>Осталось 1 день | callback:deposit_1099_360_card | primary</code>\n\n'
    'Стиль: <code>primary</code> / <code>success</code> / <code>danger</code>.\n'
    '«-» или «очистить» — убрать кнопки.'
)

CAPTION_LIMIT = 1024
TEXT_LIMIT = 4096
DELAY_SEC = float(os.getenv('PROMO_BROADCAST_DELAY_SEC', '0.3'))
_TAG_RE = re.compile(r'<[^>]+>')
_broadcast_lock = asyncio.Lock()


class BroadcastComposer(StatesGroup):
    idle = State()
    waiting_text = State()
    waiting_photo = State()
    waiting_buttons = State()
    confirming = State()


def _plain_len(html: str | None) -> int:
    if not html:
        return 0
    return len(unescape(_TAG_RE.sub('', html)))


def _draft_from_data(data: dict) -> dict:
    return {
        'text': (data.get('text') or '').strip() or None,
        'photo_id': data.get('photo_id') or None,
        'buttons': list(data.get('buttons') or []),
        'recipients': int(data.get('recipients') or 0),
        'panel_chat_id': data.get('panel_chat_id'),
        'panel_message_id': data.get('panel_message_id'),
    }


def _build_post_markup(buttons: list) -> InlineKeyboardMarkup | None:
    if not buttons:
        return None
    rows = []
    for item in buttons:
        kwargs = {'text': item['text']}
        if item.get('url'):
            kwargs['url'] = item['url']
        else:
            kwargs['callback_data'] = item['callback_data']
        if item.get('style'):
            kwargs['style'] = item['style']
        rows.append([InlineKeyboardButton(**kwargs)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _normalize_url(raw: str) -> str | None:
    s = (raw or '').strip()
    if s.startswith(('http://', 'https://', 'tg://')):
        return s
    if s.startswith(('t.me/', 'telegram.me/')):
        return 'https://' + s
    return None


def _parse_style(raw: str | None) -> str | None:
    if not raw:
        return None
    style = raw.strip().lower()
    if style in ('primary', 'success', 'danger'):
        return style
    return None


def parse_broadcast_buttons(raw: str) -> list[dict]:
    buttons = []
    for line in (raw or '').splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 2 and ' - ' in line:
            left, right = line.split(' - ', 1)
            parts = [left.strip(), right.strip()]
        if len(parts) < 2 or not parts[0]:
            raise ValueError(f'Не разобрал строку: {line}')
        text, target = parts[0], parts[1]
        style = _parse_style(parts[2]) if len(parts) > 2 else None
        item = {'text': text}
        if style:
            item['style'] = style
        url = _normalize_url(target)
        if url:
            item['url'] = url
        else:
            cb = target
            for prefix in ('callback:', 'cb:', 'callback_data:'):
                if cb.lower().startswith(prefix):
                    cb = cb[len(prefix):].strip()
                    break
            else:
                raise ValueError(
                    f'Нужна ссылка или callback:… — строка: {line}'
                )
            if not cb or len(cb.encode('utf-8')) > 64:
                raise ValueError(f'callback_data слишком длинный: {cb}')
            item['callback_data'] = cb
        buttons.append(item)
    if not buttons:
        raise ValueError('Пустой список кнопок')
    return buttons


def render_composer_text(draft: dict, hint: str | None = None) -> str:
    body = draft.get('text') or PLACEHOLDER
    extra = ''
    if draft.get('photo_id'):
        extra += '\n\n🖼 Фото прикреплено'
    buttons = draft.get('buttons') or []
    if buttons:
        extra += f'\n🔘 Кнопок: {len(buttons)}'
    extra += f'\n\nПолучателей: {draft.get("recipients") or 0}'
    if hint:
        extra += f'\n\n{hint}'
    header = '📩 <b>Рассылка</b>\n\n'
    limit = 3900 - len(extra)
    if len(body) > limit:
        body = body[:limit] + '…'
    return header + body + extra


def _composer_kb(draft: dict) -> InlineKeyboardMarkup:
    return ikb_broadcast_composer(
        has_text=bool(draft.get('text')),
        has_photo=bool(draft.get('photo_id')),
        has_buttons=bool(draft.get('buttons')),
    )


def _content_error(draft: dict) -> str | None:
    text = draft.get('text')
    photo = draft.get('photo_id')
    if not text and not photo:
        return 'Сначала добавьте текст или фото.'
    if photo and _plain_len(text) > CAPTION_LIMIT:
        return f'Подпись к фото больше {CAPTION_LIMIT} символов.'
    if not photo and _plain_len(text) > TEXT_LIMIT:
        return f'Текст длиннее {TEXT_LIMIT} символов.'
    return None


async def _send_post(bot: Bot, chat_id: int, draft: dict) -> None:
    markup = _build_post_markup(draft.get('buttons') or [])
    text = draft.get('text')
    photo_id = draft.get('photo_id')
    if photo_id:
        await bot.send_photo(
            chat_id=chat_id,
            photo=photo_id,
            caption=text,
            parse_mode='HTML',
            reply_markup=markup,
        )
        return
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode='HTML',
        reply_markup=markup,
        disable_web_page_preview=False,
    )


async def _remember_panel(message: Message, state: FSMContext) -> None:
    await state.update_data(
        panel_chat_id=message.chat.id,
        panel_message_id=message.message_id,
    )


async def _try_edit_panel(message_or_bot, *, text: str, kb, chat_id=None, message_id=None) -> bool:
    try:
        if message_or_bot is not None and hasattr(message_or_bot, 'edit_text'):
            await message_or_bot.edit_text(text, parse_mode='HTML', reply_markup=kb)
        else:
            await message_or_bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode='HTML',
                reply_markup=kb,
            )
        return True
    except Exception as e:
        err = str(e).lower()
        if 'message is not modified' in err:
            return True
        return False


async def _show_composer(
    bot: Bot,
    state: FSMContext,
    *,
    chat_id: int,
    hint: str | None = None,
    replace_message: Message | None = None,
) -> None:
    data = await state.get_data()
    draft = _draft_from_data(data)
    text = render_composer_text(draft, hint)
    kb = _composer_kb(draft)
    if replace_message is not None:
        if await _try_edit_panel(replace_message, text=text, kb=kb):
            await _remember_panel(replace_message, state)
            return
        await safe_delete_message(replace_message)
    panel_id = data.get('panel_message_id')
    panel_chat = data.get('panel_chat_id') or chat_id
    if panel_id:
        if await _try_edit_panel(bot, text=text, kb=kb, chat_id=panel_chat, message_id=panel_id):
            return
        try:
            await bot.delete_message(panel_chat, panel_id)
        except Exception:
            pass
    sent = await bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
    await _remember_panel(sent, state)


async def _open_composer(
    bot: Bot,
    callback: CallbackQuery,
    state: FSMContext,
    fetch_recipients,
) -> None:
    await state.clear()
    recipients = await asyncio.to_thread(fetch_recipients)
    await state.set_state(BroadcastComposer.idle)
    await state.update_data(
        text=None,
        photo_id=None,
        buttons=[],
        recipients=len(recipients),
    )
    await safe_delete_message(callback.message)
    sent = await callback.message.answer(
        render_composer_text(_draft_from_data(await state.get_data())),
        parse_mode='HTML',
        reply_markup=_composer_kb(_draft_from_data(await state.get_data())),
    )
    await _remember_panel(sent, state)


async def _back_to_admin(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await safe_delete_message(callback.message)
    await callback.message.answer(
        '👤 Админ панель',
        parse_mode='HTML',
        reply_markup=ikb_admin,
    )


async def _run_broadcast(bot: Bot, draft: dict, fetch_recipients) -> str:
    user_ids = await asyncio.to_thread(fetch_recipients)
    success = 0
    failed = 0
    blocked = 0
    for user_id in user_ids:
        if await asyncio.to_thread(is_user_bot_blocked, user_id):
            continue
        try:
            await _send_post(bot, user_id, draft)
            success += 1
        except TelegramRetryAfter as e:
            wait_s = float(getattr(e, 'retry_after', 5) or 5)
            await asyncio.sleep(wait_s + 0.5)
            try:
                await _send_post(bot, user_id, draft)
                success += 1
            except Exception as retry_err:
                if is_telegram_unreachable(retry_err):
                    await asyncio.to_thread(mark_user_bot_blocked, user_id)
                    blocked += 1
                else:
                    failed += 1
                    logging.warning('broadcast retry user_id=%s: %s', user_id, retry_err)
        except TelegramNetworkError as e:
            try:
                await asyncio.sleep(1)
                await _send_post(bot, user_id, draft)
                success += 1
            except Exception:
                failed += 1
                logging.warning('broadcast net user_id=%s: %s', user_id, e)
        except Exception as e:
            if is_telegram_unreachable(e):
                await asyncio.to_thread(mark_user_bot_blocked, user_id)
                blocked += 1
                logging.info('broadcast skip user_id=%s (blocked bot or deleted)', user_id)
            else:
                failed += 1
                logging.warning('broadcast user_id=%s: %s', user_id, e)
        await asyncio.sleep(DELAY_SEC)
    return (
        f'{CHECK_EMOJI_HTML} Рассылка завершена.\n\n'
        f'В базе (не blocked): {len(user_ids)}\n'
        f'Отправлено: {success}\n'
        f'🚫 Заблокировали бота: {blocked}\n'
        f'Ошибок: {failed}'
    )


def setup_admin_broadcast(dp: Dispatcher, bot: Bot, *, admin_ids, fetch_recipients):
    admin_filter = F.from_user.id.in_(admin_ids)

    @dp.callback_query(F.data == 'admin_broadcast', admin_filter)
    async def open_composer(callback: CallbackQuery, state: FSMContext):
        if _broadcast_lock.locked():
            await callback.answer('Уже идёт другая рассылка. Дождитесь итога.', show_alert=True)
            return
        await callback.answer()
        await _open_composer(bot, callback, state, fetch_recipients)

    @dp.callback_query(F.data == 'bc_cancel', admin_filter)
    async def cancel_composer(callback: CallbackQuery, state: FSMContext):
        await callback.answer('Отмена')
        await _back_to_admin(callback, state)

    @dp.callback_query(F.data == 'bc_draft', admin_filter)
    async def back_to_draft(callback: CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.set_state(BroadcastComposer.idle)
        await _show_composer(bot, state, chat_id=callback.message.chat.id, replace_message=callback.message)

    @dp.callback_query(F.data == 'bc_text', admin_filter)
    async def ask_text(callback: CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.set_state(BroadcastComposer.waiting_text)
        await _show_composer(
            bot, state, chat_id=callback.message.chat.id,
            hint=HINT_TEXT, replace_message=callback.message,
        )

    @dp.callback_query(F.data == 'bc_photo', admin_filter)
    async def ask_photo(callback: CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.set_state(BroadcastComposer.waiting_photo)
        await _show_composer(
            bot, state, chat_id=callback.message.chat.id,
            hint=HINT_PHOTO, replace_message=callback.message,
        )

    @dp.callback_query(F.data == 'bc_buttons', admin_filter)
    async def ask_buttons(callback: CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.set_state(BroadcastComposer.waiting_buttons)
        await _show_composer(
            bot, state, chat_id=callback.message.chat.id,
            hint=HINT_BUTTONS, replace_message=callback.message,
        )

    @dp.callback_query(F.data == 'bc_preview', admin_filter)
    async def preview_post(callback: CallbackQuery, state: FSMContext):
        draft = _draft_from_data(await state.get_data())
        err = _content_error(draft)
        if err:
            await callback.answer(err, show_alert=True)
            return
        await callback.answer('Превью')
        try:
            await _send_post(bot, callback.from_user.id, draft)
        except Exception as e:
            logging.warning('broadcast preview failed: %s', e)
            await callback.message.answer(
                f'{CROSS_EMOJI_HTML} Не удалось показать превью: {html_escape_short(e)}',
                parse_mode='HTML',
            )

    @dp.callback_query(F.data == 'bc_send', admin_filter)
    async def ask_confirm(callback: CallbackQuery, state: FSMContext):
        draft = _draft_from_data(await state.get_data())
        err = _content_error(draft)
        if err:
            await callback.answer(err, show_alert=True)
            return
        if _broadcast_lock.locked():
            await callback.answer('Уже идёт другая рассылка.', show_alert=True)
            return
        await callback.answer()
        await state.set_state(BroadcastComposer.confirming)
        n = draft.get('recipients') or 0
        flags = []
        flags.append('текст' if draft.get('text') else 'без текста')
        flags.append('фото' if draft.get('photo_id') else 'без фото')
        flags.append(f'кнопок: {len(draft.get("buttons") or [])}')
        text = (
            f'📥 <b>Отправить рассылку?</b>\n\n'
            f'Получателей: <b>{n}</b>\n'
            f'{", ".join(flags)}\n\n'
            'Сообщение уйдёт всем, кто не заблокировал бота.'
        )
        try:
            await callback.message.edit_text(
                text, parse_mode='HTML', reply_markup=ikb_broadcast_confirm,
            )
        except Exception:
            await safe_delete_message(callback.message)
            sent = await callback.message.answer(
                text, parse_mode='HTML', reply_markup=ikb_broadcast_confirm,
            )
            await _remember_panel(sent, state)

    @dp.callback_query(F.data == 'bc_confirm', admin_filter)
    async def confirm_send(callback: CallbackQuery, state: FSMContext):
        if _broadcast_lock.locked():
            await callback.answer('Уже идёт другая рассылка.', show_alert=True)
            return
        async with _broadcast_lock:
            current = await state.get_state()
            if current != BroadcastComposer.confirming.state:
                await callback.answer('Сначала нажмите «Отправить».', show_alert=True)
                return
            draft = _draft_from_data(await state.get_data())
            err = _content_error(draft)
            if err:
                await callback.answer(err, show_alert=True)
                return
            await callback.answer('Рассылаем…')
            await state.clear()
            await safe_delete_message(callback.message)
            status = await callback.message.answer('⏳ Рассылаем…')
            summary = await _run_broadcast(bot, draft, fetch_recipients)
        await safe_delete_message(status)
        await callback.message.answer(
            summary, parse_mode='HTML', reply_markup=ikb_admin_back,
        )

    @dp.message(BroadcastComposer.waiting_text, admin_filter)
    async def on_text(message: Message, state: FSMContext):
        raw = (message.text or '').strip()
        if raw in ('-', 'очистить', 'удалить'):
            await state.update_data(text=None)
        else:
            html = (message.html_text or message.caption or message.text or '').strip()
            photo_id = None
            if message.photo:
                photo_id = message.photo[-1].file_id
                html = (message.html_text or message.caption or '').strip()
            elif message.document and (message.document.mime_type or '').startswith('image/'):
                photo_id = message.document.file_id
                html = (message.html_text or message.caption or '').strip()
            updates = {'text': html or None}
            if photo_id:
                updates['photo_id'] = photo_id
            if not html and not photo_id:
                await message.answer('Пришлите текст или фото с подписью.')
                return
            await state.update_data(**updates)
        await safe_delete_message(message)
        await state.set_state(BroadcastComposer.idle)
        await _show_composer(bot, state, chat_id=message.chat.id)

    @dp.message(BroadcastComposer.waiting_photo, admin_filter)
    async def on_photo(message: Message, state: FSMContext):
        raw = (message.text or '').strip()
        if raw in ('-', 'очистить', 'удалить'):
            await state.update_data(photo_id=None)
        elif message.photo:
            await state.update_data(photo_id=message.photo[-1].file_id)
            caption = (message.html_text or message.caption or '').strip()
            if caption:
                data = await state.get_data()
                if not data.get('text'):
                    await state.update_data(text=caption)
        elif message.document and (message.document.mime_type or '').startswith('image/'):
            await state.update_data(photo_id=message.document.file_id)
        else:
            await message.answer('Пришлите фото или «-», чтобы убрать.')
            return
        await safe_delete_message(message)
        await state.set_state(BroadcastComposer.idle)
        await _show_composer(bot, state, chat_id=message.chat.id)

    @dp.message(BroadcastComposer.waiting_buttons, admin_filter)
    async def on_buttons(message: Message, state: FSMContext):
        raw = (message.text or message.caption or '').strip()
        if not raw:
            await message.answer('Пришлите кнопки текстом или «-».')
            return
        if raw.lower() in ('-', 'очистить', 'удалить'):
            await state.update_data(buttons=[])
        else:
            try:
                buttons = parse_broadcast_buttons(raw)
            except ValueError as e:
                await message.answer(
                    f'{CROSS_EMOJI_HTML} {e}\n\n{HINT_BUTTONS}',
                    parse_mode='HTML',
                )
                return
            await state.update_data(buttons=buttons)
        await safe_delete_message(message)
        await state.set_state(BroadcastComposer.idle)
        await _show_composer(bot, state, chat_id=message.chat.id)


def html_escape_short(exc: BaseException, limit: int = 180) -> str:
    from html import escape
    text = escape(f'{type(exc).__name__}: {exc}')
    if len(text) > limit:
        return text[:limit] + '…'
    return text
