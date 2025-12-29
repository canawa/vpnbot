from aiogram.types import LabeledPrice, PreCheckoutQuery, Message

# В обработчике выбора способа оплаты (пример — внутри process_deposit):
# method == "Stars" branch
star_rate = 1.50  # 1 звезда = 1.50 рубля
stars_needed = int(amount_rub * star_rate)

if stars_needed <= 0:
    await callback.message.answer("❌ Сумма слишком мала для оплаты", parse_mode='HTML', reply_markup=ikb_deposit_methods)
    return

try:
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=f"Пополнение баланса на {amount_rub} ₽",
        description=f"Пополнение баланса в боте на сумму {amount_rub} рублей",
        payload=f"deposit_{amount_rub}_{callback.from_user.id}",
        provider_token="",  # Для Telegram Stars обычно не нужен provider_token
        currency="XTR",  # Валюта Telegram Stars
        prices=[LabeledPrice(label=f"Пополнение {amount_rub} ₽", amount=stars_needed)],
        start_parameter=f"deposit_{amount_rub}"
    )
except Exception as e:
    await callback.message.answer(
        f"❌ Не удалось создать заявку:\n{str(e)}",
        parse_mode="HTML",
        reply_markup=ikb_deposit_methods
    )

# Обработчик pre-checkout (подтверждение платежа)
@dp.pre_checkout_query()
async def pre_checkout_query_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

# Обработчик успешного платежа (парсит payload: deposit_{amount}_{user_id})
@dp.message(lambda m: m.successful_payment is not None)
async def successful_payment_handler(message: Message):
    payment = message.successful_payment
    payload = payment.invoice_payload

    try:
        parts = payload.split('_')
        if len(parts) >= 3 and parts[0] == 'deposit':
            amount_rub = int(parts[1])
            user_id = int(parts[2])

            if message.from_user.id != user_id:
                await message.answer("❌ Ошибка: несоответствие пользователя")
                return

            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount_rub, user_id))

                # Начисление рефмастеру 50% (если есть)
                cur.execute('SELECT ref_master_id FROM referal_users WHERE referral_id = ?', (user_id,))
                ref_master_result = cur.fetchone()
                if ref_master_result:
                    ref_master_id = ref_master_result[0]
                    cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                    ref_master_role_result = cur.fetchone()
                    if ref_master_role_result and ref_master_role_result[0] == 'refmaster':
                        ref_bonus = int(amount_rub * 0.5)
                        cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (ref_bonus, ref_master_id))
                        try:
                            await bot.send_message(ref_master_id, f"💵 Вам начислено {ref_bonus} ₽ реферального бонуса", parse_mode='HTML')
                        except:
                            pass

                con.commit()

            await message.answer(
                f"✅ Платёж получен!\n\n➕ Начислено {amount_rub} ₽ на баланс 💸",
                parse_mode="HTML",
                reply_markup=ikb_back
            )

            # Уведомление админам (опционально)
            username = message.from_user.username or "Без имени"
            first_name = message.from_user.first_name or "Не указано"
            notify_text = (
                f"💰 <b>Пополнение баланса</b>\n\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"👤 Имя: {first_name}\n"
                f"📝 Username: @{username}\n"
                f"💵 Сумма: {amount_rub} ₽\n"
                f"💳 Способ: Telegram Stars"
            )
            await notify_admins(notify_text)
        else:
            await message.answer("❌ Ошибка обработки платежа")
    except Exception as e:
        await message.answer(f"❌ Ошибка обработки платежа: {e}")