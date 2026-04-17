import requests
# PAY FUNCTIONS
def get_pay_link(amount):
    headers = {"Crypto-Pay-API-Token": API_TOKEN}
    data = {"asset": "USDT", "amount": amount}
    response = requests.post('https://pay.crypt.bot/api/createInvoice', headers=headers, json=data)
    response = response.json()
    return response


def check_payment_status(invoice_id):
    headers = {"Crypto-Pay-API-Token": API_TOKEN,
               "Content-Type": "application/json"
               }
    # Получаем только нужный инвойс по ID, а не все инвойсы
    response = requests.post('https://pay.crypt.bot/api/getInvoices', headers=headers,
                             json={"invoice_ids": [invoice_id]})
    response = response.json()
    if response.get('ok') and response.get('result', {}).get('items'):
        inv = response['result']['items'][0]
        if inv['invoice_id'] == invoice_id:
            return inv['status'], float(inv['amount']) * rub_to_usdt  # возвращаем статус оплаты и сумму в рублях

    return None, None
@dp.callback_query(lambda c: c.data.startswith('check_'))
async def check_payment_yookassa_callback(callback: CallbackQuery): # сюды
    await callback.answer("🔄 Проверка статуса оплаты") # на пол экрана хуйня высветится
    _ , amount , payment_id = callback.data.split('_')
    # Убрали лишний print для экономии памяти
    if check_payment_yookassa_status(int(amount), payment_id, callback.from_user.id):
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount, callback.from_user.id))
            cur.execute('SELECT ref_master_id, registration_date FROM referal_users WHERE referral_id = ?', (callback.from_user.id,))
            ref_master = cur.fetchone()
            if ref_master: # если есть рефовод то:
                ref_master_id = ref_master[0]
                registration_date_str = ref_master[1]
                if registration_date_str:
                    registration_date = date.fromisoformat(registration_date_str)
                    three_months_later = registration_date + timedelta(days=90)
                    if date.today() <= three_months_later:
                        # Проверяем роль рефмастера
                        cur.execute('SELECT role FROM users WHERE id = ?', (ref_master_id,))
                        ref_master_role = cur.fetchone()
                        if ref_master_role and ref_master_role[0] == 'refmaster':
                            cur.execute('UPDATE users SET ref_balance = ref_balance + ? WHERE id = ?', (int(amount)/2, ref_master_id)) # начислить 50% реферального бонуса рефоводу
            con.commit()
        handled_vpn = await _maybe_complete_vpn_after_topup(callback.from_user.id, int(amount), callback.message)
        if handled_vpn:
            await callback.message.delete()
            return
        await callback.message.answer(f'🤑 Оплачено! \n\n ➕ Начислено {amount} ₽ на баланс', parse_mode='HTML', reply_markup=ikb_back)
        await callback.message.delete()

    else:
        await callback.message.answer(f'👀 Ожидаем оплату, оплатите и попробуйте снова!', parse_mode='HTML', reply_markup=ikb_back)


def check_payment_yookassa_status(amount, payment_id, user_id): # функция для проверки статуса оплаты через Юкассу
    payment = Payment.find_one(payment_id)
    if payment.status == 'succeeded':
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('INSERT INTO transactions (user_id, amount, type, date) VALUES (?, ?, ?, ?)', (user_id, amount, 'yookassa', datetime.now().isoformat() ))
            con.commit()
        return True
    else:
        return False