import aiogram
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, invoice, LabeledPrice, FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import asyncio # для работы с асинхронными функциями
import sqlite3 as sq
import requests
import pprint
import dotenv
import os

print('BOT STARTED!!!')

### РАБОТА С ФОТКАМИ:
try:
    WELCOME_PHOTO = FSInputFile("photos/welcome.png")
    BUY_VPN_PHOTO = FSInputFile("photos/buy_vpn.png")
    PROFILE_PHOTO = FSInputFile("photos/profile.png")
    DOCUMENTS_PHOTO = FSInputFile("photos/documents.jpg")
    INVITE_FRIEND_PHOTO = FSInputFile("photos/invite_friend.png")
    MY_KEYS_PHOTO = FSInputFile("photos/my_keys.png")
    DEPOSIT_PHOTO = FSInputFile("photos/deposit.png")
except FileNotFoundError:
    print("Файлы фото не найдены")
    exit()


dotenv.load_dotenv() # загружаем переменные окружения

bot = Bot(token=os.getenv('BOT_TOKEN')) # объект бота
API_TOKEN = os.getenv('CRYPTO_BOT_API_TOKEN') # это криптобот

def get_rate():
    r = requests.get('https://v6.exchangerate-api.com/v6/d8e4beb763d54112c6a63999/latest/USD')
    return r.json()['conversion_rates']['RUB']

rub_to_usdt = get_rate()

dp = Dispatcher() # объект диспетчера


with sq.connect('database.db') as con:
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT, balance INTEGER, ref_balance INTEGER DEFAULT 0, ref_amount INTEGER DEFAULT 0, keys TEXT)")
    cur.execute('CREATE TABLE IF NOT EXISTS referal_users (id INTEGER PRIMARY KEY, referral_id INTEGER UNIQUE, ref_master_id INTEGER)')


@dp.message(CommandStart())
async def start_command(message):
    
    try:
        ref = message.text.split()[1] # получить реферальный код
        ref = int(ref)
    except:
        ref = None
    if ref:
        with sq.connect('database.db') as con:
            cur = con.cursor()
            # Проверяем, что referral_id != ref_master_id перед вставкой
            if message.from_user.id != ref:
                bot.send_message(ref, f' <b>🎉 У вас новый реферал - {message.from_user.username}! </b>', parse_mode='HTML')
                cur.execute(
                    "INSERT OR IGNORE INTO referal_users (referral_id, ref_master_id) VALUES (?, ?)", (message.from_user.id, ref)
                )
                cur.execute("UPDATE users SET balance = balance + 50 WHERE id = ?", (ref,))
                cur.execute('UPDATE users SET ref_amount = ref_amount + 1 WHERE id = ?', (ref,))
            con.commit()

    await message.answer_photo(FSInputFile("photos/welcome.png"), caption="""👋 Добро пожаловать в Кофеманию
    \n🔐 Vless/Xray протоколы
    \n💡 Пополняйте баланс, покупайте VPN и подключайтесь за пару минут
    \n⏳ Доступ выдается сразу после покупки
    \n <b>🎁 Если ранее вы не были зарегистрированы, то вам будет начислен стартовый бонус 50 ₽ </b>
    \n Если возникнут вопросы — поддержка всегда на связи 👇""", parse_mode='HTML', reply_markup=ikb) # парсинг HTML чтобы работали теги с хтмл и прилепили маркап к сообщению
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO users (id, username, balance) VALUES (?, ?, ?)", (message.from_user.id, message.from_user.username, 50))
    
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
    response = requests.post('https://pay.crypt.bot/api/getInvoices', headers=headers, json={})
    response = response.json()
    pprint.pprint(response)
    for inv in response['result']['items']:
        if inv['invoice_id'] == invoice_id:
            return inv['status'], float(inv['amount'])*rub_to_usdt # возвращаем статус оплаты и сумму в рублях
    return None, None
    

ikb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🛒 Купить VPN', callback_data='buy_vpn')],
    [InlineKeyboardButton(text='👤 Личный кабинет', callback_data='profile')],
    [InlineKeyboardButton(text='🤝 Пригласить друга', callback_data='referral')],
    [InlineKeyboardButton(text='ℹ️ Поддержка', callback_data='support')],
    [InlineKeyboardButton(text='📄 Документы', callback_data='documents')],
    [InlineKeyboardButton(text='⚠️ Баг репорт', callback_data='bug_report')]
])
ikb_back = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
    ])

ikb_profile = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔗 Мои ключи', callback_data='my_keys')],
    [InlineKeyboardButton(text='💰 Пополнить', callback_data='deposit')],
    [InlineKeyboardButton(text='💸 Вывести реферальный баланс', callback_data='ref_balance')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])


ikb_documents = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='📄 Пользовательское соглашение', url='https://telegra.ph/Polzovatelskoe-soglashenie-12-22-25')],
    [InlineKeyboardButton(text='🔒 Политика конфиденциальности', url='https://telegra.ph/Politika-konfidencialnosti-12-22-25')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

ikb_referral = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

ikb_support = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💬 Написать в поддержку', url='https://t.me/star3alight')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

ikb_plans = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='👶🏻 Неделя (50₽)', callback_data='plan_week')],
    [InlineKeyboardButton(text='🧑 Месяц (100₽)', callback_data='plan_month')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

ikb_deposit = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💰 Пополнить', callback_data='deposit')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

ikb_deposit_methods = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💳 Криптобот', callback_data='deposit_crypssto')],
    [InlineKeyboardButton(text='🍀 СБП', callback_data='deposit_sbp')],
    [InlineKeyboardButton(text='🌟 Звёзды', callback_data='deposit_stars')],
    [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
])

def deposit_keyboard(method):
    amount = [50, 100, 200, 300, 400, 500]
    ikb_deposit_sums = InlineKeyboardMarkup(inline_keyboard=[])
    for sum in amount:
        ikb_deposit_sums.inline_keyboard.append([InlineKeyboardButton(text=f'🟣 {sum}₽', callback_data=f'deposit_{sum}_{method}')])
    ikb_deposit_sums.inline_keyboard.append([InlineKeyboardButton(text='🔙 Назад', callback_data='back')])
    return ikb_deposit_sums
 


# ОБРАБОТЧИКИ КОЛЛБЭКОВ
@dp.callback_query(lambda c: c.data == 'buy_vpn')
async def buy_vpn_callback(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("🛒 Раздел покупки VPN") # на пол экрана хуйня высветится
    await callback.message.answer_photo(FSInputFile("photos/buy_vpn.png"), caption="🛒 <b>Купить VPN</b>\n\nВыберите тарифный план:", parse_mode='HTML', reply_markup=ikb_plans)

@dp.callback_query(lambda c: c.data == 'profile')
async def profile_callback(callback: CallbackQuery):
    await callback.answer("👤 Личный кабинет") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT balance FROM users WHERE id = ?", (callback.from_user.id,)) # вытащить баланс из базы данных текущего пользователя
        result = cur.fetchone() # получить результат из базы данных
        balance = result[0] if result else 0 # если результат не пустой, то вытащить баланс, иначе 0
        cur.execute("SELECT ref_balance FROM users WHERE id = ?", (callback.from_user.id,)) # вытащить реферальный баланс из базы данных текущего пользователя
        result = cur.fetchone() # получить результат из базы данных
        ref_balance = result[0] if result else 0 # если результат не пустой, то вытащить реферальный баланс, иначе 0
    await callback.message.answer_photo(PROFILE_PHOTO, caption=f"👤 <b>Личный кабинет</b>\n\n💰 Баланс: {balance} ₽\n💸 Реферальный баланс: {ref_balance} ₽\n🆔 ID: {callback.from_user.id}", parse_mode='HTML', reply_markup=ikb_profile)

@dp.callback_query(lambda c: c.data == 'documents')
async def documents_callback(callback: CallbackQuery):
    await callback.answer("📄 Документы") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer_photo(DOCUMENTS_PHOTO, caption="📄 <b>Документы</b>", parse_mode='HTML', reply_markup=ikb_documents)

# ДЛЯ ДОКУМЕНТОВ КОЛБЕК НЕ НУЖЕН, ОНИ ОТКРЫВАЮТСЯ КАК СТАТЬЯ

@dp.callback_query(lambda c: c.data == 'referral')
async def referral_callback(callback: CallbackQuery):
    await callback.answer("🤝 Пригласить друга") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute("SELECT ref_amount FROM users WHERE id = ?", (callback.from_user.id,)) # вытащить реферальное количество из базы данных текущего пользователя
        result = cur.fetchone() # получить результат из базы данных
        ref_amount = result[0] if result else 0 # если результат не пустой, то вытащить реферальное количество, иначе 0
    await callback.message.answer_photo(INVITE_FRIEND_PHOTO, caption=f"🤝 <b>Пригласить друга</b>\n\nВаша реферальная ссылка:\n<code>https://t.me/coffemaniaVPNbot?start={callback.from_user.id}</code>\n\n👁️ Всего заработано: {ref_amount*50} ₽ \n\n🤔 За каждого приглашенного друга вы получите 50 ₽ на баланс", parse_mode='HTML', reply_markup=ikb_referral)


@dp.callback_query(lambda c: c.data == 'support')
async def support_callback(callback: CallbackQuery):
    await callback.answer("ℹ️ Поддержка") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("ℹ️ <b>Поддержка</b>\n\nЕсли у вас возникли вопросы, напишите нам в поддержку!", parse_mode='HTML', reply_markup=ikb_support)


@dp.callback_query(lambda c: c.data == 'back')
async def back_callback(callback: CallbackQuery):
    await callback.answer("🔙 Назад") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer_photo(WELCOME_PHOTO, caption="""👋 Добро пожаловать в Кофеманию
    \n🔐 Vless/Xray протоколы
    \n💡 Пополняйте баланс, покупайте VPN и подключайтесь за пару минут
    \n⏳ Доступ выдается сразу после покупки
    \n <b>🎁 Если ранее вы не были зарегистрированы, то вам будет начислен стартовый бонус 50 ₽ </b>
    \n Если возникнут вопросы — поддержка всегда на связи 👇""", parse_mode='HTML', reply_markup=ikb) # парсинг HTML чтобы работали теги с хтмл и прилепили маркап к сообщению

@dp.callback_query(lambda c: c.data == 'plan_week')
async def plan_week_callback(callback: CallbackQuery):
    await callback.answer("👶🏻 Неделя (50₽)") # на пол экрана хуйня высветится
    await callback.message.delete()

    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT balance FROM users WHERE id = ?', (callback.from_user.id,))
        result = cur.fetchone() # получить результат из базы данных
        balance = result[0] if result else 0 # если результат не пустой, то вытащить баланс, иначе 0
        # con.commit() # сохранить изменения в базе данных
        if balance >= 50:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 7 AND SOLD = 0 ORDER BY RANDOM() LIMIT 1')
                result = cur.fetchone() # получить результат из базы данных
                print(result)
                if result:
                    cur.execute('UPDATE users SET balance = balance - 50 WHERE id = ? AND balance >= 50' , (callback.from_user.id,)) # вычесть 100 из баланса текущего пользователя
                    con.commit() # сохранить изменения в базе данных
                    await callback.message.answer(f"🙋🏻‍♂️ ВАШ КЛЮЧ:\n\n<code>{result[0]}</code>\n<i>(нажмите чтобы скопировать)</i> \n\n<b>⌛Срок действия: 7 дней</b>\n🧐 Гайд на установку: https://telegra.ph/Instrukciya-kak-podklyuchitsya-k-VPN-12-22", parse_mode='HTML', reply_markup=ikb_back)
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],)) # обновить статус ключа в базе данных
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0])) # обновить ID покупателя в базе данных
                    
                else:
                    await callback.message.answer('‼️ Нет доступных ключей. Свяжитесь с поддержкой.', parse_mode='HTML', reply_markup=ikb_support)
                con.commit() # сохранить изменения в базе данных
        else:
            await callback.message.answer('💰 Недостаточно средств на балансе. Пополните баланс и попробуйте снова.', parse_mode='HTML', reply_markup=ikb_deposit)
          
@dp.callback_query(lambda c: c.data == 'plan_month')
async def plan_month_callback(callback: CallbackQuery):
    await callback.answer("🧑 Месяц (100₽)") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('UPDATE users SET balance = balance - 100 WHERE id = ? AND balance >= 100' , (callback.from_user.id,)) # вычесть 100 из баланса текущего пользователя
        con.commit() # сохранить изменения в базе данных
        if cur.rowcount > 0:
            with sq.connect('database.db') as con:
                cur = con.cursor()
                cur.execute('SELECT key FROM keys WHERE duration = 30 AND SOLD = 0 ORDER BY RANDOM() LIMIT 1')
                result = cur.fetchone() # получить результат из базы данных
                if result:
                    await callback.message.answer(f"🙋🏻‍♂️ ВАШ КЛЮЧ:\n\n<code>{result[0]}</code>\n<i>(нажмите чтобы скопировать)</i> \n\n<b>⌛Срок действия: 30 дней</b>\n🧐 Гайд на установку: https://telegra.ph/Instrukciya-kak-podklyuchitsya-k-VPN-12-22", parse_mode='HTML', reply_markup=ikb_back)
                    cur.execute('UPDATE keys SET SOLD = 1 WHERE key = ?', (result[0],)) # обновить статус ключа в базе данных
                    cur.execute('UPDATE keys SET buyer_id = ? WHERE key = ?', (callback.from_user.id, result[0])) # обновить ID покупателя в базе данных
                    con.commit() # сохранить изменения в базе данных
                else:
                    await callback.message.answer('‼️ Нет доступных ключей. Свяжитесь с поддержкой.', parse_mode='HTML', reply_markup=ikb_support)
                con.commit() # сохранить изменения в базе данных
        else:
            await callback.message.answer('💰 Недостаточно средств на балансе. Пополните баланс и попробуйте снова.', parse_mode='HTML', reply_markup=ikb_deposit)
            
@dp.callback_query(lambda c: c.data == 'my_keys')
async def my_keys_callback(callback: CallbackQuery):
    await callback.answer("🔗 Мои ключи") # на пол экрана хуйня высветится
    await callback.message.delete()
    ikb_my_keys = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
    ])
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT key FROM keys WHERE buyer_id = ?', (callback.from_user.id,)) # вытащить ключи из базы данных текущего пользователя
        result = cur.fetchall() # получить результат из базы данных
        for key_id, key in enumerate(result): # перебрать все ключи и вывести их номер
            ikb_my_keys.inline_keyboard.append([InlineKeyboardButton(text=f'🔑 {key_id + 1}', callback_data=f'use_key_{key_id}')])
        if result:
            await callback.message.answer_photo(MY_KEYS_PHOTO, caption=f"🔗 Мои ключи:", parse_mode='HTML', reply_markup=ikb_my_keys)
        else:
            await callback.message.answer_photo(MY_KEYS_PHOTO, caption="🔗 У вас нет ключей. Купите ключ и используйте его.", parse_mode='HTML', reply_markup=ikb_plans)

@dp.callback_query(lambda c: c.data.startswith('use_key_')) # ЭТО ПОСМОТРЕТЬ КЛЮЧИ
async def use_key_callback(callback: CallbackQuery):
    await callback.answer(f"🔑 Использовать ключ {callback.data.split('_')[2]}") # на пол экрана хуйня высветится
    await callback.message.delete()
    with sq.connect('database.db') as con:
        cur = con.cursor()
        cur.execute('SELECT key FROM keys WHERE buyer_id = ? LIMIT 1 OFFSET ?' , (callback.from_user.id, callback.data.split('_')[2])) # вытащить ключ из базы данных по ID
        result = cur.fetchone() # получить результат из базы данных
    await callback.message.answer(f"🔑 Использовать ключ: \n\n<code>{result[0]}</code>", parse_mode='HTML', reply_markup=ikb_back)


@dp.callback_query(lambda c: c.data == 'deposit')
async def deposit_callback(callback: CallbackQuery):
    await callback.answer("💰 Пополнить") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer_photo(DEPOSIT_PHOTO, caption="💰 Выберите способ пополнения:", parse_mode='HTML', reply_markup=ikb_deposit_methods)

@dp.callback_query(lambda c: c.data == 'deposit_crypto')
async def deposit_crypto_callback(callback: CallbackQuery):
    await callback.answer("💳 Криптобот") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("💳 Выберите сумму пополнения:", parse_mode='HTML', reply_markup=deposit_keyboard('CryptoBot'))

@dp.callback_query(lambda c: c.data == ('deposit_sbp'))
async def deposit_sbp_callback(callback: CallbackQuery):
    await callback.answer("🍀 СБП") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("🍀 Выберите сумму пополнения:", parse_mode='HTML', reply_markup=deposit_keyboard('SBP'))

@dp.callback_query(lambda c: c.data == ('deposit_stars'))
async def deposit_stars_callback(callback : CallbackQuery):
    await callback.answer('🌟 Звёзды')
    await callback.message.delete()
    await callback.message.answer('🌟 Выберите сумму пополнения:', parse_mode='HTML', reply_markup=deposit_keyboard('stars')) 


@dp.callback_query(lambda c: c.data.startswith('deposit_'))
async def process_deposit(callback: CallbackQuery):
    print(callback.data.split())
    _ , sum , method = callback.data.split('_')
    # await callback.message.delete()
    amount = int(sum)

    await callback.message.answer(f"💰 Пополнение на {amount} ₽\n\n<b>💳 Способ пополнения: {method}</b> \n\n Создаем заявку...", parse_mode='HTML')
    
    if method == 'stars':
        stars_rate = 1.50 # 1 звезда = 1.50 рубля
        amount_stars = amount * stars_rate
        try:
            await bot.send_invoice(
                chat_id=callback.from_user.id,
                title=f"Пополнение баланса на {amount} ₽",
                description=f"Пополнение баланса в боте на сумму {amount} рублей",
                payload=f"deposit_{amount}_{callback.from_user.id}",
                provider_token="", # для звезд не нужен provider_token
                currency="XTR",
                prices=[LabeledPrice(label=f"Пополнение {amount} ₽", amount=amount)],
            )
        except Exception as e:
            await callback.message.answer(f'❌ Не удалось создать заявку: {e}', parse_mode='HTML', reply_markup=ikb_deposit_methods)
            raise e
    if method == 'CryptoBot': # рассматриваем оплату криптой
        response = get_pay_link(amount/rub_to_usdt) # переводим рубли в доллары от руки пока что пох
        print(response)
        ok = response['ok'] # тру фолс
        result = response['result'] # содержит инфу о результате запроса
        pay_url = result['pay_url'] # ссылка на оплату
        bot_invoice_url = result['bot_invoice_url'] # ссылка на оплату в боте
        invoice_id = result['invoice_id'] # id заявки
        # print(pay_url, bot_invoice_url, ok)

        ikb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f'✅️ Оплатить {amount} ₽', url=pay_url)],
            [InlineKeyboardButton(text='🔄 Проверить статус оплаты', callback_data=f'check_payment_{invoice_id}')],
            [InlineKeyboardButton(text='🔙 Назад', callback_data='back')],
        ])
        

        if ok:
            await callback.message.answer('👉 Создали заявку на оплату, переходите по ссылке и оплатите', parse_mode='HTML', reply_markup=ikb)
        else:
            await callback.message.answer('❌ Не удалось создать заявку. Попробуйте позже.', parse_mode='HTML', reply_markup=ikb_deposit_methods)

@dp.callback_query(lambda c: c.data.startswith('check_payment_'))
async def check_payment_callback(callback: CallbackQuery):
    await callback.answer("🔄 Проверить статус оплаты") # на пол экрана хуйня высветится
    try:
        await callback.message.delete()
    except:
        pass
    invoice_id = int(callback.data.split('_')[2])
    status, amount = check_payment_status(invoice_id)
    print(invoice_id, status)
    if status == 'paid':
        await callback.message.answer(f'🤑 Оплачено! \n\n ➕ Начислено {amount} ₽ на баланс', parse_mode='HTML', reply_markup=ikb_back)
        with sq.connect('database.db') as con:
            cur = con.cursor()
            cur.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount, callback.from_user.id))
            con.commit()
    else:
        await callback.message.answer('👀 Ожидаем оплату, оплатите и попробуйте снова!', parse_mode='HTML')

@dp.callback_query(lambda c: c.data == 'bug_report')
async def bug_report_callback(callback: CallbackQuery):
    await callback.answer("⚠️ Баг репорт") # на пол экрана хуйня высветится
    await callback.message.delete()
    await callback.message.answer("⚠️ <b>Баг репорт</b>\n\nhttps://forms.gle/Pwdm8uzAgtu9T2296!", parse_mode='HTML', reply_markup=ikb_back)

async def main():
    await dp.start_polling(bot) # отправить соединение к серверам телеграмма

if __name__ == "__main__": # если файл запускается напрямую, то запустить главную функцию (подключение к серверам телеграмма)
    asyncio.run(main())