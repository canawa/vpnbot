from vpn import *
def get_devices_list_text(tg_id):
    devices = Vpn().get_hwid_devices(tg_id)
    active_device_amount = len(devices)
    user = Vpn().get_user_by_tg_id(tg_id)
    user_device_limit = user['response'][0]['hwidDeviceLimit']

    devices_list_text = (
        f"<b><tg-emoji emoji-id='5310037891051691834'>🌎</tg-emoji> У вас в подписке: {user_device_limit} устройств </b>\n"
        f"<b><tg-emoji emoji-id='5436087613456918666'>✅</tg-emoji> Используется: {active_device_amount} / {user_device_limit} </b>\n\n"
        "Вы можете управлять подключёнными устройствами ниже: "
        "отключать старые и добавлять новые в пределах лимита подписки."
    )
    return devices_list_text

hwid_deleted_text = (
    "Устройство успешно удалено.\n"
    "Нажмите на кнопку ниже, чтобы вернуться в меню."
)