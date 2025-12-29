from vpn import generate_vpn_key, get_marzban_token
import asyncio


async def main():
    key = await generate_vpn_key(1, 7)
    print(key)

asyncio.run(main())