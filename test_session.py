"""Test if session is valid."""
import asyncio
from telethon import TelegramClient

API_ID = 36247451
API_HASH = "2e2eb7e170aeb1db78285a352120585f"

async def main():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.connect()
    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"Session OK: {me.first_name} ({me.phone})")
    else:
        print("Session NOT authorized")
    await client.disconnect()

asyncio.run(main())
