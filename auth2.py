"""Interactive auth - run in terminal."""
from telethon.sync import TelegramClient

client = TelegramClient("session", 36247451, "2e2eb7e170aeb1db78285a352120585f")
client.start(phone="+79091607038")
me = client.get_me()
print(f"OK: {me.first_name} ({me.phone})")
client.disconnect()
