"""Generate a Telethon StringSession. Run locally, copy output to Railway env."""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

client = TelegramClient(StringSession(), 36247451, "2e2eb7e170aeb1db78285a352120585f")
client.start(phone="+79091607038")
print("\n=== COPY THIS STRING SESSION ===")
print(client.session.save())
print("================================\n")
client.disconnect()
