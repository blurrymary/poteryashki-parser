import os
import json
import asyncio
import logging
from telethon import TelegramClient, events
from openai import OpenAI
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Config ---
API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
PHONE = os.environ["TELEGRAM_PHONE"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
CHANNELS = [ch.strip() for ch in os.environ["CHANNELS"].split(",") if ch.strip()]

openai_client = OpenAI(api_key=OPENAI_API_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
tg_client = TelegramClient("session", API_ID, API_HASH)

GPT_PARSE_PROMPT = """Ты парсер объявлений о животных. Из текста ниже извлеки данные и верни ТОЛЬКО валидный JSON без пояснений.

Поля:
- type: "lost" | "found" | "give_away" (определи по смыслу)
- animal: "кошка" | "собака" | "другое"
- breed: порода или null
- color: цвет/окрас или null
- name: кличка или null
- district: район города или null
- features: особые приметы или null
- description: краткое описание ситуации
- contact: телефон или мессенджер или null
- lat: широта (если есть геолокация) или null
- lng: долгота (если есть геолокация) или null

Если поле не упомянуто — верни null.
Текст: {post_text}"""


async def parse_with_gpt(text: str) -> dict | None:
    """Send post text to GPT and get structured JSON."""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты извлекаешь структурированные данные из объявлений о животных. Отвечай ТОЛЬКО валидным JSON."},
                {"role": "user", "content": GPT_PARSE_PROMPT.format(post_text=text)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        return json.loads(raw)
    except Exception as e:
        logger.error("GPT parse error: %s", e)
        return None


async def upload_photo(photo_bytes: bytes, listing_id: str, filename: str) -> str | None:
    """Upload photo to Supabase Storage and return public URL."""
    path = f"{listing_id}/{filename}"
    try:
        supabase.storage.from_("animal-photos").upload(
            path,
            photo_bytes,
            {"content-type": "image/jpeg"},
        )
        res = supabase.storage.from_("animal-photos").get_public_url(path)
        return res
    except Exception as e:
        logger.error("Photo upload error: %s", e)
        return None


async def save_listing(data: dict, photo_url: str | None, channel: str, post_url: str) -> None:
    """Insert parsed listing into Supabase."""
    row = {
        "type": data.get("type", "found"),
        "animal": data.get("animal", "другое"),
        "breed": data.get("breed"),
        "color": data.get("color"),
        "name": data.get("name"),
        "city": "Минск",
        "district": data.get("district"),
        "features": data.get("features"),
        "description": data.get("description"),
        "contact": data.get("contact"),
        "photo_url": photo_url,
        "source": "telegram",
        "telegram_post_url": post_url,
        "telegram_channel": channel,
        "lat": data.get("lat"),
        "lng": data.get("lng"),
        "moderation_status": "approved",
        "status": "active",
    }
    try:
        supabase.table("listings").insert(row).execute()
        logger.info("Saved listing from %s", channel)
    except Exception as e:
        logger.error("DB insert error: %s", e)


@tg_client.on(events.NewMessage(chats=CHANNELS))
async def handler(event):
    """Handle new messages from watched channels."""
    text = event.raw_text
    if not text or len(text) < 20:
        return

    logger.info("New post from %s: %s...", event.chat.username or event.chat_id, text[:80])

    # Parse with GPT
    data = await parse_with_gpt(text)
    if not data or "type" not in data:
        logger.warning("Could not parse post, skipping")
        return

    # Generate listing ID for photo path
    import uuid
    listing_id = str(uuid.uuid4())

    # Download & upload photo
    photo_url = None
    if event.message.photo:
        try:
            photo_bytes = await tg_client.download_media(event.message, bytes)
            if photo_bytes:
                photo_url = await upload_photo(photo_bytes, listing_id, "photo.jpg")
        except Exception as e:
            logger.error("Photo download error: %s", e)

    # Build post URL
    channel_username = event.chat.username or str(event.chat_id)
    post_url = f"https://t.me/{channel_username}/{event.message.id}"

    # Save to DB
    await save_listing(data, photo_url, channel_username, post_url)


async def main():
    logger.info("Starting parser, watching %d channels: %s", len(CHANNELS), CHANNELS)
    await tg_client.start(phone=PHONE)
    logger.info("Telegram client connected")
    await tg_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
