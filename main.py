import os
import json
import uuid
import asyncio
import logging
from telethon import TelegramClient, events
from telethon.tl.types import Channel
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
BACKFILL_COUNT = int(os.environ.get("BACKFILL_COUNT", "10"))

openai_client = OpenAI(api_key=OPENAI_API_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
tg_client = TelegramClient("session", API_ID, API_HASH)

# Track processed post URLs to avoid duplicates
processed_urls: set[str] = set()

GPT_PARSE_PROMPT = """Ты парсер объявлений о животных. Из текста ниже извлеки данные и верни ТОЛЬКО валидный JSON без пояснений.

Поля:
- type: "lost" | "found" | "give_away" | "help" (определи по смыслу; help = нужна помощь, донор крови, сбор средств на лечение)
- animal: "кошка" | "собака" | "другое"
- breed: порода или null
- color: цвет/окрас или null
- age: возраст животного — выбери из вариантов: "до 6 месяцев" | "6–12 месяцев" | "1–3 года" | "3–5 лет" | "5–10 лет" | "старше 10 лет" | null
- sex: пол животного — "мальчик" | "девочка" | null
- event_date: когда произошло событие (дата и время пропажи/находки) — строка как в тексте, например "10 апреля, около 18:00" или null
- name: кличка или null
- district: район города или null
- features: особые приметы или null
- description: краткое описание ситуации (2-3 предложения)
- contact: телефон или мессенджер или null
- lat: широта (если есть геолокация) или null
- lng: долгота (если есть геолокация) или null

Если поле не упомянуто — верни null.
Если текст НЕ является объявлением о животном (реклама, новости, опрос, обсуждение) — верни JSON с полем "type": "skip".
Текст: {post_text}"""


async def parse_with_gpt(text: str) -> dict | None:
    """Send post text to GPT and get structured JSON."""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты извлекаешь структурированные данные из объявлений о животных. Отвечай ТОЛЬКО валидным JSON объектом."},
                {"role": "user", "content": GPT_PARSE_PROMPT.format(post_text=text)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        if not raw:
            return None
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None
        return data
    except json.JSONDecodeError as e:
        logger.error("GPT JSON decode error: %s (raw: %s)", e, raw[:200] if raw else "empty")
        return None
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


def is_duplicate(post_url: str) -> bool:
    """Check if post was already saved to DB."""
    if post_url in processed_urls:
        return True
    try:
        result = supabase.table("listings").select("id").eq("telegram_post_url", post_url).execute()
        if result.data:
            processed_urls.add(post_url)
            return True
    except Exception:
        pass
    return False


async def process_message(message, channel_username: str) -> None:
    """Process a single message: parse with GPT, upload photo, save to DB."""
    text = message.raw_text
    if not text or len(text) < 20:
        return

    post_url = f"https://t.me/{channel_username}/{message.id}"

    # Skip duplicates
    if is_duplicate(post_url):
        logger.debug("Skipping duplicate: %s", post_url)
        return

    logger.info("Processing post from %s: %s...", channel_username, text[:80])

    # Parse with GPT
    data = await parse_with_gpt(text)
    if not data or data.get("type") in (None, "skip"):
        logger.info("Skipped (not a listing or parse failed)")
        return

    listing_id = str(uuid.uuid4())

    # Download & upload photo
    photo_url = None
    if message.photo:
        try:
            photo_bytes = await tg_client.download_media(message, bytes)
            if photo_bytes:
                photo_url = await upload_photo(photo_bytes, listing_id, "photo.jpg")
        except Exception as e:
            logger.error("Photo download error: %s", e)

    # Save to DB
    row = {
        "id": listing_id,
        "type": data.get("type", "found"),
        "animal": data.get("animal", "другое"),
        "breed": data.get("breed"),
        "color": data.get("color"),
        "age": data.get("age"),
        "sex": data.get("sex"),
        "event_date": data.get("event_date"),
        "name": data.get("name"),
        "city": "Минск",
        "district": data.get("district"),
        "features": data.get("features"),
        "description": data.get("description"),
        "contact": data.get("contact"),
        "photo_url": photo_url,
        "source": "telegram",
        "telegram_post_url": post_url,
        "telegram_channel": channel_username,
        "lat": data.get("lat"),
        "lng": data.get("lng"),
        "moderation_status": "approved",
        "status": "active",
    }
    try:
        supabase.table("listings").insert(row).execute()
        processed_urls.add(post_url)
        logger.info("SAVED listing: %s [%s] from %s", data.get("animal"), data.get("type"), channel_username)
    except Exception as e:
        logger.error("DB insert error: %s", e)


async def backfill_channels():
    """Fetch and process the last N messages from each channel."""
    logger.info("=== BACKFILL: fetching last %d posts from %d channels ===", BACKFILL_COUNT, len(CHANNELS))

    resolved = []
    for ch_name in CHANNELS:
        try:
            entity = await tg_client.get_entity(ch_name)
            resolved.append((ch_name, entity))
            logger.info("Resolved channel: %s → %s (id=%s)", ch_name, getattr(entity, 'title', '?'), entity.id)
        except Exception as e:
            logger.error("Could not resolve channel '%s': %s", ch_name, e)

    total_saved = 0
    for ch_name, entity in resolved:
        try:
            username = getattr(entity, 'username', None) or ch_name
            count = 0
            async for message in tg_client.iter_messages(entity, limit=BACKFILL_COUNT):
                if message.raw_text and len(message.raw_text) >= 20:
                    await process_message(message, username)
                    count += 1
                    await asyncio.sleep(0.5)  # rate limit GPT calls
            logger.info("Backfilled %d posts from %s", count, ch_name)
            total_saved += count
        except Exception as e:
            logger.error("Backfill error for %s: %s", ch_name, e)

    logger.info("=== BACKFILL COMPLETE: processed %d posts ===", total_saved)
    return resolved


async def main():
    logger.info("Starting parser, watching %d channels: %s", len(CHANNELS), CHANNELS)
    await tg_client.start(phone=PHONE)
    logger.info("Telegram client connected")

    # Step 1: Backfill recent posts
    resolved = await backfill_channels()

    # Step 2: Register event handler for new messages using resolved entities
    entity_ids = [entity.id for _, entity in resolved]

    @tg_client.on(events.NewMessage(chats=entity_ids))
    async def handler(event):
        channel_username = event.chat.username or str(event.chat_id)
        await process_message(event.message, channel_username)

    logger.info("Live listener active for %d channels, waiting for new posts...", len(entity_ids))
    await tg_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
