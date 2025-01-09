from datetime import datetime
from app.telegram_client import telegram_client
from app.services.message_service import scrape_historical_messages
from app.database import groups_collection
from telethon.errors import FloodWaitError
import asyncio


async def register_or_update_group(username: str):
    try:
        # Remove "@" from the username
        clean_username = username.lstrip("@")

        # Fetch group details from Telegram
        entity = await telegram_client.get_entity(username)

        # Extract group info
        group_info = {
            "username": clean_username,
            "title": entity.title,
            "member_count": entity.participants_count if hasattr(entity, "participants_count") else None,
            "group_id": entity.id,
            "is_active": True,
            "created_at": datetime.utcnow()
        }

        # Insert or update group in the database
        await groups_collection.update_one(
            {"username": clean_username},
            {"$set": group_info},
            upsert=True
        )

        print(f"Group '{entity.title}' registered/updated successfully.")

        # Scrape historical messages for the group
        await scrape_historical_messages(username)

    except FloodWaitError as e:
        # Handle rate limiting by waiting for the specified duration
        wait_time = e.seconds
        print(
            f"FloodWaitError: Waiting for {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)
        await register_or_update_group(username)  # Retry after waiting

    except Exception as e:
        print(f"Error registering/updating group {username}: {e}")
