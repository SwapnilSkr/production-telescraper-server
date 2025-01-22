from app.database import messages_collection, groups_collection
from datetime import datetime, timezone
from app.telegram_client import telegram_client
from telethon.errors import FloodWaitError
import asyncio


async def scrape_historical_messages(username: str):
    """Scrape all historical messages from a group."""
    try:
        # Remove "@" from username
        clean_username = username.lstrip("@")

        # Fetch group details
        entity = await telegram_client.get_entity(username)
        group_info = await groups_collection.find_one({"username": clean_username})

        if not group_info:
            print(f"Group {username} not found in database.")
            return

        group_id = group_info.get("group_id", entity.id)
        print(
            f"Scraping historical messages for group: {group_info['title']} ({clean_username})")

        message_count = 0
        async for message in telegram_client.iter_messages(entity):
            # Check if message already exists in the database
            existing_msg = await messages_collection.find_one({
                "message_id": message.id,
                "group_id": group_id
            })

            if existing_msg:
                continue

            # Prepare the message document
            message_doc = {
                "message_id": message.id,
                "group_id": group_id,
                "group_username": clean_username,
                "group_title": group_info["title"],
                "date": message.date,
                "sender_id": message.sender_id,
                "text": message.text,
                "reply_to_msg_id": message.reply_to_msg_id,
                "forward": message.forward is not None,
                "media": message.media is not None,
                "edited": message.edit_date is not None,
                "edit_date": message.edit_date,
                "created_at": datetime.now(timezone.utc)
            }

            # Insert into the database
            await messages_collection.insert_one(message_doc)
            message_count += 1

            if message_count % 100 == 0:
                print(f"Processed {message_count} messages...")

        print(
            f"Completed scraping historical messages for group: {group_info['title']}. Total: {message_count}")

    except FloodWaitError as e:
        print(f"FloodWaitError: Waiting for {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
        await scrape_historical_messages(username)  # Retry after waiting
    except Exception as e:
        print(f"Error scraping historical messages for group {username}: {e}")
