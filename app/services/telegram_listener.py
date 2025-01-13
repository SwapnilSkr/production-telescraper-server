from telethon import events
from telethon.errors import FloodWaitError
from app.telegram_client import telegram_client
from app.database import groups_collection, messages_collection
from datetime import datetime
import asyncio

monitored_groups = []  # Global list to track active groups


async def process_message(event):
    """
    Process a new message and save it to the database.
    """
    try:
        chat = await event.get_chat()
        group_info = await groups_collection.find_one({"username": chat.username})

        if not group_info:
            print(f"Group {chat.username} not found in database.")
            return

        message_doc = {
            "message_id": event.message.id,
            "group_id": group_info["group_id"],
            "group_username": group_info["username"],
            "text": event.message.text or "",
            "date": event.message.date,
            "sender_id": event.message.sender_id,
            "created_at": datetime.utcnow(),
        }

        result = await messages_collection.update_one(
            {"message_id": message_doc["message_id"],
                "group_id": message_doc["group_id"]},
            {"$set": message_doc},
            upsert=True,
        )
        if result.upserted_id or result.modified_count > 0:
            # Debug log
            print(f"New message saved: {message_doc['text'][:50]}")
        else:
            print(f"Message already exists: {message_doc['text'][:50]}")

    except Exception as e:
        print(f"Error processing message: {e}")


async def update_listener():
    """
    Update the listener to reflect the current list of active groups.
    """
    global monitored_groups
    print("Updating listener with current active groups...")

    try:
        # Fetch active groups from the database
        active_groups = await groups_collection.find({"is_active": True}).to_list(length=100)
        new_group_usernames = [group["username"] for group in active_groups]

        # Validate groups on Telegram
        valid_groups = []
        for username in new_group_usernames:
            try:
                entity = await telegram_client.get_entity(username)
                if entity:  # If Telegram validates the group
                    valid_groups.append(username)
            except Exception:
                print(f"Group '{username}' is invalid or banned on Telegram.")

                # Deactivate the group in the database
                await groups_collection.update_one(
                    {"username": username}, {"$set": {"is_active": False}}
                )

        # Check if there are any changes in monitored groups
        if set(valid_groups) != set(monitored_groups):
            print("Removing old listeners...")
            telegram_client.remove_event_handler(new_message_listener)

            monitored_groups = valid_groups

            print(f"Attaching listeners for groups: {monitored_groups}")
            telegram_client.add_event_handler(
                new_message_listener, events.NewMessage(chats=monitored_groups)
            )
            print(f"Updated monitored groups: {monitored_groups}")
        else:
            print("No changes in monitored groups.")

    except FloodWaitError as e:
        wait_time = e.seconds
        print(
            f"FloodWaitError: Waiting for {wait_time} seconds before retrying update_listener..."
        )
        await asyncio.sleep(wait_time)
        await update_listener()

    except Exception as e:
        print(f"Error updating listener: {e}")


async def new_message_listener(event):
    """
    Handle new messages for monitored groups.
    """
    try:
        chat = await event.get_chat()
        print(
            f"New message detected in group {chat.username}: {event.message.text[:50]}")
        await process_message(event)
    except Exception as e:
        print(f"Error in new_message_listener: {e}")
