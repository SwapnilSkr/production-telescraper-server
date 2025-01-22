from telethon import events
from telethon.errors import FloodWaitError
from app.telegram_client import telegram_client
from app.database import groups_collection, messages_collection, tags_collection, categories_collection
from datetime import datetime
import asyncio
import os
from openai import OpenAI
from app.config import OPENAI_API_KEY


client = OpenAI(api_key=OPENAI_API_KEY)

monitored_groups = []  # Global list to track active groups


async def fetch_categories():
    """
    Fetch all categories from the database and format them into a numbered list.
    """
    try:
        categories = await categories_collection.find().to_list(length=100)
        formatted_categories = []

        for idx, category in enumerate(categories, start=1):
            name = category.get('name', 'Unknown Category')
            description = category.get(
                'description', 'No description available.')
            formatted_categories.append(f"{idx}. {name}: {description}")

        return "\n".join(formatted_categories)
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return ""


async def categorize_message_with_gpt(message_text):
    """
    Categorize a message using OpenAI's GPT-4 model with dynamically fetched category descriptions.
    """
    try:
        # Fetch and format categories for the prompt
        formatted_categories = await fetch_categories()

        # Construct the prompt with detailed category information
        prompt = (
            "You are a highly intelligent AI trained to categorize messages based on their content. "
            "Please analyze the following message, determine what the message is about and categorize it into one of these categories:\n\n"
            f"{formatted_categories}\n\n"
            "If the message doesn't fit any of the categories, you can create a new category in all uppercase and then store it there. "
            "Consider the context, keywords, and any implications in the message. "
            "Just provide the category name in all uppercase and nothing else. "
            f"Message: '{message_text}'"
        )

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        category = response.choices[0].message.content.strip()
        return category
    except Exception as e:
        print(f"Error categorizing message with GPT: {e}")
        return None


async def generate_tags(message_text):
    """
    Generate tags for the message using OpenAI's GPT-4 model.
    """
    prompt = (
        "You are an intelligent AI trained to generate relevant tags for messages. "
        "Given the following message, please provide a list of relevant tags that capture the main themes, "
        "keywords, and topics discussed in the message. The tags should be concise, descriptive, "
        "and should not exceed 5 tags. Tags should be lowercase, without any hashes or special characters, "
        "and separated by commas. Message: '{message_text}'"
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": prompt.format(
                    message_text=message_text)}
            ]
        )

        # Assuming tags are comma-separated
        tags = response.choices[0].message.content.strip().split(",")
        return [tag.strip() for tag in tags]  # Clean up whitespace around tags
    except Exception as e:
        print(f"Error generating tags with GPT: {e}")
        return []


async def save_tags(tags):
    """
    Save unique tags to the tags collection, avoiding duplicates.
    """
    for tag in tags:
        # Check if the tag already exists in the database
        existing_tag = await tags_collection.find_one({"tag": tag})
        if not existing_tag:
            # If it doesn't exist, insert it into the collection
            await tags_collection.insert_one({"tag": tag})
            print(f"New tag saved: {tag}")
        else:
            print(f"Tag already exists: {tag}")


async def save_media_to_storage(message):
    """
    Save media from a message to storage and return the file path or URL.
    """
    try:
        if not message.media:
            return None  # No media to save

        # Define the file path
        media_path = f"media/{message.id}_{datetime.utcnow().timestamp()}"
        os.makedirs(os.path.dirname(media_path), exist_ok=True)

        # Download the media file
        file_path = await message.download_media(file=media_path)
        print(f"Media saved to: {file_path}")
        return file_path  # Return the file path or modify to return a URL if needed
    except Exception as e:
        print(f"Error saving media: {e}")
        return None


async def process_message(event):
    """
    Process a new message and save it to the database.
    Handle cases where chat.username is None by checking chat.usernames.
    """
    try:
        chat = await event.get_chat()

        # Initialize matched_groups to store groups from the database
        matched_groups = []

        if chat.username:
            group_info = await groups_collection.find_one({"username": chat.username})
            if group_info:
                matched_groups.append(group_info)
        elif hasattr(chat, "usernames") and chat.usernames:
            for username_entry in chat.usernames:
                username = username_entry.username
                group_info = await groups_collection.find_one({"username": username})
                if group_info:
                    matched_groups.append(group_info)

        if not matched_groups:
            print(
                f"No matching groups found in the database for chat: {chat}.")
            return

        for group_info in matched_groups:
            file_url = await save_media_to_storage(event.message)

            category = await categorize_message_with_gpt(event.message.text or "")

            # Generate tags for the message using GPT-4
            tags = await generate_tags(event.message.text or "")

            # Save unique tags to the database
            await save_tags(tags)

            message_doc = {
                "message_id": event.message.id,
                "group_id": group_info["group_id"],
                "group_username": group_info["username"],
                "text": event.message.text or "",
                "date": event.message.date,
                "sender_id": event.message.sender_id,
                "file": file_url,
                "category": category,
                "tags": tags,  # Save generated tags with the message document
                "created_at": datetime.utcnow(),
            }

            result = await messages_collection.update_one(
                {"message_id": message_doc["message_id"],
                 "group_id": message_doc["group_id"]},
                {"$set": message_doc},
                upsert=True,
            )

            if result.upserted_id or result.modified_count > 0:
                print(
                    f"New message saved for group '{group_info['username']}': {message_doc['text'][:50]}")
            else:
                print(
                    f"Message already exists for group '{group_info['username']}': {message_doc['text'][:50]}")

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
