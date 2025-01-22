from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from datetime import datetime
from app.database import groups_collection, messages_collection
from app.telegram_client import telegram_client
from app.services.telegram_listener import update_listener
from app.utils.serialize_mongo import serialize_mongo_document
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel
from telethon.errors import FloodWaitError
import asyncio
import re

router = APIRouter()


class Group(BaseModel):
    username: str


async def get_group_with_messages(username: str):
    """Fetch group details and associated messages."""
    try:
        # Fetch group details
        group = await groups_collection.find_one({"username": username.lstrip("@")})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Serialize the group document
        group = serialize_mongo_document(group)

        # Fetch associated messages
        messages = await messages_collection.find({"group_id": group.get("group_id")}).to_list(length=100)
        messages = [serialize_mongo_document(msg) for msg in messages]

        return {"group": group, "messages": messages}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups")
async def get_groups():
    """
    Fetch all groups from the database and verify their validity on Telegram.
    Only return groups that are active and recognized by Telegram.
    """
    try:
        groups = await groups_collection.find().to_list(length=100)
        valid_groups = []

        for group in groups:
            try:
                # Verify group on Telegram
                entity = await telegram_client.get_entity(group["username"])
                if entity:
                    valid_groups.append(serialize_mongo_document(group))
            except Exception:
                # Skip invalid or banned groups
                continue

        await update_listener()  # Refresh the listener with updated groups
        return {"groups": valid_groups}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups")
async def add_group(group: Group):
    """
    Add a group to the database and add a listener. 
    Does not join the group, only validates and tracks public groups.
    """
    try:
        # Validate the group on Telegram
        try:
            entity = await telegram_client.get_entity(group.username.lstrip("@"))

            if not isinstance(entity, Channel):
                raise HTTPException(
                    status_code=403, detail="Entity is not a channel or supergroup"
                )

            # Check if the group is public or private
            if entity.username is None:
                raise HTTPException(
                    status_code=403, detail="Cannot track private groups or channels"
                )

        except Exception as e:
            raise HTTPException(
                status_code=404, detail=f"Failed to validate group: {str(e)}"
            )

        # Add or update group in the database
        group_data = {
            "username": group.username.lstrip("@"),
            "group_id": entity.id,
            "title": entity.title,
            "member_count": entity.participants_count
            if hasattr(entity, "participants_count")
            else None,
            "is_active": True,
            "created_at": datetime.utcnow(),
        }

        await groups_collection.update_one(
            {"username": group_data["username"]},
            {"$set": group_data},
            upsert=True,
        )

        # Add the group to the listener
        await update_listener()

        return {"message": "Group added successfully and listener updated.",
                "group": group_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/bulk_add_from_file")
async def bulk_add_from_file(file: UploadFile = File(...)):
    """
    Extract Telegram links from an uploaded file, validate them, and add them to the database.
    Processes in batches of 20 groups with a 60-second delay between batches to avoid rate limits.
    """
    added_groups = []
    skipped_groups = []
    invalid_links = []

    try:
        # Read the file content
        content = (await file.read()).decode("utf-8")

        # Regular expression to match Telegram links
        link_pattern = re.compile(r"https://t\.me/([\w\d_]+)")
        matches = link_pattern.findall(content)

        if not matches:
            raise HTTPException(
                status_code=400, detail="No valid Telegram links found in the file."
            )

        # Remove duplicates and process usernames in batches of 20
        usernames = list(set(matches))
        batch_size = 20  # Number of groups per batch

        for i in range(0, len(usernames), batch_size):
            batch = usernames[i:i + batch_size]
            for username in batch:
                try:
                    # Check if the group already exists in the database
                    existing_group = await groups_collection.find_one(
                        {"username": username}
                    )
                    if existing_group:
                        skipped_groups.append(username)
                        continue

                    # Validate the group on Telegram
                    try:
                        entity = await telegram_client.get_entity(username)
                        if not isinstance(entity, Channel):
                            skipped_groups.append(username)
                            continue
                        if entity.username is None:  # Private group check
                            skipped_groups.append(username)
                            continue

                        # Add the group to the database
                        group_data = {
                            "username": username,
                            "group_id": entity.id,
                            "title": entity.title,
                            "member_count": entity.participants_count
                            if hasattr(entity, "participants_count")
                            else None,
                            "is_active": True,
                            "created_at": datetime.utcnow(),
                        }
                        await groups_collection.update_one(
                            {"username": group_data["username"]},
                            {"$set": group_data},
                            upsert=True,
                        )
                        added_groups.append(username)

                    except FloodWaitError as e:
                        wait_time = e.seconds
                        print(
                            f"Rate limited while processing '{username}', waiting for {wait_time} seconds..."
                        )
                        await asyncio.sleep(wait_time)
                        # Retry this username after the wait
                        usernames.append(username)
                    except Exception:
                        invalid_links.append(f"https://t.me/{username}")

                except Exception as e:
                    print(f"Unexpected error for '{username}': {e}")
                    invalid_links.append(f"https://t.me/{username}")

            # Delay between batches to avoid rate limits
            if i + batch_size < len(usernames):
                print("Sleeping for 60 seconds to avoid rate limits...")
                await asyncio.sleep(60)

        # Update the listener with all valid groups
        await update_listener()

        return {
            "message": "Bulk group addition completed.",
            "added_groups": added_groups,
            "skipped_groups": skipped_groups,
            "invalid_links": invalid_links,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}"
        )


@router.get("/groups/{username}")
async def get_group(username: str):
    """
    Fetch a group and its associated messages by username.
    Only display the messages already added to the database.
    """
    try:
        clean_username = username.lstrip("@")

        # Fetch group details from the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database"
            )

        # Verify group validity on Telegram
        try:
            entity = await telegram_client.get_entity(clean_username)
            if not entity:
                raise HTTPException(
                    status_code=404, detail="Group is invalid or banned on Telegram"
                )
        except Exception:
            raise HTTPException(
                status_code=404, detail="Group is invalid or banned on Telegram"
            )

        # Serialize the group document
        group = serialize_mongo_document(group)

        # Fetch messages from the database
        messages_in_db = await messages_collection.find(
            {"group_id": group.get("group_id")}
        ).to_list(length=100)
        serialized_messages = [serialize_mongo_document(
            msg) for msg in messages_in_db]

        return {
            "group": group,
            "messages": serialized_messages,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/groups/{username}/deactivate")
async def deactivate_group(username: str):
    """Deactivate (soft delete) a group."""
    try:
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Deactivate the group
        result = await groups_collection.update_one({"username": clean_username}, {"$set": {"is_active": False}})
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404, detail="Failed to deactivate the group")

        await update_listener()  # Refresh the listener
        response = await get_group_with_messages(username)
        return {
            "message": "Group deactivated successfully, listener updated.",
            "deactivated_group": response["group"],
            "messages": response["messages"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/groups/{username}/reactivate")
async def reactivate_group(username: str):
    """Reactivate a previously deactivated group."""
    try:
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Reactivate the group
        result = await groups_collection.update_one({"username": clean_username}, {"$set": {"is_active": True}})
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404, detail="Failed to reactivate the group")

        await update_listener()  # Refresh the listener
        response = await get_group_with_messages(username)
        return {
            "message": "Group reactivated successfully, listener updated.",
            "reactivated_group": response["group"],
            "messages": response["messages"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/{username}/scrape_historical_data")
async def scrape_historical_data(username: str, limit: int = 100):
    """
    Scrape historical messages from a group and store them in the database.
    :param username: The username of the group.
    :param limit: The maximum number of messages to scrape (default: 100).
    """
    try:
        clean_username = username.lstrip("@")

        # Fetch group details from the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database"
            )

        # Validate group on Telegram
        try:
            entity = await telegram_client.get_entity(clean_username)
            if not entity:
                raise HTTPException(
                    status_code=404, detail="Group is invalid or banned on Telegram"
                )
        except Exception as e:
            raise HTTPException(
                status_code=404, detail=f"Failed to validate group on Telegram: {str(e)}"
            )

        # Fetch historical messages from Telegram
        message_count = 0
        async for message in telegram_client.iter_messages(entity, limit=limit):
            try:
                message_doc = {
                    "message_id": message.id,
                    "group_id": group["group_id"],
                    "group_username": clean_username,
                    "text": message.text or "",
                    "date": message.date,
                    "sender_id": message.sender_id,
                    "media": message.media is not None,
                    "created_at": datetime.utcnow(),
                }

                # Save the message to the database
                result = await messages_collection.update_one(
                    {
                        "message_id": message_doc["message_id"],
                        "group_id": message_doc["group_id"],
                    },
                    {"$set": message_doc},
                    upsert=True,
                )

                if result.upserted_id or result.modified_count > 0:
                    message_count += 1

            except Exception as e:
                print(f"Error saving message {message.id}: {e}")

        return {
            "message": f"Scraped {message_count} messages from the group '{username}'.",
            "group": serialize_mongo_document(group),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
