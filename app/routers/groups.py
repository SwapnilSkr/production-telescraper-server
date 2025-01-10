from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from app.database import groups_collection, messages_collection
from app.telegram_client import telegram_client
from app.services.group_service import register_or_update_group
from app.services.telegram_listener import update_listener
from datetime import datetime
from app.utils.serialize_mongo import serialize_mongo_document
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
                    # Serialize the group document
                    valid_groups.append(serialize_mongo_document(group))
            except Exception:
                # Skip groups that are invalid or banned on Telegram
                continue

        await update_listener()  # Refresh the listener with updated groups
        return {"groups": valid_groups}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups")
async def add_group(group: Group):
    """Add or update a group and fetch its details with messages."""
    try:
        await register_or_update_group(group.username)
        await update_listener()  # Refresh the listener with updated groups
        response = await get_group_with_messages(group.username)
        return {
            "message": "Group added/updated successfully, listener updated.",
            "added_group": response["group"],
            "messages": response["messages"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groups/{username}")
async def delete_group(username: str):
    """Permanently delete a group and associated messages from the database."""
    try:
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Delete the group
        await groups_collection.delete_one({"username": clean_username})

        # Delete associated messages
        await messages_collection.delete_many({"group_id": group.get("group_id")})

        await update_listener()  # Refresh the listener
        return {
            "message": "Group and associated messages deleted successfully.",
            "deleted_group": serialize_mongo_document(group)
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


@router.post("/groups/bulk_add_from_file")
async def bulk_add_from_file(file: UploadFile = File(...)):
    """
    Extract Telegram links from an uploaded file and add groups.
    :param file: Uploaded text file containing Telegram links.
    :return: List of added, skipped, and invalid links.
    """
    added_groups = []
    skipped_groups = []
    invalid_links = []

    try:
        # Read the file content
        content = (await file.read()).decode('utf-8')

        # Regular expression to match Telegram links
        link_pattern = re.compile(r"https://t\.me/([\w\d_]+)")
        matches = link_pattern.findall(content)

        if not matches:
            raise HTTPException(
                status_code=400, detail="No valid Telegram links found in the file.")

        # Process each extracted username
        for username in set(matches):  # Remove duplicates
            try:
                # Check if the group already exists in the database
                existing_group = await groups_collection.find_one({"username": username})
                if existing_group:
                    skipped_groups.append(username)
                    continue

                # Register the group
                await register_or_update_group(username)
                await update_listener()  # Update the listener after each operation
                added_groups.append(username)
            except Exception:
                invalid_links.append(f"https://t.me/{username}")

        # Update the listener after all operations
        await update_listener()

        return {
            "message": "Bulk group addition from file completed.",
            "added_groups": added_groups,
            "skipped_groups": skipped_groups,
            "invalid_links": invalid_links,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}")


@router.get("/groups/{username}")
async def get_group(username: str):
    """
    Fetch a group and its associated messages by username.
    - Verify group validity on Telegram.
    - Scrape missing historical messages if necessary.
    :param username: The username of the group.
    :return: Updated group details and messages.
    """
    try:
        clean_username = username.lstrip("@")

        # Fetch group details from the database
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database")

        try:
            # Verify group on Telegram
            entity = await telegram_client.get_entity(clean_username)
            if not entity:
                raise HTTPException(
                    status_code=404, detail="Group is invalid or banned on Telegram")
        except Exception:
            raise HTTPException(
                status_code=404, detail="Group is invalid or banned on Telegram")

        # Serialize the group document
        group = serialize_mongo_document(group)

        # Fetch messages from Telegram
        messages_in_telegram = []
        async for message in telegram_client.iter_messages(entity, limit=100):
            messages_in_telegram.append({
                "message_id": message.id,
                "group_id": group.get("group_id"),
                "text": message.text,
                "date": message.date,
                "sender_id": message.sender_id
            })

        # Fetch messages from the database
        messages_in_db = await messages_collection.find({"group_id": group.get("group_id")}).to_list(length=100)
        db_message_ids = {msg["message_id"] for msg in messages_in_db}

        # Add missing messages to the database
        new_messages = []
        for msg in messages_in_telegram:
            if msg["message_id"] not in db_message_ids:
                await messages_collection.insert_one(msg)
                new_messages.append(msg)

        # Serialize the messages
        messages_in_db.extend(new_messages)
        serialized_messages = [serialize_mongo_document(
            msg) for msg in messages_in_db]

        await update_listener()  # Refresh the listener
        return {
            "group": group,
            "messages": serialized_messages
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
