from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional
from fastapi import Query
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from app.database import messages_collection, groups_collection, tags_collection, categories_collection
from app.services.telegram_listener import update_listener
from app.utils.aws_translate import translate_to_english
from app.utils.serialize_mongo import serialize_mongo_document
from app.telegram_client import telegram_client
from app.middlewares.auth_middleware import get_current_user
import os
import re

router = APIRouter()


class TranslateRequest(BaseModel):
    """
    Request model for translating text.
    """
    id: str
    text: str
    target_language: Optional[str] = "en"

@router.post("/messages/translate")
async def translate_messages(request: TranslateRequest, get_current_user: dict = Depends(get_current_user)):
    """
    Translate text to English using AWS Translate and store in MongoDB.
    First checks if translation already exists in database.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Try to find existing message and translation
        message = await messages_collection.find_one({"_id": ObjectId(request.id)})
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")

        # Check if translation already exists
        if message.get("translated_text"):
            if request.text == message["translated_text"]:
                # If incoming text matches stored translation, return original text
                return {"translated_text": message["text"]}
            else:
                # Otherwise return the stored translation
                return {"translated_text": message["translated_text"]}

        # Translate and store if no existing translation
        translated_text = translate_to_english(request.text)

        # Update message document with translation
        await messages_collection.update_one(
            {"_id": ObjectId(request.id)},
            {"$set": {"translated_text": translated_text}}
        )

        return {"translated_text": translated_text}

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/messages")
async def get_messages(username: str, minutes: int, get_current_user: dict = Depends(get_current_user)):
    """
    Fetch messages from a group by username within the last X minutes.
    Validate the group against Telegram and return its details with messages.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Fetch the group by username from the database
        clean_username = username.lstrip("@")
        group = await groups_collection.find_one({"username": clean_username})
        if not group:
            raise HTTPException(
                status_code=404, detail="Group not found in the database")

        # Validate the group on Telegram
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

        # Get the group_id from the group document
        group_id = group.get("group_id")
        if not group_id:
            raise HTTPException(
                status_code=404, detail="Group ID not found for the provided username"
            )

        # Calculate the cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes)

        # Query messages within the cutoff time
        messages = await messages_collection.find(
            {
                "group_id": group_id,
                "date": {"$gte": cutoff_time},
            }
        ).to_list(None)

        # Serialize messages
        serialized_messages = [
            serialize_mongo_document(msg) for msg in messages]

        # Update listener to ensure it's up-to-date
        await update_listener()

        # Return the group and its messages
        return {
            "group": serialize_mongo_document(group),
            "messages": serialized_messages,
        }

    except HTTPException as e:
        # Raise HTTP exceptions as-is
        raise e

    except Exception as e:
        # Catch-all for unexpected errors
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/messages/search")
async def search_messages(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    group_ids: List[str] | None = Query(None, alias="group_ids"),
    category: str | None = None,
    tag: str | None = None,
    keyword: str | None = None,
    sortOrder: str = "latest",  # Sorting order: "latest" (default) or "oldest"
    page: int = 1,  # Pagination
    limit: int = 10,  # Number of items per page
    get_current_user: dict = Depends(get_current_user)
):
    """
    High-performance search for messages, optimized for a large collection.
    Shows the latest message per channel, with efficient pagination and filtering.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        # Create indexes if they don't exist (run this rarely, not on every request in production)
        # await messages_collection.create_index([("group_id", 1), ("date", -1)])
        # await messages_collection.create_index([("text", "text")])
        # await messages_collection.create_index([("media", 1)])
        # await messages_collection.create_index([("date", 1)])
        # await groups_collection.create_index([("group_id", 1)])

        # Start with indexed fields for the base query
        base_query = {}

        # Handle group_ids (uses index)
        if group_ids:
            try:
                int_group_ids = [int(gid) for gid in group_ids]
                base_query["group_id"] = {"$in": int_group_ids}
            except (ValueError, TypeError):
                return {"messages": [], "total_pages": 0, "current_page": page}

        # Add date filtering (uses index)
        if start_date or end_date:
            base_query["date"] = {}
            if start_date:
                base_query["date"]["$gte"] = start_date
            if end_date:
                base_query["date"]["$lte"] = end_date

        # Handle keyword search - improved implementation
        if keyword:
            keyword = keyword.strip()
            if keyword:
                # Simple contains approach - finds keyword anywhere in text
                base_query["text"] = {"$regex": re.escape(keyword), "$options": "i"}
        else:
            # If no keyword, ensure we only get messages with content or media
            base_query["$or"] = [
                {"text": {"$ne": ""}},
                {"media": {"$ne": None}}
            ]

        # Category filtering
        if category:
            category_doc = await categories_collection.find_one({"name": category.upper()})
            if not category_doc:
                return {"messages": [], "total_pages": 0, "current_page": page}
            base_query["category"] = category_doc.get("name")

        # Tag filtering
        if tag:
            tag_doc = await tags_collection.find_one({"tag": tag})
            if not tag_doc:
                return {"messages": [], "total_pages": 0, "current_page": page}
            base_query["tags"] = tag_doc.get("tag")

        # Sort direction
        sort_direction = -1 if sortOrder == "latest" else 1

        # Step 1: Get distinct group_ids that match the query
        distinct_group_ids = await messages_collection.distinct("group_id", base_query)
        
        if not distinct_group_ids:
            return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Step 2: For each group_id, find the latest message timestamp
        group_latest_timestamps = []
        
        # Use a cursor to process groups in batches for better memory usage
        for group_id in distinct_group_ids:
            latest_message = await messages_collection.find_one(
                {**base_query, "group_id": group_id},
                sort=[("date", -1)],  # Always get the newest message first
                projection={"date": 1, "group_id": 1}
            )
            if latest_message:
                group_latest_timestamps.append({
                    "group_id": group_id,
                    "latest_timestamp": latest_message["date"]
                })
        
        # Step 3: Sort the groups by their latest message timestamp
        group_latest_timestamps.sort(
            key=lambda x: x["latest_timestamp"], 
            reverse=(sortOrder == "latest")  # True for latest, False for oldest
        )
        
        # Step 4: Count total unique groups for pagination
        total_groups = len(group_latest_timestamps)
        
        if total_groups == 0:
            return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Step 5: Apply pagination to the sorted group_ids
        paginated_groups = group_latest_timestamps[(page-1)*limit:page*limit]
        paginated_group_ids = [group["group_id"] for group in paginated_groups]
        
        if not paginated_group_ids:
            return {
                "messages": [],
                "total_pages": total_groups // limit + (1 if total_groups % limit > 0 else 0),
                "current_page": page,
            }
        
        # Step 6: Fetch the actual latest message for each paginated group
        latest_messages = []
        for group_id in paginated_group_ids:
            latest_message = await messages_collection.find_one(
                {**base_query, "group_id": group_id},
                sort=[("date", -1)]  # Always get the newest message first
            )
            
            if latest_message:
                latest_messages.append(latest_message)
        
        # Calculate total pages
        total_pages = (total_groups + limit - 1) // limit  # Ceiling division

        # Get group titles in a single efficient query
        group_ids = list(set(msg["group_id"] for msg in latest_messages))
        groups = await groups_collection.find(
            {"group_id": {"$in": group_ids}},
            {"group_id": 1, "title": 1}  # Only get needed fields
        ).to_list(None)
        group_map = {group["group_id"]: group["title"] for group in groups}

        # Efficient media classification
        media_extensions = {
            **{ext: "image" for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]},
            **{ext: "video" for ext in [".mp4", ".mkv", ".avi", ".mov"]}
        }
        
        def classify_media(media_url):
            if not media_url:
                return None
            extension = os.path.splitext(media_url)[-1].lower()
            return media_extensions.get(extension, "file")

        # Process has_previous checks more efficiently
        has_previous_checks = []
        for msg in latest_messages:
            has_previous = await messages_collection.count_documents({
                "group_id": msg["group_id"],
                "date": {"$lt": msg["date"]}
            }, limit=1) > 0
            has_previous_checks.append((str(msg["_id"]), has_previous))
        
        has_previous_map = dict(has_previous_checks)

        # Format the final response
        formatted_messages = []
        for msg in latest_messages:
            media_url = msg.get("media", None)
            media_type = classify_media(media_url)
            media_obj = None
            
            if media_url:
                if media_type == "image":
                    media_obj = {"type": "image", "url": media_url, "alt": "Image content"}
                elif media_type == "video":
                    media_obj = {"type": "video", "url": media_url, "thumbnail": "Telegram Video"}
                else:
                    media_obj = {"type": "file", "url": media_url, "name": os.path.basename(media_url)}

            formatted_messages.append({
                "id": str(msg["_id"]),
                "message_id": msg.get("message_id"),
                "group_id": msg["group_id"],
                "channel": group_map.get(msg["group_id"], "Unknown Group"),
                "timestamp": msg["date"].isoformat(),
                "content": msg.get("text", "") or "No content",
                "tags": msg.get("tags", ["no tags found"]),
                "media": media_obj,
                "has_previous": has_previous_map.get(str(msg["_id"]), False)
            })

        return {
            "messages": formatted_messages,
            "total_pages": total_pages,
            "current_page": page,
        }

    except Exception as e:
        print(f"Error in search_messages: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/messages/previous")
async def get_previous_message(
    group_id: int, 
    message_id: int, 
    get_current_user: dict = Depends(get_current_user)
):
    """
    Fetch the message that came before the specified message_id in the same group.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Find the current message to get its date
        current_message = await messages_collection.find_one({
            "group_id": group_id,
            "message_id": message_id
        })
        
        if not current_message:
            raise HTTPException(status_code=404, detail="Current message not found")
        
        # Find the previous message (older than the current one)
        previous_message = await messages_collection.find_one(
            {
                "group_id": group_id,
                "date": {"$lt": current_message["date"]}
            },
            sort=[("date", -1)]  # Get the most recent message before the current one
        )
        
        if not previous_message:
            return {"message": None, "has_previous": False}
        
        # Get the group info for the channel name
        group = await groups_collection.find_one({"group_id": group_id})
        group_title = group["title"] if group else "Unknown Group"
        
        # Helper function for media type classification
        def classify_media(media_url):
            if not media_url:
                return None
            media_types = {
                "image": [".jpg", ".jpeg", ".png", ".gif", ".webp"],
                "video": [".mp4", ".mkv", ".avi", ".mov"],
            }
            extension = os.path.splitext(media_url)[-1].lower()
            for media_type, extensions in media_types.items():
                if extension in extensions:
                    return media_type
            return "file"
        
        # Format the message response
        media_url = previous_message.get("media", None)
        media_type = classify_media(media_url)
        media_obj = None
        
        if media_url:
            if media_type == "image":
                media_obj = {"type": "image", "url": media_url, "alt": "Image content"}
            elif media_type == "video":
                media_obj = {"type": "video", "url": media_url, "thumbnail": "Telegram Video"}
            else:
                media_obj = {"type": "file", "url": media_url, "name": os.path.basename(media_url)}
        
        formatted_message = {
            "id": str(previous_message["_id"]),
            "message_id": previous_message["message_id"],
            "group_id": previous_message["group_id"],
            "channel": group_title,
            "timestamp": previous_message["date"].isoformat(),
            "content": previous_message["text"] or "No content",
            "tags": previous_message.get("tags", ["no tags found"]),
            "media": media_obj if media_url else None,
            "has_previous": True  # Assume there might be more previous messages
        }
        
        # Check if there are more previous messages
        older_count = await messages_collection.count_documents({
            "group_id": group_id,
            "date": {"$lt": previous_message["date"]}
        })
        
        formatted_message["has_previous"] = older_count > 0
        
        return formatted_message
        
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

