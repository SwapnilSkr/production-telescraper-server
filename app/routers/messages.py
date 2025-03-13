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
    Search messages using keyword logic and group by channel, showing only the latest message per channel.
    Groups are sorted by their latest message timestamp.
    """

    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Base query to ensure messages have content or media
        query = {
            "$or": [
                {"text": {"$ne": ""}},
                {"media": {"$exists": True, "$ne": None}},
            ]
        }

        print(f"group_ids: {group_ids}")
        # Filter by date range
        if start_date and end_date:
            query["date"] = {"$gte": start_date, "$lte": end_date}
        elif start_date:
            query["date"] = {"$gte": start_date}
        elif end_date:
            query["date"] = {"$lte": end_date}

        if group_ids:
            try:
                int_group_ids = [int(group_id) for group_id in group_ids]
                matching_groups = await groups_collection.find(
                    {"group_id": {"$in": int_group_ids}},
                    {"group_id": 1}  # Only retrieve group_id field
                ).to_list(None)

                print(f"matching_groups: {matching_groups}")

                if not matching_groups:
                    return {
                        "messages": [],
                        "total_pages": 0,
                        "current_page": page,
                    }

                # Add group_ids to the query
                query["group_id"] = {"$in": [group["group_id"] for group in matching_groups]}
            except ValueError:
                # Handle invalid group_id conversion
                return {
                    "messages": [],
                    "total_pages": 0,
                    "current_page": page,
                }

        # Filter by category
        if category:
            category_doc = await categories_collection.find_one({"name": category.upper()})
            if not category_doc:
                return {
                    "messages": [],
                    "total_pages": 0,
                    "current_page": page,
                }
            query["category"] = category_doc.get("name")

        # Filter by tag
        if tag:
            tag_doc = await tags_collection.find_one({"tag": tag})
            if not tag_doc:
                return {
                    "messages": [],
                    "total_pages": 0,
                    "current_page": page,
                }
            query["tags"] = tag_doc.get("tag")

        # **Keyword Search Logic**
        if keyword:
            keyword = keyword.strip()  # Remove leading/trailing spaces

            if keyword == "":  # Ignore empty searches
                return {
                    "messages": [],
                    "total_pages": 0,
                    "current_page": page,
                }

            if keyword.endswith(" "):
                # **Exact word match** (if keyword has space at the end)
                query["text"] = {
                    "$regex": f"\\b{re.escape(keyword.strip())}\\b", "$options": "i"}
            else:
                # **Prefix match until a space is hit**
                query["text"] = {
                    "$regex": f"\\b{re.escape(keyword)}[^ ]*", "$options": "i"}

        # Sorting order
        sort_order = -1 if sortOrder == "latest" else 1

        # Use a simpler approach to avoid memory issues with aggregation
        # 1. Get distinct group_ids that match the query
        distinct_group_ids = await messages_collection.distinct("group_id", query)
        
        if not distinct_group_ids:
            return {
                "messages": [],
                "total_pages": 0,
                "current_page": page,
            }
        
        # 2. For each group_id, find the latest message timestamp
        # This uses a more efficient approach with projection
        group_latest_timestamps = []
        for group_id in distinct_group_ids:
            latest_message = await messages_collection.find_one(
                {**query, "group_id": group_id},
                sort=[("date", -1)],  # Always get the newest message first
                projection={"date": 1, "group_id": 1}
            )
            if latest_message:
                group_latest_timestamps.append({
                    "group_id": group_id,
                    "latest_timestamp": latest_message["date"]
                })
        
        # 3. Sort the groups by their latest message timestamp
        group_latest_timestamps.sort(
            key=lambda x: x["latest_timestamp"], 
            reverse=(sortOrder == "latest")  # True for latest, False for oldest
        )
        
        # 4. Count total unique groups for pagination
        total_groups = len(group_latest_timestamps)
        
        if total_groups == 0:
            return {
                "messages": [],
                "total_pages": 0,
                "current_page": page,
            }
        
        # 5. Apply pagination to the sorted group_ids
        paginated_groups = group_latest_timestamps[(page-1)*limit:page*limit]
        paginated_group_ids = [group["group_id"] for group in paginated_groups]
        
        if not paginated_group_ids:
            return {
                "messages": [],
                "total_pages": total_groups // limit + (1 if total_groups % limit > 0 else 0),
                "current_page": page,
            }
        
        # 6. For each paginated group_id, find the latest message (fetch in one query if possible)
        latest_messages = []
        for group_id in paginated_group_ids:
            # Find the latest message for this group
            latest_message = await messages_collection.find_one(
                {**query, "group_id": group_id},
                sort=[("date", -1)]  # Always get the newest message first
            )
            
            if latest_message:
                latest_messages.append(latest_message)
        
        # Calculate total pages correctly
        total_pages = (total_groups + limit - 1) // limit  # Ceiling division

        # Fetch group titles in one query
        group_ids = list(set(msg["group_id"] for msg in latest_messages))
        groups = await groups_collection.find(
            {"group_id": {"$in": group_ids}},
            {"group_id": 1, "title": 1}  # Only get the fields we need
        ).to_list(None)
        group_map = {group["group_id"]: group["title"] for group in groups}

        # Optimize media classification
        media_extensions = {
            **{ext: "image" for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]},
            **{ext: "video" for ext in [".mp4", ".mkv", ".avi", ".mov"]}
        }
        
        def classify_media_optimized(media_url):
            if not media_url:
                return None
            extension = os.path.splitext(media_url)[-1].lower()
            return media_extensions.get(extension, "file")

        # Format messages for response
        formatted_messages = []
        for msg in latest_messages:
            media_url = msg.get("media", None)
            media_type = classify_media_optimized(media_url)
            media_obj = None
            if media_url:
                if media_type == "image":
                    media_obj = {"type": "image",
                                "url": media_url, "alt": "Image content"}
                elif media_type == "video":
                    media_obj = {"type": "video", "url": media_url,
                                "thumbnail": "Telegram Video"}
                else:
                    media_obj = {"type": "file", "url": media_url,
                                "name": os.path.basename(media_url)}

            # Check if there are previous messages for this group - using a more efficient count
            has_previous = await messages_collection.count_documents({
                "group_id": msg["group_id"],
                "date": {"$lt": msg["date"]}
            }, limit=1) > 0

            formatted_messages.append({
                "id": str(msg["_id"]),  # Convert MongoDB ObjectId to string
                "message_id": msg["message_id"],  # Include the original message_id
                "group_id": msg["group_id"],  # Include the group_id
                "channel": group_map.get(msg["group_id"], "Unknown Group"),
                "timestamp": msg["date"].isoformat(),
                "content": msg["text"] or "No content",
                "tags": msg.get("tags", ["no tags found"]),
                "media": media_obj if media_url else None,
                "has_previous": has_previous  # Indicate if there are previous messages
            })

        return {
            "messages": formatted_messages,
            "total_pages": total_pages,
            "current_page": page,
        }

    except Exception as e:
        print(f"Error: {e}")
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

