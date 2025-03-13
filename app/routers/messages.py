from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
import asyncio
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

        # Build query with minimal conditions first
        base_query = {}

        # Handle group_ids - this is the most important filter for performance
        if group_ids:
            try:
                # Convert to integers and validate before adding to query
                int_group_ids = [int(gid) for gid in group_ids if gid.strip()]
                if int_group_ids:  # Only add if we have valid IDs
                    base_query["group_id"] = {"$in": int_group_ids}
                else:
                    # Return empty results for completely invalid group_ids
                    return {"messages": [], "total_pages": 0, "current_page": page}
            except (ValueError, TypeError):
                # Return empty results for invalid group_ids
                return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Date filtering - add to base query as it uses an index
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query["$gte"] = start_date
            if end_date:
                date_query["$lte"] = end_date
            if date_query:
                base_query["date"] = date_query
        
        # Prepare text search conditions
        if keyword:
            keyword = keyword.strip()
            if keyword:
                # Use text index search instead of regex for better performance
                if len(keyword) > 2:  # Only use text search for meaningful terms
                    base_query["$text"] = {"$search": keyword}
                else:
                    # For very short keywords, fallback to regex but make sure it's anchored
                    base_query["text"] = {"$regex": f"\\b{re.escape(keyword)}\\b", "$options": "i"}
        
        # Category filtering
        if category and category.strip():
            category_upper = category.strip().upper()
            category_doc = await categories_collection.find_one({"name": category_upper})
            if category_doc:
                base_query["category"] = category_doc.get("name")
            else:
                return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Tag filtering
        if tag and tag.strip():
            tag_doc = await tags_collection.find_one({"tag": tag.strip()})
            if tag_doc:
                base_query["tags"] = tag_doc.get("tag")
            else:
                return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Sort direction
        sort_direction = -1 if sortOrder == "latest" else 1

        # Use optimized aggregation pipeline
        pipeline = []
        
        # 1. Match stage - apply all filtering first to reduce document set
        pipeline.append({"$match": base_query})
        
        # 2. Sort by date before grouping
        pipeline.append({"$sort": {"date": -1}})
        
        # 3. Group by group_id, keeping the first document (latest per group)
        pipeline.append({
            "$group": {
                "_id": "$group_id",
                "doc": {"$first": "$$ROOT"},
                "latest_date": {"$first": "$date"}
            }
        })
        
        # 4. Sort the groups by date according to user's preference
        pipeline.append({"$sort": {"latest_date": sort_direction}})
        
        # 5. Count total groups before pagination for accurate pagination
        count_pipeline = pipeline.copy()
        count_pipeline.append({"$count": "total"})
        
        # Continue with pagination for main query
        main_pipeline = pipeline.copy()
        main_pipeline.append({"$skip": (page - 1) * limit})
        main_pipeline.append({"$limit": limit})
        main_pipeline.append({"$replaceRoot": {"newRoot": "$doc"}})
        
        # Execute count and main query in parallel - properly awaited
        count_result = await messages_collection.aggregate(count_pipeline).to_list(length=None)
        latest_messages = await messages_collection.aggregate(main_pipeline).to_list(length=None)
        
        # If no messages found, return empty result
        if not latest_messages:
            return {"messages": [], "total_pages": 0, "current_page": page}
        
        # Get total count
        total_groups = count_result[0]["total"] if count_result else 0
        
        # Calculate total pages
        total_pages = (total_groups + limit - 1) // limit  # Ceiling division
        
        # Fetch group information
        group_ids = [msg["group_id"] for msg in latest_messages]
        groups = await groups_collection.find(
            {"group_id": {"$in": group_ids}},
            {"group_id": 1, "title": 1}
        ).to_list(length=None)
        
        # Map group_id to title for quick lookups
        group_map = {group["group_id"]: group["title"] for group in groups}
        
        # Process has_previous in a single aggregation
        has_previous_map = {}
        if latest_messages:
            # Build OR conditions for all messages
            has_previous_conditions = [
                {"group_id": msg["group_id"], "date": {"$lt": msg["date"]}}
                for msg in latest_messages
            ]
            
            # Execute aggregation to find groups with previous messages
            has_previous_results = await messages_collection.aggregate([
                {"$match": {"$or": has_previous_conditions}},
                {"$group": {"_id": "$group_id"}}
            ]).to_list(length=None)
            
            # Convert to lookup map
            has_previous_map = {result["_id"]: True for result in has_previous_results}
        
        # Media classification with lookup table for efficiency
        media_extensions = {
            **{ext: "image" for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]},
            **{ext: "video" for ext in [".mp4", ".mkv", ".avi", ".mov"]}
        }
        
        def classify_media(url):
            if not url:
                return None
            extension = os.path.splitext(url)[-1].lower()
            return media_extensions.get(extension, "file")
        
        # Format messages for response
        formatted_messages = []
        for msg in latest_messages:
            group_id = msg["group_id"]
            
            # Process media
            media_url = msg.get("media")
            media_obj = None
            
            if media_url:
                media_type = classify_media(media_url)
                if media_type == "image":
                    media_obj = {"type": "image", "url": media_url, "alt": "Image content"}
                elif media_type == "video":
                    media_obj = {"type": "video", "url": media_url, "thumbnail": "Telegram Video"}
                else:
                    media_obj = {"type": "file", "url": media_url, "name": os.path.basename(media_url)}
            
            # Add formatted message
            formatted_messages.append({
                "id": str(msg["_id"]),
                "message_id": msg.get("message_id"),
                "group_id": group_id,
                "channel": group_map.get(group_id, "Unknown Group"),
                "timestamp": msg["date"].isoformat(),
                "content": msg.get("text", "") or "No content",
                "tags": msg.get("tags", ["no tags found"]),
                "media": media_obj,
                "has_previous": has_previous_map.get(group_id, False)
            })
        
        return {
            "messages": formatted_messages,
            "total_pages": total_pages,
            "current_page": page
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

