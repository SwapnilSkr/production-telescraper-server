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
    Search messages using keyword logic:
    - If keyword has no space, match it as a prefix (until a space is hit).
    - If keyword has a space at the end, match it exactly.
    - If keyword is entirely spaces, ignore search.
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
            group_ids = [int(group_id) for group_id in group_ids]
            matching_groups = await groups_collection.find(
                {"group_id": {"$in": group_ids}}
            ).to_list(None)

            print(f"matching_groups: {matching_groups}")

            if not matching_groups:
                raise HTTPException(
                    status_code=404, detail="No matching groups found")

            # Add group_ids to the query
            query["group_id"] = {"$in": [group["group_id"]
                                         for group in matching_groups]}
            print(f"query: {query['group_id']}")

        # Filter by category
        if category:
            category_doc = await categories_collection.find_one({"name": category.upper()})
            if not category_doc:
                raise HTTPException(
                    status_code=404, detail="Category not found")
            query["category"] = category_doc.get("name")

        # Filter by tag
        if tag:
            tag_doc = await tags_collection.find_one({"tag": tag})
            if not tag_doc:
                raise HTTPException(status_code=404, detail="Tag not found")
            query["tags"] = tag_doc.get("tag")

        # **Keyword Search Logic**
        if keyword:
            keyword = keyword.strip()  # Remove leading/trailing spaces

            if keyword == "":  # Ignore empty searches
                raise HTTPException(
                    status_code=400, detail="Invalid search keyword")

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

        # Get total message count
        total_messages = await messages_collection.count_documents(query)

        # Paginate messages
        print(f"query: {query}")
        messages_cursor = messages_collection.find(query).sort("date", sort_order).skip(
            (page - 1) * limit
        ).limit(limit)

        # Fetch all messages in the range
        messages = await messages_cursor.to_list(None)

        # Fetch group titles
        group_ids = list(set(msg["group_id"] for msg in messages))
        groups = await groups_collection.find({"group_id": {"$in": group_ids}}).to_list(None)
        group_map = {group["group_id"]: group["title"] for group in groups}

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

        # Format messages for response
        formatted_messages = []
        for msg in messages:
            media_url = msg.get("media", None)
            media_type = classify_media(media_url)
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

            formatted_messages.append({
                "id": str(msg["_id"]),  # Convert MongoDB ObjectId to string
                "channel": group_map.get(msg["group_id"], "Unknown Group"),
                "timestamp": msg["date"].isoformat(),
                "content": msg["text"] or "No content",
                "tags": msg.get("tags", ["no tags found"]),
                "media": media_obj if media_url else None,
            })

        return {
            "messages": formatted_messages,
            "total_pages": (total_messages // limit) + (1 if total_messages % limit > 0 else 0),
            "current_page": page,
        }

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

