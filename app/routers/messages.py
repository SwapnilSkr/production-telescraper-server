from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta, timezone
from app.database import messages_collection, groups_collection, tags_collection, categories_collection
from app.services.telegram_listener import update_listener
from app.utils.serialize_mongo import serialize_mongo_document
from app.telegram_client import telegram_client

router = APIRouter()


@router.get("/messages")
async def get_messages(username: str, minutes: int):
    """
    Fetch messages from a group by username within the last X minutes.
    Validate the group against Telegram and return its details with messages.
    """
    try:
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
        ).to_list(length=100)

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
    group_name: str | None = None,
    category: str | None = None,
    tag: str | None = None,
    keyword: str | None = None,
    limit: int = 100,
):
    """
    Search and filter messages based on various criteria:
    1. Sort messages in descending order of created_at.
    2. Filter messages within a date range (start_date and end_date).
    3. Filter messages by group name.
    4. Filter messages by categories.
    5. Filter messages by tags.
    6. Search for a keyword across group names, categories, tags, and message text.
    """
    try:
        # Build the query
        query = {}

        # Filter by date range
        if start_date and end_date:
            query["date"] = {"$gte": start_date, "$lte": end_date}
        elif start_date:
            query["date"] = {"$gte": start_date}
        elif end_date:
            query["date"] = {"$lte": end_date}

        # Filter by group name
        if group_name:
            group = await groups_collection.find_one({"username": group_name.lstrip("@")})
            if not group:
                raise HTTPException(status_code=404, detail="Group not found")
            query["group_id"] = group.get("group_id")

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

        # Filter by keyword
        if keyword:
            keyword_filter = []

            # Search in group names
            group_docs = await groups_collection.find({"username": {"$regex": keyword, "$options": "i"}}).to_list(length=100)
            group_ids = [group_doc.get("group_id") for group_doc in group_docs]
            if group_ids:
                keyword_filter.append({"group_id": {"$in": group_ids}})

            # Search in categories
            category_docs = await categories_collection.find({"name": {"$regex": keyword, "$options": "i"}}).to_list(length=100)
            category_names = [category_doc.get(
                "name") for category_doc in category_docs]
            if category_names:
                keyword_filter.append({"category": {"$in": category_names}})

            # Search in tags
            tag_docs = await tags_collection.find({"tag": {"$regex": keyword, "$options": "i"}}).to_list(length=100)
            tag_names = [tag_doc.get("tag") for tag_doc in tag_docs]
            if tag_names:
                keyword_filter.append({"tags": {"$in": tag_names}})

            # Search in message text
            keyword_filter.append(
                {"text": {"$regex": keyword, "$options": "i"}})

            # Combine all keyword filters with an OR condition
            query["$or"] = keyword_filter

        # Query messages
        messages_cursor = messages_collection.find(
            query).sort("date", -1).limit(limit)
        messages = await messages_cursor.to_list(length=limit)

        # Serialize messages
        serialized_messages = [
            serialize_mongo_document(msg) for msg in messages
        ]

        return {"messages": serialized_messages}

    except HTTPException as e:
        # Raise HTTP exceptions as-is
        raise e

    except Exception as e:
        # Catch-all for unexpected errors
        raise HTTPException(status_code=500, detail=str(e))
