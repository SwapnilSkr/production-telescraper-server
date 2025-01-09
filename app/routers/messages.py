from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from app.database import messages_collection, groups_collection
from app.services.telegram_listener import update_listener
from app.utils.serialize_mongo import serialize_mongo_document


router = APIRouter()


@router.get("/messages")
async def get_messages(username: str, minutes: int):
    """Fetch messages from a group by username within the last X minutes."""
    try:
        # Fetch the group by username
        group = await groups_collection.find_one({"username": username.lstrip("@")})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        # Get the group_id from the group document
        group_id = group.get("group_id")
        if not group_id:
            raise HTTPException(
                status_code=404, detail="Group ID not found for the provided username")

        # Calculate the cutoff time
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)

        # Query messages within the cutoff time
        messages = await messages_collection.find({
            "group_id": group_id,
            "date": {"$gte": cutoff_time}
        }).to_list(length=100)

        # Serialize messages
        serialized_messages = [
            serialize_mongo_document(msg) for msg in messages]

        await update_listener()  # Ensure the listener is up-to-date
        return {"messages": serialized_messages}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
