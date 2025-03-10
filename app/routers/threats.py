from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from app.database import threat_library_collection
from app.middlewares.auth_middleware import get_current_user
from bson import ObjectId

router = APIRouter()

@router.get("/threat-count", response_model=dict)
async def get_threats_count(
    get_current_user: dict = Depends(get_current_user),
    keyword: Optional[str] = None,
    group_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    Get the count of threats for the authenticated user with optional filters.
    
    Args:
        user_id: The authenticated user's ID
        keyword: Optional filter for matched keyword
        group_id: Optional filter for specific group/channel
        start_date: Optional filter for threats after this date (ISO format)
        end_date: Optional filter for threats before this date (ISO format)
    
    Returns:
        The count of threats matching the criteria
    """
    # Build query filter

    if not get_current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    query = {}
    
    # Add optional filters
    if keyword:
        query["matched_keyword"] = keyword
    if group_id:
        query["group_id"] = group_id
    
    # Add date range filters if provided
    date_filter = {}
    if start_date:
        date_filter["$gte"] = start_date
    if end_date:
        date_filter["$lte"] = end_date
    if date_filter:
        query["date_detected"] = date_filter
    
    # Count matching threats
    count = await threat_library_collection.count_documents(query)
    print(f"Count: {count}")
    return {"count": count}

@router.get("/alert-types", response_model=List[str])
async def get_unique_alert_types(
    get_current_user: dict = Depends(get_current_user)
):
    """
    Get a list of unique alert types used across all threats for the authenticated user.
    
    Args:
        user_id: The authenticated user's ID
    
    Returns:
        A list of unique alert types
    """

    if not get_current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Use aggregation to get unique alert types
    pipeline = [
        {"$unwind": "$alert_types"},
        {"$group": {"_id": "$alert_types"}},
        {"$project": {"_id": 0, "alert_type": "$_id"}}
    ]
    
    result = await threat_library_collection.aggregate(pipeline).to_list(length=None)
    
    # Extract alert types from result
    alert_types = [doc.get("alert_type") for doc in result if "alert_type" in doc]
    return alert_types
