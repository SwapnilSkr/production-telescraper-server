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

@router.get("/alert-types-percentage", response_model=List[dict])
async def get_alert_types_percentage(
    get_current_user: dict = Depends(get_current_user),
    keyword: Optional[str] = None,
    group_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    time_period: Optional[str] = Query("weekly", enum=["weekly", "monthly", "all"])
):
    """
    Get the percentage of each alert type out of the total threat count.
    
    Args:
        user_id: The authenticated user's ID
        keyword: Optional filter for matched keyword
        group_id: Optional filter for specific group/channel
        start_date: Optional filter for threats after this date (ISO format)
        end_date: Optional filter for threats before this date (ISO format)
        time_period: Optional time period grouping (weekly, monthly, or all). Default is weekly.
    
    Returns:
        A list of dictionaries containing alert types and their percentages
    """
    if not get_current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Build query filter
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
    
    # If time_period is "all", return overall percentages
    if time_period == "all":
        # Get total count of threats
        total_count = await threat_library_collection.count_documents(query)
        
        if total_count == 0:
            return []
        
        # Use aggregation to get count of each alert type
        pipeline = [
            {"$match": query},
            {"$unwind": "$alert_types"},
            {"$group": {"_id": "$alert_types", "count": {"$sum": 1}}},
            {"$project": {
                "_id": 0,
                "alert_type": "$_id",
                "count": 1,
                "percentage": {"$round": [{"$multiply": [{"$divide": ["$count", total_count]}, 100]}, 0]},
                "total_count": {"$literal": total_count}
            }},
            {"$sort": {"count": -1}}
        ]
        
        result = await threat_library_collection.aggregate(pipeline).to_list(length=None)
        
        return result
    
    # For weekly or monthly time periods
    time_format = "%Y-%U" if time_period == "weekly" else "%Y-%m"
    
    # First, get the count of documents per time period
    count_pipeline = [
        {"$match": query},
        {"$addFields": {
            "time_group": {"$dateToString": {"format": time_format, "date": "$date_detected"}}
        }},
        {"$group": {
            "_id": "$time_group",
            "doc_count": {"$sum": 1}
        }}
    ]
    
    time_period_counts = await threat_library_collection.aggregate(count_pipeline).to_list(length=None)
    time_period_counts_dict = {item["_id"]: item["doc_count"] for item in time_period_counts}
    
    # Now get the alert type counts
    pipeline = [
        {"$match": query},
        # Add a field with formatted date for grouping
        {"$addFields": {
            "time_group": {"$dateToString": {"format": time_format, "date": "$date_detected"}}
        }},
        {"$unwind": "$alert_types"},
        # Group by time period and alert type
        {"$group": {
            "_id": {
                "time_group": "$time_group",
                "alert_type": "$alert_types"
            },
            "count": {"$sum": 1}
        }},
        # Group by time period
        {"$group": {
            "_id": "$_id.time_group",
            "alert_types": {
                "$push": {
                    "alert_type": "$_id.alert_type",
                    "count": "$count"
                }
            }
        }},
        # Sort by time period (most recent first)
        {"$sort": {"_id": -1}}
    ]
    
    result = await threat_library_collection.aggregate(pipeline).to_list(length=None)
    
    # Format the result to flatten the structure and calculate percentages
    formatted_result = []
    for period in result:
        time_group = period["_id"]
        total_docs = time_period_counts_dict.get(time_group, 0)
        
        if total_docs == 0:
            continue
            
        time_label = time_group
        if time_period == "weekly":
            year, week = time_group.split("-")
            time_label = f"Week {week}, {year}"
        elif time_period == "monthly":
            year, month = time_group.split("-")
            time_label = f"{year}-{month}"
            
        # Calculate percentages and add total_count
        alert_types_with_percentages = []
        for alert_type in period["alert_types"]:
            percentage = round((alert_type["count"] / total_docs) * 100)
            alert_types_with_percentages.append({
                "alert_type": alert_type["alert_type"],
                "count": alert_type["count"],
                "percentage": percentage,
            })
            
        # Sort alert types by count (descending)
        sorted_alert_types = sorted(alert_types_with_percentages, key=lambda x: x["count"], reverse=True)
        
        formatted_result.append({
            "time_period": time_label,
            "total_count": total_docs,
            "alert_types": sorted_alert_types
        })
    
    return formatted_result
