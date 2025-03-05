from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from app.database import alert_settings_collection
from app.utils.serialize_mongo import serialize_mongo_document
from app.middlewares.auth_middleware import get_current_user

router = APIRouter()

class AlertSettingsModel(BaseModel):
    keyword: str
    alertTypes: List[str]
    frequency: str

@router.post("/alerts")
async def create_or_update_alert_settings(alert_settings: AlertSettingsModel, current_user: dict = Depends(get_current_user)):
    """
    Create or update alert settings for the current user.
    This will create a new document if one doesn't exist, or update the existing one.
    """
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Check if alert settings already exist for this user
        existing_settings = await alert_settings_collection.find_one({"user_id": current_user["user_id"]})
        
        # Prepare updated alert settings
        alert_settings_data = {
            "keyword": alert_settings.keyword,
            "alert_types": alert_settings.alertTypes,
            "frequency": alert_settings.frequency,
            "updated_at": datetime.now()
        }
        
        if existing_settings:
            # Update existing settings
            result = await alert_settings_collection.update_one(
                {"user_id": current_user["user_id"]},
                {"$set": alert_settings_data}
            )
            
            if result.modified_count == 0:
                raise HTTPException(status_code=500, detail="Failed to update alert settings")
                
            # Get the updated document
            updated_settings = await alert_settings_collection.find_one({"user_id": current_user["user_id"]})
            
            return {
                "status": "success",
                "message": "Alert settings updated successfully",
                "data": serialize_mongo_document(updated_settings)
            }
        else:
            # Create new settings
            alert_settings_data.update({
                "user_id": current_user["user_id"],
                "created_at": datetime.now()
            })
            
            result = await alert_settings_collection.insert_one(alert_settings_data)
            
            # Add the ID to the document for the response
            alert_settings_data["_id"] = result.inserted_id
            
            return {
                "status": "success",
                "message": "Alert settings created successfully",
                "data": serialize_mongo_document(alert_settings_data)
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts")
async def get_alert_settings(current_user: dict = Depends(get_current_user)):
    """
    Get the alert settings for the current user.
    """
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
            
        # Get the alert settings for the user
        alert_settings = await alert_settings_collection.find_one({"user_id": current_user["user_id"]})
        
        if not alert_settings:
            return {
                "status": "success",
                "message": "No alert settings found",
                "data": None
            }
            
        return {
            "status": "success",
            "message": "Alert settings retrieved successfully",
            "data": serialize_mongo_document(alert_settings)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))