from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from bson import ObjectId
from app.database import categories_collection
from app.utils.serialize_mongo import serialize_mongo_document
from app.middlewares.auth_middleware import get_current_user
router = APIRouter()


class Category(BaseModel):
    name: str
    description: str | None = None


@router.post("/categories")
async def add_category(category: Category, get_current_user: dict = Depends(get_current_user)):
    """
    Add a new category. Ensures the name is stored in uppercase.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Convert the category name to uppercase
        category_name_upper = category.name.upper()

        # Check if a category with the same name already exists
        existing_category = await categories_collection.find_one({"name": category_name_upper})
        if existing_category:
            raise HTTPException(
                status_code=400, detail="Category already exists")

        # Create the category document
        category_doc = {
            "name": category_name_upper,
            "description": category.description,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }

        # Insert into the database
        result = await categories_collection.insert_one(category_doc)
        # Add the inserted ID to the document
        category_doc["_id"] = result.inserted_id
        return {"message": "Category added successfully", "category": serialize_mongo_document(category_doc)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/categories/{category_id}")
async def edit_category(category_id: str, category: Category, get_current_user: dict = Depends(get_current_user)):
    """
    Edit an existing category. Ensures the name is stored in uppercase.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        if not ObjectId.is_valid(category_id):
            raise HTTPException(status_code=400, detail="Invalid category ID")

        # Convert the category name to uppercase
        category_name_upper = category.name.upper()

        # Update the category document
        result = await categories_collection.update_one(
            {"_id": ObjectId(category_id)},
            {"$set": {
                "name": category_name_upper,
                "description": category.description,
                "updated_at": datetime.now(timezone.utc)
            }}
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Category not found")

        # Fetch the updated category
        updated_category = await categories_collection.find_one({"_id": ObjectId(category_id)})
        return {"message": "Category updated successfully", "category": serialize_mongo_document(updated_category)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/categories/{category_id}")
async def delete_category(category_id: str, get_current_user: dict = Depends(get_current_user)):
    """
    Delete a category by its ID.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        if not ObjectId.is_valid(category_id):
            raise HTTPException(status_code=400, detail="Invalid category ID")

        # Delete the category
        result = await categories_collection.delete_one({"_id": ObjectId(category_id)})

        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Category not found")

        return {"message": "Category deleted successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories")
async def get_all_categories(get_current_user: dict = Depends(get_current_user)):
    """
    Fetch all categories from the database.
    """
    try:
        if not get_current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Retrieve all categories from the database
        categories = await categories_collection.find().to_list(length=100)

        # Serialize the categories
        serialized_categories = [
            serialize_mongo_document(category) for category in categories
        ]

        return {"categories": serialized_categories}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
