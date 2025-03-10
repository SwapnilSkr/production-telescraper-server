from fastapi import APIRouter, HTTPException, Depends
from app.schemas.user_schema import UserCreate, UserLogin, UserResponse
from app.services.auth_service import create_user, authenticate_user, create_user_token
from app.middlewares.auth_middleware import get_current_user
from app.utils.serialize_mongo import serialize_mongo_document
from datetime import datetime, timezone


router = APIRouter()


@router.post("/register_account", status_code=201)
async def register_user(user: UserCreate):
    """Register a new user."""
    # Email validation is now handled by the UserCreate schema
    
    existing_user = await authenticate_user(user.email, user.password)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    new_user = await create_user(user.username, user.email, user.password)
    return {"message": "User created successfully"}


@router.post("/authorize_account")
async def login_user(user: UserLogin):
    """Login a user and return a JWT token."""
    authenticated_user = await authenticate_user(user.email, user.password)
    if not authenticated_user:
        raise HTTPException(
            status_code=401, detail="Invalid email or password")
    token = create_user_token(authenticated_user)
    return {"access_token": token, "user": serialize_mongo_document(authenticated_user)}


@router.get("/user")
async def get_user(get_current_user: dict = Depends(get_current_user)):
    """Fetch the current user along with remaining time for token expiration (in minutes)."""
    if not get_current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Extract expiration timestamp (Unix timestamp)
    exp_timestamp = get_current_user["exp"]

    if exp_timestamp:
        # Convert expiration timestamp to UTC datetime
        exp_datetime = datetime.fromtimestamp(exp_timestamp, tz=timezone.utc)

        # Calculate remaining time in minutes
        now = datetime.now(timezone.utc)
        remaining_time = (exp_datetime - now).total_seconds() / 60

        # Ensure it doesn't return negative values
        remaining_time = max(0, round(remaining_time))
    else:
        exp_datetime = None
        remaining_time = None

    return {
        "user": serialize_mongo_document(get_current_user),
        "token_expiration": remaining_time  # Time left in minutes
    }
