from fastapi import APIRouter, HTTPException, Depends
from app.schemas.user_schema import UserCreate, UserLogin, UserResponse
from app.services.auth_service import create_user, authenticate_user, create_user_token
from app.middlewares.auth_middleware import get_current_user
from app.utils.serialize_mongo import serialize_mongo_document

router = APIRouter()


@router.post("/register_account", status_code=201)
async def register_user(user: UserCreate):
    """Register a new user."""
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
    """Fetch the current user."""
    if not get_current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {"user": serialize_mongo_document(get_current_user)}
