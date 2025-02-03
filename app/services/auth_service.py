from passlib.context import CryptContext
from app.config import ACCESS_TOKEN_EXPIRE_MINUTES
from app.database import users_collection
from app.utils.jwt import create_access_token
from datetime import datetime, timedelta, timezone

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def authenticate_user(email: str, password: str):
    """Authenticate user by email and password."""
    user = await users_collection.find_one({"email": email})
    if user and pwd_context.verify(password, user["hashed_password"]):
        return user
    return None


def hash_password(password: str):
    """Hash a plain password."""
    return pwd_context.hash(password)


async def create_user(username: str, email: str, password: str):
    """Create a new user."""
    hashed_password = hash_password(password)
    user = {
        "username": username,
        "email": email,
        "hashed_password": hashed_password,
        "created_at": str(datetime.now(timezone.utc)),
    }
    await users_collection.insert_one(user)
    return user


def create_user_token(user: dict):
    """Create a JWT token for a user."""
    access_token = create_access_token(
        data={"sub": user["email"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return access_token
