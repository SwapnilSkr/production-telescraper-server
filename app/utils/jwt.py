from app.config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from datetime import datetime, timedelta, timezone
from app.database import users_collection
from jose import jwt, JWTError

async def get_user_by_email(email: str):
    """Get a user by email."""
    return await users_collection.find_one({"email": email})


def create_access_token(data: dict, expires_delta: timedelta = None):
    """Create a JWT token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + \
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def decode_access_token(token: str):
    """Decode and verify a JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username_from_payload_email = await get_user_by_email(payload["sub"])
        return {
            "sub": payload["sub"],
            "exp": payload["exp"],
            "username": username_from_payload_email["username"],
            "user_id": username_from_payload_email["_id"],
        }
    except JWTError:
        return None
