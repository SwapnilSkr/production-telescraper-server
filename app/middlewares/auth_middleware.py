from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.utils.jwt import decode_access_token

security = HTTPBearer()


async def get_current_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Authenticate the current user via bearer token."""
    token = credentials.credentials
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload


async def isAuthenticated(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Check if the user is authenticated."""
    token = credentials.credentials
    payload = decode_access_token(token)
    if not payload:
        return False
    return True
