"""
JWT authentication module for the API Gateway.

Handles token creation (RS256 signing) and validation.
Provides `get_current_user` as a FastAPI dependency for protected endpoints.
"""

from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from services.api_gateway.app.config import settings
from shared.models.models import UserPayload

# --- Load RSA keys at module level (once on import) ---

def _load_key(path: str) -> str:
    """Read a PEM key file from disk."""
    with open(path, "r") as f:
        return f.read()


_private_key: str = _load_key(settings.jwt_private_key_path)
_public_key: str = _load_key(settings.jwt_public_key_path)

# FastAPI's HTTPBearer extracts the token from the "Authorization: Bearer <token>" header.
# It returns 403 automatically if the header is missing — we override with 401 below.
_bearer_scheme = HTTPBearer(auto_error=False)


# --- Hardcoded users (this project doesn't have a user database) ---
# In production, this would be a database lookup or external identity provider.

USERS_DB: dict[str, dict[str, str]] = {
    "admin": {"password": "admin123", "role": "admin"},
    "operator": {"password": "operator123", "role": "operator"},
    "viewer": {"password": "viewer123", "role": "viewer"},
}


def create_access_token(username: str, role: str) -> tuple[str, int]:
    """
    Create a signed JWT with the given username and role.

    Returns:
        Tuple of (encoded_token, expires_in_seconds)
    """
    expires_in = settings.jwt_expiry_minutes * 60  # convert to seconds
    expire_at = datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expiry_minutes)

    payload = {
        "sub": username,
        "role": role,
        "exp": expire_at,
        "iat": datetime.now(timezone.utc),
    }

    token = jwt.encode(payload, _private_key, algorithm=settings.jwt_algorithm)
    return token, expires_in


def decode_token(token: str) -> UserPayload:
    """
    Decode and validate a JWT token.

    Raises:
        HTTPException(401) if the token is invalid, expired, or malformed.
    """
    try:
        payload = jwt.decode(token, _public_key, algorithms=[settings.jwt_algorithm])
        return UserPayload(
            username=payload["sub"],
            role=payload["role"],
            exp=datetime.fromtimestamp(payload["exp"], tz=timezone.utc),
        )
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired token: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token missing required claims: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> UserPayload:
    """
    FastAPI dependency that extracts and validates the JWT from the request.

    Usage in route handlers:
        current_user: UserPayload = Depends(get_current_user)

    Returns the decoded user payload (username + role).
    Raises 401 if no token or invalid token.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return decode_token(credentials.credentials)

