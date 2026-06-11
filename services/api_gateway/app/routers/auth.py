"""
Authentication endpoint.

Issues signed JWT tokens in exchange for valid credentials.
This is the only unprotected endpoint (besides /health) — users
call this first to obtain a token, then include it in subsequent requests.
"""

import logging

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from services.api_gateway.app.auth import USERS_DB, create_access_token
from shared.models.models import TokenResponse

logger = logging.getLogger("api_gateway")

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class TokenRequest(BaseModel):
    """Credentials submitted to obtain a JWT."""

    username: str = Field(..., min_length=5, examples=["operator"])
    password: str = Field(..., min_length=8, examples=["operator123"])


@router.post(
    "/token",
    response_model=TokenResponse,
    summary="Obtain an access token",
    description="Authenticate with username/password and receive a signed JWT. "
    "Include the token in subsequent requests as: Authorization: Bearer <token>",
)
async def login(credentials: TokenRequest):
    """
    Validate credentials and issue a signed JWT.

    Available test accounts:
    - admin / admin123 (full access)
    - operator / operator123 (read + write telemetry)
    - viewer / viewer123 (read-only)
    """
    user = USERS_DB.get(credentials.username)

    if user is None or user["password"] != credentials.password:
        logger.warning(f"Failed login attempt for username: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token, expires_in = create_access_token(credentials.username, user["role"])

    logger.info(f"Token issued for user: {credentials.username} (role: {user['role']})")

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        role=user["role"],
    )
