"""
Rate limiting using Redis sliding window counters.

Uses Redis sorted sets (same pattern as anomaly detection rolling windows)
to track request timestamps per user per endpoint. Each request adds a
timestamp; old entries outside the window are trimmed; the count determines
if the limit is exceeded.

Key pattern: ratelimit:{user_id}:{endpoint_name}

This approach is more accurate than fixed-window counters because it doesn't
have the "boundary spike" problem — a user can't double their limit by
timing requests across a window boundary.
"""

import logging
import time
from uuid import uuid4

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request, status

from services.api_gateway.app.config import settings
from services.api_gateway.app.auth import get_current_user
from services.api_gateway.app.dependencies import get_redis_client
from shared.models.models import UserPayload

logger = logging.getLogger("api_gateway")

# Window size for rate limiting (60 seconds = per-minute limits)
WINDOW_SIZE_SECONDS: int = 60


async def _check_rate_limit(
    redis_client: aioredis.Redis,
    user_id: str,
    endpoint_name: str,
    max_requests: int,
) -> None:
    """
    Check and record a request against the sliding window rate limit.

    Args:
        redis_client: Redis connection
        user_id: Identifier for the user (from JWT 'sub' claim)
        endpoint_name: Logical name for the endpoint group (e.g., "ingestion", "query")
        max_requests: Maximum allowed requests within the window

    Raises:
        HTTPException(429) if the rate limit is exceeded.
    """
    key = f"ratelimit:{user_id}:{endpoint_name}"
    now = time.time()
    window_start = now - WINDOW_SIZE_SECONDS

    # Pipeline: trim old entries, add new entry, count, set expiry — one round-trip
    pipe = redis_client.pipeline(transaction=True)
    pipe.zremrangebyscore(key, "-inf", window_start)
    # Use timestamp + random suffix as member to guarantee uniqueness
    # (same user could hit endpoint multiple times in the same millisecond)
    member = f"{now}:{uuid4().hex[:8]}"
    pipe.zadd(key, {member: now})
    pipe.zcard(key)
    pipe.expire(key, WINDOW_SIZE_SECONDS * 2)  # Auto-cleanup for inactive users
    pipe.zrange(key, 0, 0, withscores=True)  # Get oldest entry's timestamp

    results = await pipe.execute()
    request_count = results[2]  # ZCARD result is the third command
    oldest_entry = results[4]  # List of (member, score) tuples

    if request_count > max_requests:
        if oldest_entry:
            oldest_timestamp = oldest_entry[0][1]
            # Time until the oldest request falls outside the window
            retry_after = int((oldest_timestamp + WINDOW_SIZE_SECONDS) - now)
            retry_after = max(retry_after, 1)  # At least 1 second
        else:
            retry_after = WINDOW_SIZE_SECONDS
        logger.warning(
            f"Rate limit exceeded: user={user_id}, endpoint={endpoint_name}, "
            f"count={request_count}/{max_requests}"
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Max {max_requests} requests per minute.",
            headers={"Retry-After": str(retry_after)},
        )


def require_rate_limit(endpoint_name: str, max_requests: int | None = None):
    """
    FastAPI dependency factory for rate limiting.

    Args:
        endpoint_name: Logical name for the rate limit bucket (e.g., "ingestion", "query").
        max_requests: Override the default limit. If None, uses the config-based
                      defaults per endpoint name.

    Returns:
        A dependency function that checks the rate limit before allowing the request.
    """

    async def _rate_limit_checker(
        request: Request,
        current_user: UserPayload = Depends(get_current_user),
        redis_client: aioredis.Redis = Depends(get_redis_client),
    ) -> None:
        # Determine the limit for this endpoint
        if max_requests is not None:
            limit = max_requests
        elif endpoint_name == "ingestion":
            limit = settings.rate_limit_ingestion
        elif endpoint_name == "query":
            limit = settings.rate_limit_query
        else:
            limit = settings.rate_limit_query  # Default to query limit

        try:
            await _check_rate_limit(redis_client, current_user.username, endpoint_name, limit)
        except HTTPException:
            raise  # Re-raise 429s
        except Exception as e:
            # Redis failure — fail open (allow request, log warning)
            logger.warning(f"Rate limiter Redis error (failing open): {e}")

    return _rate_limit_checker
