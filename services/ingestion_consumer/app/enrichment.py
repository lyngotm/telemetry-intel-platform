"""
Device metadata enrichment via Redis cache.

Pattern: cache-aside (look-aside)
1. Check Redis hash for device metadata
2. On cache hit — return cached data
3. On cache miss — query PostgreSQL, populate Redis hash, return data
4. On Redis failure — query PostgreSQL directly, log warning

Redis key format: device:{device_id}
Redis data structure: Hash (HSET/HGETALL)
"""

import json
import logging
from uuid import UUID

import asyncpg
import redis.asyncio as aioredis

logger = logging.getLogger("ingestion_consumer")

# TTL for cached device metadata (1 hour).
# If device firmware is updated, stale cache expires within this window.
DEVICE_CACHE_TTL_SECONDS = 3600


class DeviceEnrichment:
    """Handles device metadata lookup with Redis caching."""

    def __init__(self, redis_client: aioredis.Redis, db_pool: asyncpg.Pool):
        self.redis = redis_client
        self.db_pool = db_pool

    async def get_device_metadata(self, device_id: UUID) -> dict | None:
        """
        Retrieve device metadata, checking Redis first with PostgreSQL fallback.
        Returns a dict with device_type, location, firmware_version, or None if device not found.
        """
        cache_key = f"device:{device_id}"

        # --- Step 1: Try Redis cache ---
        try:
            cached = await self.redis.hgetall(cache_key)
            if cached:
                logger.debug(f"Cache HIT for {cache_key}")
                return self._decode_hash(cached)
        except Exception as e:
            # Redis is unreachable — fall through to PostgreSQL
            logger.warning(f"Redis unavailable during lookup for {cache_key}: {e}")

        # --- Step 2: Cache miss or Redis failure — query PostgreSQL ---
        logger.debug(f"Cache MISS for {cache_key}, querying PostgreSQL")
        metadata = await self._fetch_from_postgres(device_id)

        if metadata is None:
            # Device doesn't exist in the database
            return None

        # --- Step 3: Populate Redis cache (best effort — don't fail if Redis is down) ---
        try:
            await self.redis.hset(cache_key, mapping=metadata)
            await self.redis.expire(cache_key, DEVICE_CACHE_TTL_SECONDS)
            logger.debug(f"Cached device metadata for {cache_key} (TTL: {DEVICE_CACHE_TTL_SECONDS}s)")
        except Exception as e:
            logger.warning(f"Failed to cache device metadata for {cache_key}: {e}")

        return metadata

    async def _fetch_from_postgres(self, device_id: UUID) -> dict | None:
        """Query PostgreSQL for device metadata."""
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT device_type, location, firmware_version
                FROM devices
                WHERE device_id = $1
                """,
                device_id,
            )

        if row is None:
            return None

        return {
            "device_type": row["device_type"] or "",
            "location": row["location"] or "",
            "firmware_version": row["firmware_version"] or "",
        }

    @staticmethod
    def _decode_hash(cached: dict) -> dict:
        """
        Decode Redis hash response.
        redis-py returns bytes keys/values by default unless decode_responses=True.
        We use decode_responses=True in our client, so values are already strings.
        """
        return {
            "device_type": cached.get("device_type", ""),
            "location": cached.get("location", ""),
            "firmware_version": cached.get("firmware_version", ""),
        }
    
    