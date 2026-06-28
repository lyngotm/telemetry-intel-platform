"""
Integration tests for the Redis cache-aside pattern on GET /api/v1/telemetry.
Requires: Docker infrastructure + API Gateway running.
"""

import hashlib
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest
import redis.asyncio as aioredis

pytestmark = pytest.mark.integration

API_BASE_URL = "http://localhost:8000"
REDIS_URL = "redis://localhost:6379/0"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"


@pytest.fixture
async def api_client():
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0) as client:
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "operator", "password": "operator123"},
        )
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
async def db_connection():
    conn = await asyncpg.connect(POSTGRES_DSN)
    yield conn
    await conn.close()


def _compute_cache_key(params: dict) -> str:
    """Reproduce the same cache key logic from the API Gateway."""
    device_id = params.get("device_id")
    metric_type = params.get("metric_type")
    start = params.get("start")
    end = params.get("end")
    limit = params.get("limit", 100)
    offset = params.get("offset", 0)

    if start and isinstance(start, str):
        start = datetime.fromisoformat(start)
    if end and isinstance(end, str):
        end = datetime.fromisoformat(end)

    raw = f"{device_id}:{metric_type}:{start}:{end}:{limit}:{offset}"
    query_hash = hashlib.md5(raw.encode()).hexdigest()
    return f"cache:telemetry:{query_hash}"


class TestCacheAside:
    """Tests for the Redis cache-aside pattern on the telemetry read endpoint."""

    async def test_first_request_is_cache_miss(
        self, api_client: httpx.AsyncClient, redis_client: aioredis.Redis
    ):
        """First request for a query should miss the cache and populate it."""
        params = {
            "metric_type": "temperature",
            "limit": 5,
            "start": datetime.now(timezone.utc).isoformat(),
        }
        expected_key = _compute_cache_key(params)

        # Verify key does NOT exist before the request
        exists_before = await redis_client.exists(expected_key)
        assert exists_before == 0, f"Cache key {expected_key} already exists before request"

        # Make the request
        response = await api_client.get("/api/v1/telemetry", params=params)
        assert response.status_code == 200

        # Verify key NOW exists
        exists_after = await redis_client.exists(expected_key)
        assert exists_after == 1, "Cache key was not created after request"

    async def test_second_request_is_cache_hit(
        self,
        api_client: httpx.AsyncClient,
        redis_client: aioredis.Redis,
        db_connection: asyncpg.Connection,
    ):
        """Prove the second request is served from cache by deleting the DB data between requests."""
        params = {
            "metric_type": "pressure",
            "limit": 3,
            "start": datetime.now(timezone.utc).isoformat(),
        }

        # First request — populates cache
        response1 = await api_client.get("/api/v1/telemetry", params=params)
        assert response1.status_code == 200

        # Delete ALL pressure events from PostgreSQL
        await db_connection.execute("DELETE FROM telemetry_events WHERE metric_type = 'pressure'")

        # Second request — if it returns the same data, it MUST be from cache
        response2 = await api_client.get("/api/v1/telemetry", params=params)
        assert response2.status_code == 200
        assert response1.json() == response2.json()

    async def test_cache_expires_after_ttl(
        self, api_client: httpx.AsyncClient, redis_client: aioredis.Redis
    ):
        """Cache keys should have a TTL of 60 seconds."""
        params = {
            "metric_type": "humidity",
            "limit": 2,
            "start": datetime.now(timezone.utc).isoformat(),
        }
        expected_key = _compute_cache_key(params)

        await api_client.get("/api/v1/telemetry", params=params)

        ttl = await redis_client.ttl(expected_key)
        assert 0 < ttl <= 60, f"Expected TTL between 1-60 seconds, got {ttl}"

    async def test_different_params_different_cache_keys(
        self, api_client: httpx.AsyncClient, redis_client: aioredis.Redis
    ):
        """Different query parameters should produce different cache keys."""
        now = datetime.now(timezone.utc).isoformat()
        params_a = {"metric_type": "temperature", "limit": 5, "start": now}
        params_b = {"metric_type": "humidity", "limit": 5, "start": now}

        key_a = _compute_cache_key(params_a)
        key_b = _compute_cache_key(params_b)

        await api_client.get("/api/v1/telemetry", params=params_a)
        await api_client.get("/api/v1/telemetry", params=params_b)

        assert await redis_client.exists(key_a) == 1
        assert await redis_client.exists(key_b) == 1
        assert key_a != key_b
