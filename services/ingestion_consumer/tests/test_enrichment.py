"""
Unit tests for device metadata enrichment logic.
Tests cache hit, cache miss (PostgreSQL fallback), and Redis failure scenarios.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from services.ingestion_consumer.app.enrichment import DeviceEnrichment, DEVICE_CACHE_TTL_SECONDS


def _make_mock_pool(mock_conn):
    """
    Create a mock asyncpg pool where `async with pool.acquire() as conn`
    yields mock_conn. asyncpg's acquire() returns an async context manager
    directly (not a coroutine), so we replicate that with @asynccontextmanager.
    """
    pool = MagicMock()

    @asynccontextmanager
    async def mock_acquire():
        yield mock_conn

    pool.acquire = mock_acquire
    return pool


@pytest.fixture
def mock_redis():
    """Mock Redis async client."""
    return AsyncMock()


class TestDeviceEnrichment:
    """Tests for the DeviceEnrichment class."""

    async def test_cache_hit_returns_redis_data(self, mock_redis):
        """When Redis has the device metadata, return it without querying PostgreSQL."""
        device_id = uuid4()
        mock_db_pool = MagicMock()  # Should not be called

        enrichment = DeviceEnrichment(redis_client=mock_redis, db_pool=mock_db_pool)

        # Redis returns cached data
        mock_redis.hgetall.return_value = {
            "device_type": "temperature_sensor",
            "location": "Building A",
            "firmware_version": "v2.1.0",
        }

        result = await enrichment.get_device_metadata(device_id)

        assert result == {
            "device_type": "temperature_sensor",
            "location": "Building A",
            "firmware_version": "v2.1.0",
        }
        mock_redis.hgetall.assert_called_once_with(f"device:{device_id}")
        # PostgreSQL was NOT called (cache hit = no DB query)
        mock_db_pool.acquire.assert_not_called()

    async def test_cache_miss_queries_postgres_and_populates_cache(self, mock_redis):
        """When Redis returns empty, query PostgreSQL and populate the cache."""
        device_id = uuid4()

        # Redis returns empty (cache miss)
        mock_redis.hgetall.return_value = {}

        # PostgreSQL returns device data
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "device_type": "environmental_monitor",
            "location": "Zone-B",
            "firmware_version": "v1.0.0",
        }
        mock_db_pool = _make_mock_pool(mock_conn)

        enrichment = DeviceEnrichment(redis_client=mock_redis, db_pool=mock_db_pool)
        result = await enrichment.get_device_metadata(device_id)

        assert result == {
            "device_type": "environmental_monitor",
            "location": "Zone-B",
            "firmware_version": "v1.0.0",
        }
        # Redis cache was populated
        mock_redis.hset.assert_called_once_with(
            f"device:{device_id}",
            mapping={
                "device_type": "environmental_monitor",
                "location": "Zone-B",
                "firmware_version": "v1.0.0",
            },
        )
        # TTL was set
        mock_redis.expire.assert_called_once_with(f"device:{device_id}", DEVICE_CACHE_TTL_SECONDS)

    async def test_device_not_found_returns_none(self, mock_redis):
        """When device doesn't exist in Redis or PostgreSQL, return None."""
        device_id = uuid4()

        # Redis returns empty
        mock_redis.hgetall.return_value = {}

        # PostgreSQL returns None (device not registered)
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        mock_db_pool = _make_mock_pool(mock_conn)

        enrichment = DeviceEnrichment(redis_client=mock_redis, db_pool=mock_db_pool)
        result = await enrichment.get_device_metadata(device_id)

        assert result is None
        # Should NOT attempt to cache a None result
        mock_redis.hset.assert_not_called()

    async def test_redis_failure_falls_through_to_postgres(self, mock_redis):
        """When Redis is unreachable, fall through to PostgreSQL without crashing."""
        device_id = uuid4()

        # Redis raises an exception (simulating Redis being down)
        mock_redis.hgetall.side_effect = ConnectionError("Redis connection refused")

        # PostgreSQL returns device data
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "device_type": "server_node",
            "location": "Datacenter 1",
            "firmware_version": "v3.0.0",
        }
        mock_db_pool = _make_mock_pool(mock_conn)

        enrichment = DeviceEnrichment(redis_client=mock_redis, db_pool=mock_db_pool)
        result = await enrichment.get_device_metadata(device_id)

        # Should still return valid data from PostgreSQL
        assert result == {
            "device_type": "server_node",
            "location": "Datacenter 1",
            "firmware_version": "v3.0.0",
        }

    async def test_redis_failure_on_cache_write_still_returns_data(self, mock_redis):
        """When Redis fails during cache population, still return the data (best effort caching)."""
        device_id = uuid4()

        # Redis cache miss
        mock_redis.hgetall.return_value = {}
        # Redis fails when trying to write cache
        mock_redis.hset.side_effect = ConnectionError("Redis connection lost")

        # PostgreSQL returns device data
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "device_type": "edge_gateway",
            "location": "Remote Site",
            "firmware_version": "v1.5.0",
        }
        mock_db_pool = _make_mock_pool(mock_conn)

        enrichment = DeviceEnrichment(redis_client=mock_redis, db_pool=mock_db_pool)
        result = await enrichment.get_device_metadata(device_id)

        # Data should still be returned despite cache write failure
        assert result == {
            "device_type": "edge_gateway",
            "location": "Remote Site",
            "firmware_version": "v1.5.0",
        }
