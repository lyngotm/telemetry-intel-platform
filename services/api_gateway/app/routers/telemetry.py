"""
Telemetry ingestion and query endpoints.
POST publishes to Kafka (async processing).
GET queries PostgreSQL directly (no cache until Redis added).
"""

import hashlib
import json
import logging
from datetime import datetime
from uuid import UUID

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Depends, HTTPException, status

from services.api_gateway.app.config import settings
from services.api_gateway.app.dependencies import (
    get_db_connection,
    get_kafka_producer,
    get_redis_client,
)
from shared.models.models import (
    APIListResponse,
    APIResponse,
    TelemetryEventCreate,
    TelemetryEventResponse,
)

logger = logging.getLogger("api_gateway")

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


@router.post(
    "",
    response_model=APIResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_telemetry(
    event: TelemetryEventCreate,
    conn: asyncpg.Connection = Depends(get_db_connection),
    producer: AIOKafkaProducer = Depends(get_kafka_producer),
):
    """
    Ingest a telemetry event.

    The event is validated, then published to Kafka for async processing.
    Returns 202 Accepted — the event will be processed by the Ingestion Consumer.

    The device_id must reference a registered device.
    """
    # Verify the device exists before accepting the event
    device = await conn.fetchrow(
        "SELECT device_id FROM devices WHERE device_id = $1",
        event.device_id,
    )
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {event.device_id} not found. Register it first via POST /api/v1/devices.",
        )

    # Serialize the event to JSON and publish to Kafka
    message_value = event.model_dump_json()

    await producer.send_and_wait(
        topic=settings.kafka_topic_raw,
        value=message_value,
        # Use device_id as the partition key — ensures all events from one device
        # go to the same partition, maintaining per-device ordering.
        key=str(event.device_id).encode("utf-8"),
    )

    logger.info(
        f"Published event to {settings.kafka_topic_raw}: "
        f"device={event.device_id}, metric={event.metric_type}, value={event.value}"
    )

    return APIResponse(
        success=True,
        message="Event accepted for processing",
    )


@router.get(
    "",
    response_model=APIListResponse,
)
async def query_telemetry(
    device_id: UUID | None = None,
    metric_type: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    conn: asyncpg.Connection = Depends(get_db_connection),
    redis_client: aioredis.Redis = Depends(get_redis_client),
):
    """
    Query historical telemetry events with optional filters.
    Results are ordered by timestamp descending (most recent first).

    Filters:
    - device_id: filter to a specific device
    - metric_type: filter to a specific metric (temperature, humidity, etc.)
    - start: only events at or after this time (ISO 8601)
    - end: only events before this time (ISO 8601)
    - limit: max results (default 100, max 1000)
    - offset: pagination offset
    """
    limit = min(limit, 1000)

    # --- Cache-aside: check Redis first ---
    cache_key = _build_cache_key(device_id, metric_type, start, end, limit, offset)

    try:
        cached = await redis_client.get(cache_key)
        if cached:
            logger.debug(f"Cache HIT: {cache_key}")
            return APIListResponse.model_validate_json(cached)
    except Exception as e:
        # Redis unavailable — fall through to PostgreSQL
        logger.warning(f"Redis cache read failed: {e}")

    logger.debug(f"Cache MISS: {cache_key}")

    # --- Cache miss: query PostgreSQL ---
    conditions = []
    params = []
    param_idx = 1

    if device_id:
        conditions.append(f"device_id = ${param_idx}")
        params.append(device_id)
        param_idx += 1

    if metric_type:
        conditions.append(f"metric_type = ${param_idx}")
        params.append(metric_type)
        param_idx += 1

    if start:
        conditions.append(f"timestamp >= ${param_idx}")
        params.append(start)
        param_idx += 1

    if end:
        conditions.append(f"timestamp < ${param_idx}")
        params.append(end)
        param_idx += 1

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT event_id, device_id, metric_type, value, timestamp, metadata, ingested_at
        FROM telemetry_events
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """
    params.extend([limit, offset])

    rows = await conn.fetch(query, *params)

    count_query = f"SELECT COUNT(*) FROM telemetry_events {where_clause}"
    total = await conn.fetchval(count_query, *params[:-2])

    events = [
        TelemetryEventResponse(
            event_id=row["event_id"],
            device_id=row["device_id"],
            metric_type=row["metric_type"],
            value=row["value"],
            timestamp=row["timestamp"],
            metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"],
            ingested_at=row["ingested_at"],
        )
        for row in rows
    ]

    response = APIListResponse(
        success=True,
        data=[e.model_dump(mode="json") for e in events],
        count=total,
    )

    # --- Populate cache (best effort — don't fail the request if Redis is down) ---
    try:
        await redis_client.set(cache_key, response.model_dump_json(), ex=60)
        logger.debug(f"Cached response: {cache_key} (TTL: 60s)")
    except Exception as e:
        logger.warning(f"Redis cache write failed: {e}")

    return response


def _build_cache_key(
    device_id: UUID | None,
    metric_type: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    offset: int,
) -> str:
    """
    Build a deterministic cache key from query parameters.
    Uses an MD5 hash of the sorted parameters to keep the key short
    while still being unique per query combination.
    """
    raw = f"{device_id}:{metric_type}:{start}:{end}:{limit}:{offset}"
    query_hash = hashlib.md5(raw.encode()).hexdigest()
    return f"cache:telemetry:{query_hash}"


