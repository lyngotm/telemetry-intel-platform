"""
Message processing logic for the Ingestion Consumer.

For each message from telemetry.raw:
1. Parse and validate the JSON payload against the Pydantic schema
2. Write the validated event to PostgreSQL (telemetry_events table)
3. Publish the enriched event to telemetry.enriched
4. On any validation failure, route to telemetry.dlq
"""

import json
import logging
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaProducer
from pydantic import ValidationError

from services.ingestion_consumer.app.config import settings
from services.ingestion_consumer.app.enrichment import DeviceEnrichment
from shared.metrics import EVENTS_DLQ, EVENTS_INGESTED
from shared.models.models import TelemetryEnrichedMessage, TelemetryRawMessage

logger = logging.getLogger("ingestion_consumer")


async def process_message(
    message,
    db_pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    enrichment: DeviceEnrichment,
) -> None:
    """
    Process a single message from the telemetry.raw topic.
    Validates, persists to PostgreSQL, and publishes to the enriched topic.
    On failure, routes to the dead-letter queue.
    """
    raw_value = message.value  # Already decoded to str by the consumer's deserializer

    # --- Step 1: Parse and validate ---
    try:
        event = TelemetryRawMessage.model_validate_json(raw_value)
    except (ValidationError, json.JSONDecodeError) as e:
        # Invalid message — send to DLQ
        await send_to_dlq(
            producer=producer,
            original_payload=raw_value,
            error_reason=f"Schema validation failed: {str(e)}",
        )
        EVENTS_DLQ.labels(reason="validation_failed").inc()
        return

    # --- Step 2: Enrich with device metadata ---
    device_metadata = await enrichment.get_device_metadata(event.device_id)

    if device_metadata is None:
        # Device not found in PostgreSQL — route to DLQ
        await send_to_dlq(
            producer=producer,
            original_payload=raw_value,
            error_reason=f"Device {event.device_id} not found during enrichment",
        )
        EVENTS_DLQ.labels(reason="device_not_found").inc()
        return

    # --- Step 3: Write to PostgreSQL ---
    event_id = uuid4()

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO telemetry_events (event_id, device_id, metric_type, value, timestamp, metadata)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                event_id,
                event.device_id,
                event.metric_type,
                event.value,
                event.timestamp,
                json.dumps(event.metadata),
            )
    except asyncpg.ForeignKeyViolationError:
        # device_id doesn't exist in the devices table
        await send_to_dlq(
            producer=producer,
            original_payload=raw_value,
            error_reason=f"Foreign key violation: device {event.device_id} not registered",
        )
        EVENTS_DLQ.labels(reason="fk_violation").inc()
        return
    except Exception as e:
        # Unexpected DB error — log and send to DLQ rather than crashing
        logger.error(f"Database write failed: {e}", exc_info=True)
        await send_to_dlq(
            producer=producer,
            original_payload=raw_value,
            error_reason=f"Database error: {str(e)}",
        )
        EVENTS_DLQ.labels(reason="db_error").inc()
        return

    # --- Step 4: Publish to telemetry.enriched ---
    enriched_event = TelemetryEnrichedMessage(
        event_id=event_id,
        device_id=event.device_id,
        metric_type=event.metric_type,
        value=event.value,
        timestamp=event.timestamp,
        metadata=event.metadata,
        device_type=device_metadata.get("device_type") or None,
        device_location=device_metadata.get("location") or None,
        firmware_version=device_metadata.get("firmware_version") or None,
    )

    await producer.send_and_wait(
        topic=settings.kafka_topic_enriched,
        value=enriched_event.model_dump_json(),
        key=str(event.device_id).encode("utf-8"),
    )
    EVENTS_INGESTED.inc()

    logger.info(
        f"Processed event: id={event_id}, device={event.device_id}, "
        f"metric={event.metric_type}, value={event.value}, "
        f"device_type={device_metadata.get('device_type')}"
    )


async def send_to_dlq(
    producer: AIOKafkaProducer,
    original_payload: str,
    error_reason: str,
) -> None:
    """
    Send a failed message to the dead-letter queue topic.
    Includes the original payload and the reason for failure.
    """
    dlq_message = json.dumps({
        "original_payload": original_payload,
        "error_reason": error_reason,
        "original_topic": settings.kafka_topic_raw,
    })

    await producer.send_and_wait(
        topic=settings.kafka_topic_dlq,
        value=dlq_message,
    )

    logger.warning(f"Sent to DLQ: {error_reason}")
    