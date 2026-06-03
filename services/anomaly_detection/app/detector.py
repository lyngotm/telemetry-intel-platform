"""
Core anomaly detection logic.
Maintains rolling windows in Redis and computes z-scores to identify anomalies.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

from services.anomaly_detection.app.config import settings
from services.anomaly_detection.app.rolling_window import RollingWindow, WindowStats
from shared.models.models import TelemetryEnrichedMessage, AnomalyDetectedMessage

logger = logging.getLogger("anomaly_detection")


def compute_z_score(value: float, stats: WindowStats) -> float:
    """
    Compute the z-score of a value given window statistics.
    Returns 0.0 if stddev is zero (all values identical — no deviation possible).
    """
    if stats.stddev == 0.0:
        return 0.0
    return (value - stats.mean) / stats.stddev


def classify_severity(z_score: float) -> str | None:
    """
    Map absolute z-score to severity level.
    Returns None if z-score is below the lowest threshold (not anomalous).
    """
    abs_z = abs(z_score)
    if abs_z >= settings.z_score_threshold_critical:
        return "critical"
    elif abs_z >= settings.z_score_threshold_high:
        return "high"
    elif abs_z >= settings.z_score_threshold_medium:
        return "medium"
    elif abs_z >= settings.z_score_threshold_low:
        return "low"
    return None


async def process_event(
    message,
    redis_client: aioredis.Redis,
    db_pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
) -> None:
    """
    Process a single enriched telemetry event through the anomaly detection pipeline.

    1. Parse the enriched event
    2. Add value to rolling window, get current stats
    3. Compute z-score
    4. If anomalous: write to PostgreSQL + publish to anomalies.detected
    """
    # --- Step 1: Parse enriched event ---
    try:
        event = TelemetryEnrichedMessage.model_validate_json(message.value)
    except Exception as e:
        logger.warning(f"Failed to parse enriched event: {e}")
        return

    # Convert event timestamp to Unix seconds for the rolling window
    event_timestamp = event.timestamp.timestamp()

    # --- Step 2: Update rolling window and get stats ---
    window = RollingWindow(redis_client=redis_client)

    try:
        stats = await window.add_and_get_stats(
            device_id=str(event.device_id),
            metric_type=event.metric_type,
            value=event.value,
            timestamp=event_timestamp,
        )
    except Exception as e:
        # Redis failure — log and skip this event. We don't want to halt
        # the consumer; the next event will retry window access.
        logger.error(f"Rolling window update failed: {e}")
        return

    # Not enough samples yet — skip analysis
    if stats is None:
        return

    # --- Step 3: Compute z-score and classify ---
    z_score = compute_z_score(event.value, stats)
    severity = classify_severity(z_score)

    logger.info(
    f"Z-SCORE CHECK: device={event.device_id}, metric={event.metric_type}, "
    f"value={event.value}, z_score={z_score:.2f}, "
    f"mean={stats.mean:.2f}, stddev={stats.stddev:.2f}, count={stats.count}"
    )
    if severity is None:
        # Value is within normal range — no anomaly
        logger.debug(
            f"Normal: device={event.device_id}, metric={event.metric_type}, "
            f"value={event.value}, z_score={z_score:.2f}"
        )
        return

    # --- Step 4: Anomaly detected! ---
    anomaly_id = uuid4()
    detected_at = datetime.now(timezone.utc)

    # Expected range: mean ± (threshold * stddev) for context
    expected_range = {
        "mean": round(stats.mean, 4),
        "stddev": round(stats.stddev, 4),
        "lower": round(stats.mean - (settings.z_score_threshold_low * stats.stddev), 4),
        "upper": round(stats.mean + (settings.z_score_threshold_low * stats.stddev), 4),
        "window_count": stats.count,
    }

    logger.warning(
        f"ANOMALY DETECTED: id={anomaly_id}, device={event.device_id}, "
        f"metric={event.metric_type}, value={event.value}, "
        f"z_score={z_score:.2f}, severity={severity}, "
        f"expected_mean={stats.mean:.2f}±{stats.stddev:.2f}"
    )

    # --- Write to PostgreSQL ---
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO anomalies
                    (anomaly_id, device_id, metric_type, observed_value,
                     expected_range, z_score, severity, detected_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                anomaly_id,
                event.device_id,
                event.metric_type,
                event.value,
                json.dumps(expected_range),
                z_score,
                severity,
                detected_at,
            )
    except Exception as e:
        logger.error(f"Failed to write anomaly to PostgreSQL: {e}", exc_info=True)
        # Don't return — still try to publish to Kafka so downstream isn't blocked

    # --- Publish to anomalies.detected ---
    anomaly_event = AnomalyDetectedMessage(
        anomaly_id=anomaly_id,
        device_id=event.device_id,
        metric_type=event.metric_type,
        observed_value=event.value,
        expected_range=expected_range,
        z_score=round(z_score, 4),
        severity=severity,
        detected_at=detected_at,
        device_type=event.device_type,
        device_location=event.device_location,
    )

    try:
        await producer.send_and_wait(
            topic=settings.kafka_topic_anomalies,
            value=anomaly_event.model_dump_json(),
            key=str(event.device_id).encode("utf-8"),
        )
    except Exception as e:
        logger.error(f"Failed to publish anomaly event: {e}", exc_info=True)

