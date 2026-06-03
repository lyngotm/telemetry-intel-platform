"""
Anomaly Detection Service — consumes enriched telemetry events, maintains
rolling statistical windows in Redis, and detects anomalies via z-score.
"""

import asyncio
import logging
import signal

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from services.anomaly_detection.app.config import settings
from services.anomaly_detection.app.detector import process_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Only set DEBUG for our code
logging.getLogger("anomaly_detection").setLevel(
    getattr(logging, settings.log_level.upper(), logging.INFO))

logger = logging.getLogger("anomaly_detection")


async def run_service():
    """
    Main entry point for the Anomaly Detection Service.
    Connects to Kafka, Redis, and PostgreSQL, then processes enriched events.
    """
    # --- Setup resources ---
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=20,
    )

    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_enriched,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    logger.info(
        f"Anomaly Detection Service started. "
        f"Consuming: {settings.kafka_topic_enriched}, "
        f"Group: {settings.kafka_consumer_group}, "
        f"Window: {settings.window_size_seconds}s"
    )

    # --- Graceful shutdown handling ---
    shutdown_event = asyncio.Event()

    def handle_shutdown(sig):
        logger.info(f"Received {sig.name}. Shutting down gracefully...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown, sig)

    # --- Main processing loop ---
    try:
        while not shutdown_event.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=100)

            for topic_partition, messages in batch.items():
                for message in messages:
                    await process_event(
                        message=message,
                        redis_client=redis_client,
                        db_pool=db_pool,
                        producer=producer,
                    )

            if batch:
                await consumer.commit()

    except Exception as e:
        logger.error(f"Anomaly Detection Service error: {e}", exc_info=True)
    finally:
        logger.info("Shutting down Anomaly Detection Service...")
        await consumer.stop()
        await producer.stop()
        await redis_client.aclose()
        await db_pool.close()
        logger.info("Anomaly Detection Service stopped.")


if __name__ == "__main__":
    asyncio.run(run_service())
