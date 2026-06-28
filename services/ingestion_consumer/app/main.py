"""
Ingestion Consumer — reads from telemetry.raw, validates, persists to PostgreSQL,
and publishes enriched events to telemetry.enriched.
"""

import asyncio
import logging
import signal

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server

from services.ingestion_consumer.app.config import settings
from services.ingestion_consumer.app.consumer import process_message
from services.ingestion_consumer.app.enrichment import DeviceEnrichment

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Only set DEBUG for our code
logging.getLogger("ingestion_consumer").setLevel(
    getattr(logging, settings.log_level.upper(), logging.INFO)
)

logger = logging.getLogger("ingestion_consumer")


async def run_consumer():
    """
    Main entry point for the Ingestion Consumer.
    Connects to Kafka and PostgreSQL, then processes messages in a loop.
    """

    start_http_server(settings.metrics_port)
    logger.info("Prometheus metrics available on port 9090")

    # --- Setup resources ---
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=20,
    )

    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,  # Return strings instead of bytes
    )

    enrichment = DeviceEnrichment(redis_client=redis_client, db_pool=db_pool)

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_raw,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        # Don't auto-commit — we commit manually after successful processing
        enable_auto_commit=False,
        # Start from earliest unprocessed message if no committed offset exists
        auto_offset_reset="earliest",
        # Deserialize message values from bytes to string
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    logger.info(
        f"Ingestion Consumer started. "
        f"Consuming: {settings.kafka_topic_raw}, Group: {settings.kafka_consumer_group}"
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
            # Fetch a batch of messages (waits up to 1 second)
            batch = await consumer.getmany(timeout_ms=1000, max_records=100)

            for topic_partition, messages in batch.items():
                for message in messages:
                    await process_message(
                        message=message,
                        db_pool=db_pool,
                        producer=producer,
                        enrichment=enrichment,
                    )

            # Commit offsets after the batch is fully processed
            # If we crash before this, messages get reprocessed (at-least-once)
            if batch:
                await consumer.commit()

    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        # --- Cleanup ---
        logger.info("Shutting down consumer...")
        await consumer.stop()
        await producer.stop()
        await redis_client.aclose()
        await db_pool.close()
        logger.info("Ingestion Consumer stopped.")


if __name__ == "__main__":
    asyncio.run(run_consumer())
