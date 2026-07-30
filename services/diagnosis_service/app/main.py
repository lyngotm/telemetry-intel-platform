"""
Diagnosis Service — Kafka consumer on anomalies.detected.

Reads anomaly events, runs the RAG diagnosis pipeline (context → retrieval → generation),
and persists structured diagnoses to PostgreSQL.
"""

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone

import asyncpg
import chromadb
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

from services.diagnosis_service.app.config import settings
from services.diagnosis_service.app.rag_pipeline import run_diagnosis_pipeline
from shared.metrics import DIAGNOSES_FAILED, DIAGNOSES_GENERATED, DIAGNOSIS_GENERATION_SECONDS
from shared.models.models import AnomalyDetectedMessage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("diagnosis_service").setLevel(
    getattr(logging, settings.log_level.upper(), logging.INFO)
)

logger = logging.getLogger("diagnosis_service")


async def persist_diagnosis(
    db_pool: asyncpg.Pool,
    anomaly_id,
    diagnosis: dict,
) -> None:
    """
    Write a completed diagnosis to the diagnoses table.

    Args:
        db_pool: PostgreSQL connection pool.
        anomaly_id: UUID of the anomaly this diagnosis is for.
        diagnosis: Dict returned by run_diagnosis_pipeline containing
            root_cause_summary, confidence_score, supporting_evidence,
            recommended_actions, retrieved_incident_ids, raw_llm_response,
            generation_time_seconds.
    """
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO diagnoses
                (anomaly_id, root_cause_summary, confidence_score,
                 supporting_evidence, recommended_actions,
                 retrieved_incident_ids, raw_llm_response,
                 model_id, generation_time_seconds, generated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            anomaly_id,
            diagnosis["root_cause_summary"],
            diagnosis["confidence_score"],
            json.dumps(diagnosis["supporting_evidence"]),
            json.dumps(diagnosis["recommended_actions"]),
            json.dumps(diagnosis.get("retrieved_incident_ids", [])),
            diagnosis.get("raw_llm_response"),
            settings.claude_model_id,
            diagnosis.get("generation_time_seconds"),
            datetime.now(timezone.utc),
        )


async def run_consumer():
    """
    Main entry point for the Diagnosis Service.
    Connects to Kafka, PostgreSQL, Redis, and ChromaDB, then processes
    anomaly events through the RAG pipeline in a loop.
    """
    # --- Prometheus metrics ---
    start_http_server(settings.metrics_port)
    logger.info(f"Prometheus metrics available on port {settings.metrics_port}")

    # --- Setup resources ---
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
    )

    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    chroma_client = chromadb.HttpClient(
        host=settings.chroma_host,
        port=settings.chroma_port,
    )

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_anomalies,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    await consumer.start()
    logger.info(
        f"Diagnosis Service started. "
        f"Consuming: {settings.kafka_topic_anomalies}, "
        f"Group: {settings.kafka_consumer_group}"
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
            # Fetch messages (wait up to 1 second)
            batch = await consumer.getmany(timeout_ms=1000, max_records=5)

            for topic_partition, messages in batch.items():
                for message in messages:
                    await process_anomaly_message(
                        message=message,
                        db_pool=db_pool,
                        redis_client=redis_client,
                        chroma_client=chroma_client,
                    )

            # Commit offsets after processing
            if batch:
                await consumer.commit()

    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        # --- Cleanup ---
        logger.info("Shutting down Diagnosis Service...")
        await consumer.stop()
        await redis_client.aclose()
        await db_pool.close()
        logger.info("Diagnosis Service stopped.")


async def process_anomaly_message(
    message,
    db_pool: asyncpg.Pool,
    redis_client: aioredis.Redis,
    chroma_client: chromadb.ClientAPI,
) -> None:
    """
    Process a single anomaly message through the full RAG diagnosis pipeline.

    Steps:
        1. Parse the anomaly event from Kafka message
        2. Run the RAG pipeline (context → retrieval → generation)
        3. Persist the diagnosis to PostgreSQL
    """
    # --- Step 1: Parse anomaly event ---
    try:
        anomaly = AnomalyDetectedMessage.model_validate_json(message.value)
    except Exception as e:
        logger.error(f"Failed to parse anomaly message: {e}")
        DIAGNOSES_FAILED.labels(phase="context").inc()
        return

    logger.info(
        f"Processing anomaly {anomaly.anomaly_id} "
        f"(device={anomaly.device_id}, metric={anomaly.metric_type}, "
        f"severity={anomaly.severity})"
    )

    # --- Step 2: Run RAG pipeline (timed) ---
    with DIAGNOSIS_GENERATION_SECONDS.time():
        try:
            diagnosis = await run_diagnosis_pipeline(
                anomaly=anomaly,
                db_pool=db_pool,
                redis_client=redis_client,
                chroma_client=chroma_client,
            )
        except Exception as e:
            logger.error(
                f"RAG pipeline failed for anomaly {anomaly.anomaly_id}: {e}",
                exc_info=True,
            )
            DIAGNOSES_FAILED.labels(phase="generation").inc()
            return

    # --- Step 3: Persist diagnosis ---
    try:
        await persist_diagnosis(db_pool, anomaly.anomaly_id, diagnosis)
        DIAGNOSES_GENERATED.labels(severity=anomaly.severity).inc()
        logger.info(
            f"Diagnosis persisted for anomaly {anomaly.anomaly_id}: "
            f"confidence={diagnosis['confidence_score']:.2f}, "
            f"generation_time={diagnosis.get('generation_time_seconds', 0):.2f}s"
        )
    except Exception as e:
        logger.error(
            f"Failed to persist diagnosis for anomaly {anomaly.anomaly_id}: {e}",
            exc_info=True,
        )
        DIAGNOSES_FAILED.labels(phase="persistence").inc()


if __name__ == "__main__":
    asyncio.run(run_consumer())
