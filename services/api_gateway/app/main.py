"""
API Gateway — main application entry point.
"""

from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

from services.api_gateway.app.config import settings
from services.api_gateway.app.routers.anomalies import router as anomalies_router
from services.api_gateway.app.routers.devices import router as devices_router
from services.api_gateway.app.routers.telemetry import router as telemetry_router
from shared.logging_config import setup_logging

logger = setup_logging("api_gateway", settings.log_level.upper())

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown of shared resources.
    Everything before 'yield' runs on startup.
    Everything after 'yield' runs on shutdown.
    """
    # --- Startup ---
    # Create PostgreSQL connection pool
    app.state.db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=20,
    )

    # Create Kafka producer
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        # Serialize message values as UTF-8 JSON strings
        value_serializer=lambda v: v.encode("utf-8"),
    )
    await app.state.kafka_producer.start()

    # Redis client for cache-aside pattern
    app.state.redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    logger.info(f"API Gateway started. Kafka: {settings.kafka_bootstrap_servers}, Redis: {settings.redis_url}")

    yield  # App is running and serving requests

    # --- Shutdown ---
    await app.state.kafka_producer.stop()
    await app.state.redis_client.aclose()
    await app.state.db_pool.close()
    logger.info("API Gateway shut down.")


app = FastAPI(
    title="Telemetry Intelligence Platform",
    description="Real-time telemetry ingestion and analysis API",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(devices_router)
app.include_router(telemetry_router)
app.include_router(anomalies_router)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

