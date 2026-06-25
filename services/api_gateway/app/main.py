"""
API Gateway — main application entry point.
"""

from contextlib import asynccontextmanager

import asyncpg
import chromadb
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

from services.api_gateway.app.config import settings
from services.api_gateway.app.routers.anomalies import router as anomalies_router
from services.api_gateway.app.routers.auth import router as auth_router
from services.api_gateway.app.routers.devices import router as devices_router
from services.api_gateway.app.routers.knowledge import router as knowledge_router
from services.api_gateway.app.routers.telemetry import router as telemetry_router
from shared.logging_config import setup_logging
from shared.metrics import PrometheusMiddleware, metrics_response

logger = setup_logging("api_gateway", settings.log_level.upper())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown of shared resources.
    Everything before 'yield' runs on startup.
    Everything after 'yield' runs on shutdown.
    """
    # --- Startup ---
    app.state.db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=20,
    )

    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    await app.state.kafka_producer.start()

    app.state.redis_client = aioredis.from_url(
        settings.redis_url,
        socket_connect_timeout=5,
        socket_timeout=5,
        decode_responses=True,
    )

    app.state.chroma_client = chromadb.HttpClient(
        host=settings.chroma_host,
        port=settings.chroma_port,
    )
    
    app.state.chroma_client.get_or_create_collection(
        name="incident_embeddings",
        metadata={"hnsw:space": "cosine"},
    )

    logger.info(
        f"API Gateway started. Kafka: {settings.kafka_bootstrap_servers}, "
        f"Redis: {settings.redis_url}, ChromaDB: {settings.chroma_host}:{settings.chroma_port}"
    )
    yield

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

app.add_middleware(PrometheusMiddleware)

app.include_router(auth_router)
app.include_router(devices_router)
app.include_router(telemetry_router)
app.include_router(anomalies_router)
app.include_router(knowledge_router)


@app.get("/health")
async def health_check():
    """Liveness probe — process is running."""
    return {"status": "healthy"}


@app.get("/ready")
async def readiness_check():
    """Readiness probe — checks critical dependencies."""
    try:
        # Check PostgreSQL
        async with app.state.db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        # Check Redis
        await app.state.redis_client.ping()
        return {"status": "ready"}
    except Exception as e:
        logger.warning(f"Readiness check failed: {e}")
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "detail": str(e)},
        )


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return metrics_response()
