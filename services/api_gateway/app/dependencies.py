"""
FastAPI dependencies for the API Gateway.
These are injected into route handlers via Depends().
"""

from typing import AsyncGenerator

import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import Request


async def get_db_connection(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Yields a database connection from the pool.
    The connection is automatically released back to the pool
    when the route handler finishes (even if it raises an exception).
    """
    pool: asyncpg.Pool = request.app.state.db_pool
    async with pool.acquire() as connection:
        yield connection


async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    """Returns the shared Kafka producer."""
    return request.app.state.kafka_producer


async def get_redis_client(request: Request) -> aioredis.Redis:
    """Returns the shared Redis client."""
    return request.app.state.redis_client

