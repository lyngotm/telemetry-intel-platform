"""
Integration tests for the full telemetry ingestion pipeline.
Requires: Docker infrastructure (PostgreSQL + Kafka) + API Gateway + Ingestion Consumer running.

Run with:
    PYTHONPATH=. uv run pytest tests/integration/test_pipeline.py -v
"""

import asyncio
import json
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Adjust these if your local setup differs
API_BASE_URL = "http://localhost:8000"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"
KAFKA_BOOTSTRAP = "localhost:9093"


@pytest.fixture
async def api_client():
    """Provides an async HTTP client for the API Gateway."""
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0) as client:
        yield client


@pytest.fixture
async def db_connection():
    """Provides a direct database connection for verification."""
    conn = await asyncpg.connect(POSTGRES_DSN)
    yield conn
    await conn.close()


@pytest.fixture
async def registered_device(api_client: httpx.AsyncClient) -> UUID:
    """Register a test device and return its ID."""
    response = await api_client.post(
        "/api/v1/devices",
        json={
            "device_name": f"integration-test-device-{datetime.now().timestamp():.0f}",
            "device_type": "temperature_sensor",
            "location": "Test Lab",
            "firmware_version": "v1.0.0-test",
        },
    )
    assert response.status_code == 201, f"Device registration failed: {response.text}"
    return UUID(response.json()["data"]["device_id"])


class TestFullPipeline:
    """Tests the complete ingestion path: API → Kafka → Consumer → PostgreSQL → GET."""

    async def test_event_flows_through_pipeline(
        self,
        api_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
        registered_device: UUID,
    ):
        """
        POST a telemetry event → wait for consumer to process → verify in PostgreSQL → GET via API.
        """
        # --- Arrange ---
        timestamp = datetime.now(timezone.utc).isoformat()
        payload = {
            "device_id": str(registered_device),
            "metric_type": "temperature",
            "value": 67.3,
            "timestamp": timestamp,
            "metadata": {"unit": "celsius"},
        }

        # --- Act: POST the event ---
        response = await api_client.post("/api/v1/telemetry", json=payload)
        assert response.status_code == 202, f"Expected 202, got {response.status_code}: {response.text}"

        # --- Wait for the consumer to process (poll database) ---
        event_found = False
        for _ in range(20):  # Wait up to 10 seconds (20 × 0.5s)
            row = await db_connection.fetchrow(
                """
                SELECT event_id, device_id, metric_type, value
                FROM telemetry_events
                WHERE device_id = $1 AND metric_type = $2 AND value = $3
                ORDER BY ingested_at DESC
                LIMIT 1
                """,
                registered_device,
                "temperature",
                67.3,
            )
            if row:
                event_found = True
                break
            await asyncio.sleep(0.5)

        assert event_found, "Event did not appear in PostgreSQL within 10 seconds"
        assert row["device_id"] == registered_device
        assert row["metric_type"] == "temperature"
        assert row["value"] == 67.3

        # --- Verify: GET via the API returns the event ---
        get_response = await api_client.get(
            "/api/v1/telemetry",
            params={"device_id": str(registered_device), "metric_type": "temperature"},
        )
        assert get_response.status_code == 200
        data = get_response.json()
        assert data["success"] is True
        assert data["count"] >= 1

        # Find our specific event in the results
        events = data["data"]
        matching = [e for e in events if e["value"] == 67.3]
        assert len(matching) >= 1, "Event not found via GET endpoint"

    async def test_multiple_events_same_device(
        self,
        api_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
        registered_device: UUID,
    ):
        """Multiple events from the same device all get persisted."""
        values = [20.1, 21.5, 22.8, 23.4, 24.0]

        for val in values:
            response = await api_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": str(registered_device),
                    "metric_type": "humidity",
                    "value": val,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            assert response.status_code == 202

        # Wait for all to be processed
        await asyncio.sleep(5)

        count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = $1 AND metric_type = $2",
            registered_device,
            "humidity",
        )
        assert count >= len(values), f"Expected at least {len(values)} events, found {count}"

    async def test_invalid_device_rejected_at_api(
        self,
        api_client: httpx.AsyncClient,
    ):
        """Telemetry for a non-existent device is rejected at the API level."""
        response = await api_client.post(
            "/api/v1/telemetry",
            json={
                "device_id": "00000000-0000-0000-0000-000000000000",
                "metric_type": "temperature",
                "value": 50.0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        assert response.status_code == 404


class TestDeadLetterQueue:
    """Tests that malformed messages are routed to the DLQ."""

    async def test_malformed_message_routes_to_dlq(self):
        # --- Arrange: publish a malformed message directly to Kafka ---
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        await producer.start()

        malformed_payload = '{"bad": "message", "no_device_id": true}'
        await producer.send_and_wait(topic="telemetry.raw", value=malformed_payload)
        await producer.stop()

        # --- Assert: message appears on the DLQ ---
        # Use a unique group ID + "earliest" so we read all messages on the DLQ topic
        # from the beginning. The unique group ensures no prior committed offset exists.
        consumer = AIOKafkaConsumer(
            "telemetry.dlq",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=f"test-dlq-{datetime.now().timestamp():.0f}",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        await consumer.start()

        dlq_message_found = False
        try:
            for _ in range(20):
                batch = await consumer.getmany(timeout_ms=500, max_records=10)
                for tp, messages in batch.items():
                    for msg in messages:
                        parsed = json.loads(msg.value)
                        if "no_device_id" in parsed.get("original_payload", ""):
                            dlq_message_found = True
                            assert "error_reason" in parsed
                            assert parsed["original_topic"] == "telemetry.raw"
                            break
                if dlq_message_found:
                    break
        finally:
            await consumer.stop()

        assert dlq_message_found, "Malformed message did not appear on telemetry.dlq within 10 seconds"


