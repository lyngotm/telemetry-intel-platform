"""
Integration test for the Anomaly Detection pipeline.
Requires: Docker infrastructure + API Gateway + Ingestion Consumer + Anomaly Detection Service running.

Run with:
    PYTHONPATH=. uv run pytest tests/integration/test_anomaly_detection.py -v
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest

pytestmark = pytest.mark.integration

API_BASE_URL = "http://localhost:8000"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"


@pytest.fixture
async def api_client():
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0) as client:
        # Authenticate the client for all subsequent requests
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "operator", "password": "operator123"},
        )
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def db_connection():
    conn = await asyncpg.connect(POSTGRES_DSN)
    yield conn
    await conn.close()


@pytest.fixture
async def registered_device(api_client: httpx.AsyncClient) -> UUID:
    """Register a fresh device for anomaly testing."""
    response = await api_client.post(
        "/api/v1/devices",
        json={
            "device_name": f"anomaly-test-device-{datetime.now().timestamp():.0f}",
            "device_type": "temperature_sensor",
            "location": "Test Lab",
            "firmware_version": "v1.0.0",
        },
    )
    assert response.status_code == 201
    return UUID(response.json()["data"]["device_id"])


class TestAnomalyDetection:
    """Tests the anomaly detection pipeline end-to-end."""

    async def test_anomaly_detected_after_spike(
        self,
        api_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
        registered_device: UUID,
    ):
        """
        Send enough normal readings to build a window, then inject a spike.
        Verify that an anomaly record appears in PostgreSQL.
        """
        # --- Step 1: Send 15 normal readings to build the rolling window ---
        # Mean=65, stddev≈5 — these values will establish the baseline
        normal_values = [
            64.2,
            65.8,
            63.9,
            66.1,
            64.7,
            65.3,
            66.5,
            63.8,
            65.0,
            64.5,
            66.2,
            65.1,
            64.8,
            65.7,
            63.5,
        ]

        base_time = datetime.now(timezone.utc)

        for i, value in enumerate(normal_values):
            response = await api_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": str(registered_device),
                    "metric_type": "temperature",
                    "value": value,
                    "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                },
            )
            assert response.status_code == 202

        # Wait for the window to be populated (consumer + anomaly detection processing)
        await asyncio.sleep(5)

        # --- Step 2: Inject a spike (well above the normal range) ---
        # With mean≈65 and stddev≈0.9, a value of 95 is ~33 stddevs away — clearly anomalous
        spike_value = 95.0
        response = await api_client.post(
            "/api/v1/telemetry",
            json={
                "device_id": str(registered_device),
                "metric_type": "temperature",
                "value": spike_value,
                "timestamp": (base_time + timedelta(seconds=20)).isoformat(),
            },
        )
        assert response.status_code == 202

        # --- Step 3: Wait for the anomaly to be detected and persisted ---
        anomaly_found = False
        for _ in range(20):  # Wait up to 10 seconds
            row = await db_connection.fetchrow(
                """
                SELECT anomaly_id, device_id, metric_type, observed_value, z_score, severity
                FROM anomalies
                WHERE device_id = $1 AND metric_type = $2 AND observed_value = $3
                LIMIT 1
                """,
                registered_device,
                "temperature",
                spike_value,
            )
            if row:
                anomaly_found = True
                break
            await asyncio.sleep(0.5)

        assert anomaly_found, (
            f"Anomaly for device {registered_device} with value {spike_value} "
            "did not appear in PostgreSQL within 10 seconds"
        )

        # --- Step 4: Verify the anomaly record contents ---
        assert row["device_id"] == registered_device
        assert row["metric_type"] == "temperature"
        assert row["observed_value"] == spike_value
        assert row["z_score"] > 3.0, f"Expected z_score > 3.0, got {row['z_score']}"
        assert row["severity"] in ("low", "medium", "high", "critical")

    async def test_anomaly_available_via_api(
        self,
        api_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
        registered_device: UUID,
    ):
        """
        After an anomaly is detected, verify it's queryable via GET /api/v1/anomalies.
        """
        # Insert a known anomaly directly for a deterministic test
        anomaly_id = await db_connection.fetchval(
            """
            INSERT INTO anomalies (device_id, metric_type, observed_value, expected_range, z_score, severity)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING anomaly_id
            """,
            registered_device,
            "temperature",
            99.0,
            json.dumps(
                {"mean": 65.0, "stddev": 1.0, "lower": 60.0, "upper": 70.0, "window_count": 15}
            ),
            34.0,
            "critical",
        )

        # Query via the API
        response = await api_client.get(
            "/api/v1/anomalies",
            params={"device_id": str(registered_device)},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] >= 1

        # Verify the specific anomaly is in the response
        anomalies = data["data"]
        matching = [a for a in anomalies if a["anomaly_id"] == str(anomaly_id)]
        assert len(matching) == 1
        assert matching[0]["severity"] == "critical"
        assert matching[0]["observed_value"] == 99.0
