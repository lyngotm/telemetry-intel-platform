"""
End-to-end integration tests for the full telemetry pipeline.

Tests the complete flow under various conditions:
- Happy path: telemetry → anomaly detection → diagnosis generation
- Validates counts in PostgreSQL match expectations

Requires: Full stack running (all 4 services + infrastructure)
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import asyncpg
import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.call_llm]

BASE_URL = "http://localhost:8000"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"

# Timeouts for async pipeline processing
ANOMALY_TIMEOUT = 60  # seconds to wait for anomaly detection
DIAGNOSIS_TIMEOUT = 120  # seconds to wait for diagnosis generation


@pytest.fixture
async def operator_client():
    """Authenticated operator client for telemetry ingestion."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "operator", "password": "operator123"},
        )
        assert response.status_code == 200
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def db_pool():
    """Direct database connection for verification."""
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=3)
    yield pool
    await pool.close()


class TestHappyPathEndToEnd:
    """
    Happy path: send 100 telemetry events including 3 deliberate anomalies →
    wait for processing → verify 3 anomaly records in PostgreSQL →
    wait for diagnosis generation → verify 3 diagnoses exist with
    non-empty root_cause_summary and confidence_score > 0.
    """

    async def test_full_pipeline_100_events_3_anomalies(self, operator_client, db_pool):
        """
        Send 100 events with 3 extreme spikes, verify the full pipeline
        processes them into anomalies and diagnoses.
        """
        # --- Step 1: Register a dedicated test device ---
        device_response = await operator_client.post(
            "/api/v1/devices",
            json={
                "device_name": f"e2e-test-device-{uuid4().hex[:8]}",
                "device_type": "edge_gateway",
                "location": "E2E Test Rack",
                "firmware_version": "v2.1.0",
            },
        )
        assert device_response.status_code == 201, (
            f"Device registration failed: {device_response.text}"
        )
        device_id = device_response.json()["data"]["device_id"]

        # --- Step 2: Send 100 telemetry events ---
        # First 40: normal baseline (temperature ~50°C, stddev ~2)
        # Event 41: anomaly spike (95°C)
        # Events 42-70: normal
        # Event 71: anomaly spike (98°C)
        # Events 72-95: normal
        # Event 96: anomaly spike (92°C)
        # Events 97-100: normal

        now = datetime.now(timezone.utc)
        anomaly_indices = {40, 70, 95}  # 0-indexed positions of anomalies
        sent_count = 0
        anomaly_values = []

        for i in range(100):
            timestamp = now - timedelta(seconds=(100 - i) * 10)

            if i in anomaly_indices:
                # Extreme spike — guaranteed z-score > 4 against a ~50°C baseline
                value = 92.0 + (i % 3) * 3  # 92, 95, 98
                anomaly_values.append(value)
            else:
                # Normal readings with small variance
                import random

                value = 50.0 + random.uniform(-2.0, 2.0)

            response = await operator_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": device_id,
                    "metric_type": "temperature",
                    "value": round(value, 2),
                    "timestamp": timestamp.isoformat(),
                },
            )
            assert response.status_code == 202, f"Event {i} failed: {response.text}"
            sent_count += 1

        assert sent_count == 100
        print(f"\n  Sent {sent_count} events (3 anomalies at values {anomaly_values})")

        # --- Step 3: Wait for anomaly detection ---
        print(f"  Waiting for anomaly detection (timeout {ANOMALY_TIMEOUT}s)...")
        detected_anomalies = []
        start_time = time.time()

        while time.time() - start_time < ANOMALY_TIMEOUT:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT anomaly_id, observed_value, severity, detected_at
                    FROM anomalies
                    WHERE device_id = $1 AND metric_type = 'temperature'
                    AND observed_value > 85.0
                    ORDER BY detected_at ASC
                    """,
                    device_id,
                )
                detected_anomalies = rows

            if len(detected_anomalies) >= 3:
                break
            await asyncio.sleep(2)

        assert len(detected_anomalies) >= 3, (
            f"Expected at least 3 anomalies, got {len(detected_anomalies)}. "
            f"Values detected: {[float(r['observed_value']) for r in detected_anomalies]}"
        )
        print(f"  Detected {len(detected_anomalies)} anomalies ✓")

        # --- Step 4: Wait for diagnosis generation ---
        print(f"  Waiting for diagnosis generation (timeout {DIAGNOSIS_TIMEOUT}s)...")
        anomaly_ids = [row["anomaly_id"] for row in detected_anomalies[:3]]
        diagnoses = []
        start_time = time.time()

        while time.time() - start_time < DIAGNOSIS_TIMEOUT:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT diagnosis_id, anomaly_id, root_cause_summary,
                           confidence_score, generation_time_seconds
                    FROM diagnoses
                    WHERE anomaly_id = ANY($1::uuid[])
                    """,
                    anomaly_ids,
                )
                diagnoses = rows

            if len(diagnoses) >= 3:
                break
            await asyncio.sleep(5)

        assert len(diagnoses) >= 3, (
            f"Expected at least 3 diagnoses, got {len(diagnoses)}. "
            f"Anomaly IDs: {anomaly_ids}. "
            f"Check that the Diagnosis Service is running with valid AWS credentials."
        )
        print(f"  Generated {len(diagnoses)} diagnoses ✓")

        # --- Step 5: Verify diagnosis quality ---
        for diag in diagnoses:
            assert diag["root_cause_summary"] is not None
            assert len(diag["root_cause_summary"]) > 20, (
                f"Root cause too short: '{diag['root_cause_summary']}'"
            )
            assert diag["confidence_score"] > 0.0, (
                f"Confidence should be > 0, got {diag['confidence_score']}"
            )
            assert diag["confidence_score"] <= 1.0
            assert diag["generation_time_seconds"] is not None

        avg_confidence = sum(d["confidence_score"] for d in diagnoses) / len(diagnoses)
        avg_gen_time = sum(d["generation_time_seconds"] for d in diagnoses) / len(diagnoses)

        print(f"  Avg confidence: {avg_confidence:.2f}")
        print(f"  Avg generation time: {avg_gen_time:.1f}s")
        print("  All diagnoses have non-empty root_cause_summary ✓")
        print("  All confidence_scores in (0, 1] ✓")

    async def test_auth_enforcement_on_all_endpoints(self, operator_client):
        """
        Verify auth is enforced: no token → 401, wrong role → 403.
        """
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as unauth_client:
            # No token → 401 on protected endpoints
            endpoints = [
                ("GET", "/api/v1/telemetry"),
                ("GET", "/api/v1/anomalies"),
                ("GET", "/api/v1/devices"),
                ("POST", "/api/v1/telemetry"),
                ("POST", "/api/v1/knowledge/ingest"),
            ]

            for method, path in endpoints:
                if method == "GET":
                    resp = await unauth_client.get(path)
                else:
                    resp = await unauth_client.post(path, json={})
                assert resp.status_code in (401, 403), (
                    f"{method} {path} without token returned {resp.status_code}, expected 401/403"
                )

        # Viewer cannot POST telemetry (wrong role → 403)
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as viewer_client:
            resp = await viewer_client.post(
                "/api/v1/auth/token",
                json={"username": "viewer", "password": "viewer123"},
            )
            viewer_token = resp.json()["access_token"]
            viewer_client.headers["Authorization"] = f"Bearer {viewer_token}"

            resp = await viewer_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": str(uuid4()),
                    "metric_type": "temperature",
                    "value": 50.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            assert resp.status_code == 403

        # Viewer cannot ingest knowledge (admin only → 403)
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as viewer_client:
            resp = await viewer_client.post(
                "/api/v1/auth/token",
                json={"username": "viewer", "password": "viewer123"},
            )
            viewer_token = resp.json()["access_token"]
            viewer_client.headers["Authorization"] = f"Bearer {viewer_token}"

            resp = await viewer_client.post(
                "/api/v1/knowledge/ingest",
                json={
                    "title": "Test",
                    "description": "Test",
                    "affected_device_types": ["test"],
                    "root_cause": "Test",
                    "resolution_steps": ["Test"],
                    "severity": "low",
                    "failure_category": "test",
                    "tags": ["test"],
                },
            )
            assert resp.status_code == 403


class TestDLQPath:
    """
    DLQ path: send 10 malformed events → verify all 10 land in
    dead_letter_events PostgreSQL table with error reasons.
    """

    async def test_malformed_events_persist_to_dlq_table(self, operator_client, db_pool):
        """
        Send 10 malformed messages directly to Kafka telemetry.raw →
        verify they appear in the dead_letter_events table.
        """
        from aiokafka import AIOKafkaProducer

        KAFKA_BOOTSTRAP = "localhost:9093"

        # Record current DLQ count to compare after
        async with db_pool.acquire() as conn:
            initial_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_events")

        # Publish 10 malformed messages directly to Kafka
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        await producer.start()

        malformed_payloads = [
            '{"bad": "no device_id field"}',
            '{"device_id": "not-a-uuid", "metric_type": "temperature", "value": 50}',
            "completely invalid json {{{{",
            '{"device_id": "00000000-0000-0000-0000-000000000001", "metric_type": "invalid_metric", "value": 50, "timestamp": "2026-06-28T00:00:00Z"}',
            "",  # empty string
            '{"device_id": null}',
            "42",  # not an object
            '{"device_id": "00000000-0000-0000-0000-000000000002", "value": "not_a_number"}',
            '{"device_id": "00000000-0000-0000-0000-000000000003", "metric_type": "temperature", "value": "NaN"}',
            '{"missing_everything": true}',
        ]

        for payload in malformed_payloads:
            await producer.send_and_wait(topic="telemetry.raw", value=payload)

        await producer.stop()
        print(f"\n  Sent {len(malformed_payloads)} malformed messages to telemetry.raw")

        # Wait for the ingestion consumer to process and route to DLQ
        print("  Waiting for DLQ processing...")
        expected_new = len(malformed_payloads)
        start_time = time.time()

        while time.time() - start_time < 30:
            async with db_pool.acquire() as conn:
                current_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_events")

            new_entries = current_count - initial_count
            if new_entries >= expected_new:
                break
            await asyncio.sleep(1)

        async with db_pool.acquire() as conn:
            final_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_events")

        new_entries = final_count - initial_count
        assert new_entries >= expected_new, (
            f"Expected at least {expected_new} new DLQ entries, got {new_entries}. "
            f"Initial: {initial_count}, Final: {final_count}"
        )
        print(f"  {new_entries} events persisted to dead_letter_events ✓")

        # Verify the entries have error_reason populated
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT error_reason FROM dead_letter_events
                ORDER BY failed_at DESC LIMIT $1
                """,
                expected_new,
            )

        for row in rows:
            assert row["error_reason"] is not None
            assert len(row["error_reason"]) > 0

        print("  All entries have non-empty error_reason ✓")

    async def test_dlq_api_endpoint(self, operator_client, db_pool):
        """Verify GET /api/v1/telemetry/dlq returns DLQ entries."""
        response = await operator_client.get("/api/v1/telemetry/dlq?limit=5")
        assert response.status_code == 200

        data = response.json()
        assert data["success"] is True
        assert "count" in data

        # If there are entries, verify structure
        if data["data"]:
            entry = data["data"][0]
            assert "id" in entry
            assert "original_topic" in entry
            assert "original_payload" in entry
            assert "error_reason" in entry
            assert "failed_at" in entry
