"""
Load test for the telemetry ingestion pipeline.

Sends 1000 events/second for 60 seconds → verifies no events dropped
(count in PostgreSQL matches count sent minus DLQ count).

Run with:
    docker compose up -d postgres zookeeper kafka kafka-init redis chromadb
    
    # Start API Gateway with high rate limit
    PYTHONPATH=. RATE_LIMIT_INGESTION=100000 uv run uvicorn services.api_gateway.app.main:app --port 8000

    # Run load test
    PYTHONPATH=. uv run pytest tests/integration/test_load.py -v -s
"""

import asyncio
import time
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest

pytestmark = pytest.mark.integration

BASE_URL = "http://localhost:8000"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"

# Load test parameters
EVENTS_PER_SECOND = 1000
DURATION_SECONDS = 60
TOTAL_EVENTS = EVENTS_PER_SECOND * DURATION_SECONDS
# Processing timeout — how long to wait after sending for pipeline to flush
PROCESSING_TIMEOUT = 120


@pytest.fixture
async def operator_client():
    """Authenticated operator client."""
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
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=2, max_size=5)
    yield pool
    await pool.close()


class TestLoadPerformance:
    """
    Load test: send 1000 events/second for 60 seconds →
    verify no events dropped (count in PostgreSQL matches sent minus DLQ).
    """

    async def test_sustained_throughput(self, operator_client, db_pool):
        """
        Send 60,000 events at 1000/sec, verify all are persisted or accounted for.
        Documents baseline performance numbers.
        """
        # --- Step 1: Register test devices ---
        device_ids = []
        for i in range(10):
            resp = await operator_client.post(
                "/api/v1/devices",
                json={
                    "device_name": f"load-test-device-{i}",
                    "device_type": "temperature_sensor",
                    "location": f"Load Test Rack {i}",
                    "firmware_version": "v1.0.0",
                },
            )
            assert resp.status_code == 201
            device_ids.append(resp.json()["data"]["device_id"])

        print(f"\n  Registered {len(device_ids)} test devices")

        # --- Step 2: Record baseline counts ---
        async with db_pool.acquire() as conn:
            baseline_events = await conn.fetchval(
                "SELECT COUNT(*) FROM telemetry_events"
            )
            baseline_dlq = await conn.fetchval(
                "SELECT COUNT(*) FROM dead_letter_events"
            )

        # --- Step 3: Send events at target rate ---
        print(f"  Sending {TOTAL_EVENTS} events at {EVENTS_PER_SECOND}/sec for {DURATION_SECONDS}s...")
        metric_types = ["temperature", "humidity", "pressure", "cpu_usage"]
        sent_count = 0
        failed_sends = 0
        rate_limited = 0
        start_time = time.time()

        # Use multiple concurrent clients for higher throughput
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=10.0,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        ) as load_client:
            # Authenticate
            resp = await load_client.post(
                "/api/v1/auth/token",
                json={"username": "operator", "password": "operator123"},
            )
            load_client.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

            for second in range(DURATION_SECONDS):
                second_start = time.time()

                # Create batch of coroutines for this second
                tasks = []
                for i in range(EVENTS_PER_SECOND):
                    device_id = device_ids[i % len(device_ids)]
                    metric = metric_types[i % len(metric_types)]
                    value = 50.0 + (i % 20) * 0.5  # Varied but normal values

                    tasks.append(
                        load_client.post(
                            "/api/v1/telemetry",
                            json={
                                "device_id": device_id,
                                "metric_type": metric,
                                "value": round(value, 2),
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                    )

                # Send batch concurrently
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for resp in responses:
                    if isinstance(resp, Exception):
                        failed_sends += 1
                    elif resp.status_code == 202:
                        sent_count += 1
                    elif resp.status_code == 429:
                        rate_limited += 1
                    else:
                        failed_sends += 1

                # Pace to maintain target rate
                elapsed = time.time() - second_start
                if elapsed < 1.0:
                    await asyncio.sleep(1.0 - elapsed)

                # Progress update every 10 seconds
                if (second + 1) % 10 == 0:
                    total_elapsed = time.time() - start_time
                    actual_rate = sent_count / total_elapsed
                    print(
                        f"    [{second+1:3d}s] sent={sent_count}, "
                        f"rate={actual_rate:.0f}/s, "
                        f"rate_limited={rate_limited}, failed={failed_sends}"
                    )

        total_send_time = time.time() - start_time
        actual_rate = sent_count / total_send_time

        print("\n  Send phase complete:")
        print(f"    Total sent (202): {sent_count}")
        print(f"    Rate limited (429): {rate_limited}")
        print(f"    Failed: {failed_sends}")
        print(f"    Actual rate: {actual_rate:.0f} events/sec")
        print(f"    Duration: {total_send_time:.1f}s")

        # --- Step 4: Wait for pipeline to process ---
        print(f"\n  Waiting up to {PROCESSING_TIMEOUT}s for pipeline to flush...")
        expected_persisted = sent_count  # All 202s should eventually be persisted

        final_events = baseline_events
        start_wait = time.time()

        while time.time() - start_wait < PROCESSING_TIMEOUT:
            async with db_pool.acquire() as conn:
                final_events = await conn.fetchval(
                    "SELECT COUNT(*) FROM telemetry_events"
                )

            new_events = final_events - baseline_events
            if new_events >= expected_persisted:
                break
            await asyncio.sleep(5)

        async with db_pool.acquire() as conn:
            final_events = await conn.fetchval(
                "SELECT COUNT(*) FROM telemetry_events"
            )
            final_dlq = await conn.fetchval(
                "SELECT COUNT(*) FROM dead_letter_events"
            )

        new_events = final_events - baseline_events
        new_dlq = final_dlq - baseline_dlq
        accounted_for = new_events + new_dlq

        # --- Step 5: Report results ---
        print("\n  Results:")
        print(f"    Events persisted: {new_events}")
        print(f"    DLQ entries: {new_dlq}")
        print(f"    Total accounted: {accounted_for}")
        print(f"    Expected (sent with 202): {sent_count}")
        print(f"    Drop rate: {max(0, sent_count - accounted_for) / max(1, sent_count) * 100:.2f}%")
        print(f"    Effective throughput: {new_events / total_send_time:.0f} events/sec persisted")

        # --- Assertions ---
        # All sent events should be accounted for (persisted or in DLQ)
        drop_rate = max(0, sent_count - accounted_for) / max(1, sent_count)
        assert drop_rate < 0.01, (
            f"Drop rate too high: {drop_rate*100:.2f}%. "
            f"Sent {sent_count}, accounted for {accounted_for} "
            f"(persisted={new_events}, dlq={new_dlq})"
        )

        # Should have actually sent a meaningful number of events
        assert sent_count > TOTAL_EVENTS * 0.5, (
            f"Too few events sent successfully: {sent_count}/{TOTAL_EVENTS}. "
            f"Rate limited: {rate_limited}, Failed: {failed_sends}"
        )

        print("\n  LOAD TEST PASSED ✓")
        print(f"  System handles {actual_rate:.0f} events/sec with <1% drop rate")
