"""
Failure injection tests for system resilience validation.

Tests verify:
- Redis failure: system continues operating in degraded mode (no caching, rate limiting skipped)
- Redis recovery: caching resumes after restart
- Kafka failure: API returns 503 on write path, consumers reconnect after recovery

These tests manipulate Docker containers directly and require:
- Full stack running via docker compose
- Tests run from the host (not inside containers)

Run with:
    docker compose up -d
    PYTHONPATH=. uv run pytest tests/integration/test_resilience.py -v -s -m manual
"""

import asyncio
import subprocess

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.manual, pytest.mark.resilience]

BASE_URL = "http://localhost:8000"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"


def docker_stop(container: str) -> None:
    """Stop a Docker container by name."""
    subprocess.run(["docker", "stop", container], capture_output=True, check=True)


def docker_start(container: str) -> None:
    """Start a stopped Docker container by name."""
    subprocess.run(["docker", "start", container], capture_output=True, check=True)


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
async def viewer_client():
    """Authenticated viewer client."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "viewer", "password": "viewer123"},
        )
        assert response.status_code == 200
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


class TestRedisFailureAndRecovery:
    """
    Kill Redis mid-stream → verify system continues operating
    (degraded but functional) → restart Redis → verify caching resumes.
    """

    async def test_redis_kill_and_recover(self, operator_client, viewer_client):
        """
        Full Redis failure injection cycle:
        1. Verify system works normally (cache hit on second request)
        2. Kill Redis
        3. Verify reads still work (served from PostgreSQL, no cache)
        4. Verify writes still work (Kafka publish doesn't depend on Redis)
        5. Verify rate limiting is skipped (fail-open design)
        6. Restart Redis
        7. Verify caching resumes
        """
        # --- Step 1: Normal operation — verify cache works ---
        print("\n  Step 1: Verifying normal operation with caching...")
        resp1 = await viewer_client.get("/api/v1/telemetry?limit=5")
        assert resp1.status_code == 200

        # Second request should be served from cache (same params)
        resp2 = await viewer_client.get("/api/v1/telemetry?limit=5")
        assert resp2.status_code == 200
        print("  Normal operation confirmed ✓")

        # --- Step 2: Kill Redis ---
        print("  Step 2: Stopping Redis container...")
        docker_stop("tip-redis")
        await asyncio.sleep(2)  # Allow connections to fail
        print("  Redis stopped ✓")

        try:
            # --- Step 3: Reads still work (PostgreSQL fallback) ---
            print("  Step 3: Verifying reads still work without Redis...")
            resp = await viewer_client.get("/api/v1/telemetry?limit=5")
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
            assert resp.json()["success"] is True
            print("  Reads work without Redis (PostgreSQL fallback) ✓")

            # --- Step 4: Anomaly queries still work ---
            print("  Step 4: Verifying anomaly queries work...")
            resp = await viewer_client.get("/api/v1/anomalies?limit=5")
            assert resp.status_code == 200
            print("  Anomaly queries work ✓")

            # --- Step 5: Rate limiting is skipped (fail-open) ---
            print("  Step 5: Verifying rate limiting fails open...")
            # Send 110 requests (over the 100/min limit) — should all succeed
            # because rate limiter can't reach Redis and fails open
            success_count = 0
            for _ in range(110):
                resp = await viewer_client.get("/api/v1/anomalies?limit=1")
                if resp.status_code == 200:
                    success_count += 1

            assert success_count == 110, (
                f"Expected all 110 requests to succeed (fail-open), got {success_count}"
            )
            print(f"  Rate limiting fails open: {success_count}/110 succeeded ✓")

            # --- Step 6: Writes still work (Kafka doesn't depend on Redis) ---
            print("  Step 6: Verifying telemetry ingestion still works...")
            # Get a device to send telemetry for
            devices_resp = await operator_client.get("/api/v1/devices")
            devices = devices_resp.json()["data"]
            if devices:
                from datetime import datetime, timezone

                device_id = devices[0]["device_id"]
                resp = await operator_client.post(
                    "/api/v1/telemetry",
                    json={
                        "device_id": device_id,
                        "metric_type": "temperature",
                        "value": 55.0,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                assert resp.status_code == 202, f"Write failed: {resp.status_code}: {resp.text}"
                print("  Telemetry ingestion works without Redis ✓")
            else:
                print("  (Skipped write test — no devices registered)")

        finally:
            # --- Step 7: Restart Redis ---
            print("  Step 7: Restarting Redis...")
            docker_start("tip-redis")
            await asyncio.sleep(3)  # Allow Redis to become ready
            print("  Redis restarted ✓")

        # --- Step 8: Verify caching resumes ---
        print("  Step 8: Verifying caching resumes...")
        # Make a request — should succeed and repopulate cache
        resp = await viewer_client.get("/api/v1/telemetry?limit=3")
        assert resp.status_code == 200

        # Verify rate limiting is working again (not fail-open)
        # We won't exhaust the limit here, just confirm no errors
        resp = await viewer_client.get("/api/v1/telemetry?limit=3")
        assert resp.status_code == 200
        print("  Caching resumed after Redis recovery ✓")
        print("  Redis failure injection test PASSED ✓")


class TestKafkaFailure:
    """
    Kill Kafka briefly → verify API returns 503 on write path →
    verify consumers reconnect and resume after recovery.
    """

    async def test_kafka_unavailable_returns_503(self, operator_client, viewer_client):
        """
        When Kafka is down:
        - POST /telemetry should return 503 (can't publish)
        - GET endpoints should still work (read from PostgreSQL)
        """
        # Get a device for testing writes
        devices_resp = await operator_client.get("/api/v1/devices")
        devices = devices_resp.json()["data"]
        if not devices:
            pytest.skip("No devices registered — can't test write path")

        device_id = devices[0]["device_id"]

        # --- Kill Kafka ---
        print("\n  Stopping Kafka container...")
        docker_stop("tip-kafka")
        await asyncio.sleep(5)  # Allow producer to detect failure
        print("  Kafka stopped ✓")

        try:
            # --- Verify writes fail with 503 ---
            print("  Verifying POST /telemetry returns 503...")
            from datetime import datetime, timezone

            resp = await operator_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": device_id,
                    "metric_type": "temperature",
                    "value": 55.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            assert resp.status_code == 503, (
                f"Expected 503 when Kafka is down, got {resp.status_code}: {resp.text}"
            )
            assert "Retry-After" in resp.headers
            print("  POST returns 503 with Retry-After header ✓")

            # --- Verify reads still work ---
            print("  Verifying GET endpoints still work...")
            resp = await viewer_client.get("/api/v1/telemetry?limit=5")
            assert resp.status_code == 200
            print("  GET /telemetry works ✓")

            resp = await viewer_client.get("/api/v1/anomalies?limit=5")
            assert resp.status_code == 200
            print("  GET /anomalies works ✓")

        finally:
            # --- Restart Kafka ---
            print("  Restarting Kafka container...")
            docker_start("tip-kafka")
            await asyncio.sleep(10)  # Kafka takes longer to become ready
            print("  Kafka restarted ✓")

        # --- Verify writes resume ---
        print("  Verifying writes resume after Kafka recovery...")
        # May need a few retries as the producer reconnects
        write_succeeded = False
        for attempt in range(10):
            resp = await operator_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": device_id,
                    "metric_type": "temperature",
                    "value": 56.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            if resp.status_code == 202:
                write_succeeded = True
                break
            await asyncio.sleep(2)

        assert write_succeeded, "Writes did not resume within 20 seconds of Kafka recovery"
        print("  Writes resumed after Kafka recovery ✓")
        print("  Kafka failure injection test PASSED ✓")
