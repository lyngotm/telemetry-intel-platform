"""
Integration tests for authentication, RBAC, rate limiting, and resilience.

Requires:
- API Gateway running on localhost:8000
- Redis running on localhost:6379
- Kafka running on localhost:9093

Run with: PYTHONPATH=. uv run pytest tests/integration/test_auth.py -v -m "not manual"

docker stop tip-redis tip-kafka
PYTHONPATH=. uv run pytest tests/integration/test_auth.py -v -m "manual"

docker start tip-redis tip-kafka
"""

import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import redis.asyncio as aioredis
from jose import jwt

pytestmark = pytest.mark.integration

# --- Test Configuration ---

BASE_URL = "http://localhost:8000"
REDIS_URL = "redis://localhost:6379/0"

# These match the hardcoded users in auth.py
CREDENTIALS = {
    "admin": {"username": "admin", "password": "admin123"},
    "operator": {"username": "operator", "password": "operator123"},
    "viewer": {"username": "viewer", "password": "viewer123"},
}


# --- Fixtures ---


@pytest.fixture
def client():
    """Synchronous httpx client for simple request/response tests."""
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as c:
        yield c


@pytest.fixture
def get_token(client: httpx.Client):
    """Factory fixture that returns a token for a given role."""

    def _get_token(role: str) -> str:
        response = client.post("/api/v1/auth/token", json=CREDENTIALS[role])
        assert response.status_code == 200, f"Login failed: {response.text}"
        return response.json()["access_token"]

    return _get_token


@pytest.fixture
def auth_header(get_token):
    """Factory fixture that returns an Authorization header dict for a given role."""

    def _auth_header(role: str) -> dict[str, str]:
        token = get_token(role)
        return {"Authorization": f"Bearer {token}"}

    return _auth_header


@pytest.fixture
async def redis_client():
    """Async Redis client for inspecting/cleaning rate limit keys."""
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture(autouse=True)
async def clear_rate_limit_keys(redis_client: aioredis.Redis):
    """Clear rate limit keys before each test to prevent cross-test contamination."""
    try:
        keys = await redis_client.keys("ratelimit:*")
        if keys:
            await redis_client.delete(*keys)
    except Exception:
        pass  # Redis may be intentionally down for resilience tests
    yield


# ============================================================
# AUTHENTICATION TESTS
# ============================================================


class TestAuthentication:
    """Tests for JWT token issuance and validation."""

    def test_login_success(self, client: httpx.Client):
        """Valid credentials return a token with expected fields."""
        response = client.post("/api/v1/auth/token", json=CREDENTIALS["operator"])
        assert response.status_code == 200
        body = response.json()
        assert "access_token" in body
        assert body["token_type"] == "bearer"
        assert body["role"] == "operator"
        assert body["expires_in"] > 0

    def test_login_invalid_password(self, client: httpx.Client):
        """Wrong password returns 401."""
        response = client.post(
            "/api/v1/auth/token",
            json={"username": "operator", "password": "wrongpassword"},
        )
        assert response.status_code == 401
        assert "Invalid username or password" in response.json()["detail"]

    def test_login_unknown_user(self, client: httpx.Client):
        """Unknown username returns 401."""
        response = client.post(
            "/api/v1/auth/token",
            json={"username": "nonexistent", "password": "password123"},
        )
        assert response.status_code == 401

    def test_unauthenticated_request(self, client: httpx.Client):
        """Request without Authorization header returns 401."""
        response = client.get("/api/v1/telemetry")
        assert response.status_code == 401
        assert "Authentication required" in response.json()["detail"]

    def test_invalid_token(self, client: httpx.Client):
        """Malformed token returns 401."""
        response = client.get(
            "/api/v1/telemetry",
            headers={"Authorization": "Bearer invalid.token.here"},
        )
        assert response.status_code == 401

    def test_expired_token(self, client: httpx.Client):
        """Expired token returns 401."""
        # Read the public key to get the private key path for crafting a token
        # We'll craft an already-expired token manually
        with open("keys/private.pem", "r") as f:
            private_key = f.read()

        expired_payload = {
            "sub": "operator",
            "role": "operator",
            "iat": datetime.now(timezone.utc) - timedelta(hours=2),
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),  # Expired 1 hour ago
        }
        expired_token = jwt.encode(expired_payload, private_key, algorithm="RS256")

        response = client.get(
            "/api/v1/telemetry",
            headers={"Authorization": f"Bearer {expired_token}"},
        )
        assert response.status_code == 401
        assert "expired" in response.json()["detail"].lower()

    def test_health_endpoint_no_auth_required(self, client: httpx.Client):
        """/health is accessible without authentication."""
        response = client.get("/health")
        assert response.status_code == 200


# ============================================================
# RBAC TESTS
# ============================================================


class TestRBAC:
    """Tests for role-based access control enforcement."""

    def test_viewer_can_get_telemetry(self, client: httpx.Client, auth_header):
        """Viewer role can access GET endpoints."""
        response = client.get("/api/v1/telemetry", headers=auth_header("viewer"))
        assert response.status_code == 200

    def test_viewer_can_get_anomalies(self, client: httpx.Client, auth_header):
        """Viewer role can access anomaly listing."""
        response = client.get("/api/v1/anomalies", headers=auth_header("viewer"))
        assert response.status_code == 200

    def test_viewer_cannot_post_telemetry(self, client: httpx.Client, auth_header):
        """Viewer role is forbidden from POST telemetry (requires operator)."""
        payload = {
            "device_id": "00000000-0000-0000-0000-000000000001",
            "metric_type": "temperature",
            "value": 25.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        response = client.post(
            "/api/v1/telemetry", json=payload, headers=auth_header("viewer")
        )
        assert response.status_code == 403
        assert "Insufficient permissions" in response.json()["detail"]

    def test_viewer_cannot_register_device(self, client: httpx.Client, auth_header):
        """Viewer role is forbidden from POST devices (requires operator)."""
        payload = {
            "device_name": "test-rbac-device",
            "device_type": "sensor",
            "location": "test-lab",
            "firmware_version": "1.0.0",
        }
        response = client.post(
            "/api/v1/devices", json=payload, headers=auth_header("viewer")
        )
        assert response.status_code == 403

    def test_operator_can_post_telemetry(self, client: httpx.Client, auth_header):
        """Operator role can POST telemetry (returns 202 or 404 for unknown device)."""
        payload = {
            "device_id": "00000000-0000-0000-0000-000000000001",
            "metric_type": "temperature",
            "value": 25.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        response = client.post(
            "/api/v1/telemetry", json=payload, headers=auth_header("operator")
        )
        # 202 if device exists, 404 if not — either proves auth passed
        assert response.status_code in (202, 404)

    def test_operator_can_register_device(self, client: httpx.Client, auth_header):
        """Operator role can register devices."""
        payload = {
            "device_name": f"test-rbac-{time.time_ns()}",
            "device_type": "sensor",
            "location": "test-lab",
            "firmware_version": "1.0.0",
        }
        response = client.post(
            "/api/v1/devices", json=payload, headers=auth_header("operator")
        )
        assert response.status_code == 201

    def test_admin_has_full_access(self, client: httpx.Client, auth_header):
        """Admin role can access all endpoints."""
        # GET (viewer-level)
        response = client.get("/api/v1/telemetry", headers=auth_header("admin"))
        assert response.status_code == 200

        # POST device (operator-level)
        payload = {
            "device_name": f"test-admin-{time.time_ns()}",
            "device_type": "gateway",
            "location": "admin-test",
            "firmware_version": "2.0.0",
        }
        response = client.post(
            "/api/v1/devices", json=payload, headers=auth_header("admin")
        )
        assert response.status_code == 201


# ============================================================
# RATE LIMITING TESTS
# ============================================================


class TestRateLimiting:
    """Tests for Redis sliding window rate limiting."""

    def test_rate_limit_exceeded(self, client: httpx.Client, auth_header):
        """
        Exceeding the query rate limit returns 429.
        Query limit is 100/min for viewers — we send 105 requests.
        """
        headers = auth_header("viewer")

        status_codes = []
        for _ in range(105):
            response = client.get("/api/v1/anomalies", headers=headers)
            status_codes.append(response.status_code)

        # First 100 should succeed, subsequent should be 429
        assert 200 in status_codes, "Some requests should succeed"
        assert 429 in status_codes, "Rate limit should trigger"

        # Verify the 429 response has Retry-After header
        final_response = client.get("/api/v1/anomalies", headers=headers)
        if final_response.status_code == 429:
            assert "Retry-After" in final_response.headers

    def test_rate_limit_per_user(self, client: httpx.Client, get_token):
        """Different users have independent rate limit buckets."""
        viewer_headers = {"Authorization": f"Bearer {get_token('viewer')}"}
        admin_headers = {"Authorization": f"Bearer {get_token('admin')}"}

        # Exhaust viewer's limit
        for _ in range(105):
            client.get("/api/v1/anomalies", headers=viewer_headers)

        # Admin should still be able to make requests
        response = client.get("/api/v1/anomalies", headers=admin_headers)
        assert response.status_code == 200


# ============================================================
# RESILIENCE TESTS
# ============================================================


class TestResilience:
    """
    Tests for graceful degradation.

    NOTE: These tests require manually stopping/starting Docker containers.
    Run them individually with manual intervention, or skip in CI.
    Mark with pytest.mark.manual for selective execution.
    """

    @pytest.mark.manual
    def test_redis_down_api_still_serves(self, client: httpx.Client, auth_header):
        """
        When Redis is down, GET endpoints still return data from PostgreSQL.
        Manual step: run `docker stop tip-redis` before this test.
        """
        # First, verify Redis is actually unreachable
        import redis
        r = redis.Redis(host="localhost", port=6379, socket_connect_timeout=2)
        with pytest.raises(redis.ConnectionError):
            r.ping()

        # Now verify the API still responds successfully
        headers = auth_header("viewer")
        response = client.get("/api/v1/telemetry?limit=3", headers=headers)
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.manual
    def test_kafka_down_returns_503(self, client: httpx.Client, auth_header):
        """
        When Kafka is down, POST telemetry returns 503.

        Manual step: run `docker stop tip-kafka` before this test.
        """
        headers = auth_header("operator")

        # Register a device first (this only hits PostgreSQL, not Kafka)
        device_payload = {
            "device_name": f"test-kafka-resilience-{time.time_ns()}",
            "device_type": "sensor",
            "location": "test-lab",
            "firmware_version": "1.0.0",
        }
        device_response = client.post("/api/v1/devices", json=device_payload, headers=headers)
        assert device_response.status_code == 201
        device_id = device_response.json()["data"]["device_id"]

        # Now stop Kafka, then attempt telemetry ingestion
        payload = {
            "device_id": device_id,
            "metric_type": "temperature",
            "value": 25.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        response = client.post("/api/v1/telemetry", json=payload, headers=headers)
        assert response.status_code == 503
        assert "Retry-After" in response.headers
