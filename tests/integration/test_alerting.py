"""
Integration tests for the alerting stack.
Requires: Docker infrastructure running (docker compose up -d).

Tests verify:
1. Alert receiver webhook endpoint accepts Alertmanager payloads
2. Alert state is persisted to PostgreSQL
3. Firing → resolved state transitions work correctly
4. Monitor CRUD API creates/lists/deletes rules
5. AI triage is generated for critical alerts
6. Metrics are exposed on the metrics port

Run with:
    PYTHONPATH=. uv run pytest tests/integration/test_alerting.py -v
"""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import httpx
import pytest

pytestmark = pytest.mark.integration

# Alert receiver service URLs
ALERT_RECEIVER_URL = "http://localhost:9096"
ALERT_RECEIVER_METRICS_URL = "http://localhost:9097"
POSTGRES_DSN = "postgresql://telemetry_user:telemetry_pass@localhost:5432/telemetry"


@pytest.fixture
async def alert_client():
    """Provides an async HTTP client for the Alert Receiver API."""
    async with httpx.AsyncClient(base_url=ALERT_RECEIVER_URL, timeout=30.0) as client:
        yield client


@pytest.fixture
async def db_connection():
    """Provides a direct database connection for verification."""
    conn = await asyncpg.connect(POSTGRES_DSN)
    yield conn
    await conn.close()


def _make_alertmanager_payload(
    alert_name: str = "TestAlert",
    status: str = "firing",
    severity: str = "warning",
    pipeline: str = "test",
    fingerprint: str | None = None,
) -> dict:
    """Build an Alertmanager webhook payload for testing."""
    if fingerprint is None:
        fingerprint = str(uuid4())[:16]

    now = datetime.now(timezone.utc).isoformat()

    return {
        "version": "4",
        "groupKey": f"test-group-{alert_name}",
        "truncatedAlerts": 0,
        "status": status,
        "receiver": "webhook-all",
        "groupLabels": {"alertname": alert_name},
        "commonLabels": {
            "alertname": alert_name,
            "severity": severity,
            "pipeline": pipeline,
            "tier": "platform",
        },
        "commonAnnotations": {
            "summary": f"Test alert: {alert_name}",
            "description": f"Integration test alert for {alert_name}",
        },
        "externalURL": "http://alertmanager:9093",
        "alerts": [
            {
                "status": status,
                "labels": {
                    "alertname": alert_name,
                    "severity": severity,
                    "pipeline": pipeline,
                    "tier": "platform",
                },
                "annotations": {
                    "summary": f"Test alert: {alert_name}",
                    "description": f"Integration test alert for {alert_name}",
                    "runbook_url": "https://wiki.internal/test",
                },
                "startsAt": now,
                "endsAt": "0001-01-01T00:00:00Z" if status == "firing" else now,
                "generatorURL": "http://prometheus:9090/graph?test",
                "fingerprint": fingerprint,
            }
        ],
    }


# ============================================================
# Test: Health & Readiness
# ============================================================


class TestAlertReceiverHealth:
    """Verify service is running and healthy."""

    async def test_health_endpoint(self, alert_client: httpx.AsyncClient):
        response = await alert_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    async def test_readiness_endpoint(self, alert_client: httpx.AsyncClient):
        response = await alert_client.get("/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"

    async def test_metrics_endpoint(self):
        """Verify Prometheus metrics are exposed on separate port."""
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{ALERT_RECEIVER_METRICS_URL}/metrics")
            assert response.status_code == 200
            body = response.text
            # Verify key metrics are present
            assert "alert_events_received_total" in body
            assert "alerts_currently_firing" in body
            assert "alert_webhook_requests_total" in body


# ============================================================
# Test: Webhook Alert Ingestion
# ============================================================


class TestWebhookIngestion:
    """Verify the webhook endpoint accepts and persists alert payloads."""

    async def test_firing_alert_accepted(self, alert_client: httpx.AsyncClient):
        """POST a firing alert and verify 202 response."""
        payload = _make_alertmanager_payload(
            alert_name="IntegrationTestFiring",
            status="firing",
            severity="warning",
        )
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["processed"] == 1
        assert data["errors"] == 0

    async def test_alert_persisted_to_db(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """Verify the alert is written to PostgreSQL."""
        fingerprint = f"test-persist-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="PersistenceTest",
            status="firing",
            severity="critical",
            pipeline="detection",
            fingerprint=fingerprint,
        )
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202

        # Query DB directly
        row = await db_connection.fetchrow(
            "SELECT * FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert row is not None
        assert row["alert_name"] == "PersistenceTest"
        assert row["status"] == "firing"
        assert row["severity"] == "critical"
        assert row["pipeline"] == "detection"
        assert row["tier"] == "platform"

    async def test_resolved_alert_updates_state(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """Fire then resolve an alert — verify state transition."""
        fingerprint = f"test-resolve-{uuid4().hex[:8]}"

        # Fire
        payload = _make_alertmanager_payload(
            alert_name="ResolveTest",
            status="firing",
            fingerprint=fingerprint,
        )
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202

        # Verify firing
        row = await db_connection.fetchrow(
            "SELECT status FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert row["status"] == "firing"

        # Resolve
        payload = _make_alertmanager_payload(
            alert_name="ResolveTest",
            status="resolved",
            fingerprint=fingerprint,
        )
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202

        # Verify resolved
        row = await db_connection.fetchrow(
            "SELECT status, resolved_at FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert row["status"] == "resolved"
        assert row["resolved_at"] is not None

    async def test_duplicate_firing_deduplicated(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """Same fingerprint firing twice should not create duplicates."""
        fingerprint = f"test-dedup-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="DedupTest",
            status="firing",
            fingerprint=fingerprint,
        )

        # Send twice
        await alert_client.post("/alerts", json=payload)
        await alert_client.post("/alerts", json=payload)

        # Should only have one row
        count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert count == 1

    async def test_batch_alerts_processed(self, alert_client: httpx.AsyncClient):
        """Payload with multiple alerts in one webhook."""
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "version": "4",
            "groupKey": "batch-test",
            "truncatedAlerts": 0,
            "status": "firing",
            "receiver": "webhook-all",
            "groupLabels": {},
            "commonLabels": {},
            "commonAnnotations": {},
            "externalURL": "",
            "alerts": [
                {
                    "status": "firing",
                    "labels": {
                        "alertname": f"BatchAlert{i}",
                        "severity": "warning",
                        "pipeline": "test",
                        "tier": "platform",
                    },
                    "annotations": {"summary": f"Batch alert {i}"},
                    "startsAt": now,
                    "endsAt": "0001-01-01T00:00:00Z",
                    "generatorURL": "",
                    "fingerprint": f"batch-{uuid4().hex[:8]}",
                }
                for i in range(5)
            ],
        }
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert data["processed"] == 5
        assert data["errors"] == 0


# ============================================================
# Test: Query Endpoints
# ============================================================


class TestQueryEndpoints:
    """Verify alert query APIs work correctly."""

    async def test_list_alerts(self, alert_client: httpx.AsyncClient):
        """GET /alerts returns paginated results."""
        response = await alert_client.get("/alerts", params={"limit": 10})
        assert response.status_code == 200
        data = response.json()
        assert "alerts" in data
        assert "total" in data
        assert "limit" in data
        assert "offset" in data
        assert data["limit"] == 10

    async def test_list_alerts_filter_by_severity(self, alert_client: httpx.AsyncClient):
        """Filter alerts by severity."""
        response = await alert_client.get("/alerts", params={"severity": "critical"})
        assert response.status_code == 200
        data = response.json()
        for alert in data["alerts"]:
            assert alert["severity"] == "critical"

    async def test_active_alerts(self, alert_client: httpx.AsyncClient):
        """GET /alerts/active returns summary of firing alerts."""
        response = await alert_client.get("/alerts/active")
        assert response.status_code == 200
        data = response.json()
        assert "total_firing" in data
        assert "by_severity" in data
        assert "by_pipeline" in data
        assert "alerts" in data

    async def test_get_alert_by_id(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """GET /alerts/{id} returns a specific alert."""
        # Insert a known alert
        fingerprint = f"test-getid-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="GetByIdTest",
            status="firing",
            fingerprint=fingerprint,
        )
        await alert_client.post("/alerts", json=payload)

        # Get its ID from DB
        row = await db_connection.fetchrow(
            "SELECT id FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        alert_id = str(row["id"])

        # Query by ID
        response = await alert_client.get(f"/alerts/{alert_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["alert_name"] == "GetByIdTest"
        assert data["fingerprint"] == fingerprint

    async def test_get_nonexistent_alert_404(self, alert_client: httpx.AsyncClient):
        """GET /alerts/{bad_id} returns 404."""
        fake_id = str(uuid4())
        response = await alert_client.get(f"/alerts/{fake_id}")
        assert response.status_code == 404


# ============================================================
# Test: Monitor CRUD API
# ============================================================


class TestMonitorCRUD:
    """Verify the Monitor CRUD endpoints for programmatic alert rule management."""

    async def test_create_monitor(self, alert_client: httpx.AsyncClient):
        """POST /monitors creates a new custom alert rule."""
        response = await alert_client.post(
            "/monitors",
            json={
                "name": "IntegrationTestMonitor",
                "expr": "up == 0",
                "duration": "1m",
                "severity": "warning",
                "pipeline": "test",
                "summary": "Test monitor for integration tests",
            },
        )
        assert response.status_code == 201
        data = response.json()
        assert "monitor" in data
        assert "prometheus_reload" in data
        assert data["monitor"]["name"] == "IntegrationTestMonitor"
        assert data["monitor"]["expr"] == "up == 0"
        assert data["monitor"]["enabled"] is True
        return data["monitor"]["id"]

    async def test_list_monitors(self, alert_client: httpx.AsyncClient):
        """GET /monitors returns all custom monitors."""
        response = await alert_client.get("/monitors")
        assert response.status_code == 200
        data = response.json()
        assert "monitors" in data
        assert "total" in data
        assert data["total"] >= 0

    async def test_create_and_delete_monitor(self, alert_client: httpx.AsyncClient):
        """Full lifecycle: create → verify → delete → verify gone."""
        # Create
        response = await alert_client.post(
            "/monitors",
            json={
                "name": "DeleteMeMonitor",
                "expr": "sum(rate(http_requests_total[5m])) > 1000",
                "duration": "5m",
                "severity": "info",
                "pipeline": "api",
            },
        )
        assert response.status_code == 201
        monitor_id = response.json()["monitor"]["id"]

        # Verify it exists
        response = await alert_client.get(f"/monitors/{monitor_id}")
        assert response.status_code == 200
        assert response.json()["name"] == "DeleteMeMonitor"

        # Delete
        response = await alert_client.delete(f"/monitors/{monitor_id}")
        assert response.status_code == 200

        # Verify gone
        response = await alert_client.get(f"/monitors/{monitor_id}")
        assert response.status_code == 404

    async def test_update_monitor_enable_disable(self, alert_client: httpx.AsyncClient):
        """PATCH /monitors/{id} can toggle enabled state."""
        # Create enabled
        response = await alert_client.post(
            "/monitors",
            json={
                "name": "ToggleMonitor",
                "expr": "up == 0",
                "duration": "1m",
                "severity": "warning",
                "pipeline": "test",
            },
        )
        monitor_id = response.json()["monitor"]["id"]

        # Disable
        response = await alert_client.patch(
            f"/monitors/{monitor_id}",
            json={"enabled": False},
        )
        assert response.status_code == 200
        assert response.json()["monitor"]["enabled"] is False

        # Re-enable
        response = await alert_client.patch(
            f"/monitors/{monitor_id}",
            json={"enabled": True},
        )
        assert response.status_code == 200
        assert response.json()["monitor"]["enabled"] is True

        # Cleanup
        await alert_client.delete(f"/monitors/{monitor_id}")

    async def test_delete_nonexistent_monitor_404(self, alert_client: httpx.AsyncClient):
        """DELETE /monitors/{bad_id} returns 404."""
        response = await alert_client.delete("/monitors/nonexist")
        assert response.status_code == 404


# ============================================================
# Test: AI Triage
# ============================================================


class TestAITriage:
    """Verify triage is generated for critical alerts."""

    async def test_critical_alert_triggers_triage(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """A critical firing alert should generate a triage summary."""
        fingerprint = f"test-triage-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="ServiceDown",
            status="firing",
            severity="critical",
            pipeline="ingestion",
            fingerprint=fingerprint,
        )
        response = await alert_client.post("/alerts", json=payload)
        assert response.status_code == 202

        # Wait for background triage task to complete
        # (rule-based is instant, LLM would take longer)
        await asyncio.sleep(2)

        # Check triage was generated
        row = await db_connection.fetchrow(
            "SELECT triage_summary FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert row is not None
        assert row["triage_summary"] is not None
        assert len(row["triage_summary"]) > 50  # Should be a substantial summary
        assert "ingestion" in row["triage_summary"].lower()

    async def test_triage_endpoint_returns_summary(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """GET /alerts/{id}/triage returns the triage summary."""
        fingerprint = f"test-triage-ep-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="IngestionPipelineStalled",
            status="firing",
            severity="critical",
            pipeline="ingestion",
            fingerprint=fingerprint,
        )
        await alert_client.post("/alerts", json=payload)
        await asyncio.sleep(2)

        # Get alert ID
        row = await db_connection.fetchrow(
            "SELECT id FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        alert_id = str(row["id"])

        # Get triage via API
        response = await alert_client.get(f"/alerts/{alert_id}/triage")
        assert response.status_code == 200
        data = response.json()
        assert data["alert_name"] == "IngestionPipelineStalled"
        assert data["severity"] == "critical"
        assert len(data["triage_summary"]) > 50

    async def test_warning_alert_no_triage(
        self,
        alert_client: httpx.AsyncClient,
        db_connection: asyncpg.Connection,
    ):
        """Warning alerts should NOT trigger triage."""
        fingerprint = f"test-no-triage-{uuid4().hex[:8]}"
        payload = _make_alertmanager_payload(
            alert_name="HighAPILatency",
            status="firing",
            severity="warning",
            pipeline="api",
            fingerprint=fingerprint,
        )
        await alert_client.post("/alerts", json=payload)
        await asyncio.sleep(1)

        # Triage should NOT be generated
        row = await db_connection.fetchrow(
            "SELECT triage_summary FROM alert_events WHERE fingerprint = $1",
            fingerprint,
        )
        assert row is not None
        assert row["triage_summary"] is None
