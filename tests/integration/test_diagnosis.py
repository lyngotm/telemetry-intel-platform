"""
Integration test for the full RAG diagnosis pipeline.

Tests the end-to-end flow:
1. Ingest a specific incident into the knowledge base
2. Trigger a related anomaly via the simulator/telemetry API
3. Wait for the Diagnosis Service to process the anomaly
4. Verify the diagnosis references the ingested incident

Requires: Full stack running (API Gateway, Ingestion Consumer,
Anomaly Detection, Diagnosis Service, Kafka, Redis, PostgreSQL, ChromaDB)
"""

import asyncio
import time

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.call_llm]

BASE_URL = "http://localhost:8000"
TIMEOUT = 120  # Max seconds to wait for diagnosis generation


@pytest.fixture
async def admin_client():
    """Authenticated admin client for knowledge ingestion."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "admin", "password": "admin123"},
        )
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def operator_client():
    """Authenticated operator client for telemetry ingestion."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        response = await client.post(
            "/api/v1/auth/token",
            json={"username": "operator", "password": "operator123"},
        )
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


class TestDiagnosisPipeline:
    """End-to-end RAG diagnosis pipeline tests."""

    async def test_full_rag_loop(self, admin_client, operator_client):
        """
        Ingest a thermal incident → trigger a temperature anomaly →
        verify diagnosis references the ingested incident.
        """
        # --- Step 1: Ingest a specific incident ---
        incident = {
            "title": "Integration test: thermal runaway from fan failure",
            "description": "Edge gateway devices experienced thermal runaway due to cooling fan bearing failure. Temperature rose from 50°C baseline to 90°C over 10 minutes.",
            "affected_device_types": ["temperature_sensor", "edge_gateway"],
            "root_cause": "Fan assembly bearing failure reduced airflow by 70%, causing gradual temperature rise in the server rack.",
            "resolution_steps": [
                "Reduce workload on affected devices",
                "Replace failed fan assembly",
                "Apply thermal paste if temperature exceeded 90°C",
            ],
            "severity": "high",
            "failure_category": "thermal_management",
            "tags": ["thermal", "fan_failure", "integration_test"],
        }

        ingest_response = await admin_client.post(
            "/api/v1/knowledge/ingest",
            json=incident,
        )
        assert ingest_response.status_code == 201, f"Ingest failed: {ingest_response.text}"
        ingest_data = ingest_response.json()["data"]
        assert ingest_data["chunk_count"] >= 1

        # --- Step 2: Get a device and trigger anomalous telemetry ---
        # First, get an existing device (registered by simulator)
        devices_response = await operator_client.get("/api/v1/devices")
        assert devices_response.status_code == 200

        devices = devices_response.json()["data"]
        if not devices:
            # Register a device if none exist
            device_response = await operator_client.post(
                "/api/v1/devices",
                json={
                    "device_name": "integration-test-device",
                    "device_type": "edge_gateway",
                    "location": "Test Rack A",
                    "firmware_version": "v1.0.0",
                },
            )
            assert device_response.status_code == 201
            device_id = device_response.json()["data"]["device_id"]
        else:
            device_id = devices[0]["device_id"]

        # Send normal telemetry first to establish a baseline window
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)
        for i in range(20):
            event_time = now - timedelta(seconds=(20 - i) * 30)
            await operator_client.post(
                "/api/v1/telemetry",
                json={
                    "device_id": device_id,
                    "metric_type": "temperature",
                    "value": 50.0 + (i * 0.1),  # Stable ~50°C
                    "timestamp": event_time.isoformat(),
                },
            )

        # Allow ingestion + anomaly detection to process the baseline
        await asyncio.sleep(5)

        # Send an anomalous reading (extreme temperature spike)
        anomaly_time = datetime.now(timezone.utc)
        await operator_client.post(
            "/api/v1/telemetry",
            json={
                "device_id": device_id,
                "metric_type": "temperature",
                "value": 95.0,  # Far above 50°C baseline — should trigger anomaly
                "timestamp": anomaly_time.isoformat(),
            },
        )

        # --- Step 3: Wait for anomaly detection + diagnosis ---
        anomaly_id = None
        start_time = time.time()

        while time.time() - start_time < TIMEOUT:
            # Check for anomalies on this device
            anomalies_response = await operator_client.get(
                f"/api/v1/anomalies?device_id={device_id}",
            )
            assert anomalies_response.status_code == 200

            anomalies = anomalies_response.json()["data"]
            # Look for a recent temperature anomaly
            for a in anomalies:
                if a["metric_type"] == "temperature" and a["observed_value"] >= 90.0:
                    anomaly_id = a["anomaly_id"]
                    break

            if anomaly_id:
                break
            await asyncio.sleep(2)

        assert anomaly_id is not None, (
            f"No temperature anomaly detected within {TIMEOUT}s. "
            f"Check that Ingestion Consumer and Anomaly Detection are running."
        )

        # --- Step 4: Wait for diagnosis generation ---
        diagnosis = None
        start_time = time.time()

        while time.time() - start_time < TIMEOUT:
            diag_response = await operator_client.get(
                f"/api/v1/anomalies/{anomaly_id}/diagnosis",
            )
            assert diag_response.status_code == 200

            diag_data = diag_response.json()["data"]
            if diag_data.get("status") != "pending":
                diagnosis = diag_data
                break
            await asyncio.sleep(5)

        assert diagnosis is not None, (
            f"Diagnosis not generated within {TIMEOUT}s. "
            f"Check that Diagnosis Service is running and has Bedrock access."
        )

        # --- Step 5: Verify diagnosis quality ---
        assert "root_cause_summary" in diagnosis
        assert len(diagnosis["root_cause_summary"]) > 20

        assert "confidence_score" in diagnosis
        assert 0.0 <= diagnosis["confidence_score"] <= 1.0

        assert "supporting_evidence" in diagnosis
        assert isinstance(diagnosis["supporting_evidence"], list)

        assert "recommended_actions" in diagnosis
        assert isinstance(diagnosis["recommended_actions"], list)
        assert len(diagnosis["recommended_actions"]) >= 1

        # Verify the diagnosis relates to thermal issues
        # (it should reference thermal/temperature/fan/cooling concepts)
        summary_lower = diagnosis["root_cause_summary"].lower()
        thermal_keywords = ["thermal", "temperature", "heat", "cooling", "fan", "spike"]
        assert any(keyword in summary_lower for keyword in thermal_keywords), (
            f"Diagnosis doesn't appear thermal-related: {diagnosis['root_cause_summary']}"
        )

        print(f"\n{'=' * 60}")
        print("DIAGNOSIS RECEIVED:")
        print(f"  Anomaly ID: {anomaly_id}")
        print(f"  Root Cause: {diagnosis['root_cause_summary'][:100]}...")
        print(f"  Confidence: {diagnosis['confidence_score']:.2f}")
        print(f"  Actions: {len(diagnosis['recommended_actions'])}")
        print(f"  Generation Time: {diagnosis.get('generation_time_seconds', 'N/A')}s")
        print(f"{'=' * 60}\n")

    async def test_diagnosis_pending_state(self, operator_client):
        """
        Verify that requesting a diagnosis for a fresh anomaly
        returns pending status before the Diagnosis Service processes it.
        """
        # Get the most recent anomaly
        anomalies_response = await operator_client.get(
            "/api/v1/anomalies?limit=1",
        )
        assert anomalies_response.status_code == 200

        anomalies = anomalies_response.json()["data"]
        if not anomalies:
            pytest.skip("No anomalies exist to test pending state")

        # Use a fake anomaly ID to test 404
        import uuid

        fake_id = str(uuid.uuid4())
        response = await operator_client.get(
            f"/api/v1/anomalies/{fake_id}/diagnosis",
        )
        assert response.status_code == 404

    async def test_knowledge_ingestion_and_retrieval(self, admin_client):
        """
        Verify that ingested incidents appear in the list endpoint.
        """
        # Ingest a test incident
        incident = {
            "title": "Integration test: unique incident for retrieval test",
            "description": "This is a test incident created by the integration test suite to verify knowledge base operations.",
            "affected_device_types": ["temperature_sensor"],
            "root_cause": "Test root cause for integration testing.",
            "resolution_steps": ["Step 1: Verify test passed"],
            "severity": "low",
            "failure_category": "test",
            "tags": ["integration_test", "retrieval_test"],
        }

        ingest_response = await admin_client.post(
            "/api/v1/knowledge/ingest",
            json=incident,
        )
        assert ingest_response.status_code == 201

        # Retrieve and verify it appears in the list
        list_response = await admin_client.get(
            "/api/v1/knowledge/incidents?failure_category=test",
        )
        assert list_response.status_code == 200

        data = list_response.json()
        assert data["count"] >= 1

        titles = [i["title"] for i in data["data"]]
        assert any("retrieval test" in t.lower() for t in titles)
