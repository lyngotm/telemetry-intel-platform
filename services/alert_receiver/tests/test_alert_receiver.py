"""
Unit tests for the Alert Receiver Service.

Run with:
    PYTHONPATH=. uv run pytest services/alert_receiver/tests/ -v
"""

from datetime import datetime, timezone

import pytest

from services.alert_receiver.app.database import _parse_alertmanager_timestamp
from services.alert_receiver.app.models import (
    AlertmanagerAlert,
    AlertmanagerWebhookPayload,
    AlertStatus,
)
from services.alert_receiver.app.monitors import MonitorCreate, MonitorUpdate
from services.alert_receiver.app.triage import (
    PLATFORM_ALERTS,
    PRODUCT_ALERTS,
    _build_llm_prompt,
    _generate_rule_based_triage,
)

# ============================================================
# Test: Alertmanager Payload Parsing
# ============================================================


class TestAlertmanagerPayloadParsing:
    """Verify Alertmanager webhook payloads are parsed correctly."""

    def test_minimal_payload_parses(self):
        """A minimal valid payload should parse without errors."""
        payload = AlertmanagerWebhookPayload(
            status=AlertStatus.firing,
            alerts=[],
        )
        assert payload.status == AlertStatus.firing
        assert payload.alerts == []
        assert payload.version == "4"

    def test_full_payload_parses(self):
        """A complete Alertmanager payload with all fields."""
        raw = {
            "version": "4",
            "groupKey": "test-group",
            "truncatedAlerts": 0,
            "status": "firing",
            "receiver": "webhook-all",
            "groupLabels": {"alertname": "TestAlert"},
            "commonLabels": {"severity": "critical"},
            "commonAnnotations": {"summary": "test"},
            "externalURL": "http://alertmanager:9093",
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "TestAlert", "severity": "critical"},
                    "annotations": {"summary": "test summary"},
                    "startsAt": "2026-07-29T10:00:00.000Z",
                    "endsAt": "0001-01-01T00:00:00Z",
                    "generatorURL": "http://prometheus:9090/graph",
                    "fingerprint": "abc123def456",
                }
            ],
        }
        payload = AlertmanagerWebhookPayload(**raw)
        assert payload.status == AlertStatus.firing
        assert len(payload.alerts) == 1
        assert payload.alerts[0].labels["alertname"] == "TestAlert"
        assert payload.alerts[0].fingerprint == "abc123def456"

    def test_resolved_status_parses(self):
        """Resolved alerts have a valid endsAt timestamp."""
        alert = AlertmanagerAlert(
            status=AlertStatus.resolved,
            labels={"alertname": "Test"},
            annotations={},
            startsAt="2026-07-29T10:00:00Z",
            endsAt="2026-07-29T10:05:00Z",
            fingerprint="resolved123",
        )
        assert alert.status == AlertStatus.resolved
        assert alert.endsAt == "2026-07-29T10:05:00Z"

    def test_missing_optional_fields_use_defaults(self):
        """Missing optional fields should use sensible defaults."""
        alert = AlertmanagerAlert(
            status=AlertStatus.firing,
            startsAt="2026-07-29T10:00:00Z",
            endsAt="0001-01-01T00:00:00Z",
        )
        assert alert.labels == {}
        assert alert.annotations == {}
        assert alert.fingerprint == ""
        assert alert.generatorURL == ""

    def test_multiple_alerts_in_batch(self):
        """Payload can contain multiple alerts."""
        payload = AlertmanagerWebhookPayload(
            status=AlertStatus.firing,
            alerts=[
                AlertmanagerAlert(
                    status=AlertStatus.firing,
                    labels={"alertname": f"Alert{i}"},
                    annotations={},
                    startsAt="2026-07-29T10:00:00Z",
                    endsAt="0001-01-01T00:00:00Z",
                    fingerprint=f"fp-{i}",
                )
                for i in range(10)
            ],
        )
        assert len(payload.alerts) == 10


# ============================================================
# Test: Timestamp Parsing
# ============================================================


class TestTimestampParsing:
    """Verify Alertmanager timestamp parsing handles edge cases."""

    def test_standard_iso_format(self):
        """Standard ISO 8601 with milliseconds and Z suffix."""
        result = _parse_alertmanager_timestamp("2026-07-29T10:30:00.000Z")
        assert result.year == 2026
        assert result.month == 7
        assert result.hour == 10
        assert result.minute == 30

    def test_nanosecond_precision_truncated(self):
        """Nanosecond precision is truncated to microseconds."""
        result = _parse_alertmanager_timestamp("2026-07-29T10:30:00.123456789Z")
        assert result.year == 2026
        assert result.microsecond == 123456  # Truncated from 123456789

    def test_no_fractional_seconds(self):
        """Timestamps without fractional seconds are valid."""
        result = _parse_alertmanager_timestamp("2026-07-29T10:30:00Z")
        assert result.year == 2026
        assert result.second == 0

    def test_zero_time_alertmanager(self):
        """Alertmanager's 'not resolved' zero time parses without crashing."""
        result = _parse_alertmanager_timestamp("0001-01-01T00:00:00Z")
        assert result.year == 1  # Year 1 — caller checks this

    def test_malformed_timestamp_returns_now(self):
        """Unparseable timestamps fall back to current time."""
        result = _parse_alertmanager_timestamp("not-a-timestamp")
        # Should be close to now (within 5 seconds)
        now = datetime.now(timezone.utc)
        delta = abs((now - result).total_seconds())
        assert delta < 5

    def test_timezone_offset_format(self):
        """Timestamps with explicit timezone offset."""
        result = _parse_alertmanager_timestamp("2026-07-29T10:30:00.000+00:00")
        assert result.year == 2026
        assert result.hour == 10


# ============================================================
# Test: Triage Decision Logic
# ============================================================


class TestTriageDecisionLogic:
    """Verify triage correctly categorizes alerts and selects strategy."""

    def test_platform_alerts_identified(self):
        """All expected platform alerts are in the PLATFORM_ALERTS set."""
        expected = {
            "ServiceDown",
            "IngestionPipelineStalled",
            "DiagnosisPipelineDown",
            "AlertReceiverDown",
            "CascadingFailure",
            "FullPipelineStall",
            "KafkaIngestionFailure",
            "PipelineDegradation",
        }
        assert PLATFORM_ALERTS == expected

    def test_product_alerts_identified(self):
        """All expected product alerts are in the PRODUCT_ALERTS set."""
        expected = {
            "CriticalAnomalyBurst",
            "DetectionWithoutDiagnosis",
        }
        assert PRODUCT_ALERTS == expected

    def test_no_overlap_between_platform_and_product(self):
        """Platform and product alert sets should not overlap."""
        assert PLATFORM_ALERTS.isdisjoint(PRODUCT_ALERTS)


# ============================================================
# Test: Rule-Based Triage Templates
# ============================================================


class TestRuleBasedTriage:
    """Verify rule-based triage generates useful content for each alert type."""

    def _make_alert(self, alert_name: str, pipeline: str = "ingestion") -> AlertmanagerAlert:
        """Helper to create a test alert."""
        return AlertmanagerAlert(
            status=AlertStatus.firing,
            labels={
                "alertname": alert_name,
                "severity": "critical",
                "pipeline": pipeline,
                "tier": "platform",
            },
            annotations={
                "summary": f"Test: {alert_name}",
                "description": f"Test description for {alert_name}",
                "runbook_url": "https://wiki.internal/test",
            },
            startsAt="2026-07-29T10:00:00Z",
            endsAt="0001-01-01T00:00:00Z",
            fingerprint="test-fp",
        )

    def test_service_down_triage(self):
        """ServiceDown generates triage mentioning container/pod checks."""
        alert = self._make_alert("ServiceDown", pipeline="ingestion")
        result = _generate_rule_based_triage(alert)
        assert "ServiceDown" in result
        assert "container" in result.lower() or "pod" in result.lower()
        assert "Investigation" in result or "investigation" in result.lower()

    def test_ingestion_stalled_triage(self):
        """IngestionPipelineStalled mentions Kafka and consumer groups."""
        alert = self._make_alert("IngestionPipelineStalled")
        result = _generate_rule_based_triage(alert)
        assert "Kafka" in result or "kafka" in result
        assert "consumer" in result.lower()

    def test_diagnosis_pipeline_down_triage(self):
        """DiagnosisPipelineDown mentions LLM/ChromaDB."""
        alert = self._make_alert("DiagnosisPipelineDown", pipeline="diagnosis")
        result = _generate_rule_based_triage(alert)
        assert "ChromaDB" in result or "LLM" in result or "Bedrock" in result or "API" in result

    def test_cascading_failure_triage(self):
        """CascadingFailure mentions downstream and propagation."""
        alert = self._make_alert("CascadingFailure")
        result = _generate_rule_based_triage(alert)
        assert "downstream" in result.lower() or "propagat" in result.lower()

    def test_full_pipeline_stall_triage(self):
        """FullPipelineStall mentions Kafka and Zookeeper."""
        alert = self._make_alert("FullPipelineStall")
        result = _generate_rule_based_triage(alert)
        assert "Kafka" in result or "kafka" in result

    def test_pipeline_degradation_triage(self):
        """PipelineDegradation mentions Redis and enrichment."""
        alert = self._make_alert("PipelineDegradation", pipeline="detection")
        result = _generate_rule_based_triage(alert)
        assert "Redis" in result or "redis" in result.lower()

    def test_kafka_ingestion_failure_triage(self):
        """KafkaIngestionFailure mentions broker and disk."""
        alert = self._make_alert("KafkaIngestionFailure")
        result = _generate_rule_based_triage(alert)
        assert "Kafka" in result or "broker" in result.lower()

    def test_unknown_alert_gets_generic_triage(self):
        """An unrecognized alert name gets a generic but useful triage."""
        alert = self._make_alert("SomeNewAlert", pipeline="custom")
        result = _generate_rule_based_triage(alert)
        assert "SomeNewAlert" in result
        assert "custom" in result
        assert len(result) > 100  # Should be substantial

    def test_all_platform_alerts_have_templates(self):
        """Every platform alert produces non-empty triage."""
        for alert_name in PLATFORM_ALERTS:
            alert = self._make_alert(alert_name)
            result = _generate_rule_based_triage(alert)
            assert len(result) > 100, f"{alert_name} produced too-short triage: {len(result)} chars"
            assert "--" in result, f"{alert_name} missing section separators"

    def test_triage_has_structured_sections(self):
        """Triage output contains expected sections."""
        alert = self._make_alert("ServiceDown")
        result = _generate_rule_based_triage(alert)
        assert "Root Cause" in result
        assert "Investigation" in result
        assert "Blast Radius" in result
        assert "Action" in result


# ============================================================
# Test: LLM Prompt Construction
# ============================================================


class TestLLMPrompt:
    """Verify the LLM triage prompt is well-structured."""

    def _make_alert(self) -> AlertmanagerAlert:
        return AlertmanagerAlert(
            status=AlertStatus.firing,
            labels={
                "alertname": "ServiceDown",
                "severity": "critical",
                "pipeline": "ingestion",
                "tier": "platform",
            },
            annotations={
                "summary": "Service ingestion-consumer is DOWN",
                "description": "The service has been unreachable for 1 minute.",
                "runbook_url": "https://wiki.internal/runbooks/service-down",
            },
            startsAt="2026-07-29T10:00:00Z",
            endsAt="0001-01-01T00:00:00Z",
            fingerprint="test-llm",
        )

    def test_prompt_contains_alert_details(self):
        """Prompt includes alert name, severity, pipeline."""
        alert = self._make_alert()
        prompt = _build_llm_prompt(alert)
        assert "ServiceDown" in prompt
        assert "critical" in prompt
        assert "ingestion" in prompt

    def test_prompt_contains_system_context(self):
        """Prompt includes platform architecture context."""
        alert = self._make_alert()
        prompt = _build_llm_prompt(alert)
        assert "Kafka" in prompt
        assert "PostgreSQL" in prompt
        assert "Redis" in prompt

    def test_prompt_contains_description(self):
        """Prompt includes the alert description."""
        alert = self._make_alert()
        prompt = _build_llm_prompt(alert)
        assert "unreachable" in prompt

    def test_prompt_contains_runbook(self):
        """Prompt includes the runbook URL."""
        alert = self._make_alert()
        prompt = _build_llm_prompt(alert)
        assert "runbooks/service-down" in prompt

    def test_prompt_requests_structured_output(self):
        """Prompt asks for specific sections in the response."""
        alert = self._make_alert()
        prompt = _build_llm_prompt(alert)
        assert "ROOT CAUSE" in prompt
        assert "INVESTIGATION" in prompt
        assert "BLAST RADIUS" in prompt
        assert "IMMEDIATE ACTIONS" in prompt


# ============================================================
# Test: Monitor Rules File Generation
# ============================================================


class TestMonitorRulesGeneration:
    """Verify custom rules YAML generation logic."""

    def test_monitor_create_model_defaults(self):
        """MonitorCreate has correct defaults."""
        monitor = MonitorCreate(name="Test", expr="up == 0")
        assert monitor.duration == "5m"
        assert monitor.severity == "warning"
        assert monitor.pipeline == "custom"
        assert monitor.enabled is True

    def test_monitor_create_validation(self):
        """MonitorCreate requires name and expr."""
        with pytest.raises(Exception):
            MonitorCreate()  # Missing required fields

    def test_monitor_update_all_fields_optional(self):
        """MonitorUpdate allows partial updates."""
        # Only updating enabled
        update = MonitorUpdate(enabled=False)
        fields = update.model_dump(exclude_none=True)
        assert fields == {"enabled": False}

        # Empty update
        update = MonitorUpdate()
        fields = update.model_dump(exclude_none=True)
        assert fields == {}
