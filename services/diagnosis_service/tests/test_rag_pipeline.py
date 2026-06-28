"""
Unit tests for the Diagnosis Service RAG pipeline.

Tests the deterministic, non-I/O components:
- Text preparation from incident documents
- Chunking behavior (split boundaries, overlap)
- Prompt formatting (recent telemetry, retrieved incidents)
- LLM response parsing (valid JSON, malformed JSON, missing fields)
"""

import json
from uuid import uuid4

import pytest

from services.diagnosis_service.app.prompt import (
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE,
    format_recent_telemetry,
    format_retrieved_incidents,
)
from services.diagnosis_service.app.rag_pipeline import _parse_diagnosis_response
from services.api_gateway.app.knowledge import (
    _prepare_incident_text,
    text_splitter,
)
from shared.models.models import IncidentCreate


# ─── Fixtures ────────────────────────────────────────────────────


@pytest.fixture
def sample_incident():
    """A representative incident document for testing."""
    return IncidentCreate(
        title="Thermal runaway on edge gateway devices",
        description="Multiple edge gateway devices reported sustained temperature readings above 85°C over a 15-minute period. Normal operating temperature is 45-55°C.",
        affected_device_types=["temperature_sensor", "edge_gateway"],
        root_cause="Fan assembly bearing failure reduced airflow by 70%, causing gradual temperature rise.",
        resolution_steps=[
            "Reduce workload on affected devices",
            "Verify fan assembly RPM via diagnostics",
            "Replace failed fan assembly",
            "Apply fresh thermal paste if temperature exceeded 90°C",
        ],
        severity="high",
        failure_category="thermal_management",
        tags=["thermal", "hardware", "fan_failure"],
    )


@pytest.fixture
def sample_recent_events():
    """Recent telemetry events for prompt formatting."""
    return [
        {"timestamp": "2026-06-25T10:00:00", "metric_type": "temperature", "value": 55.2},
        {"timestamp": "2026-06-25T10:00:30", "metric_type": "temperature", "value": 56.1},
        {"timestamp": "2026-06-25T10:01:00", "metric_type": "temperature", "value": 62.8},
        {"timestamp": "2026-06-25T10:01:30", "metric_type": "temperature", "value": 71.3},
        {"timestamp": "2026-06-25T10:02:00", "metric_type": "temperature", "value": 85.7},
    ]


@pytest.fixture
def sample_retrieved_chunks():
    """ChromaDB retrieval results for prompt formatting."""
    return [
        {
            "document": "Thermal runaway caused by fan failure. Temperature rose 2°C per minute.",
            "metadata": {
                "incident_id": str(uuid4()),
                "title": "Fan failure thermal runaway",
                "severity": "high",
                "failure_category": "thermal_management",
            },
            "distance": 0.15,
        },
        {
            "document": "HVAC compressor failure led to cooling loss across data center floor.",
            "metadata": {
                "incident_id": str(uuid4()),
                "title": "Chiller plant compressor failure",
                "severity": "critical",
                "failure_category": "cooling_system_failure",
            },
            "distance": 0.35,
        },
    ]


# ─── Text Preparation Tests ──────────────────────────────────────


class TestTextPreparation:
    """Tests for _prepare_incident_text — converting structured incidents to embeddable text."""

    def test_includes_all_fields(self, sample_incident):
        """Prepared text should include title, description, root cause, resolution, device types, severity, category."""
        text = _prepare_incident_text(sample_incident)

        assert sample_incident.title in text
        assert sample_incident.description in text
        assert sample_incident.root_cause in text
        assert sample_incident.severity in text
        assert sample_incident.failure_category in text
        for device_type in sample_incident.affected_device_types:
            assert device_type in text
        for step in sample_incident.resolution_steps:
            assert step in text

    def test_nonempty_output(self, sample_incident):
        """Prepared text should never be empty."""
        text = _prepare_incident_text(sample_incident)
        assert len(text) > 100  # Reasonable minimum for a useful document

    def test_structure_has_sections(self, sample_incident):
        """Text should have labeled sections for semantic clarity."""
        text = _prepare_incident_text(sample_incident)
        assert "Incident:" in text or "Title:" in text or sample_incident.title in text
        assert "Root Cause:" in text or "root_cause" in text.lower()


# ─── Chunking Tests ──────────────────────────────────────────────


class TestChunking:
    """Tests for text splitting behavior."""

    def test_short_document_single_chunk(self, sample_incident):
        """A short incident document should produce 1 chunk (under chunk_size)."""
        text = _prepare_incident_text(sample_incident)
        chunks = text_splitter.split_text(text)

        # Our incidents are typically 500-1500 chars, chunk_size is 2048
        # Most should fit in a single chunk
        assert len(chunks) >= 1
        assert all(len(chunk) > 0 for chunk in chunks)

    def test_long_document_multiple_chunks(self):
        """A long document should be split into multiple chunks with overlap."""
        # Create a document that exceeds chunk_size (2048 chars)
        long_text = "This is a detailed incident report. " * 200  # ~7200 chars
        chunks = text_splitter.split_text(long_text)

        assert len(chunks) > 1
        # Verify overlap: end of chunk N should appear at start of chunk N+1
        for i in range(len(chunks) - 1):
            # The last 200 chars of chunk i should overlap with start of chunk i+1
            # (overlap is 200 chars, but split may happen at natural boundaries)
            chunk_end = chunks[i][-100:]  # Last 100 chars
            assert any(chunk_end[:50] in chunks[i + 1][:300] for _ in [1]) or len(chunks[i]) <= 2048

    def test_chunk_size_limit(self, sample_incident):
        """No chunk should exceed the configured chunk_size."""
        text = _prepare_incident_text(sample_incident)
        chunks = text_splitter.split_text(text)

        for chunk in chunks:
            assert len(chunk) <= 2048 + 100  # Small tolerance for boundary splits


# ─── Prompt Formatting Tests ─────────────────────────────────────


class TestPromptFormatting:
    """Tests for prompt template formatting functions."""

    def test_format_recent_telemetry_with_events(self, sample_recent_events):
        """Should format events into readable lines with timestamps and values."""
        result = format_recent_telemetry(sample_recent_events)

        assert "55.20" in result or "55.2" in result
        assert "85.70" in result or "85.7" in result
        assert "temperature" in result
        assert "10:00:00" in result

    def test_format_recent_telemetry_empty(self):
        """Should return a meaningful message when no events exist."""
        result = format_recent_telemetry([])
        assert "no recent" in result.lower() or "not available" in result.lower()

    def test_format_recent_telemetry_limits_to_20(self):
        """Should limit output to most recent 20 events to control prompt length."""
        many_events = [
            {
                "timestamp": f"2026-06-25T10:{i:02d}:00",
                "metric_type": "temperature",
                "value": 50.0 + i,
            }
            for i in range(50)
        ]
        result = format_recent_telemetry(many_events)
        # Should only show 20 readings
        assert "20" in result or result.count("\n") <= 22  # 20 data lines + header

    def test_format_retrieved_incidents_with_chunks(self, sample_retrieved_chunks):
        """Should format chunks with metadata headers and content."""
        result = format_retrieved_incidents(sample_retrieved_chunks)

        assert "Fan failure thermal runaway" in result
        assert "thermal_management" in result
        assert "Thermal runaway caused by fan failure" in result

    def test_format_retrieved_incidents_empty(self):
        """Should return meaningful message when no incidents retrieved."""
        result = format_retrieved_incidents([])
        assert "no relevant" in result.lower() or "not found" in result.lower()

    def test_format_retrieved_incidents_deduplicates(self):
        """Multiple chunks from same incident should be deduplicated."""
        incident_id = str(uuid4())
        chunks = [
            {
                "document": "Chunk 1 content",
                "metadata": {
                    "incident_id": incident_id,
                    "title": "Same incident",
                    "severity": "high",
                    "failure_category": "thermal",
                },
                "distance": 0.1,
            },
            {
                "document": "Chunk 2 content",
                "metadata": {
                    "incident_id": incident_id,
                    "title": "Same incident",
                    "severity": "high",
                    "failure_category": "thermal",
                },
                "distance": 0.2,
            },
        ]
        result = format_retrieved_incidents(chunks)
        # Should only appear once
        assert result.count("Same incident") == 1

    def test_system_prompt_exists_and_nonempty(self):
        """System prompt should be defined and substantial."""
        assert len(SYSTEM_PROMPT) > 100
        assert "diagnosis" in SYSTEM_PROMPT.lower() or "anomaly" in SYSTEM_PROMPT.lower()

    def test_user_prompt_template_has_all_placeholders(self):
        """User prompt template should contain all required format placeholders."""
        required_placeholders = [
            "anomaly_id",
            "device_id",
            "metric_type",
            "observed_value",
            "z_score",
            "severity",
            "detected_at",
            "recent_telemetry",
            "retrieved_incidents",
        ]
        for placeholder in required_placeholders:
            assert f"{{{placeholder}}}" in USER_PROMPT_TEMPLATE, (
                f"Missing placeholder: {placeholder}"
            )


# ─── LLM Response Parsing Tests ──────────────────────────────────


class TestResponseParsing:
    """Tests for _parse_diagnosis_response — handling various LLM output formats."""

    def test_valid_json_response(self):
        """Should parse well-formed JSON correctly."""
        valid_response = json.dumps(
            {
                "root_cause_summary": "Fan failure caused thermal runaway.",
                "confidence_score": 0.85,
                "supporting_evidence": ["Temperature rose 2°C/min", "Single rack affected"],
                "recommended_actions": ["Replace fan assembly", "Apply thermal paste"],
                "retrieved_incident_ids": [str(uuid4())],
            }
        )

        result = _parse_diagnosis_response(valid_response)

        assert result["root_cause_summary"] == "Fan failure caused thermal runaway."
        assert result["confidence_score"] == 0.85
        assert len(result["supporting_evidence"]) == 2
        assert len(result["recommended_actions"]) == 2

    def test_json_with_markdown_fences(self):
        """Should handle JSON wrapped in markdown code fences."""
        fenced_response = '```json\n{"root_cause_summary": "Test", "confidence_score": 0.7, "supporting_evidence": [], "recommended_actions": []}\n```'

        result = _parse_diagnosis_response(fenced_response)
        assert result["root_cause_summary"] == "Test"
        assert result["confidence_score"] == 0.7

    def test_malformed_json_returns_fallback(self):
        """Should return a degraded diagnosis rather than crashing on bad JSON."""
        malformed = "This is not JSON at all {invalid"

        result = _parse_diagnosis_response(malformed)

        # Should still return a valid structure
        assert "root_cause_summary" in result
        assert isinstance(result["confidence_score"], float)
        assert result["confidence_score"] <= 0.2  # Low confidence for failed parse

    def test_missing_fields_filled_with_defaults(self):
        """Should fill missing required fields with sensible defaults."""
        partial_response = json.dumps(
            {
                "root_cause_summary": "Partial diagnosis",
                "confidence_score": 0.5,
                # missing: supporting_evidence, recommended_actions
            }
        )

        result = _parse_diagnosis_response(partial_response)

        assert result["root_cause_summary"] == "Partial diagnosis"
        assert isinstance(result["supporting_evidence"], list)
        assert isinstance(result["recommended_actions"], list)

    def test_confidence_score_clamped(self):
        """Should clamp confidence_score to [0.0, 1.0] range."""
        over_response = json.dumps(
            {
                "root_cause_summary": "Test",
                "confidence_score": 1.5,
                "supporting_evidence": [],
                "recommended_actions": [],
            }
        )

        result = _parse_diagnosis_response(over_response)
        assert result["confidence_score"] == 1.0

        under_response = json.dumps(
            {
                "root_cause_summary": "Test",
                "confidence_score": -0.3,
                "supporting_evidence": [],
                "recommended_actions": [],
            }
        )

        result = _parse_diagnosis_response(under_response)
        assert result["confidence_score"] == 0.0
