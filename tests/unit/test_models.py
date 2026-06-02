"""
Unit tests for shared Pydantic models.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared.models.models import (
    DeviceCreate,
    TelemetryEventCreate,
    TelemetryRawMessage,
    TelemetryEnrichedMessage,
)


# ============================================================
# DeviceCreate Tests
# ============================================================

class TestDeviceCreate:
    """Tests for the DeviceCreate model."""

    def test_valid_device_all_fields(self):
        """All fields provided — should succeed."""
        device = DeviceCreate(
            device_name="sensor-floor-3-east",
            device_type="temperature_sensor",
            location="Building A, Floor 3",
            firmware_version="v2.1.0",
            metadata={"calibrated": True},
        )
        assert device.device_name == "sensor-floor-3-east"
        assert device.device_type == "temperature_sensor"
        assert device.metadata == {"calibrated": True}

    def test_valid_device_minimal_fields(self):
        """Only required fields — should succeed."""
        device = DeviceCreate(
            device_name="minimal-device",
            device_type="generic",
        )
        assert device.location is None
        assert device.firmware_version is None
        assert device.metadata == {}

    def test_invalid_device_empty_name(self):
        """Empty device_name should fail min_length validation."""
        with pytest.raises(ValidationError) as exc_info:
            DeviceCreate(device_name="", device_type="sensor")
        assert "device_name" in str(exc_info.value)

    def test_invalid_device_missing_type(self):
        """Missing required device_type should fail."""
        with pytest.raises(ValidationError) as exc_info:
            DeviceCreate(device_name="test-device")
        assert "device_type" in str(exc_info.value)

    def test_invalid_device_name_too_long(self):
        """device_name exceeding max_length should fail."""
        with pytest.raises(ValidationError):
            DeviceCreate(device_name="x" * 256, device_type="sensor")


# ============================================================
# TelemetryEventCreate Tests
# ============================================================

class TestTelemetryEventCreate:
    """Tests for the TelemetryEventCreate model."""

    def test_valid_event(self):
        """Standard valid telemetry event."""
        event = TelemetryEventCreate(
            device_id=uuid4(),
            metric_type="temperature",
            value=23.5,
            timestamp=datetime.now(timezone.utc),
        )
        assert event.value == 23.5
        assert event.metric_type == "temperature"
        assert event.metadata == {}

    def test_valid_event_with_metadata(self):
        """Event with optional metadata field."""
        event = TelemetryEventCreate(
            device_id=uuid4(),
            metric_type="humidity",
            value=45.0,
            timestamp=datetime.now(timezone.utc),
            metadata={"unit": "percent", "sensor_id": "A3"},
        )
        assert event.metadata["unit"] == "percent"

    def test_all_valid_metric_types(self):
        """Each valid metric type should be accepted."""
        for metric in ["temperature", "humidity", "pressure", "cpu_usage"]:
            event = TelemetryEventCreate(
                device_id=uuid4(),
                metric_type=metric,
                value=50.0,
                timestamp=datetime.now(timezone.utc),
            )
            assert event.metric_type == metric

    def test_invalid_metric_type(self):
        """Unknown metric_type should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEventCreate(
                device_id=uuid4(),
                metric_type="invalid_metric",
                value=10.0,
                timestamp=datetime.now(timezone.utc),
            )
        assert "metric_type" in str(exc_info.value)

    def test_invalid_value_nan(self):
        """NaN value should be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEventCreate(
                device_id=uuid4(),
                metric_type="temperature",
                value=float("nan"),
                timestamp=datetime.now(timezone.utc),
            )
        assert "value" in str(exc_info.value).lower() or "finite" in str(exc_info.value).lower()

    def test_invalid_value_infinity(self):
        """Infinity value should be rejected."""
        with pytest.raises(ValidationError):
            TelemetryEventCreate(
                device_id=uuid4(),
                metric_type="temperature",
                value=float("inf"),
                timestamp=datetime.now(timezone.utc),
            )

    def test_invalid_value_negative_infinity(self):
        """Negative infinity should be rejected."""
        with pytest.raises(ValidationError):
            TelemetryEventCreate(
                device_id=uuid4(),
                metric_type="temperature",
                value=float("-inf"),
                timestamp=datetime.now(timezone.utc),
            )

    def test_missing_device_id(self):
        """Missing required device_id should fail."""
        with pytest.raises(ValidationError):
            TelemetryEventCreate(
                metric_type="temperature",
                value=23.5,
                timestamp=datetime.now(timezone.utc),
            )

    def test_invalid_device_id_format(self):
        """Non-UUID device_id should fail."""
        with pytest.raises(ValidationError):
            TelemetryEventCreate(
                device_id="not-a-uuid",
                metric_type="temperature",
                value=23.5,
                timestamp=datetime.now(timezone.utc),
            )

    def test_negative_value_is_valid(self):
        """Negative numbers are valid (e.g., temperature below zero)."""
        event = TelemetryEventCreate(
            device_id=uuid4(),
            metric_type="temperature",
            value=-15.3,
            timestamp=datetime.now(timezone.utc),
        )
        assert event.value == -15.3

    def test_zero_value_is_valid(self):
        """Zero is a valid reading."""
        event = TelemetryEventCreate(
            device_id=uuid4(),
            metric_type="pressure",
            value=0.0,
            timestamp=datetime.now(timezone.utc),
        )
        assert event.value == 0.0


# ============================================================
# TelemetryRawMessage Tests
# ============================================================

class TestTelemetryRawMessage:
    """Tests for Kafka message deserialization."""

    def test_valid_json_deserialization(self):
        """Simulate receiving a valid JSON message from Kafka."""
        json_str = '{"device_id": "550e8400-e29b-41d4-a716-446655440000", "metric_type": "temperature", "value": 72.1, "timestamp": "2026-05-29T14:00:00Z", "metadata": {}}'
        msg = TelemetryRawMessage.model_validate_json(json_str)
        assert msg.value == 72.1

    def test_invalid_json_missing_fields(self):
        """Message missing required fields should fail."""
        json_str = '{"device_id": "550e8400-e29b-41d4-a716-446655440000"}'
        with pytest.raises(ValidationError):
            TelemetryRawMessage.model_validate_json(json_str)

    def test_malformed_json(self):
        """Completely invalid JSON should raise an error."""
        with pytest.raises(Exception):  # Could be JSONDecodeError or ValidationError
            TelemetryRawMessage.model_validate_json("not json at all")


# ============================================================
# TelemetryEnrichedMessage Tests
# ============================================================

class TestTelemetryEnrichedMessage:
    """Tests for the enriched message model (Week 1: enrichment fields are None)."""

    def test_enriched_without_metadata(self):
        """In Week 1, enrichment fields are None — should still be valid."""
        msg = TelemetryEnrichedMessage(
            event_id=uuid4(),
            device_id=uuid4(),
            metric_type="temperature",
            value=23.5,
            timestamp=datetime.now(timezone.utc),
            device_type=None,
            device_location=None,
            firmware_version=None,
        )
        assert msg.device_type is None

    def test_enriched_with_metadata(self):
        """Preview of Week 2: enrichment fields populated."""
        msg = TelemetryEnrichedMessage(
            event_id=uuid4(),
            device_id=uuid4(),
            metric_type="humidity",
            value=45.0,
            timestamp=datetime.now(timezone.utc),
            device_type="environmental_monitor",
            device_location="Zone-A, Rack-1",
            firmware_version="v1.2.0",
        )
        assert msg.device_type == "environmental_monitor"