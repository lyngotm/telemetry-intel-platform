"""
Pydantic models for the Telemetry Intelligence Platform.

These models are shared between the API Gateway and Ingestion Consumer
to ensure consistent validation across the pipeline.
"""

from datetime import datetime
from typing import Any
from uuid import UUID
import math
from pydantic import BaseModel, Field, field_validator


# ============================================================
# DEVICE MODELS
# ============================================================


class DeviceCreate(BaseModel):
    """Request body for registering a new device."""

    device_name: str = Field(..., min_length=1, max_length=255, examples=["sensor-floor-3-east"])
    device_type: str = Field(..., min_length=1, max_length=100, examples=["temperature_sensor"])
    location: str | None = Field(None, max_length=255, examples=["Building A, Floor 3"])
    firmware_version: str | None = Field(None, max_length=50, examples=["v2.1.0"])
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeviceResponse(BaseModel):
    """Device data as returned by the API."""

    device_id: UUID
    device_name: str
    device_type: str
    location: str | None
    firmware_version: str | None
    metadata: dict[str, Any]
    registered_at: datetime
    updated_at: datetime


# ============================================================
# TELEMETRY EVENT MODELS
# ============================================================

# Valid metric types for this platform.
# Restricting this prevents garbage data from entering the pipeline.
VALID_METRIC_TYPES = {"temperature", "humidity", "pressure", "cpu_usage"}


class TelemetryEventCreate(BaseModel):
    """
    Request body for submitting a telemetry event.
    This is what the simulator/devices POST to the API.
    """

    device_id: UUID
    metric_type: str = Field(..., min_length=1, max_length=100, examples=["temperature"])
    value: float = Field(..., examples=[23.5])
    timestamp: datetime = Field(..., examples=["2026-05-28T14:30:00Z"])
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metric_type")
    @classmethod
    def validate_metric_type(cls, v: str) -> str:
        if v not in VALID_METRIC_TYPES:
            raise ValueError(
                f"Invalid metric_type '{v}'. Must be one of: {sorted(VALID_METRIC_TYPES)}"
            )
        return v

    @field_validator("value")
    @classmethod
    def validate_value_is_finite(cls, v: float) -> float:
        """Reject NaN and infinity — these indicate sensor malfunction, not real readings."""
        if math.isnan(v) or math.isinf(v):
            raise ValueError("Value must be a finite number")
        return v


class TelemetryEventResponse(BaseModel):
    """Telemetry event as returned by the API (includes server-generated fields)."""

    event_id: UUID
    device_id: UUID
    metric_type: str
    value: float
    timestamp: datetime
    metadata: dict[str, Any]
    ingested_at: datetime


# ============================================================
# KAFKA MESSAGE MODELS
# ============================================================


class TelemetryRawMessage(BaseModel):
    """
    Schema for messages on the telemetry.raw Kafka topic.
    Same as TelemetryEventCreate — the API publishes the validated request as-is.
    """

    device_id: UUID
    metric_type: str
    value: float
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class TelemetryEnrichedMessage(BaseModel):
    """
    Schema for messages on the telemetry.enriched Kafka topic.
    """

    event_id: UUID
    device_id: UUID
    metric_type: str
    value: float
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Enrichment fields
    device_type: str | None = None
    device_location: str | None = None
    firmware_version: str | None = None


class AnomalyDetectedMessage(BaseModel):
    """
    Schema for messages on the anomalies.detected Kafka topic.
    Published by the Anomaly Detection Service when a z-score threshold is breached.
    Consumed by the Diagnosis Service to trigger RAG-based root-cause analysis.
    """

    anomaly_id: UUID
    device_id: UUID
    metric_type: str
    observed_value: float
    expected_range: dict[str, Any]
    z_score: float
    severity: str = Field(..., pattern="^(low|medium|high|critical)$")
    detected_at: datetime

    # Device context (carried from the enriched event for downstream convenience)
    device_type: str | None = None
    device_location: str | None = None


# ============================================================
# API RESPONSE ENVELOPE
# ============================================================


class APIResponse(BaseModel):
    """Envelope for single-resource responses."""

    success: bool
    message: str | None = None
    data: Any = None


class APIListResponse(BaseModel):
    """Envelope for list responses."""

    success: bool
    message: str | None = None
    data: list[Any] = []
    count: int = 0


class AnomalyResponse(BaseModel):
    """Anomaly record as returned by the API."""

    anomaly_id: UUID
    device_id: UUID
    metric_type: str
    observed_value: float
    expected_range: dict[str, Any]
    z_score: float
    severity: str
    detected_at: datetime
    status: str
    created_at: datetime


# ============================================================
# AUTH MODELS
# ============================================================


class UserPayload(BaseModel):
    """
    Represents the decoded JWT payload (claims).
    Extracted from the token on every authenticated request.
    """

    username: str
    role: str = Field(..., pattern="^(admin|operator|viewer)$")
    exp: datetime | None = None


class TokenResponse(BaseModel):
    """Response body for POST /api/v1/auth/token."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds until expiry
    role: str


# ============================================================
# KNOWLEDGE & DIAGNOSIS MODELS
# ============================================================


class IncidentCreate(BaseModel):
    """Request body for POST /api/v1/knowledge/ingest — a single incident document."""

    title: str = Field(
        ..., min_length=1, max_length=500, examples=["Thermal runaway on edge gateway devices"]
    )
    description: str = Field(
        ...,
        min_length=10,
        examples=[
            "Multiple edge gateway devices reported sustained temperature readings above 85°C..."
        ],
    )
    affected_device_types: list[str] = Field(
        ..., min_length=1, examples=[["temperature_sensor", "edge_gateway"]]
    )
    root_cause: str = Field(
        ...,
        min_length=10,
        examples=["Fan assembly failure combined with ambient temperature spike..."],
    )
    resolution_steps: list[str] = Field(
        ...,
        min_length=1,
        examples=[
            [
                "Verify fan assembly RPM via firmware diagnostics",
                "Replace thermal paste if degraded",
            ]
        ],
    )
    severity: str = Field(..., pattern="^(low|medium|high|critical)$", examples=["high"])
    failure_category: str = Field(
        ..., min_length=1, max_length=100, examples=["thermal_management"]
    )
    tags: list[str] = Field(default_factory=list, examples=[["thermal", "hardware", "fan_failure"]])


class IncidentResponse(BaseModel):
    """Incident record as returned by the API."""

    incident_id: UUID
    title: str
    description: str
    affected_device_types: list[str]
    root_cause: str
    resolution_steps: list[str]
    severity: str
    failure_category: str
    tags: list[str]
    chunk_count: int
    ingested_at: datetime


class KnowledgeIngestResponse(BaseModel):
    """Response body for POST /api/v1/knowledge/ingest."""

    incident_id: UUID
    title: str
    chunk_count: int
    message: str


class DiagnosisResponse(BaseModel):
    """RAG-generated diagnosis as returned by GET /api/v1/anomalies/{id}/diagnosis."""

    diagnosis_id: UUID
    anomaly_id: UUID
    root_cause_summary: str
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    supporting_evidence: list[str]
    recommended_actions: list[str]
    retrieved_incident_ids: list[UUID]
    model_id: str | None = None
    generation_time_seconds: float | None = None
    generated_at: datetime
