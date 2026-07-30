"""
Pydantic models for Alertmanager webhook payloads and alert state.
"""

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


# ─── Alertmanager Webhook Payload Models ──────────────────────


class AlertStatus(str, Enum):
    """Alert lifecycle states from Alertmanager."""
    firing = "firing"
    resolved = "resolved"


class AlertmanagerAlert(BaseModel):
    """A single alert within an Alertmanager webhook payload."""
    status: AlertStatus
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: str  # ISO 8601 timestamp from Alertmanager
    endsAt: str  # "0001-01-01T00:00:00Z" if still firing
    generatorURL: str = ""
    fingerprint: str = ""  # Unique identifier for this alert instance


class AlertmanagerWebhookPayload(BaseModel):
    """
    Full webhook payload sent by Alertmanager.
    See: https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
    """
    version: str = "4"
    groupKey: str = ""
    truncatedAlerts: int = 0
    status: AlertStatus  # Group-level status (firing if any alert in group is firing)
    receiver: str = ""
    groupLabels: dict[str, str] = Field(default_factory=dict)
    commonLabels: dict[str, str] = Field(default_factory=dict)
    commonAnnotations: dict[str, str] = Field(default_factory=dict)
    externalURL: str = ""
    alerts: list[AlertmanagerAlert] = Field(default_factory=list)


# ─── Database / API Response Models ──────────────────────────


class AlertEventResponse(BaseModel):
    """Alert event as returned by the API."""
    id: UUID
    alert_name: str
    status: AlertStatus
    severity: str
    pipeline: str
    tier: str
    labels: dict[str, str]
    annotations: dict[str, str]
    fingerprint: str
    started_at: datetime
    resolved_at: datetime | None = None
    generator_url: str = ""
    triage_summary: str | None = None
    received_at: datetime


class AlertEventListResponse(BaseModel):
    """Paginated list of alert events."""
    alerts: list[AlertEventResponse]
    total: int
    limit: int
    offset: int


class ActiveAlertsSummary(BaseModel):
    """Summary of currently active (firing) alerts."""
    total_firing: int
    by_severity: dict[str, int]
    by_pipeline: dict[str, int]
    alerts: list[AlertEventResponse]


class AlertReceiverHealth(BaseModel):
    """Health check response."""
    status: str
    alerts_received_total: int
    alerts_currently_firing: int


class TriageResponse(BaseModel):
    """AI triage response for a specific alert."""
    alert_id: UUID
    alert_name: str
    severity: str
    triage_summary: str
    generated_at: datetime
