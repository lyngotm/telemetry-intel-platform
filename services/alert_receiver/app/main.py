"""
Alert Receiver Service — Webhook sink for Prometheus Alertmanager.

Receives alert payloads, persists state transitions to PostgreSQL,
exposes metrics for Prometheus self-monitoring, and optionally
triggers AI triage for critical alerts.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from prometheus_client import start_http_server

from services.alert_receiver.app.config import settings
from services.alert_receiver.app.database import (
    _parse_alertmanager_timestamp,
    get_active_alerts,
    get_alert_by_id,
    get_alert_events,
    upsert_alert_event,
)
from services.alert_receiver.app.metrics import (
    ALERT_EVENTS_RECEIVED,
    ALERT_PROCESSING_ERRORS,
    ALERT_WEBHOOK_REQUESTS,
    ALERTS_CURRENTLY_FIRING,
    ALERTS_FIRING_BY_PIPELINE,
    ALERTS_FIRING_BY_SEVERITY,
    WEBHOOK_DELIVERY_LATENCY,
)
from services.alert_receiver.app.models import (
    ActiveAlertsSummary,
    AlertEventListResponse,
    AlertEventResponse,
    AlertmanagerWebhookPayload,
    AlertReceiverHealth,
    AlertStatus,
    TriageResponse,
)
from services.alert_receiver.app.monitors import (
    MonitorCreate,
    MonitorUpdate,
    _sync_rules_file,
    create_monitor,
    delete_monitor,
    get_monitor,
    list_monitors,
    update_monitor,
)
from services.alert_receiver.app.triage import generate_triage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("alert_receiver")
logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup/shutdown of shared resources."""
    start_http_server(settings.metrics_port)
    logger.info(f"Prometheus metrics available on port {settings.metrics_port}")

    app.state.db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=3,
        max_size=10,
    )
    logger.info(f"Connected to PostgreSQL at {settings.postgres_host}:{settings.postgres_port}")

    # Initialize firing alert gauge from DB state
    await _sync_firing_gauges(app.state.db_pool)

    # Sync custom rules file from DB state (ensures file matches reality on restart)
    await _sync_rules_file(app.state.db_pool)

    logger.info(
        f"Alert Receiver Service started. "
        f"API port: {settings.api_port}, Metrics port: {settings.metrics_port}"
    )
    yield

    await app.state.db_pool.close()
    logger.info("Alert Receiver Service stopped.")


app = FastAPI(
    title="Alert Receiver Service",
    description="Webhook sink for Prometheus Alertmanager with alert persistence and AI triage",
    version="0.1.0",
    lifespan=lifespan,
)


# ============================================================
# Webhook Endpoint — receives payloads from Alertmanager
# ============================================================


@app.post("/alerts", status_code=202)
async def receive_alerts(payload: AlertmanagerWebhookPayload):
    """
    Receive alert notifications from Alertmanager.

    This is the webhook endpoint configured in alertmanager.yml.
    Processes each alert in the batch: persists to DB, updates metrics,
    and optionally triggers AI triage for critical firing alerts.
    """
    logger.info(
        f"Received webhook: receiver={payload.receiver}, "
        f"status={payload.status}, alerts={len(payload.alerts)}, "
        f"group={payload.groupLabels}"
    )

    processed = 0
    errors = 0

    for alert in payload.alerts:
        try:
            alert_name = alert.labels.get("alertname", "unknown")
            severity = alert.labels.get("severity", "unknown")

            alert_id, is_new = await upsert_alert_event(app.state.db_pool, alert)

            ALERT_EVENTS_RECEIVED.labels(
                status=alert.status.value,
                severity=severity,
            ).inc()

            _track_delivery_latency(alert)

            action = "NEW" if is_new else "UPDATED"
            logger.info(
                f"  [{action}] {alert.status.value.upper()}: "
                f"{alert_name} (severity={severity}, fingerprint={alert.fingerprint[:8]})"
            )

            # Trigger triage for new critical firing alerts.
            # Works without LLM — uses existing diagnoses or rule-based fallback.
            if (
                alert.status == AlertStatus.firing
                and severity == "critical"
                and is_new
                and settings.triage_enabled
            ):
                # Fire-and-forget: don't block webhook response
                asyncio.create_task(_trigger_triage(alert_id, alert))

            processed += 1

        except Exception as e:
            logger.error(f"  Error processing alert: {e}", exc_info=True)
            ALERT_PROCESSING_ERRORS.labels(error_type=type(e).__name__).inc()
            errors += 1

    # Update firing gauges
    await _sync_firing_gauges(app.state.db_pool)

    ALERT_WEBHOOK_REQUESTS.labels(status_code="202").inc()

    return {
        "status": "accepted",
        "processed": processed,
        "errors": errors,
    }


# ============================================================
# Query Endpoints
# ============================================================


@app.get("/alerts", response_model=AlertEventListResponse)
async def list_alerts(
    status: str | None = Query(None, description="Filter by status: firing, resolved"),
    severity: str | None = Query(None, description="Filter by severity: critical, warning, info"),
    pipeline: str | None = Query(
        None, description="Filter by pipeline: ingestion, detection, diagnosis, api"
    ),
    limit: int = Query(50, ge=1, le=200, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """
    Query alert event history with optional filters.
    Returns paginated results ordered by most recent first.
    """
    alerts, total = await get_alert_events(
        pool=app.state.db_pool,
        status=status,
        severity=severity,
        pipeline=pipeline,
        limit=limit,
        offset=offset,
    )
    return AlertEventListResponse(
        alerts=[_row_to_response(a) for a in alerts],
        total=total,
        limit=limit,
        offset=offset,
    )


@app.get("/alerts/active", response_model=ActiveAlertsSummary)
async def get_active():
    """
    Get all currently firing (unresolved) alerts with summary breakdown.
    """
    alerts = await get_active_alerts(app.state.db_pool)

    by_severity: dict[str, int] = {}
    by_pipeline: dict[str, int] = {}
    for a in alerts:
        sev = a.get("severity", "unknown")
        pipe = a.get("pipeline", "unknown")
        by_severity[sev] = by_severity.get(sev, 0) + 1
        by_pipeline[pipe] = by_pipeline.get(pipe, 0) + 1

    return ActiveAlertsSummary(
        total_firing=len(alerts),
        by_severity=by_severity,
        by_pipeline=by_pipeline,
        alerts=[_row_to_response(a) for a in alerts],
    )


@app.get("/alerts/{alert_id}", response_model=AlertEventResponse)
async def get_alert(alert_id: UUID):
    """Get a single alert event by ID."""
    alert = await get_alert_by_id(app.state.db_pool, str(alert_id))
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _row_to_response(alert)


@app.get("/alerts/{alert_id}/triage", response_model=TriageResponse)
async def get_alert_triage(alert_id: UUID):
    """
    Get the AI-generated triage summary for a specific alert.
    Returns 404 if no triage has been generated yet.
    """
    alert = await get_alert_by_id(app.state.db_pool, str(alert_id))
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    if not alert.get("triage_summary"):
        raise HTTPException(
            status_code=404,
            detail="No triage available for this alert. Possible reasons: "
            "(1) alert is not critical severity, "
            "(2) CLAUDE_MODEL_ID is not configured, "
            "(3) triage generation is still in progress, "
            "(4) triage generation failed (check service logs).",
        )

    return TriageResponse(
        alert_id=alert["id"],
        alert_name=alert["alert_name"],
        severity=alert["severity"],
        triage_summary=alert["triage_summary"],
        generated_at=alert["received_at"],  # Approximate; could add a dedicated column
    )


# ============================================================
# Monitor CRUD Endpoints — Programmatic alert rule management
# ============================================================


@app.post("/monitors", status_code=201)
async def create_monitor_endpoint(monitor: MonitorCreate):
    """
    Create a new custom alert rule (monitor).

    The rule is persisted to PostgreSQL and written to a custom rules file
    that Prometheus reloads automatically. This allows users to define
    threshold-based alerts programmatically without editing YAML.
    """
    result = await create_monitor(app.state.db_pool, monitor)
    return result


@app.get("/monitors")
async def list_monitors_endpoint():
    """
    List all custom monitors (alert rules created via API).
    Does not include hand-crafted rules from alert_rules.yml.
    """
    return await list_monitors(app.state.db_pool)


@app.get("/monitors/{monitor_id}")
async def get_monitor_endpoint(monitor_id: str):
    """Get a single monitor by ID."""
    result = await get_monitor(app.state.db_pool, monitor_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Monitor '{monitor_id}' not found")
    return result


@app.patch("/monitors/{monitor_id}")
async def update_monitor_endpoint(monitor_id: str, update: MonitorUpdate):
    """
    Update a custom monitor. Supports partial updates — only include
    the fields you want to change.

    Triggers Prometheus reload after update.
    """
    result = await update_monitor(app.state.db_pool, monitor_id, update)
    if not result:
        raise HTTPException(status_code=404, detail=f"Monitor '{monitor_id}' not found")
    return result


@app.delete("/monitors/{monitor_id}")
async def delete_monitor_endpoint(monitor_id: str):
    """
    Delete a custom monitor by ID.

    The rule is removed from PostgreSQL and the custom rules file is
    regenerated. Prometheus is signaled to reload.
    """
    result = await delete_monitor(app.state.db_pool, monitor_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Monitor '{monitor_id}' not found")
    return result


# ============================================================
# Health & Readiness
# ============================================================


@app.get("/health", response_model=AlertReceiverHealth)
async def health_check():
    """Liveness probe with basic stats."""
    active = await get_active_alerts(app.state.db_pool)
    return AlertReceiverHealth(
        status="healthy",
        alerts_received_total=int(
            sum(
                ALERT_EVENTS_RECEIVED.labels(status=s, severity=sev)._value.get()
                for s in ["firing", "resolved"]
                for sev in ["critical", "warning", "info", "unknown"]
            )
        ),
        alerts_currently_firing=len(active),
    )


@app.get("/ready")
async def readiness_check():
    """Readiness probe — verifies PostgreSQL connectivity."""
    try:
        async with app.state.db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ready"}
    except Exception as e:
        logger.warning(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "detail": str(e)},
        )


# ============================================================
# Internal Helpers
# ============================================================


def _row_to_response(row: dict) -> AlertEventResponse:
    """Convert a database row to an API response model."""
    # asyncpg returns JSONB as strings when stored via json.dumps()
    labels = row["labels"] or {}
    if isinstance(labels, str):
        labels = json.loads(labels)

    annotations = row["annotations"] or {}
    if isinstance(annotations, str):
        annotations = json.loads(annotations)

    return AlertEventResponse(
        id=row["id"],
        alert_name=row["alert_name"],
        status=row["status"],
        severity=row["severity"],
        pipeline=row["pipeline"],
        tier=row["tier"],
        labels=labels,
        annotations=annotations,
        fingerprint=row["fingerprint"],
        started_at=row["started_at"],
        resolved_at=row.get("resolved_at"),
        generator_url=row.get("generator_url", ""),
        triage_summary=row.get("triage_summary"),
        received_at=row["received_at"],
    )


def _track_delivery_latency(alert) -> None:
    """Measure time between alert start and webhook delivery."""
    try:
        started_at = _parse_alertmanager_timestamp(alert.startsAt)
        delivery_latency = (datetime.now(timezone.utc) - started_at).total_seconds()
        # Only track positive, reasonable latencies (< 1 hour)
        if 0 < delivery_latency < 3600:
            WEBHOOK_DELIVERY_LATENCY.observe(delivery_latency)
    except Exception:
        pass  # Don't let latency tracking break alert processing


async def _sync_firing_gauges(pool: asyncpg.Pool) -> None:
    """Sync Prometheus gauges with current DB state."""
    try:
        active = await get_active_alerts(pool)
        ALERTS_CURRENTLY_FIRING.set(len(active))

        # Reset severity/pipeline gauges then set from current state
        for sev in ["critical", "warning", "info"]:
            ALERTS_FIRING_BY_SEVERITY.labels(severity=sev).set(0)
        for pipe in ["ingestion", "detection", "diagnosis", "api", "alerting"]:
            ALERTS_FIRING_BY_PIPELINE.labels(pipeline=pipe).set(0)

        for a in active:
            ALERTS_FIRING_BY_SEVERITY.labels(severity=a.get("severity", "unknown")).inc()
            ALERTS_FIRING_BY_PIPELINE.labels(pipeline=a.get("pipeline", "unknown")).inc()
    except Exception as e:
        logger.warning(f"Failed to sync firing gauges: {e}")


async def _trigger_triage(alert_id: str, alert) -> None:
    """
    Fire-and-forget AI triage for critical alerts.
    This runs as a background task so the webhook response isn't delayed.
    """
    await generate_triage(app.state.db_pool, alert_id, alert)
