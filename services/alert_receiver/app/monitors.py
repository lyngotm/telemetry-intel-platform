"""
Monitor CRUD API — Programmatic alert rule management.

Users can create, list, and delete custom alert rules via REST endpoints.
Rules are persisted to monitoring/custom_rules.yml and Prometheus is
signaled to reload its configuration.

Design:
- Custom rules live in a separate file from hand-crafted alert_rules.yml
- Each monitor gets a unique ID for reference/deletion
- Prometheus reloads via POST /-/reload (requires --web.enable-lifecycle flag)
- Rules are also persisted to PostgreSQL for durability across restarts
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import httpx
import yaml
from pydantic import BaseModel, Field

from services.alert_receiver.app.config import settings

logger = logging.getLogger("alert_receiver.monitors")

# Path to custom rules file (shared volume between alert-receiver and Prometheus in Docker)
CUSTOM_RULES_PATH = Path("/etc/prometheus/custom_rules/custom_rules.yml")
# Fallback for local development — outside project tree to avoid triggering file watchers
LOCAL_CUSTOM_RULES_PATH = Path("/tmp/tip-custom-rules/custom_rules.yml")

# Prometheus reload endpoint (configurable via PROMETHEUS_URL env var)
PROMETHEUS_RELOAD_URL = None  # Set from settings at runtime


# ─── Request/Response Models ──────────────────────────────────


class MonitorCreate(BaseModel):
    """Request to create a new monitor (alert rule)."""
    name: str = Field(..., description="Alert name (e.g., 'HighCPUUsage')", max_length=100)
    expr: str = Field(..., description="PromQL expression that triggers the alert")
    duration: str = Field(
        default="5m",
        description="How long the condition must be true before firing (e.g., '5m', '10m', '1h')",
    )
    severity: str = Field(
        default="warning",
        description="Alert severity: critical, warning, or info",
    )
    pipeline: str = Field(
        default="custom",
        description="Pipeline label for routing (ingestion, detection, diagnosis, api, custom)",
    )
    summary: str = Field(
        default="",
        description="One-line summary (supports Prometheus template variables)",
    )
    description: str = Field(
        default="",
        description="Detailed description for the alert notification",
    )
    enabled: bool = Field(default=True, description="Whether this monitor is active")


class MonitorUpdate(BaseModel):
    """Request to update an existing monitor. All fields optional."""
    name: str | None = Field(default=None, max_length=100)
    expr: str | None = None
    duration: str | None = None
    severity: str | None = None
    pipeline: str | None = None
    summary: str | None = None
    description: str | None = None
    enabled: bool | None = None


class MonitorResponse(BaseModel):
    """A monitor as returned by the API."""
    id: str
    name: str
    expr: str
    duration: str
    severity: str
    pipeline: str
    summary: str
    description: str
    enabled: bool
    created_at: datetime


class MonitorListResponse(BaseModel):
    """List of all configured monitors."""
    monitors: list[MonitorResponse]
    total: int


# ─── Database Operations ─────────────────────────────────────


async def create_monitor(pool: asyncpg.Pool, monitor: MonitorCreate) -> dict:
    """Create a new monitor and persist to DB + rules file."""
    monitor_id = str(uuid.uuid4())[:8]  # Short ID for easy reference
    now = datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO monitors (id, name, expr, duration, severity, pipeline,
                                  summary, description, enabled, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            monitor_id,
            monitor.name,
            monitor.expr,
            monitor.duration,
            monitor.severity,
            monitor.pipeline,
            monitor.summary or f"{monitor.name} is firing",
            monitor.description or f"Custom monitor: {monitor.name}",
            monitor.enabled,
            now,
        )

    # Regenerate rules file and reload Prometheus
    reload_ok, reload_msg = await _sync_rules_file(pool)

    return {
        "monitor": MonitorResponse(
            id=monitor_id,
            name=monitor.name,
            expr=monitor.expr,
            duration=monitor.duration,
            severity=monitor.severity,
            pipeline=monitor.pipeline,
            summary=monitor.summary or f"{monitor.name} is firing",
            description=monitor.description or f"Custom monitor: {monitor.name}",
            enabled=monitor.enabled,
            created_at=now,
        ),
        "prometheus_reload": {"success": reload_ok, "message": reload_msg},
    }


async def list_monitors(pool: asyncpg.Pool) -> MonitorListResponse:
    """List all configured monitors."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, expr, duration, severity, pipeline,
                   summary, description, enabled, created_at
            FROM monitors
            ORDER BY created_at DESC
            """
        )

    monitors = [
        MonitorResponse(
            id=row["id"],
            name=row["name"],
            expr=row["expr"],
            duration=row["duration"],
            severity=row["severity"],
            pipeline=row["pipeline"],
            summary=row["summary"],
            description=row["description"],
            enabled=row["enabled"],
            created_at=row["created_at"],
        )
        for row in rows
    ]

    return MonitorListResponse(monitors=monitors, total=len(monitors))


async def delete_monitor(pool: asyncpg.Pool, monitor_id: str) -> dict | None:
    """Delete a monitor by ID. Returns result dict or None if not found."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM monitors WHERE id = $1", monitor_id
        )

    if result == "DELETE 0":
        return None

    # Regenerate rules file and reload Prometheus
    reload_ok, reload_msg = await _sync_rules_file(pool)
    return {"prometheus_reload": {"success": reload_ok, "message": reload_msg}}


async def update_monitor(
    pool: asyncpg.Pool, monitor_id: str, update: MonitorUpdate
) -> dict | None:
    """
    Update a monitor's fields. Only non-None fields are updated.
    Returns dict with monitor and reload status, or None if not found.
    """
    # Build dynamic SET clause from non-None fields
    fields = update.model_dump(exclude_none=True)
    if not fields:
        # Nothing to update — just return current state
        monitor = await get_monitor(pool, monitor_id)
        if not monitor:
            return None
        return {"monitor": monitor, "prometheus_reload": {"success": True, "message": "No changes"}}

    set_parts = []
    params = []
    for i, (key, value) in enumerate(fields.items(), 1):
        set_parts.append(f"{key} = ${i}")
        params.append(value)

    params.append(monitor_id)
    set_clause = ", ".join(set_parts)

    async with pool.acquire() as conn:
        result = await conn.execute(
            f"UPDATE monitors SET {set_clause} WHERE id = ${len(params)}",
            *params,
        )

    if result == "UPDATE 0":
        return None

    # Regenerate rules file and reload Prometheus
    reload_ok, reload_msg = await _sync_rules_file(pool)
    monitor = await get_monitor(pool, monitor_id)
    return {"monitor": monitor, "prometheus_reload": {"success": reload_ok, "message": reload_msg}}


async def get_monitor(pool: asyncpg.Pool, monitor_id: str) -> MonitorResponse | None:
    """Get a single monitor by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, name, expr, duration, severity, pipeline,
                   summary, description, enabled, created_at
            FROM monitors WHERE id = $1
            """,
            monitor_id,
        )

    if not row:
        return None

    return MonitorResponse(
        id=row["id"],
        name=row["name"],
        expr=row["expr"],
        duration=row["duration"],
        severity=row["severity"],
        pipeline=row["pipeline"],
        summary=row["summary"],
        description=row["description"],
        enabled=row["enabled"],
        created_at=row["created_at"],
    )


# ─── Rules File Generation ────────────────────────────────────


async def _sync_rules_file(pool: asyncpg.Pool) -> tuple[bool, str]:
    """
    Regenerate the custom_rules.yml file from DB state and trigger
    Prometheus to reload its configuration.

    Returns (reload_success, message) from Prometheus reload attempt.
    """
    # Fetch all enabled monitors
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, expr, duration, severity, pipeline, summary, description
            FROM monitors
            WHERE enabled = true
            ORDER BY created_at
            """
        )

    # Build Prometheus rules YAML structure
    rules = []
    for row in rows:
        rule = {
            "alert": row["name"],
            "expr": row["expr"],
            "for": row["duration"],
            "labels": {
                "severity": row["severity"],
                "tier": "custom",
                "pipeline": row["pipeline"],
                "managed_by": "monitor-api",
            },
            "annotations": {
                "summary": row["summary"],
                "description": row["description"],
            },
        }
        rules.append(rule)

    rules_doc = {
        "groups": [
            {
                "name": "tip_custom_monitors",
                "rules": rules,
            }
        ]
    }

    # Write to file
    rules_path = CUSTOM_RULES_PATH if CUSTOM_RULES_PATH.parent.exists() else LOCAL_CUSTOM_RULES_PATH
    rules_path.parent.mkdir(parents=True, exist_ok=True)

    with open(rules_path, "w") as f:
        f.write("# Auto-generated by Monitor CRUD API — do not edit manually\n")
        f.write(f"# Last updated: {datetime.now(timezone.utc).isoformat()}\n")
        yaml.dump(rules_doc, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Custom rules file written: {rules_path} ({len(rules)} rules)")

    # Trigger Prometheus reload
    return await _reload_prometheus()


async def _reload_prometheus() -> tuple[bool, str]:
    """
    Signal Prometheus to reload its configuration.
    Requires --web.enable-lifecycle flag on Prometheus.

    Returns (success, message) tuple.
    """
    reload_url = f"{settings.prometheus_url}/-/reload"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(reload_url)
            if response.status_code == 200:
                logger.info("Prometheus configuration reloaded successfully")
                return True, "Prometheus reloaded successfully"
            else:
                msg = f"Prometheus reload returned {response.status_code}: {response.text}"
                logger.warning(msg)
                return False, msg
    except httpx.ConnectError:
        msg = (
            "Could not reach Prometheus for reload — "
            "rule saved but will not be active until Prometheus restarts"
        )
        logger.warning(msg)
        return False, msg
    except Exception as e:
        msg = f"Prometheus reload failed: {e}"
        logger.warning(msg)
        return False, msg
