"""
Database operations for alert event persistence.
Handles inserting new alert events and updating resolved state.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg

from services.alert_receiver.app.models import AlertmanagerAlert, AlertStatus

logger = logging.getLogger("alert_receiver.db")


async def upsert_alert_event(
    pool: asyncpg.Pool,
    alert: AlertmanagerAlert,
) -> tuple[str, bool]:
    """
    Insert or update an alert event based on fingerprint.

    For firing alerts: insert a new record or update if already exists.
    For resolved alerts: update the existing record's resolved_at timestamp.

    Returns:
        Tuple of (alert_event_id, is_new) — whether this was a new insert.
    """
    alert_name = alert.labels.get("alertname", "unknown")
    severity = alert.labels.get("severity", "unknown")
    pipeline = alert.labels.get("pipeline", "unknown")
    tier = alert.labels.get("tier", "unknown")

    # Parse Alertmanager timestamps
    started_at = _parse_alertmanager_timestamp(alert.startsAt)
    resolved_at = _parse_alertmanager_timestamp(alert.endsAt) if alert.status == AlertStatus.resolved else None

    # Check for "zero time" which Alertmanager uses for "not resolved"
    if resolved_at and resolved_at.year == 1:
        resolved_at = None

    async with pool.acquire() as conn:
        if alert.status == AlertStatus.firing:
            # Upsert: insert if new fingerprint, update if existing
            row = await conn.fetchrow(
                """
                INSERT INTO alert_events
                    (id, alert_name, status, severity, pipeline, tier,
                     labels, annotations, fingerprint, started_at,
                     generator_url, received_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (fingerprint) WHERE status = 'firing'
                DO UPDATE SET
                    received_at = EXCLUDED.received_at,
                    labels = EXCLUDED.labels,
                    annotations = EXCLUDED.annotations
                RETURNING id, (xmax = 0) AS is_new
                """,
                uuid4(),
                alert_name,
                alert.status.value,
                severity,
                pipeline,
                tier,
                json.dumps(dict(alert.labels)),
                json.dumps(dict(alert.annotations)),
                alert.fingerprint,
                started_at,
                alert.generatorURL,
                datetime.now(timezone.utc),
            )
            return str(row["id"]), row["is_new"]

        else:
            # Resolve: update existing firing alert
            row = await conn.fetchrow(
                """
                UPDATE alert_events
                SET status = 'resolved',
                    resolved_at = $1,
                    received_at = $2
                WHERE fingerprint = $3 AND status = 'firing'
                RETURNING id
                """,
                resolved_at or datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                alert.fingerprint,
            )
            if row:
                return str(row["id"]), False
            else:
                # Resolved alert without a matching firing record — insert as resolved
                event_id = uuid4()
                await conn.execute(
                    """
                    INSERT INTO alert_events
                        (id, alert_name, status, severity, pipeline, tier,
                         labels, annotations, fingerprint, started_at,
                         resolved_at, generator_url, received_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (fingerprint) WHERE status = 'firing'
                    DO NOTHING
                    """,
                    event_id,
                    alert_name,
                    alert.status.value,
                    severity,
                    pipeline,
                    tier,
                    json.dumps(dict(alert.labels)),
                    json.dumps(dict(alert.annotations)),
                    alert.fingerprint,
                    started_at,
                    resolved_at or datetime.now(timezone.utc),
                    alert.generatorURL,
                    datetime.now(timezone.utc),
                )
                return str(event_id), True


async def get_alert_events(
    pool: asyncpg.Pool,
    status: str | None = None,
    severity: str | None = None,
    pipeline: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """
    Query alert events with optional filters.
    Returns (events, total_count).
    """
    conditions = []
    params = []
    param_idx = 1

    if status:
        conditions.append(f"status = ${param_idx}")
        params.append(status)
        param_idx += 1

    if severity:
        conditions.append(f"severity = ${param_idx}")
        params.append(severity)
        param_idx += 1

    if pipeline:
        conditions.append(f"pipeline = ${param_idx}")
        params.append(pipeline)
        param_idx += 1

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with pool.acquire() as conn:
        # Get total count
        count_query = f"SELECT COUNT(*) FROM alert_events {where_clause}"
        total = await conn.fetchval(count_query, *params)

        # Get paginated results
        query = f"""
            SELECT id, alert_name, status, severity, pipeline, tier,
                   labels, annotations, fingerprint, started_at,
                   resolved_at, generator_url, triage_summary, received_at
            FROM alert_events
            {where_clause}
            ORDER BY received_at DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)

    return [dict(row) for row in rows], total


async def get_active_alerts(pool: asyncpg.Pool) -> list[dict]:
    """Get all currently firing (unresolved) alerts."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, alert_name, status, severity, pipeline, tier,
                   labels, annotations, fingerprint, started_at,
                   resolved_at, generator_url, triage_summary, received_at
            FROM alert_events
            WHERE status = 'firing'
            ORDER BY started_at DESC
            """
        )
    return [dict(row) for row in rows]


async def get_alert_by_id(pool: asyncpg.Pool, alert_id: str) -> dict | None:
    """Get a single alert event by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, alert_name, status, severity, pipeline, tier,
                   labels, annotations, fingerprint, started_at,
                   resolved_at, generator_url, triage_summary, received_at
            FROM alert_events
            WHERE id = $1
            """,
            alert_id,
        )
    return dict(row) if row else None


async def update_triage_summary(
    pool: asyncpg.Pool,
    alert_id: str,
    triage_summary: str,
) -> None:
    """Store AI-generated triage summary for an alert."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE alert_events
            SET triage_summary = $1
            WHERE id = $2
            """,
            triage_summary,
            alert_id,
        )


def _parse_alertmanager_timestamp(ts: str) -> datetime:
    """
    Parse ISO 8601 timestamps from Alertmanager.
    Handles various formats including nanosecond precision.
    """
    # Alertmanager sends timestamps like:
    # "2024-01-15T10:30:00.000+00:00"
    # Python's fromisoformat handles most, but not nanoseconds
    try:
        # Strip nanoseconds if present
        if "." in ts:
            parts = ts.split(".")
            remainder = parts[1]
            # Separate fractional seconds from timezone suffix
            # Possible suffixes: Z, +HH:MM, -HH:MM
            tz_suffix = "+00:00"
            for tz_marker in ("+", "-"):
                if tz_marker in remainder and not remainder.startswith(tz_marker):
                    idx = remainder.index(tz_marker)
                    tz_suffix = remainder[idx:].replace("Z", "+00:00")
                    remainder = remainder[:idx]
                    break
            else:
                if remainder.endswith("Z"):
                    remainder = remainder[:-1]

            # Truncate to 6 digits (microseconds)
            fractional = remainder[:6]
            ts = f"{parts[0]}.{fractional}{tz_suffix}"
        elif ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except (ValueError, IndexError):
        logger.warning(f"Failed to parse timestamp: {ts}, using now()")
        return datetime.now(timezone.utc)
