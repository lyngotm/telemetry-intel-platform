"""
Anomaly retrieval endpoints.
Serves anomaly records detected by the Anomaly Detection Service.
"""

import json
import logging
from datetime import datetime
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, status

from services.api_gateway.app.dependencies import get_db_connection
from shared.models.models import APIListResponse, APIResponse, AnomalyResponse

logger = logging.getLogger("api_gateway")

router = APIRouter(prefix="/api/v1/anomalies", tags=["anomalies"])


@router.get(
    "",
    response_model=APIListResponse,
)
async def list_anomalies(
    device_id: UUID | None = None,
    severity: str | None = None,
    status_filter: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """
    Query detected anomalies with optional filters.

    Filters:
    - device_id: anomalies for a specific device
    - severity: low, medium, high, or critical
    - status_filter: open, acknowledged, or resolved
    - start: anomalies detected at or after this time
    - end: anomalies detected before this time
    - limit: max results (default 100, max 1000)
    - offset: pagination offset
    """
    limit = min(limit, 1000)

    # Build dynamic query
    conditions = []
    params = []
    param_idx = 1

    if device_id:
        conditions.append(f"device_id = ${param_idx}")
        params.append(device_id)
        param_idx += 1

    if severity:
        conditions.append(f"severity = ${param_idx}")
        params.append(severity)
        param_idx += 1

    if status_filter:
        conditions.append(f"status = ${param_idx}")
        params.append(status_filter)
        param_idx += 1

    if start:
        conditions.append(f"detected_at >= ${param_idx}")
        params.append(start)
        param_idx += 1

    if end:
        conditions.append(f"detected_at < ${param_idx}")
        params.append(end)
        param_idx += 1

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT anomaly_id, device_id, metric_type, observed_value,
               expected_range, z_score, severity, detected_at, status, created_at
        FROM anomalies
        {where_clause}
        ORDER BY detected_at DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """
    params.extend([limit, offset])

    rows = await conn.fetch(query, *params)

    # Total count for pagination
    count_query = f"SELECT COUNT(*) FROM anomalies {where_clause}"
    total = await conn.fetchval(count_query, *params[:-2])

    anomalies = [
        AnomalyResponse(
            anomaly_id=row["anomaly_id"],
            device_id=row["device_id"],
            metric_type=row["metric_type"],
            observed_value=row["observed_value"],
            expected_range=json.loads(row["expected_range"]) if isinstance(row["expected_range"], str) else row["expected_range"],
            z_score=row["z_score"],
            severity=row["severity"],
            detected_at=row["detected_at"],
            status=row["status"],
            created_at=row["created_at"],
        )
        for row in rows
    ]

    return APIListResponse(
        success=True,
        data=[a.model_dump(mode="json") for a in anomalies],
        count=total,
    )


@router.get(
    "/{anomaly_id}",
    response_model=APIResponse,
)
async def get_anomaly(
    anomaly_id: UUID,
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """
    Retrieve a single anomaly by ID.
    """
    row = await conn.fetchrow(
        """
        SELECT anomaly_id, device_id, metric_type, observed_value,
               expected_range, z_score, severity, detected_at, status, created_at
        FROM anomalies
        WHERE anomaly_id = $1
        """,
        anomaly_id,
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found",
        )

    anomaly = AnomalyResponse(
        anomaly_id=row["anomaly_id"],
        device_id=row["device_id"],
        metric_type=row["metric_type"],
        observed_value=row["observed_value"],
        expected_range=json.loads(row["expected_range"]) if isinstance(row["expected_range"], str) else row["expected_range"],
        z_score=row["z_score"],
        severity=row["severity"],
        detected_at=row["detected_at"],
        status=row["status"],
        created_at=row["created_at"],
    )

    return APIResponse(
        success=True,
        data=anomaly.model_dump(mode="json"),
    )
