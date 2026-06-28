"""
Knowledge base management endpoints.
Handles ingestion of incident documents and retrieval of ingested knowledge.
"""

import json
import logging

import asyncpg
import chromadb
from fastapi import APIRouter, Depends, HTTPException, status

from services.api_gateway.app.dependencies import get_chroma_client, get_db_connection
from services.api_gateway.app.knowledge import ingest_incident
from services.api_gateway.app.rate_limiter import require_rate_limit
from services.api_gateway.app.rbac import require_role
from shared.models.models import (
    APIListResponse,
    APIResponse,
    IncidentCreate,
    IncidentResponse,
    KnowledgeIngestResponse,
    UserPayload,
)

logger = logging.getLogger("api_gateway")

router = APIRouter(prefix="/api/v1/knowledge", tags=["knowledge"])


@router.post(
    "/ingest",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
)
async def ingest_incident_document(
    incident: IncidentCreate,
    conn: asyncpg.Connection = Depends(get_db_connection),
    chroma_client: chromadb.ClientAPI = Depends(get_chroma_client),
    current_user: UserPayload = Depends(require_role("admin")),
):
    """
    Ingest a historical incident document into the knowledge base.

    Requires admin role. The ingested knowledge is used by the Diagnosis Service
    to provide context when generating root-cause analyses for detected anomalies.
    """
    try:
        incident_id, chunk_count = await ingest_incident(
            incident=incident,
            conn=conn,
            chroma_client=chroma_client,
        )
    except Exception as e:
        logger.error(f"Knowledge ingestion failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed: {str(e)}",
        )

    response_data = KnowledgeIngestResponse(
        incident_id=incident_id,
        title=incident.title,
        chunk_count=chunk_count,
        message=f"Incident ingested successfully. Created {chunk_count} embedding chunks.",
    )

    return APIResponse(
        success=True,
        data=response_data.model_dump(mode="json"),
        message="Incident ingested into knowledge base",
    )


@router.get(
    "/incidents",
    response_model=APIListResponse,
)
async def list_incidents(
    severity: str | None = None,
    failure_category: str | None = None,
    limit: int = 100,
    offset: int = 0,
    conn: asyncpg.Connection = Depends(get_db_connection),
    current_user: UserPayload = Depends(require_role("viewer")),
    _rate_limit: None = Depends(require_rate_limit("query")),
):
    """
    List ingested incident documents with optional filters.

    Filters:
    - severity: low, medium, high, or critical
    - failure_category: e.g., thermal_management, firmware_bug, environmental
    - limit: max results (default 100, max 1000)
    - offset: pagination offset
    """
    limit = min(limit, 1000)

    # Build dynamic query
    conditions = []
    params = []
    param_idx = 1

    if severity:
        conditions.append(f"severity = ${param_idx}")
        params.append(severity)
        param_idx += 1

    if failure_category:
        conditions.append(f"failure_category = ${param_idx}")
        params.append(failure_category)
        param_idx += 1

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT incident_id, title, description, affected_device_types,
               root_cause, resolution_steps, severity, failure_category,
               tags, chunk_count, ingested_at
        FROM incidents_knowledge
        {where_clause}
        ORDER BY ingested_at DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """
    params.extend([limit, offset])

    rows = await conn.fetch(query, *params)

    # Total count for pagination
    count_query = f"SELECT COUNT(*) FROM incidents_knowledge {where_clause}"
    total = (
        await conn.fetchval(count_query, *params[:-2])
        if conditions
        else await conn.fetchval(count_query)
    )

    incidents = [
        IncidentResponse(
            incident_id=row["incident_id"],
            title=row["title"],
            description=row["description"],
            affected_device_types=json.loads(row["affected_device_types"])
            if isinstance(row["affected_device_types"], str)
            else row["affected_device_types"],
            root_cause=row["root_cause"],
            resolution_steps=json.loads(row["resolution_steps"])
            if isinstance(row["resolution_steps"], str)
            else row["resolution_steps"],
            severity=row["severity"],
            failure_category=row["failure_category"],
            tags=json.loads(row["tags"]) if isinstance(row["tags"], str) else row["tags"],
            chunk_count=row["chunk_count"],
            ingested_at=row["ingested_at"],
        )
        for row in rows
    ]

    return APIListResponse(
        success=True,
        data=[i.model_dump(mode="json") for i in incidents],
        count=total,
    )
