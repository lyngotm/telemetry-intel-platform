"""
Device registration and retrieval endpoints.
Devices must be registered before they can submit telemetry.
"""

import logging

import asyncpg
from fastapi import APIRouter, Depends, status

from services.api_gateway.app.dependencies import get_db_connection
from services.api_gateway.app.rate_limiter import require_rate_limit
from services.api_gateway.app.rbac import require_role
from shared.models.models import (
    APIListResponse,
    APIResponse,
    DeviceCreate,
    DeviceResponse,
    UserPayload,
)

logger = logging.getLogger("api_gateway")

router = APIRouter(prefix="/api/v1/devices", tags=["devices"])


@router.post(
    "",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_device(
    device: DeviceCreate,
    conn: asyncpg.Connection = Depends(get_db_connection),
    current_user: UserPayload = Depends(require_role("operator")),
    _rate_limit: None = Depends(require_rate_limit("ingestion")),
):
    """
    Register a new telemetry-producing device.
    The device must be registered before it can submit events.
    """
    import json

    row = await conn.fetchrow(
        """
        INSERT INTO devices (device_name, device_type, location, firmware_version, metadata)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING device_id, device_name, device_type, location,
                  firmware_version, metadata, registered_at, updated_at
        """,
        device.device_name,
        device.device_type,
        device.location,
        device.firmware_version,
        json.dumps(device.metadata),
    )

    logger.info(f"Registered device: {row['device_id']} ({device.device_name})")

    return APIResponse(
        success=True,
        message="Device registered successfully",
        data=DeviceResponse(
            device_id=row["device_id"],
            device_name=row["device_name"],
            device_type=row["device_type"],
            location=row["location"],
            firmware_version=row["firmware_version"],
            metadata=json.loads(row["metadata"])
            if isinstance(row["metadata"], str)
            else row["metadata"],
            registered_at=row["registered_at"],
            updated_at=row["updated_at"],
        ),
    )


@router.get(
    "",
    response_model=APIListResponse,
)
async def list_devices(
    device_type: str | None = None,
    conn: asyncpg.Connection = Depends(get_db_connection),
    current_user: UserPayload = Depends(require_role("viewer")),
    _rate_limit: None = Depends(require_rate_limit("query")),
):
    """
    List all registered devices, optionally filtered by device_type.
    """
    import json

    if device_type:
        rows = await conn.fetch(
            "SELECT * FROM devices WHERE device_type = $1 ORDER BY registered_at DESC",
            device_type,
        )
    else:
        rows = await conn.fetch("SELECT * FROM devices ORDER BY registered_at DESC")

    devices = [
        DeviceResponse(
            device_id=row["device_id"],
            device_name=row["device_name"],
            device_type=row["device_type"],
            location=row["location"],
            firmware_version=row["firmware_version"],
            metadata=json.loads(row["metadata"])
            if isinstance(row["metadata"], str)
            else row["metadata"],
            registered_at=row["registered_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
    ]

    return APIListResponse(
        success=True,
        data=[d.model_dump() for d in devices],
        count=len(devices),
    )
