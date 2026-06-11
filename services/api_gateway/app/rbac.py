"""
Role-Based Access Control (RBAC) for the API Gateway.

Implements a role hierarchy: admin > operator > viewer.
Higher roles inherit all permissions of lower roles.

Usage in route handlers:
    @router.post("", dependencies=[Depends(require_role("operator"))])
    async def my_endpoint(...):
        ...

Or inline:
    async def my_endpoint(
        current_user: UserPayload = Depends(require_role("viewer")),
    ):
        # current_user is available here too
        ...
"""

from fastapi import Depends, HTTPException, status

from services.api_gateway.app.auth import get_current_user
from shared.models.models import UserPayload


# Role hierarchy — index determines privilege level.
# Higher index = more permissions. Each role inherits all permissions below it.
ROLE_HIERARCHY: list[str] = ["viewer", "operator", "admin"]


def _get_role_level(role: str) -> int:
    """Get the numeric privilege level for a role."""
    try:
        return ROLE_HIERARCHY.index(role)
    except ValueError:
        return -1  # Unknown roles have no privileges


def require_role(minimum_role: str):
    """
    FastAPI dependency factory that enforces a minimum role level.

    Args:
        minimum_role: The lowest role allowed to access the endpoint.
                      e.g., "operator" means operators and admins can access,
                      but viewers cannot.

    Returns:
        A dependency function that validates the user's role and returns
        the UserPayload if authorized.

    Raises:
        HTTPException(403) if the user's role is insufficient.
    """
    required_level = _get_role_level(minimum_role)

    async def _role_checker(
        current_user: UserPayload = Depends(get_current_user),
    ) -> UserPayload:
        user_level = _get_role_level(current_user.role)

        if user_level < required_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required role: {minimum_role}, "
                f"your role: {current_user.role}",
            )

        return current_user

    return _role_checker

