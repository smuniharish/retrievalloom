"""An access policy to adapt to your authorization service.

Copy this file and replace ``ROLES`` with a lookup in your identity provider.
The engine calls:

- ``authorize(principal, scope, action)`` before ``ingest``, ``delete``,
  ``purge`` (as ``Action.DELETE``), ``retrieve``, ``backfill``, ``rebuild`` and
  ``status``. A denial raises ``AccessDeniedError`` with the reason in
  ``details``.
- ``row_filter(principal, scope)`` on every retrieval. The engine combines the
  result with the request's filters using ``And``, so callers cannot widen it.

``principal`` is an opaque string your application passes to each call:
authenticate the caller before you pass it. There is no contract kit for
access policies; ``test_access_policy.py`` checks this one through a real
engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom import Eq
from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.infra import AccessDecision, AccessPolicyABC, Action

if TYPE_CHECKING:
    from retrievalloom import Scope
    from retrievalloom.contracts.component import ComponentInfo
    from retrievalloom.domain.filters import FilterExpression

# principal -> (tenant, team); team None means every team of the tenant
ROLES: dict[str, tuple[str, str | None]] = {
    "alice": ("acme", None),
    "bob": ("acme", "network"),
}
WRITE_ACTIONS = frozenset({Action.INGEST, Action.DELETE, Action.BACKFILL, Action.REBUILD})


class TeamAccessPolicy(AccessPolicyABC):
    """Tenant membership gates every action; team members read only their team's documents."""

    def __init__(self, roles: dict[str, tuple[str, str | None]] | None = None) -> None:
        self._roles = ROLES if roles is None else roles
        self._info = make_info(component="access_policy", implementation="team", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._info

    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        role = self._roles.get(principal or "")
        if role is None or role[0] != scope.tenant_id:
            return AccessDecision(allowed=False, reason="not a member of this tenant")
        if action in WRITE_ACTIONS and role[1] is not None:
            return AccessDecision(allowed=False, reason="read-only principal")
        return AccessDecision(allowed=True)

    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        role = self._roles.get(principal or "")
        if role is None or role[1] is None:
            return None
        return Eq(field="team", value=role[1])
