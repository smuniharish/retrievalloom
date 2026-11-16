"""Default access policy."""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import AccessDecision, AccessPolicyABC

if TYPE_CHECKING:
    from retrievalloom.contracts.infra import Action
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope


class AllowAllAccessPolicy(AccessPolicyABC):
    """Allows every operation and adds no row filter.

    This is **not** a security control. Scope isolation still holds, but any caller that
    can reach the engine may use any scope. Supply your own ``AccessPolicyABC`` to enforce
    authorisation.
    """

    _INFO = make_info(component="access_policy", implementation="allow_all", version="1")

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._INFO

    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        """Always allowed."""
        del principal, scope, action
        return AccessDecision(allowed=True)

    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        """No restriction."""
        del principal, scope
        return None
