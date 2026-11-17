# Filter and authorize

Show each user only the documents they are allowed to see. Three layers decide
what a request can see: the scope it runs in, the metadata filters it passes,
and your access policy, which can deny an operation or restrict every search to
the caller's own rows.

## Isolate customers with scopes

Every operation runs in a `Scope(tenant_id, namespace="default",
collection="default")`. Scopes are isolated: documents, indexes, coverage and
caches of one scope are never visible from another. A request reads exactly
one scope.

## Filter by metadata

Narrow a search to a language, product, team or date range. Pass a filter
expression to `retrieve(..., filters=...)`. Only evidence from
documents that match is returned.

```python
from retrievalloom import And, Eq, Exists, In, Not, Or, Range

filters = And(
    clauses=(
        Eq(field="lang", value="en"),
        In(field="team", values=("network", "security")),
        Range(field="year", gte=2023),
        Not(clause=Exists(field="archived")),
    )
)
```

| Expression | Matches when |
| --- | --- |
| `Eq(field, value)` | The value equals `value`. For a list value, any element equals it. |
| `In(field, values)` | The value, or any element of a list value, is in `values`. |
| `Range(field, gt=, gte=, lt=, lte=)` | The value is within all given bounds. Numbers compare with numbers, strings with strings; booleans never match. |
| `Exists(field)` | The field is present and not null. |
| `And(clauses)`, `Or(clauses)`, `Not(clause)` | Boolean combinations. |

- Fields are dotted paths into nested metadata: `Eq(field="owner.team", value="network")`.
- A missing field never matches `Eq`, `In`, `Range` or `Exists`. It does match
  `Not(...)` of them.
- Filters are evaluated against document metadata merged over the metadata of
  the matched chunk or node. Document metadata wins on conflicts.
- Two reserved fields are always available: `_id` (the document id) and
  `_dataset`.
- A filter may hold at most `RetrievalConfig.max_filter_nodes` (64) nodes.

### Where filters are applied

Qdrant and the in-memory indexes apply filters inside the search
(`filter_pushdown`). LadybugDB and TreeSearch cannot: they search unfiltered
and the engine drops non-matching results afterwards, adding a
`filter_post_applied` info warning. A selective filter can then leave fewer
results than `top_k`. The engine re-checks every filter on every candidate in
any case.

## Enforce access rules

The access policy decides who may do what, and can add a mandatory filter to
every retrieval. The default, `AllowAllAccessPolicy`, allows everything and
adds no filter. It is **not** a security control: any caller that can reach
the engine can use any scope.

Implement `AccessPolicyABC` to enforce your own rules:

```python
import asyncio

from retrievalloom import (
    AccessDeniedError,
    Components,
    DocumentInput,
    Eq,
    RetrievalEngine,
    Scope,
    TextRepresentation,
)
from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.infra import AccessDecision, AccessPolicyABC, Action
from retrievalloom.domain.filters import FilterExpression

ROLES = {"alice": "admin", "bob": "network"}


class TeamPolicy(AccessPolicyABC):
    _INFO = make_info(component="access_policy", implementation="team", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._INFO

    async def authorize(
        self, principal: str | None, scope: Scope, action: Action
    ) -> AccessDecision:
        role = ROLES.get(principal or "")
        if role is None:
            return AccessDecision(allowed=False, reason="unknown principal")
        if action is not Action.RETRIEVE and role != "admin":
            return AccessDecision(allowed=False, reason="read-only principal")
        return AccessDecision(allowed=True)

    async def row_filter(self, principal: str | None, scope: Scope) -> FilterExpression | None:
        role = ROLES.get(principal or "")
        return None if role == "admin" else Eq(field="team", value=role)


async def main() -> None:
    scope = Scope(tenant_id="acme")
    async with RetrievalEngine(components=Components(access_policy=TeamPolicy())) as engine:
        await engine.ingest(
            [
                DocumentInput(
                    id="vpn",
                    metadata={"team": "network"},
                    representations=[TextRepresentation(text="VPN setup guide")],
                ),
                DocumentInput(
                    id="pay",
                    metadata={"team": "finance"},
                    representations=[TextRepresentation(text="Payroll setup guide")],
                ),
            ],
            scope=scope,
            principal="alice",
        )
        result = await engine.retrieve("setup guide", scope=scope, principal="bob")
        print([item.sources[0].document_id for item in result.evidence])  # ['vpn']
        try:
            await engine.delete(["vpn"], scope=scope, principal="bob")
        except AccessDeniedError as exc:
            print(exc.details)


asyncio.run(main())
```

- `authorize(principal, scope, action)` runs before `ingest`, `delete`,
  `purge` (authorized as `delete`), `retrieve`, `backfill`, `rebuild` and
  `status`. A denial raises
  `AccessDeniedError` with the action and reason in `details`.
- `row_filter(principal, scope)` runs for each retrieval. The engine combines
  it with the request's filters using `And`, so callers cannot widen it.
- `principal` is an opaque string you pass to each call. Authenticate callers
  before you pass it.

## Next steps

- See tenant isolation, roles and quotas together in the
  [multi-tenant SaaS example](../../examples/multi-tenant-saas.md).
- Review the security model on [Security](../../security.md).
