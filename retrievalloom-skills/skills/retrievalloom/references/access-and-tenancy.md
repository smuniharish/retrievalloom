# Access and tenancy

Use this page to keep each customer's documents apart and to decide what each
caller may see. Three layers apply to every request: the scope it runs in,
the access policy, and the metadata filters it passes.

## Isolate tenants with scopes

```python
from retrievalloom import Scope

scope = Scope(tenant_id="acme", namespace="support", collection="kb")
```

`namespace` and `collection` default to `"default"`. Documents, indexes,
coverage, and caches of one scope are never visible from another, and a
request reads exactly one scope. Build the scope from the authenticated
caller on the server side. Never accept a tenant id from request input
unchecked, and never emulate tenancy with a metadata filter.

Data of another scope reaching a request raises `TenantIsolationError`. It is
never degraded to a warning; treat it as a bug.

## Filter by metadata

```python
from retrievalloom import And, Eq, Exists, In, Not, Range

filters = And(
    clauses=(
        Eq(field="lang", value="en"),
        In(field="team", values=("network", "security")),
        Range(field="year", gte=2023),
        Not(clause=Exists(field="archived")),
    )
)
result = await engine.retrieve("vpn setup", scope=scope, filters=filters)
```

| Expression | Matches when |
| --- | --- |
| `Eq(field, value)` | The value equals `value`; for a list value, any element does |
| `In(field, values)` | The value, or any list element, is in `values` |
| `Range(field, gt=, gte=, lt=, lte=)` | Within all bounds; numbers with numbers, strings with strings |
| `Exists(field)` | Present and not null |
| `And`, `Or`, `Not` | Boolean combinations |

- Fields are dotted paths: `Eq(field="owner.team", value="network")`.
- A missing field never matches `Eq`, `In`, `Range`, or `Exists`, and does
  match `Not` of them.
- `_id` (the document id) and `_dataset` are always available.
- Document metadata is merged over chunk metadata; the document wins.
- At most `max_filter_nodes` (64) nodes per filter.
- The graph and structure channels filter after searching and add a
  `filter_post_applied` info warning, so selective filters can return fewer
  than `top_k` items.

## Authorize callers with an access policy

The default policy allows every call and adds no filter: it is not a security
control. To enforce rules, pass an `AccessPolicyABC` and a `principal` on
every call:

```python
from retrievalloom import Components, create_retrieval_engine

engine = create_retrieval_engine(components=Components(access_policy=TeamPolicy()))
result = await engine.retrieve("setup guide", scope=scope, principal=user.id)
```

- `authorize(principal, scope, action)` runs before `ingest`, `delete`,
  `purge` (as the `delete` action), `retrieve`, `backfill`, `rebuild`, and
  `status`. A denial raises `AccessDeniedError` with `action` and `reason` in
  `details`.
- `row_filter(principal, scope)` runs on every retrieval. The engine combines
  it with the caller's filters using `And`, so callers cannot widen it.
- `principal` is an opaque string; authenticate the caller before passing it.

Writing a policy class is covered by the `retrievalloom-extend` skill.

## Limit each tenant's load

`RetrievalConfig(tenant_policies={"acme": TenantPolicy(max_concurrent_requests=4,
max_top_k=20, allowed_channels=(Channel.INDEX,))})` bounds one
tenant; `default_tenant_policy` bounds the rest. See
[configuration.md](configuration.md#limit-each-tenant).
