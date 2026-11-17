# Multi-tenant SaaS

One engine serves several customers (tenants). Each tenant has several collections (an
employee handbook, engineering docs), and its users have roles. The example shows that:

- a tenant never sees another tenant's data, even with an admin role;
- within a tenant, a role decides what a user may do and which documents they may see;
- each tenant gets its own quotas;
- every access is audited.

## How it works

- **Scopes.** `Scope(tenant_id=..., collection=...)` partitions every index, cache entry
  and lifecycle task. Data comes from `data/saas_tenants.json`: `acme/handbook`,
  `acme/engineering` and `globex/handbook`.
- **`TenantRoleAccessPolicy`** (a custom `AccessPolicyABC`). Principals look like
  `"acme:editor"`. The policy:
    - denies acting in another tenant's scopes and anonymous calls;
    - allows actions by role: viewers retrieve and read status, editors also ingest and
      delete, admins may do anything, including rebuilds;
    - returns a row filter on the documents' `visibility` metadata. Viewers see `public`,
      editors see `public` and `internal`, and admins also see `restricted`. Documents
      without a `visibility` label are visible to admins only. The engine applies the
      filter to every method, so restricted text never reaches a viewer's results.
- **`TenantPolicy`** for `globex`: `max_ingest_batch=2` and `max_top_k=3`. Breaking a
  quota raises `ValidationError` with machine-readable details.
- **`JsonlProvenanceRecorder`** (a custom `ProvenanceRecorderABC`) appends one audit line
  per ingestion and retrieval, with the principal, scope, query and returned document
  versions.
- **`StemmingTokenizer`** (a custom `TokenizerABC`), so `holidays` matches `holiday`.

Denials are ordinary exceptions. `AccessDeniedError.details` carries `action` and
`reason`, which a service maps to HTTP 403 (see [HTTP service](http-service.md)).

## Requirements

The base install only.

## Run

```bash
python examples/multi_tenant_saas.py
```

## Source

<!-- fmt: off -->
```python title="examples/multi_tenant_saas.py"
--8<-- "examples/multi_tenant_saas.py"
```
<!-- fmt: on -->

## Expected output

```text
acme/handbook: ingested 3 document(s)
acme/engineering: ingested 2 document(s)
globex/handbook: ingested 2 document(s)

role-based row filters (acme/handbook, 'holidays expenses salary'):
  viewer sees ['holidays']
  editor sees ['expenses', 'holidays']
  admin  sees ['expenses', 'holidays', 'salary-bands']

collection and tenant isolation ('holiday' as acme:admin / globex:admin):
  acme/handbook: Acme employees receive 30 days of paid holiday per calendar ...
  acme/engineering: Engineers on the on-call rota swap shifts before taking holi...
  globex/handbook: Globex staff receive 25 days of paid holiday; up to five unu...

denied operations:
  globex admin reads acme: denied retrieve (principal of tenant 'globex' cannot access this tenant)
  acme viewer ingests: denied ingest (role 'viewer' may not ingest)
  acme editor rebuilds: denied rebuild (role 'editor' may not rebuild)
  anonymous retrieval: denied retrieve (anonymous access is disabled)

globex quotas:
  ingest 3 documents: rejected {'count': 3, 'max': 2}
  retrieve top_k=5: rejected {'limit': 3}

acme/handbook status (viewer may read it): active=3
audit log: 9 record(s) {'ingest': 3, 'retrieval': 6}
  last: event=retrieval tenant=globex principal=globex:admin query='holiday'
```

Denied calls leave no audit record: the policy rejects them before any retrieval runs.
