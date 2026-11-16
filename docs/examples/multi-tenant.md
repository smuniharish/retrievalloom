# Multi-tenant

This example shows that scopes isolate tenants inside one engine. Two tenants, `acme` and
`globex`, both ingest a document with the id `vacation`. Each retrieval returns only the
evidence of its own scope, a query that only matches `acme` content returns nothing for
`globex`, and deleting `remote` in the `globex` scope reports it as not found there.

It also configures a `TenantPolicy` that limits `globex` to two documents per ingest
call; a batch of three is rejected with a `ValidationError`. Finally, `status` reports
document counts and index coverage per scope.

## Requirements

The base install only.

## Run

```bash
python examples/multi_tenant.py
```

## Source

<!-- fmt: off -->
```python title="examples/multi_tenant.py"
--8<-- "examples/multi_tenant.py"
```
<!-- fmt: on -->

## Expected output

```text
acme: 2 evidence item(s)
  vacation: Acme employees get 30 vacation days per year. (tenants={'acme'})
  remote: Acme staff may work remotely three days a week. (tenants={'acme'})
globex: 1 evidence item(s)
  vacation: Globex employees get 20 vacation days per year. (tenants={'globex'})
globex 'work remotely': status=empty
delete 'remote' in globex: deleted=() not_found=('remote',)
globex batch of 3 rejected: {'count': 3, 'max': 2}
status acme: active=2 deleted=0 coverage={'index.fuzzy': {'ready': 2}, 'index.keyword': {'ready': 2}, 'index.sparse': {'ready': 2}}
status globex: active=1 deleted=0 coverage={'index.fuzzy': {'ready': 1}, 'index.keyword': {'ready': 1}, 'index.sparse': {'ready': 1}}
```
