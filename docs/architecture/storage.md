# Metadata store

The metadata store is the durable source of truth for documents, versions and
the state needed to keep search channels current. Index data is derived from
that source and can be rebuilt. Use a built-in store or implement
`DocumentMetadataStoreABC`.

## Store contract

The store provides asynchronous, scope-aware transactions and schema
migrations. A scoped transaction rejects access to rows outside its scope.
Lifecycle maintenance may use unscoped transactions to coordinate work across
scopes.

Use `MetadataStoreContract` from `retrievalloom.testing` to check a custom
store. See the [storage API reference](../api/storage.md) for the complete
contract.

## SQLite

`SQLiteMetadataStore` is the default. It supports in-memory use and file-backed
storage, and is suitable for a single host. Multiple processes on that host
can share a database file; writes are serialized by SQLite.

```python
from retrievalloom.storage.sqlite import SQLiteMetadataStore

store = SQLiteMetadataStore("var/metadata.db", synchronous="FULL")
```

Use `synchronous="FULL"` when stronger durability on power loss is more
important than write throughput. `busy_timeout_ms` controls how long a writer
waits for database access. A store passed to the engine remains application
owned and must be closed by the application.

## PostgreSQL

`PostgresMetadataStore` supports engines sharing metadata across hosts. Install
the `postgres` extra and configure every engine with the same database and
schema:

```python
import os

from retrievalloom import PostgresMetadataStore

store = PostgresMetadataStore(
    os.environ["RETRIEVALLOOM_DSN"],
    schema="retrievalloom",
    pool_size=5,
    max_overflow=10,
)
```

Choose pool settings with the total number of engine processes and the
database's connection limit in mind. Durable index backends must also be
shared; see [Run several engines on one store](../guide/how-to/multiple-engines.md).

## Migrations

RetrievalLoom ships versioned migrations for the built-in stores.
`LifecycleConfig.migration="auto"` applies required migrations during
initialization. Set it to `"verify"` when migrations are managed separately;
initialization then fails if the schema is not current.

## Custom stores

Implement `DocumentMetadataStoreABC` for a new backend. Applications using a
SQLAlchemy-supported database can also build on the public SQLAlchemy store
base. In either case, verify the implementation with the metadata-store
contract kit before using it with production data.
