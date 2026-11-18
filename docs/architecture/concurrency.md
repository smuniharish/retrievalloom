# Concurrency and isolation

RetrievalLoom exposes an asynchronous API and shares its event loop with the
calling application. It offloads synchronous model and backend work, bounds
concurrent work and applies scope checks throughout retrieval and storage.

## Event-loop responsiveness

Async backends are awaited directly. Synchronous model or backend calls run
through the engine's offloader rather than blocking the event loop. Custom
components should use `context.offloader` for blocking work; see
[Offloading and admission](../guide/concepts/offloading.md).

All public operations are coroutines. Applications with a synchronous entry
point should own an event loop and submit coroutines to it rather than using a
synchronous RetrievalLoom wrapper.

## Concurrency controls

Ingestion, lifecycle work and offloaded jobs use resource-aware concurrency by
default. `AutoConcurrency` adapts to available host capacity. Applications
can set a fixed integer or cap automatic growth. `EngineLimits` controls the
maximum accepted values.

Request and per-tenant limits are configured separately. When work cannot be
admitted before its deadline, RetrievalLoom surfaces a timeout or resource
admission error rather than accepting unbounded work. See
[Configuration](../getting-started/configuration.md#automatic-concurrency).

## Cancellation

Cancelling an operation releases its request and snapshot state. A synchronous
function already running in a worker thread cannot be forcibly stopped; its
capacity remains reserved until it completes. This prevents cancelled
coroutines from making the configured worker capacity appear available too
early.

## Multiple engines

Engines can share a SQLite metadata file on one host or PostgreSQL metadata
across hosts. Durable search backends must also be shared. In-memory indexes
are local to each process and are refreshed by that engine's lifecycle.

SQLite is intended for a single host. For multi-host deployments, use
PostgreSQL and a server-backed vector store. Embedded graph and local vector
database files may have a single-process owner; use a server backend or a
single owner service when multiple hosts need the same data. See
[Run several engines on one store](../guide/how-to/multiple-engines.md).

## Tenant isolation

Every operation takes a `Scope`; backend access and returned evidence are
checked against it. Configure an access policy for application-level
authorization. The default policy allows requests and is not a security
boundary. See [Filter and authorize](../guide/how-to/filters-and-access.md).
