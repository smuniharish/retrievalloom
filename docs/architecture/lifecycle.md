# Index lifecycle

This page describes the modules that keep indexes in step with the metadata
store. For the concepts (generations, coverage, tasks, leases), read
[Generations and coverage](../guide/concepts/lifecycle.md) first.

## Modules

| Module | Role |
| --- | --- |
| `lifecycle.canonical` | Canonicalises a `DocumentInput`: content hash, event id and idempotency tier. |
| `lifecycle.ingestion` | `IngestionService`: writes documents, versions, representations, events and visibility in one transaction per document. |
| `lifecycle.derivation` | Derives objects (chunks, structure nodes, graph aggregates) from representations. |
| `lifecycle.units` | `IndexUnit` (a method bound to its backend and fingerprint) and `ItemPreparer` (turns objects into index items, embedding them when needed). |
| `lifecycle.indexer` | `UnitIndexer` (builds, updates, publishes, retires and reconciles generations) and `ReaderTracker` (in-process readers and durable leases). |
| `lifecycle.coordinator` | `LifecycleCoordinator`: discovery, rounds through refresh-engine, maintenance, tasks. |

## A round

`LifecycleCoordinator.run_once(scope=None)` runs one refresh-engine refresh:

1. **Discovery.** For every scope and unit, `UnitIndexer.needs_work` checks
   whether there is no pointer, the active generation has another fingerprint,
   coverage is pending, documents lack coverage rows, or a rebuild task is
   open. Each lagging pair becomes a refresh-engine resource.
2. **Processing.** `UnitIndexer.run(scope, unit)` runs for each resource, at
   most `max_concurrency` at a time, each bounded by `unit_timeout` and retried
   up to `max_attempts` times. One lock per (scope, unit, instance) serialises
   work on a unit within an engine.
   - Missing coverage rows are created.
   - For per-document units, deleted documents are removed from the active
     generation.
   - A **full build** runs when there is a rebuild task, no pointer, a
     fingerprint change, or pending work on a unit that cannot update in place.
     Otherwise pending documents are applied **incrementally**.
3. **Settling tasks.** Tasks whose units are covered succeed. A task whose
   units still fail after `max_attempts` rounds fails with "units did not
   reach coverage".

## Publishing

A full build writes a new generation in state `building`, then publishes it in
one write transaction:

- compare-and-swap of the generation pointer (`rl_generation_pointers`) from
  the previous generation to the new one;
- the new generation becomes `active`, the previous one `retiring`;
- coverage rows and version visibility are updated for the indexed documents.

If the pointer moved in the meantime, the transaction fails and the new
generation is marked `failed` and dropped. After a successful publish the
indexer tries to retire the previous generation at once. Incremental updates
commit coverage and visibility the same way, against the active generation.

## Maintenance

`maintain()` runs at startup and at the start of each background tick:

1. Heartbeat this engine instance (`rl_engine_instances`), extending its
   registration by `instance_ttl`.
2. Purge expired reader leases.
3. For every scope and unit, `reconcile`:
   - try to retire each `retiring` generation;
   - mark `building` generations of expired instances `abandoned` and drop
     them;
   - drop backend generations unknown to the store or no longer live
     (orphans).
4. Purge the metadata rows of expired engine instances.

## Retirement

`UnitIndexer.retire` drops a `retiring` generation only when:

- no in-process request reads it (`ReaderTracker`);
- `generation_retention` seconds have passed since it was retired;
- for durable units, no other engine instance holds an unexpired reader lease
  (checked in the same write transaction that marks it `retired`).

The generation is marked `retired` before its data is dropped, so no reader
can lease it while it is being dropped. A failed drop leaves an orphan that
the next `reconcile` removes.

## Reader tracking

`ReaderTracker` counts in-process readers per generation. A generation marked
retiring accepts no new readers: the request skips that method with a
`stage_skipped` warning. For durable units, a reader also writes or renews a
row in `rl_reader_leases` keyed by generation and engine instance, expiring
after `lease_ttl`. The row is renewed when less than half of the TTL remains,
and deleted when the instance deregisters.
