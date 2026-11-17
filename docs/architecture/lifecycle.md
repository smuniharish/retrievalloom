# Index lifecycle

This page describes the modules that keep indexes in step with the metadata
store. For the concepts (generations, coverage, tasks, leases), read
[Generations and coverage](../guide/concepts/lifecycle.md) first.

## Modules

| Module | Role |
| --- | --- |
| `lifecycle.canonical` | Canonicalizes a `DocumentInput`: content hash, event id and idempotency tier. |
| `lifecycle.ingestion` | `IngestionService`: writes documents, versions, representations, events and visibility in one transaction per document. |
| `lifecycle.derivation` | Derives objects (chunks, structure nodes, graph aggregates) from representations. |
| `lifecycle.units` | `IndexUnit` (a method bound to its backend and fingerprint) and `ItemPreparer` (turns objects into index items, embedding them when needed). |
| `lifecycle.indexer` | `UnitIndexer` (builds, updates, publishes, retires and reconciles generations) and `ReaderTracker` (in-process readers and durable leases). |
| `lifecycle.coordinator` | `LifecycleCoordinator`: discovery, rounds through refresh-engine, maintenance, tasks. |

## A round

`LifecycleCoordinator.run_once(scope=None)` first evicts purged objects:
`DocumentEraser.evict_purged` reads the purge-log entries (`rl_purge_log`)
appended since this engine's watermark and removes their objects from the
live generations of this engine's engine-local (in-memory) units, under the
unit lock so a concurrent build cannot re-add them. Engines start following the
log at its end when the coordinator starts, because their in-memory units are
built from the store afterwards. If entries the engine never applied were
pruned (`purge_log_retention`), it rebuilds its engine-local units of that
scope instead. A failed eviction is logged (`purge_eviction_failed`) and
retried next round; indexing continues. Then it runs one refresh-engine
refresh:

1. **Discovery.** For every scope and unit, `UnitIndexer.needs_work` checks
   whether there is no pointer, the active generation has another fingerprint,
   coverage is pending, documents lack coverage rows, or a rebuild task is
   open. Each lagging pair becomes a refresh-engine resource.
2. **Processing.** `UnitIndexer.run(scope, unit)` runs for each resource, at
   most `max_concurrency` at a time, each bounded by `unit_timeout` and retried
   up to `max_attempts` times. One lock per (scope, unit, instance) serializes
   work on a unit within an engine.
   - Missing coverage rows are created for documents changed after the unit's
     coverage watermark (the highest `change_seq` already covered); a unit
     without a watermark reads the scope once.
   - For per-document units, deleted documents are removed from the active
     generation.
   - A **full build** runs when there is a rebuild task, no pointer, a
     fingerprint change, a keyword dictionary change (the
     `keyword_dictionary_revision` stat of the active generation differs from
     `KeywordDictionaryABC.revision`), or pending work
     on a unit that cannot update in place. Otherwise pending documents are
     applied **incrementally**.
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

## In-place updates of the in-memory indexes

The default keyword (`AhoCorasickKeywordIndex`), fuzzy (`RapidFuzzIndex`) and
sparse (`BM25sIndex`) indexes share `InMemoryGenerationalIndex`
(`index/memory.py`) and declare `mutable_in_place=True`. None of ahocorasick-rs,
RapidFuzz or bm25s has an incremental API, so the libraries are used for
matching and scoring only, and each index keeps its own postings:

- Before `seal`, writes are staged and built in one offloaded pass.
- After `seal`, a write is analyzed in the offloader (automaton scan,
  term counts) and then applied on the event loop without awaiting: the
  objects it replaces are deleted, the new ones inserted. `remove` deletes
  objects the same way. Both cost time proportional to the changed objects.
  A per-generation lock serializes changes; searches never see a
  half-applied change.
- After any change the structure equals a fresh build over the live objects,
  so search results do not depend on the update history:
  - keyword: canonical-keyword postings; `N` and `df` count live objects;
  - fuzzy: term postings plus a sorted vocabulary that is re-merged from the
    added and removed terms on the next search (a new list, so a running
    match keeps its own);
  - sparse: term-frequency postings over object slots. `N`, document
    frequencies and the average length of the live objects are applied with
    bm25s's idf and term-frequency functions at query time, so scores equal a
    fresh bm25s index up to bm25s's float32 rounding. Removal marks a slot
    dead; once dead slots outnumber live ones (and are at least 1,024), a
    compacted copy is built in the offloader and swapped in.

The generation is fixed to the keyword dictionary it was built with
(`GenerationSpec.keyword_entries`). A full build stores the dictionary's
revision in the generation's `keyword_dictionary_revision` stat (and the
digest of its entries in `keyword_entries_digest`); when the dictionary's
revision differs, the indexer builds a new generation instead of updating in
place. `MetadataKeywordDictionary` keeps its terms with document counts in
the metadata store, adjusted by ingest and delete in their own transactions,
so its revision is one row and changes only when a term appears or
disappears.

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
5. Housekeeping (at most every five minutes in background ticks): erase
   expired tombstones, then prune ingestion events, version history (events
   first, because a version stays while an event names it as `version_after`)
   and purge-log entries older than `purge_log_retention`.

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
