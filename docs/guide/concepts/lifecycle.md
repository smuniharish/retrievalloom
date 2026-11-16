# Generations and coverage

The metadata store is the source of truth. Indexes are derived from it and can
always be rebuilt from it. This page explains how they are kept in step.

## Index units

Each enabled method is an *index unit*, named `<channel>.<method>` (for
example `index.dense`). A unit has a backend and a *dependency fingerprint*:
a digest of the backend's identity and configuration and of the components it
depends on (tokenizer, keyword dictionary, embedding model). Changing any of
them changes the fingerprint.

## Generations

A *generation* is one build of one unit for one scope. Its state moves through:

```text
building -> active -> retiring -> retired
    \-> failed            (build error)
    \-> abandoned         (builder engine instance expired)
```

Each scope and unit has a *pointer* to its active generation. A new generation
is published by moving the pointer with a compare-and-swap in one metadata
transaction, together with the document visibility and coverage updates.
Readers see the old generation or the new one, never a mix.

The previous generation becomes `retiring`. It is dropped once nobody reads it
and `generation_retention` seconds have passed. See
[Reader leases](#reader-leases).

## Coverage

Coverage is tracked per document and unit:

| State | Meaning |
| --- | --- |
| `pending` | The current version still has to be indexed. |
| `ready` | The current version is in the active generation. |
| `failed` | Indexing failed `max_attempts` times. Reset by `backfill`. |
| `deleted` | The document was deleted and removed from the index. |
| `not_applicable` | The document has nothing for this unit, for example no `structure` representation. |
| `processing`, `stale` | Defined in `CoverageState` and counted as not ready, but the current indexer does not set them. |

`engine.status(scope=...)` returns the counts per unit. Retrieval reports gaps
as warnings (see [Retrieval warnings](#what-retrieval-sees)).

## How changes reach the indexes

Each lifecycle round compares every unit with the metadata store and does one
of two things:

- **Incremental update.** If the unit already has a generation with the
  current fingerprint and its backend can be changed in place, the new and
  changed documents are written to the active generation and the objects of
  their older versions are removed. Deleted documents are removed. The Qdrant
  vector store and the TreeSearch structure engine work this way.
- **Full build.** Otherwise a new generation is built from the metadata store
  and published. This happens for the first build, after a fingerprint change,
  after `rebuild`, and for every change on backends that cannot be changed in
  place (keyword, fuzzy and sparse indexes) and on the graph channel (graph
  aggregates depend on many documents).

A document version becomes visible to a unit only when it is indexed there.
Until then, hits of its previous version are dropped (a
`stale_document_version` info warning), unless `allow_stale_versions=True`. Use `engine.rebuild(...)` to force a new
generation.

## Who runs the rounds

- `ingest` and `delete` run one round for the scope and wait for it when
  `IngestionConfig.index_wait="all"` (the default), up to
  `index_wait_timeout`.
- With `LifecycleConfig.background=True` (the default) a scheduler runs a
  maintenance and indexing round every `poll_interval` seconds.
- `engine.run_lifecycle_once(scope)` runs one round on demand, for
  deployments that schedule work themselves.
- `engine.wait_for(task)` runs rounds itself when there is no background
  scheduler.

Rounds run through [refresh-engine](../../integrations/refresh-engine.md),
which retries failed units and limits concurrency (`max_concurrency`).

## Tasks

`backfill` and `rebuild` queue durable lifecycle tasks and return a
`LifecycleTaskRef`. Wait for one with `wait_for`:

```python
ref = await engine.rebuild(scope=scope, channel="index", method="dense")
result = await engine.wait_for(ref, timeout=300)
print(result.state.value)  # "succeeded"
```

- `backfill(scope=..., channels=..., methods=...)` indexes every document the
  selected units do not cover yet, and resets `failed` coverage to `pending`.
- `rebuild(scope=..., channel=..., method=...)` builds a new generation of the
  unit and switches to it atomically.

See [Backfill and rebuild](../how-to/backfill-and-rebuild.md).

## Ephemeral and durable backends

A backend is *durable* when its data outlives the engine process:

| Backend | Durable when |
| --- | --- |
| `AhoCorasickKeywordIndex`, `RapidFuzzIndex`, `BM25sIndex` | Never (in memory) |
| `QdrantVectorStore` | `path=`, `url=` or `client=` is set |
| `LadybugGraphStore` | `path` is set |
| `TreeSearchStructureEngine` | `directory` is set |

Ephemeral generations belong to one engine instance. Their metadata rows
(coverage, visibility, pointers, generations) carry the instance id and are
purged when the engine closes or its registration expires
(`instance_ttl`). With `auto_rebuild_ephemeral=True`, a new engine rebuilds
them from the metadata store at startup: before the engine opens when
`background=False`, otherwise in the first background round (see
[Keep data on disk](../how-to/durable-storage.md)).

Durable generations are shared by every engine that uses the same metadata
store and the same backend location.

## Reader leases

Retirement must never drop a generation that a request is reading.

- **In-process readers.** Every retrieval registers as a reader of each
  generation it uses until the request ends. A retiring generation accepts no
  new readers and is not dropped while readers remain.
- **Durable reader leases.** For durable backends shared by several engines on
  one metadata store, each engine also holds a durable reader lease, per
  generation and per engine instance, while it reads. A lease lasts
  `lifecycle.lease_ttl` seconds (default 120). It is renewed when less than
  half of the TTL remains.

Retirement of a generation waits until no *other* engine holds an unexpired
lease. An engine's lease may persist up to `lease_ttl` after its last read,
unless the engine closes: closing deregisters the instance and deletes its
leases. Expired leases are purged by the maintenance round.
`generation_retention` delays dropping further.

Ephemeral backends need no leases: they are per instance.

## What retrieval sees

When the snapshot is taken, each planned method is checked:

| Situation | Warning | Method runs? |
| --- | --- | --- |
| No published generation | `index_missing` | No |
| Generation built with another fingerprint | `stale_coverage` | No |
| Generation is being retired | `stage_skipped` | No |
| Not every active document is `ready` or `not_applicable` | `partial_coverage` | Yes |

All four have `warning` severity, so the result is `partial`.
`RetrievalConfig.missing_index_policy` controls missing indexes: `skip`
(default), `fail` (raise `CoverageError`) or `schedule_backfill` (queue a
backfill task and add a `backfill_scheduled` info warning).
