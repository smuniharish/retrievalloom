# Lifecycle: backfill and rebuild

This example follows documents through the index lifecycle:

1. With `IngestionConfig(index_wait=IndexWait.NONE)`, `ingest` returns once the documents
   are stored, without indexing them. `status` shows no coverage and no generation yet,
   and a retrieval returns no evidence and the `index_missing` warning.
2. `backfill` queues a task that indexes every document the selected methods (all, by
   default) do not cover yet; `wait_for` waits until the task is finished. Coverage is now
   `ready` and each method has an `active` generation.
3. `rebuild` of the `index.sparse` method builds a new generation and switches to it
   atomically; the previous generation is marked `retired`.
4. `delete` removes a document from retrieval results at once. Its coverage stays
   `pending` until a lifecycle round removes it from the indexes; here
   `run_lifecycle_once` runs that round, after which its coverage is `deleted`.
5. `health` aggregates the health of the engine's components.

`LifecycleConfig(background=False)` disables the background scheduler, so `wait_for` runs
the lifecycle rounds itself. With the default background scheduler the same calls wait
for the scheduler instead.

## Requirements

The base install only.

## Run

```bash
python examples/lifecycle_backfill_rebuild.py
```

## Source

<!-- fmt: off -->
```python title="examples/lifecycle_backfill_rebuild.py"
--8<-- "examples/lifecycle_backfill_rebuild.py"
```
<!-- fmt: on -->

## Expected output

```text
ingested: {'created': 3}
after ingest: active=3 deleted=0 coverage[index.sparse]={} generations[index.sparse]=[]
retrieve before backfill: status=empty warnings=['index_missing', 'method_unavailable', 'reranker_unavailable']
backfill task: succeeded
after backfill: active=3 deleted=0 coverage[index.sparse]={'ready': 3} generations[index.sparse]=['active']
retrieve after backfill: ['ssh', 'tls']
rebuild task: succeeded
after rebuild: active=3 deleted=0 coverage[index.sparse]={'ready': 3} generations[index.sparse]=['retired', 'active']
active sparse generation replaced: True
deleted: ('ssh',)
retrieve after delete: ['tls']
after delete: active=2 deleted=1 coverage[index.sparse]={'pending': 1, 'ready': 2} generations[index.sparse]=['retired', 'active']
after lifecycle round: active=2 deleted=1 coverage[index.sparse]={'deleted': 1, 'ready': 2} generations[index.sparse]=['retired', 'active']
tasks: {'succeeded': 2}
health: healthy units=['index.fuzzy', 'index.keyword', 'index.sparse']
```
