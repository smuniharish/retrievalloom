# Document lifecycle

Production corpora change all the time. Runbooks get edited, retired or must be erased,
and the retrieval stack gains new methods and new embedding models. The example takes one
scope of IT runbooks through three engine processes that share a durable `data_dir` (the
SQLite metadata store). Dense vectors live in Postgres through the custom `PgVectorStore`.

## How it works

1. **Lexical methods only.**
    - Ingest five runbooks.
    - Re-ingest an edited runbook: it becomes version 2.
    - Re-send an unchanged runbook: nothing changes.
    - `delete` a retired runbook. This writes a tombstone, and the runbook leaves results
      at once.
    - `purge` a runbook that must be erased (right to erasure). Its metadata rows and
      index objects are physically removed, and unknown ids are reported as `not_found`.
2. **Add the dense method.**
    - Restart with `BgeM3Embedding` and `PgVectorStore`. The new method has no coverage
      yet. With the foreground lifecycle (`background=False`), the engine catches up while
      it opens, so existing runbooks are embedded before the first request.
    - Ingest a runbook with `IndexWait.NONE`. It is stored but not indexed by the dense
      method, so dense retrieval reports `partial_coverage` until an operator-triggered
      `backfill` indexes it.
3. **Change the model configuration.**
    - Restart with a different embedding configuration (`max_length=256`). Its fingerprint
      differs, so vectors from the two configurations are never mixed. The engine builds a
      new pgvector generation at startup and switches to it atomically.
    - `rebuild` forces another full generation on demand. Earlier generations are retired.

`status` shows documents, coverage per method, index generations and lifecycle tasks.
`health` shows the health of the engine and its components.

## Requirements

- Local models in `models/` (`bge-m3`) and the `examples` dependency group (`asyncpg`).
- A pgvector database: `RL_EXAMPLES_PGVECTOR_DSN=postgresql://rl:rl@localhost:55433/rl`.

The tables use a unique per-run prefix and are dropped at the end.

## Run

```bash
python examples/document_lifecycle.py
```

## Source

<!-- fmt: off -->
```python title="examples/document_lifecycle.py"
--8<-- "examples/document_lifecycle.py"
```
<!-- fmt: on -->

## Expected output

```text
phase 1: lexical methods only
   ingest: {'created': 5}
   re-ingest cert-rotation: updated v2
   re-ingest db-failover: unchanged v1
   cert-rotation now says: 'Rotate TLS certificates every 60 days with the cert-manager renewal job.'
   delete: deleted=('legacy-vpn',)
   purge:  purged=('contractor-access',) not_found=('never-existed',)
   'VPN access' after delete+purge: []
   documents: active=3 deleted=1
   coverage index.fuzzy: {'deleted': 1, 'ready': 3}
   coverage index.keyword: {'deleted': 1, 'ready': 3}
   coverage index.sparse: {'deleted': 1, 'ready': 3}
phase 2: restart with bge-m3 on pgvector (the dense method is new)
   index.dense after startup catch-up: {'deleted': 1, 'ready': 3}
   'the server ran out of storage space' -> ['disk-full']
   after IndexWait.NONE ingest: active=4 index.dense={'deleted': 1, 'ready': 3}
   'hostnames cannot be looked up' -> ['disk-full'] warnings=['partial_coverage']
   backfill task: succeeded
   documents: active=4 deleted=1
   coverage index.dense: {'deleted': 1, 'ready': 4}
   coverage index.fuzzy: {'deleted': 1, 'ready': 4}
   coverage index.keyword: {'deleted': 1, 'ready': 4}
   coverage index.sparse: {'deleted': 1, 'ready': 4}
   'hostnames cannot be looked up' -> ['dns-outage']
phase 3: restart with a new embedding configuration (max_length=256)
   re-embedded on startup (new generation): True
   rebuild task: succeeded
   rebuild switched generation: True
   dense generations: ['active', 'retired', 'retired']
   'the server ran out of storage space' -> ['disk-full']
   tasks: {'succeeded': 2}
   health: healthy unhealthy=[]
```

Things to notice:

- The deleted runbook stays as a tombstone (`deleted=1`). The purged runbook leaves no
  trace.
- Before the backfill, the DNS question falls back to the only indexed match and warns
  `partial_coverage`. After the backfill it finds `dns-outage`.
