# Infrastructure and evidence components

Replace the parts of the engine around the indexes: where metadata lives,
what is cached, who may do what, where provenance goes, and how hits become
evidence.

## Metadata store

The metadata store holds documents, versions, objects, generations, and
leases for every scope. It is the engine's source of truth, so prefer a
built-in store: `SQLiteMetadataStore` or `PostgresMetadataStore`.

For another database, subclass `SQLAlchemyMetadataStore`
(`retrievalloom.storage.sql`) for its SQLAlchemy async dialect, or implement
`DocumentMetadataStoreABC` (`retrievalloom.storage.base`) directly. Prove it
with `MetadataStoreContract`; see
[contract-kits.md](contract-kits.md#test-a-metadata-store).

## Cache

`CacheABC` has `get(namespace, key)`, `set(namespace, key, value)`, and
`invalidate(namespace, prefix=None)`, which returns how many entries it
removed.

- The engine builds every namespace and key. A retrieval result's namespace
  is `retrieval:<scope key>`, so the cache keeps namespaces apart and never
  decides isolation itself.
- Keys include the store's commit sequence, so a write makes older entries
  unreachable. Bound memory with a TTL or a size limit.
- The engine stores only `complete` results, as Python objects. Serialize
  them so that `get` returns an equal object of the same type; pickle works.
  A value of another type counts as a miss.
- Return `None` for a miss.

`assets/cache_template.py` is a dictionary cache with comments naming the
Redis-style call for each step. Set `stores_values = False` in the test only
for a cache that never stores anything.

## Access policy

`AccessPolicyABC` has two methods:

- `authorize(principal, scope, action)` returns
  `AccessDecision(allowed=..., reason=...)`. The engine calls it before
  `ingest`, `delete`, `purge` (as `Action.DELETE`), `retrieve`, `backfill`,
  `rebuild`, and `status`. A denial raises `AccessDeniedError`.
- `row_filter(principal, scope)` returns a `FilterExpression` or `None`. The
  engine combines it with the request's filters using `And`, so a caller can
  narrow it but never widen it.

`principal` is the opaque string the application passes to each call.
Authenticate the caller before passing it. There is no kit:
`assets/test_access_policy.py` tests `assets/access_policy_template.py`
through a real engine.

## Provenance recorder

`ProvenanceRecorderABC` receives every write and every retrieval:

- `record_ingestion(report)` with an `IngestReport` or `DeleteReport`.
- `record_retrieval(request, result)` with the `RetrievalRequest` and
  `RetrievalResult`.

A `TenantIsolationError` from the recorder always fails the call. Any other
error is logged as `provenance_recording_failed`; ingestion reports and
retrieval results then carry a `provenance_recording_failed` warning. With
`fail_on_recording_error=True` in the retrieval config, the error becomes a
`ProvenanceError` instead.

The built-in `AgxaiProvenanceRecorder` (`retrievalloom.integrations.agxai`)
is a complete example.

## Fusion and score normalization

`FusionABC.fuse(lists, weights)` receives ranked lists keyed by source name.
Each entry is a `RankedEntry(item_id, rank, score, semantics)`. It returns
`FusedEntry(item_id, score)` objects, best first, with deterministic tie
order. Set the `needs_scores` property to `True` if the formula needs raw
scores; rank-only sources are then rejected.

Pass a fusion as `Components(method_fusion=...)` to combine methods within a
channel, or `channel_fusion=...` to combine channels. `FusionContract` needs
a `reference(lists, weights)` hook that returns the expected scores of your
formula.

`ScoreNormalizerABC.normalize(entries)` maps one list's raw scores to
comparable values keyed by `item_id`. Pass it to a score-based fusion such as
`WeightedScoreFusion(normalizer=...)` from `retrievalloom.evidence.fusion`,
and test it with `ScoreNormalizerContract`.

## Deduplicator and consolidator

`DeduplicatorABC.deduplicate(candidates)` returns
`DedupResult(kept, merged)`. Merge duplicates without losing any source or
hit, keep the best candidate first, and pass unrelated candidates through in
order. `DeduplicatorContract` checks these rules.

`EvidenceConsolidatorABC.consolidate(candidates, context)` groups the final
ordered candidates into evidence. It returns the selected evidence and the
ids of contradicting evidence left out by `context.contradiction_overflow`.
`EvidenceConsolidatorContract` checks that:

- every source and hit of each member survives;
- a contradicting candidate is kept;
- semantic similarity never merges different documents;
- candidates of different scopes are rejected.

The built-in `HierarchicalConsolidator` (`retrievalloom.evidence.consolidation`)
is the reference.
