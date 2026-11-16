# agxai provenance

This example records RetrievalLoom operations in an [agxai](https://pypi.org/project/agxai/)
run. Passing an agxai `XAIRuntime` as `EngineResources(xai_runtime=...)` makes the engine
use the agxai provenance recorder. Operations performed while a run is active
(`with runtime.activate_run(run):`) are recorded on that run:

- each ingestion and deletion becomes a custom evidence record (`retrievalloom ingestion`,
  `retrievalloom deletion`) with outcome counts;
- each retrieval becomes an agxai retrieval record with the retriever id
  `retrievalloom:<collection>`, the request id as query reference, and one ranked document
  per evidence item, in result order, whose content reference is the evidence id;
- each evidence item gets `derived_from` provenance links to its source documents' uids,
  which the example reads back from the runtime's `ProvenanceStore`.

Nothing is recorded outside an active run. The runtime's tenant must match the tenant of
the scope; a run of another tenant is rejected with `TenantIsolationError`.

## Requirements

The base install only (agxai is a core dependency).

## Run

```bash
python examples/agxai_provenance.py
```

## Source

<!-- fmt: off -->
```python title="examples/agxai_provenance.py"
--8<-- "examples/agxai_provenance.py"
```
<!-- fmt: on -->

## Expected output

```text
evidence record: retrievalloom ingestion {'created': 3}
evidence record: retrievalloom deletion {'deleted': 1, 'already_deleted': 0, 'not_found': 0}
retrieval: retriever=retrievalloom:support status=complete
  query reference = request id: True
  rank 1: reset score=0.0164 links=['derived_from']
    content reference = evidence id: True
    linked to source uid: True
  rank 2: lockout score=0.0161 links=['derived_from']
    content reference = evidence id: True
    linked to source uid: True
```
