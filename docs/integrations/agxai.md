# agxai

[agxai](https://pypi.org/project/agxai/) records what an AI application did and
why. RetrievalLoom can record its retrievals, ingestions and deletions into an
agxai run you control. The mapping is one way: RetrievalLoom to agxai.

## Enable it

Pass your `XAIRuntime` as `EngineResources(xai_runtime=...)`. The engine then
uses `AgxaiProvenanceRecorder` as its provenance recorder. Records are written
only while an agxai run is active:

```python
from agxai import XAIRuntime
from retrievalloom import EngineResources, RetrievalEngine

runtime = XAIRuntime(application_id="support-bot", tenant_id="acme")
engine = RetrievalEngine(resources=EngineResources(xai_runtime=runtime))

async with engine:
    run = await runtime.start_run()
    with runtime.activate_run(run):
        result = await engine.retrieve("reset password", scope=scope)
    await runtime.finish_run(run)
```

The runtime stays yours: the engine never closes it. Close it after use with
`await runtime.close()`.

## What is recorded

| Operation | agxai record |
| --- | --- |
| `retrieve` | One `record_retrieval` call. The retriever id is `retrievalloom:<collection>`; `query_reference` is the request id. Each evidence item becomes a `RetrievedDocument` in result order, with `content_reference` set to the evidence id. No text is copied. Metadata holds the request id, scope key, status, snapshot sequence and warning codes. |
| `retrieve` (links) | A `derived_from` provenance link from each source document uid to the evidence id. Disable with `record_links=False`. |
| `ingest`, `delete` | One `custom` evidence record with outcome counts only. |

When evidence carries rerank scores, the retrieval names the reranker given as
`reranker_id`.

## Configure the recorder

To change the options, create the recorder yourself and pass it as a
component:

```python
from retrievalloom import Components
from retrievalloom.integrations.agxai import AgxaiProvenanceRecorder

recorder = AgxaiProvenanceRecorder(
    runtime,
    run_provider=lambda: my_current_run(),
    record_links=False,
    reranker_id="bge-reranker-v2-m3",
)
components = Components(provenance_recorder=recorder)
```

`run_provider` returns the run to record into; when it returns `None`, the
runtime's current run is used. Without a run nothing is recorded.

## Tenant isolation and failures

If the run's tenant differs from the request's scope tenant, recording raises
`TenantIsolationError`, and the operation fails. Other recording errors add a
`provenance_recording_failed` warning to the result or ingest report, unless
`RetrievalConfig.fail_on_recording_error=True`, in which case they raise
`ProvenanceError`.

See the [agxai provenance example](../examples/agxai-provenance.md).
