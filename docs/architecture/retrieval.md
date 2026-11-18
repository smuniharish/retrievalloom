# Retrieval execution

`retrieve()` runs the configured search methods for a scoped query, combines
their candidates and returns ranked evidence with source provenance.

![A retrieval request is authorized and planned, searches configured methods, combines and refines candidates, then returns evidence with status and warnings](../assets/diagrams/retrieval-pipeline.png){ .diagram }

## Request flow

1. **Authorize and plan.** The engine checks the request scope and access
   policy, then applies the request, tenant and engine configuration.
2. **Select search state.** The engine uses the currently available index
   state for the request. Cache entries are reused only when the request,
   effective filters, configuration and search state match.
3. **Search and combine.** Configured methods run concurrently within their
   limits. Their candidates are combined using the configured fusion
   strategies.
4. **Refine evidence.** Optional reranking and NLI run when configured.
   Evidence is validated and returned with its source and provenance.

## Results and failures

`RetrievalResult` reports a status, evidence, warnings and timings. A missing
or failed method is reported rather than silently treated as a successful
search. With partial-on-deadline enabled, completed method results can be
returned with a partial status; otherwise the request raises its timeout
error.

Every result is checked against the request scope. A cross-scope result raises
`TenantIsolationError`; it is never converted into a warning or partial
success.

See the [retrieval concepts](../guide/concepts/retrieval.md) and
[results and evidence reference](../api/results.md).
