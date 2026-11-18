# Backfill and rebuild

Add semantic search to documents you already indexed, retry documents that
failed, or rebuild an index after a model change, all while searches keep
working. Indexes normally stay current on their own: background indexing
handles every ingested document and rebuilds an index when its configuration
changes. Backfill and rebuild let you start that work yourself.

Both operations queue a durable task in the metadata store and return a
`LifecycleTaskRef` at once. Background indexing runs the task; wait for it
with `wait_for`.

## Fill gaps with a backfill

A backfill indexes every document that the selected units do not cover yet.
Use it after a unit failed for some documents, or after enabling a method on
an existing scope.

```python
ref = await engine.backfill(scope=scope)  # every configured unit
ref = await engine.backfill(scope=scope, channels=["index"], methods=["dense"])

result = await engine.wait_for(ref, timeout=300)
print(result.state.value, result.attempts, result.error)
```

- Queuing a backfill resets the units' `failed` coverage to `pending`, with
  the attempt counter at zero. Failures with a retryable error, such as a
  backend outage, are retried automatically
  ([Failures](../concepts/lifecycle.md#failures)), so
  you need a backfill only after a non-retryable failure or to retry at once.
- The task succeeds once no selected unit has pending work left.

Retrieval can queue backfills itself: with
`RetrievalConfig(missing_index_policy="schedule_backfill")`, a request that
finds a method without an index queues a backfill and returns a
`backfill_scheduled` info warning. Coverage in the `failed` state also
queues one.

## Rebuild an index

A rebuild builds a complete new copy (generation) of one index and switches
to it in one step. Requests keep reading the old generation until the switch, and
requests that started before the switch finish on the old one.

```python
ref = await engine.rebuild(scope=scope, channel="index", method="dense")
result = await engine.wait_for(ref, timeout=600)
```

`channel` is `"index"`, `"graph"` or `"structure"`. `method` selects one index
method (`keyword`, `fuzzy`, `sparse`, `dense`); omit it to rebuild every method
of the channel.

You rarely need to rebuild by hand. Background indexing starts a full build on
its own when an index does not exist yet or when its configuration changes, for
example after you switch the embedding model.

## Wait for a task

`wait_for(ref, timeout=None)` returns a `LifecycleTaskResult` when the task is
terminal: `succeeded`, `failed` or `cancelled`. A failed task is returned, not
raised: check `result.state` and `result.error`.

- With the background scheduler, `wait_for` polls the task.
- Without it (`LifecycleConfig(background=False)`), `wait_for` runs lifecycle
  rounds for the task's scope itself.
- If `timeout` elapses first, it raises `OperationTimeoutError`. The task
  keeps running.

Within one round, [refresh-engine](../../integrations/refresh-engine.md)
retries a failing unit up to `LifecycleConfig.max_attempts` times. A task
whose units fail in `max_attempts` rounds ends `failed`, with
`error.message` "units did not reach coverage".

## Select indexes

`channels`, `channel` and `methods` must match configured units. A selection
that matches none raises `ConfigurationError`, for example `method="dense"`
without an embedding model.

## Watch progress

`engine.status(scope=scope)` returns a `ScopeStatus`:

```python
status = await engine.status(scope=scope)
print(status.coverage)  # {"index.dense": {"ready": 120, "pending": 3}, ...}
print(status.generations)  # method, generation id, state, backend id
print(status.tasks)  # {"succeeded": 2, "queued": 1}
```

## Next steps

- [Generations and coverage](../concepts/lifecycle.md) explains generations,
  coverage states and how changes reach the indexes.
- The [document lifecycle example](../../examples/document-lifecycle.md)
  backfills a new dense method and rebuilds after a model change.
