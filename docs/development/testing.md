# Testing

The default test run needs no services, no network and no model weights. Live
suites run against real backends when you point them at one.

## Everyday commands

```bash
uv sync --all-extras --group dev
uv run pytest -m "not live"                                    # the offline suite
uv run pytest -m "not live" --cov=retrievalloom --cov-branch \
  --cov-report=term-missing                                    # with coverage (kept at 100%)
uv run ruff check . && uv run ruff format --check .            # lint and formatting
uv run pyrefly check                                           # type checking
```

Branch coverage must stay at 100% (`fail_under = 100`). Warnings are errors:
pytest runs with `filterwarnings = ["error"]`, so a new deprecation warning
fails the suite until it is fixed.

## Layout

| Directory | Covers |
| --- | --- |
| `tests/unit` | Domain models, configuration, the engine, storage, lifecycle, retrieval, evidence, and every built-in backend |
| `tests/contracts` | The built-in components against the `retrievalloom.testing` contract kits |
| `tests/examples` | Every script under `examples/` |
| `tests/benchmarks` | Event-loop responsiveness and a small run of `scripts/benchmark.py` |
| `tests/live` | Qdrant server, real models, durable graph and structure backends, and a load and soak test |

## Markers

| Marker | Meaning |
| --- | --- |
| `live` | Needs explicitly configured external services. Added automatically to every test under `tests/live`. |
| `qdrant` | Needs `RL_TEST_QDRANT_URL`. Skipped otherwise. |
| `models` | Needs local model weights under `RL_TEST_MODELS_DIR`. Skipped otherwise. |
| `graph` | Needs the `graph` extra. |
| `structure` | Needs the `structure` extra. |
| `benchmark` | Performance regression tests. |
| `examples` | Runs the scripts under `examples/`. |

Markers are strict: an unknown marker is an error.

## Live suites

Start Qdrant with Podman:

```bash
podman run -d --name rl-qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

Then set the variables and run the live tests:

| Variable | Use |
| --- | --- |
| `RL_TEST_QDRANT_URL` | Qdrant server URL, for example `http://localhost:6333`. Enables `qdrant` tests. |
| `RL_TEST_QDRANT_GRPC_PORT` | gRPC port for the gRPC tests. Default `6334`. |
| `RL_TEST_MODELS_DIR` | Folder holding the reference models. Enables `models` tests. |
| `RL_EXAMPLES_QDRANT_URL`, `RL_EXAMPLES_MODELS_DIR` | Same for the live examples. Fall back to the `RL_TEST_*` variables. |

```bash
export RL_TEST_QDRANT_URL=http://localhost:6333
export RL_TEST_MODELS_DIR=models
uv run pytest -m live
```

In PowerShell, set the variables with `$env:RL_TEST_QDRANT_URL = "http://localhost:6333"`.

Every test uses a unique collection prefix and drops its collections
afterwards, so a shared server is safe. Continuous integration runs
`pytest -m "live and not models"` against a Qdrant service container.

### Models

The `models` tests and the live examples use local models. They are never a
runtime dependency of RetrievalLoom. `scripts/copy_models.py` copies them into
the git-ignored `models/` folder:

```bash
uv run python scripts/copy_models.py --source "/path/to/HF_MODELS"
```

It copies `bge-m3`, `bge-reranker-v2-m3` and `UltraBERT-150M-nli`. The live
examples also need the `examples` dependency group (`torch`, `transformers`).

FlashRank is not copied, and the tests never download it. Provision it by hand
(see [Provision the model files](../guide/how-to/rerank-and-nli.md#provision-the-model-files)):

```text
models/
  bge-m3/
  bge-reranker-v2-m3/
  UltraBERT-150M-nli/
  flashrank/
    ms-marco-TinyBERT-L-2-v2/
      flashrank-TinyBERT-L-2-v2.onnx
      config.json
      tokenizer.json
      tokenizer_config.json
      special_tokens_map.json
```

If the FlashRank files are missing, its contract test is skipped.

### Event-loop lag and garbage collection

The soak test measures event-loop lag while the engine is under load. A full
garbage collection over a large heap, for example many cached torch models
from earlier tests, can pause the loop for longer than any engine work. The
test runs `gc.collect()` and then `gc.freeze()` before it measures, so the
long-lived objects are left out of later collections. Do the same in
latency-sensitive applications after loading models; see
[Keep the event loop responsive](../guide/how-to/event-loop-latency.md).

## Benchmarks

```bash
uv run python scripts/benchmark.py --documents 2000 --queries 500
```

`tests/benchmarks` runs it with small counts and generous bounds, so it
catches order-of-magnitude regressions only.

## Testing your own backend

`retrievalloom.testing` ships the same contract kits the built-in components
run. See [Write your own backend](../guide/how-to/custom-backends.md).
