# Installation

Install RetrievalLoom with one command and start searching right away: the base
package runs a complete engine with no model, server or network access.
RetrievalLoom supports Python 3.12, 3.13 and 3.14.

=== "pip"

    ```bash
    pip install retrievalloom
    ```

=== "uv"

    ```bash
    uv add retrievalloom
    ```

## Add optional features

Install an extra only when you need the feature it adds:

| Extra | Adds | Install |
| --- | --- | --- |
| `graph` | Knowledge-graph search (the graph channel), stored in [LadybugDB](https://pypi.org/project/ladybug/) | `pip install "retrievalloom[graph]"` |
| `structure` | Section-tree search (the structure channel), with [TreeSearch](https://pypi.org/project/pytreesearch/) | `pip install "retrievalloom[structure]"` |
| `postgres` | The [PostgreSQL metadata store](../guide/how-to/durable-storage.md#postgresql-metadata), on [asyncpg](https://pypi.org/project/asyncpg/), shared by engines on several hosts | `pip install "retrievalloom[postgres]"` |
| `testing` | The pytest [contract kits](../api/testing.md) that test your own components | `pip install "retrievalloom[testing]"` |
| `all` | `graph`, `structure` and `postgres` | `pip install "retrievalloom[all]"` |

!!! warning "LadybugDB on Windows"

    The Windows wheel of `ladybug` (0.22.3) does not bundle the OpenSSL DLLs its native
    extension links against (`libssl-3-x64.dll`, `libcrypto-3-x64.dll`). It finds them
    only when the interpreter's own `ssl` module has already loaded DLLs with those names,
    which is the case for uv-managed Python builds (`uv python install`). On interpreters
    that name them differently, including python.org-based builds, using the graph store
    fails with `DLL load failed while importing _lbug`. Use a uv-managed Python, or put
    OpenSSL 3 DLLs with those names on the DLL search path (for example
    `choco install openssl.light`). Upstream treats this as intended: see
    [LadybugDB/ladybug#775](https://github.com/LadybugDB/ladybug/issues/775).

!!! note "Python 3.15"

    `flashrank`, a base dependency, needs `onnxruntime`, which publishes no Python 3.15
    wheels yet, so RetrievalLoom declares `requires-python = ">=3.12,<3.15"` until it does.
    The request is tracked in
    [microsoft/onnxruntime#33271](https://github.com/microsoft/onnxruntime/issues/33271).

## What the base install includes

The base package installs everything the default engine uses, so no extra is
needed for keyword, typo-tolerant, BM25 or semantic search:

| Dependency | Used for |
| --- | --- |
| `aiosqlite`, `sqlalchemy[asyncio]`, `alembic` | The SQLite metadata store and its migrations |
| `ahocorasick-rs` | The keyword method |
| `rapidfuzz` | The fuzzy method |
| `bm25s` | The sparse (BM25) method |
| `qdrant-client` | The dense method's vector store (in memory, on disk, server or cloud) |
| `ranknexus` | Rank fusion |
| `flashrank` | The default reranker. Its model is never downloaded implicitly; without it, results keep their fused order. |
| `refresh-engine` | Scheduling of background index work |
| `resourcera` | Admission control for offloaded work |
| `agxai` | The optional agxai audit-trail recorder |
| `pydantic`, `numpy`, `structlog` | Models, vectors and logging |

No embedding model is installed. Semantic (dense) search starts when you pass
your own [embedding model](../guide/how-to/embedding-model.md).

## See log output

RetrievalLoom is silent by default and never changes your logging setup. It
logs through [structlog](https://www.structlog.org/) under `retrievalloom.*`
logger names.

- If your application has configured structlog, events go through your
  structlog configuration.
- Otherwise events are rendered as `key=value` text into the standard library
  `retrievalloom.*` loggers. The `retrievalloom` logger has a `NullHandler`, so
  nothing is printed until you add handlers or a level.

To see warnings and errors with the standard library:

```python
import logging

logging.basicConfig(level=logging.WARNING)
```

Use `logging.getLogger("retrievalloom").setLevel(logging.INFO)` for more
detail.

Document text and queries stay out of the logs unless you set
`log_content=True` on `EngineConfig`.

## Let your coding agent do it

The [RetrievalLoom Agent Skills](../agent-skills/index.md) teach coding agents
such as Claude Code, Codex, Cursor and GitHub Copilot to install RetrievalLoom,
wire it into an application, and extend it with your own components.

## Verify the installation

```bash
python -c "import retrievalloom; print(retrievalloom.__version__)"
```

## Next steps

Run your first search in the [Quickstart](quickstart.md).
