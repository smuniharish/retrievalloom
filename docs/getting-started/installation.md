# Installation

RetrievalLoom needs Python 3.12 or later. It is tested on 3.12, 3.13 and 3.14.

=== "pip"

    ```bash
    pip install retrievalloom
    ```

=== "uv"

    ```bash
    uv add retrievalloom
    ```

## Extras

| Extra | Adds | Install |
| --- | --- | --- |
| `graph` | The graph channel, stored in [LadybugDB](https://pypi.org/project/ladybug/) | `pip install "retrievalloom[graph]"` |
| `structure` | The structure channel, searched with [TreeSearch](https://pypi.org/project/pytreesearch/) | `pip install "retrievalloom[structure]"` |
| `postgres` | The [PostgreSQL metadata store](../guide/how-to/durable-storage.md#a-shared-postgresql-store), on [asyncpg](https://pypi.org/project/asyncpg/), shared by engines on several hosts | `pip install "retrievalloom[postgres]"` |
| `testing` | The pytest [contract kits](../api/testing.md) for your own components | `pip install "retrievalloom[testing]"` |
| `all` | `graph` and `structure` | `pip install "retrievalloom[all]"` |

!!! warning "LadybugDB on Windows"

    The Windows wheel of `ladybug` (0.22.3) does not bundle the OpenSSL DLLs its native
    extension links against (`libssl-3-x64.dll`, `libcrypto-3-x64.dll`). It finds them
    only when the interpreter's own `ssl` module has already loaded DLLs with those names,
    which is the case for uv-managed Python builds (`uv python install`). On interpreters
    that name them differently, including python.org-based builds, using the graph store
    fails with `DLL load failed while importing _lbug`. Use a uv-managed Python, or put
    OpenSSL 3 DLLs with those names on the DLL search path.

## What the base install includes

The base package installs everything the default engine uses:

| Dependency | Used for |
| --- | --- |
| `aiosqlite`, `sqlalchemy[asyncio]`, `alembic` | The SQLite metadata store and its migrations |
| `ahocorasick-rs` | The keyword method |
| `rapidfuzz` | The fuzzy method |
| `bm25s` | The sparse (BM25) method |
| `qdrant-client` | The dense method's vector store (in memory, on disk, server or cloud) |
| `ranknexus` | Rank fusion |
| `flashrank` | The default reranker (models are never downloaded implicitly; without one, results keep the RRF-fused order) |
| `refresh-engine` | Scheduling of background index work |
| `resourcera` | Admission control for offloaded work |
| `agxai` | The optional agxai provenance recorder |
| `pydantic`, `numpy`, `structlog` | Models, vectors and logging |

No embedding model is installed. Dense retrieval starts when you pass your own
[embedding model](../guide/how-to/embedding-model.md).

## Logging

RetrievalLoom logs through [structlog](https://www.structlog.org/) under
`retrievalloom.*` logger names. It never configures structlog or the standard
`logging` module, and it is silent by default.

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

Document text and queries are not logged unless you set `log_content=True` on
`EngineConfig`.

## Next

Continue with the [Quickstart](quickstart.md).
