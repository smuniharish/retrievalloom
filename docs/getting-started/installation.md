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
| `testing` | The pytest [contract kits](../api/testing.md) for your own components | `pip install "retrievalloom[testing]"` |
| `all` | `graph` and `structure` | `pip install "retrievalloom[all]"` |

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
| `flashrank` | The optional FlashRank reranker |
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
