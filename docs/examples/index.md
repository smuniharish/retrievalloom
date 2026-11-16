# Examples

Runnable scripts live in the `examples/` directory of the repository. Each one is a
self-contained `async def main()` program that uses only the public API and prints the
evidence it retrieves. Run them from the repository root, for example:

```bash
python examples/quickstart.py
```

The scripts import a small helper module, `examples/_shared.py`. It limits the library's
structured logging to warnings and errors on stderr, and reads the environment variables
used by the live examples.

| Example | What it shows | Requirements | Run |
| --- | --- | --- | --- |
| [Quickstart](quickstart.md) | In-memory engine; ingest, retrieve, evidence sources, provenance and a metadata filter | base install | `python examples/quickstart.py` |
| [Multi-tenant](multi-tenant.md) | Scope isolation between two tenants, a per-tenant ingest limit, per-scope status | base install | `python examples/multi_tenant.py` |
| [Pre-chunked and multi-representation](prechunked-and-multi-representation.md) | Application chunks with spans, pages and section paths; a second (structure) representation of the same document | `structure` extra | `python examples/prechunked_and_multi_representation.py` |
| [Graph channel](graph-channel.md) | Application-supplied entities and relationships on LadybugDB; multi-hop neighbourhood evidence with traversal paths | `graph` extra | `python examples/graph_channel.py` |
| [Structure channel](structure-channel.md) | TreeSearch over an application-provided document tree; section paths and parent, children and sibling navigation | `structure` extra | `python examples/structure_channel.py` |
| [Lifecycle: backfill and rebuild](lifecycle-backfill-rebuild.md) | Deferred indexing, `backfill`, `rebuild`, `wait_for`, `status` (coverage, generations), deletion, `health` | base install | `python examples/lifecycle_backfill_rebuild.py` |
| [agxai provenance](agxai-provenance.md) | Ingestion, deletion and retrieval recorded in an agxai run, with provenance links | base install | `python examples/agxai_provenance.py` |
| [Own embeddings on Qdrant](byo-embeddings-qdrant.md) | A user-implemented bge-m3 `EmbeddingModelABC` with `QdrantVectorStore` on a Qdrant server | Qdrant server, bge-m3 weights, `torch`, `transformers` | `python examples/byo_embeddings_qdrant.py` |
| [Custom reranker and NLI](custom-reranker-nli.md) | User-implemented `RerankerABC` and `NLIABC`; reranked order, claim entailment and contradiction-closed selection | bge-reranker-v2-m3 and UltraBERT-150M-nli weights, `torch`, `transformers` | `python examples/custom_reranker_nli.py` |

## Installing the requirements

- `structure` extra: `pip install "retrievalloom[structure]"`
- `graph` extra: `pip install "retrievalloom[graph]"`
- `torch` and `transformers`: `pip install torch transformers`, or `uv sync --group examples`
  in a checkout of the repository.

## Live examples

The two live examples need local model weights, and the embedding example also needs a
Qdrant server. They read these environment variables:

| Variable | Fallback | Default | Used by |
| --- | --- | --- | --- |
| `RL_EXAMPLES_QDRANT_URL` | `RL_TEST_QDRANT_URL` | none | `byo_embeddings_qdrant.py` |
| `RL_EXAMPLES_MODELS_DIR` | `RL_TEST_MODELS_DIR` | `models` | both live examples |

The models directory holds one folder per model, as downloaded from Hugging Face:
`bge-m3`, `bge-reranker-v2-m3` and `UltraBERT-150M-nli`. If the Qdrant URL is not set or a
model folder is missing, the example prints what is missing and exits with status 0.

Start Qdrant with the compose file shipped with the examples:

```bash
podman compose -f examples/compose.yaml up -d
```

or without compose:

```bash
podman run -d --name rl-qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

Docker accepts the same commands (`docker compose -f examples/compose.yaml up -d`). Then set
`RL_EXAMPLES_QDRANT_URL=http://localhost:6333`.

```yaml title="examples/compose.yaml"
--8<-- "examples/compose.yaml"
```

## Tests

`tests/examples/test_examples.py` runs every script as a subprocess and checks its output.
All of these tests carry the `examples` marker. The live ones are also marked `live` and
`models` (and `qdrant`), and are skipped unless `RL_EXAMPLES_MODELS_DIR` and
`RL_EXAMPLES_QDRANT_URL` (or the `RL_TEST_*` equivalents) are set. To run only the offline
examples:

```bash
python -m pytest tests/examples -m "not live"
```
