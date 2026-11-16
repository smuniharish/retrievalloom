"""Durable restart: SQLite metadata on disk, dense and learned-sparse vectors on Qdrant.

A service restarts (deploy, crash, scale-out) and must answer immediately without
re-ingesting. Durable state lives in two places:

- ``data_dir``: the SQLite metadata store (documents, versions, coverage, generations,
  lifecycle tasks);
- a Qdrant server: dense vectors from ``BgeM3Embedding`` (``QdrantVectorStore``) and
  learned-sparse lexical weights from the same bge-m3 model (``BgeM3SparseIndex``, a
  ``SparseIndexABC`` that replaces the built-in BM25 index).

The keyword and fuzzy indexes are in-memory (ephemeral); on startup the engine rebuilds
them from metadata, while the durable Qdrant generations are reused untouched.

Requires the local models and a Qdrant server (``examples/compose.yaml``)::

    $env:RL_EXAMPLES_MODELS_DIR="models"; $env:RL_EXAMPLES_QDRANT_URL="http://localhost:6333"
    python examples/durable_restart.py
"""

import asyncio
import tempfile
from pathlib import Path

from _shared import doc_ids, drop_qdrant_collections, load, methods, quiet_logs, require, run_prefix
from adapters import BgeM3Embedding, BgeM3SparseIndex

from retrievalloom import (
    Components,
    DocumentInput,
    EngineResources,
    LifecycleConfig,
    RerankingConfig,
    RetrievalConfig,
    RetrievalEngine,
    Scope,
    ScopeStatus,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.index.vector import QdrantVectorStore

SCOPE = Scope(tenant_id="acme", collection="help-center")
QUESTION = "my laptop is running out of disk space because of synced folders"


def generations(status: ScopeStatus) -> dict[str, str]:
    return {g.method: g.generation_id for g in status.generations if g.state.value == "active"}


async def main() -> None:
    quiet_logs()
    settings = require("MODELS_DIR", "QDRANT_URL")
    if settings is None:
        return
    models, url = settings["MODELS_DIR"], settings["QDRANT_URL"]
    prefix = run_prefix("durable")

    def engine(data_dir: Path) -> RetrievalEngine:
        components = Components(
            embedding=BgeM3Embedding(f"{models}/bge-m3"),
            vector_store=QdrantVectorStore(url=url, collection_prefix=prefix),
            sparse_index=BgeM3SparseIndex(f"{models}/bge-m3", url=url, collection_prefix=prefix),
        )
        return create_retrieval_engine(
            retrieval=RetrievalConfig(
                reranking=RerankingConfig(enabled=False),
                request_timeout=300.0,
                method_timeout=240.0,
            ),
            components=components,
            resources=EngineResources(data_dir=data_dir),
            lifecycle=LifecycleConfig(background=False),
        )

    try:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            async with engine(data_dir) as first:
                documents = [
                    DocumentInput(
                        _id=article["id"],
                        metadata={"product": article["product"]},
                        representations=(TextRepresentation(text=article["text"]),),
                    )
                    for article in load("support_kb.json")
                ]
                report = await first.ingest(documents, scope=SCOPE)
                print(f"first process: ingested {len(report.documents)} documents")
                before = generations(await first.status(scope=SCOPE))
                result = await first.retrieve(QUESTION, scope=SCOPE, top_k=2)
                print(f"   answer: {doc_ids(result.evidence)} via [{methods(result.evidence[0])}]")
            print("first process closed\n")

            async with engine(data_dir) as second:
                status = await second.status(scope=SCOPE)
                after = generations(status)
                print(f"second process: documents_active={status.documents_active} (no re-ingest)")
                for method in sorted(after):
                    kept = "reused" if before.get(method) == after[method] else "rebuilt"
                    print(f"   {method}: {kept}")
                result = await second.retrieve(QUESTION, scope=SCOPE, top_k=2)
                print(f"   answer: {doc_ids(result.evidence)} via [{methods(result.evidence[0])}]")
                top = result.evidence[0].provenance.hits
                terms = sorted(
                    {
                        d.term
                        for hit in top
                        for d in hit.matched
                        if d.kind == "learned_term" and d.term
                    }
                )
                print(f"   learned-sparse terms behind the top hit: {terms}")
    finally:
        await drop_qdrant_collections(url, prefix)


if __name__ == "__main__":
    asyncio.run(main())
