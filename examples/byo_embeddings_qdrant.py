"""Bring your own embeddings: bge-m3 (transformers) with a Qdrant server vector store.

The application implements ``EmbeddingModelABC`` itself, here bge-m3 dense embeddings
(1024 dimensions, CLS pooling, L2-normalised) with transformers and torch. Blocking
inference runs through the engine's offloader with ``WorkloadKind.EMBEDDING``. Supplying
an embedding model enables the ``index.dense`` method; ``QdrantVectorStore`` stores the
vectors on a Qdrant server, alongside the built-in lexical methods (keyword, fuzzy and
BM25 sparse), which still run on the in-process indexes.

Requirements:

* A Qdrant server. Start one with Podman (or Docker)::

      podman compose -f examples/compose.yaml up -d
      # or: podman run -d --name rl-qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant

  and set ``RL_EXAMPLES_QDRANT_URL=http://localhost:6333`` (fallback ``RL_TEST_QDRANT_URL``).
* The ``bge-m3`` weights under ``RL_EXAMPLES_MODELS_DIR`` (fallback ``RL_TEST_MODELS_DIR``,
  default ``models``), plus ``torch`` and ``transformers``.

The example creates collections prefixed ``rl_examples`` and deletes them at the end.

Run from the repository root::

    python examples/byo_embeddings_qdrant.py
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from _shared import env, models_dir, quiet_logs

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.runtime.offloader import Offloader

PREFIX = "rl_examples"


class BgeM3Embedding(EmbeddingModelABC):
    def __init__(self, model_dir: Path) -> None:
        self._dir = model_dir
        self._model: Any = None
        self._tokenizer: Any = None
        self._offloader: Offloader | None = None

    @property
    def info(self) -> ComponentInfo:
        # The fingerprint (model identity, dimensions) ties index generations to the model.
        return make_info(
            component="embedding",
            implementation="bge-m3-transformers",
            version="1",
            model_identity="BAAI/bge-m3",
            dimensions=1024,
        )

    @property
    def dimensions(self) -> int:
        return 1024

    @property
    def max_batch_size(self) -> int:
        return 8

    def _load(self) -> None:
        from transformers import AutoModel, AutoTokenizer
        from transformers.utils import logging as hf_logging

        hf_logging.disable_progress_bar()
        self._tokenizer = AutoTokenizer.from_pretrained(self._dir)
        self._model = AutoModel.from_pretrained(self._dir).eval()

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader
        await context.offloader.run(WorkloadKind.EMBEDDING, self._load)

    def _embed(self, texts: list[str]) -> list[list[float]]:
        import torch

        with torch.inference_mode():
            batch = self._tokenizer(
                texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
            )
            cls = self._model(**batch).last_hidden_state[:, 0]
            return torch.nn.functional.normalize(cls, dim=-1).tolist()

    async def _run(self, texts: list[str]) -> list[list[float]]:
        if self._offloader is None:
            raise RuntimeError("embedding model used before initialize()")
        return await self._offloader.run(WorkloadKind.EMBEDDING, self._embed, texts)

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return await self._run(list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        return (await self._run([text]))[0]


async def drop_collections(url: str) -> None:
    from qdrant_client import AsyncQdrantClient

    client = AsyncQdrantClient(url=url)
    try:
        for collection in (await client.get_collections()).collections:
            if collection.name.startswith(PREFIX):
                await client.delete_collection(collection.name)
    finally:
        await client.close()


CORPUS = {
    "eiffel": "The Eiffel Tower is a wrought-iron lattice tower in Paris, completed in 1889.",
    "photosynthesis": "Plants turn sunlight, water and carbon dioxide into glucose and oxygen.",
    "everest": "Mount Everest, on the border of Nepal and China, is the highest mountain.",
    "asyncio": "Python's asyncio library runs coroutines on a single-threaded event loop.",
}


async def main() -> None:
    quiet_logs()
    url, model_dir = env("QDRANT_URL"), models_dir() / "bge-m3"
    if url is None:
        print(
            "Skipping: set RL_EXAMPLES_QDRANT_URL (e.g. http://localhost:6333).\n"
            "Start one with: podman compose -f examples/compose.yaml up -d"
        )
        return
    if not model_dir.is_dir():
        print(f"Skipping: bge-m3 not found at {model_dir}; set RL_EXAMPLES_MODELS_DIR.")
        return
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(request_timeout=120.0, method_timeout=60.0),
        components=Components(
            embedding=BgeM3Embedding(model_dir),
            vector_store=QdrantVectorStore(url=url, collection_prefix=PREFIX),
        ),
        lifecycle=LifecycleConfig(background=False),
    )
    scope = Scope(tenant_id="acme")
    try:
        async with engine:
            await engine.ingest(
                [
                    DocumentInput(_id=key, representations=(TextRepresentation(text=text),))
                    for key, text in CORPUS.items()
                ],
                scope=scope,
            )
            status = await engine.status(scope=scope)
            print(f"index.dense coverage: {status.coverage['index.dense']}")
            # The queries share no content words with the answers; dense retrieval finds them.
            for query in ("famous landmark in France", "how do green leaves make sugar"):
                result = await engine.retrieve(
                    query, scope=scope, top_k=1, config=RetrievalExecutionConfig(dense=True)
                )
                (top,) = result.evidence
                dense = next(h for h in top.provenance.hits if h.method.key == "index.dense")
                methods = sorted({hit.method.key for hit in top.provenance.hits})
                print(f"{query!r} -> {top.sources[0].document_id} via {methods}")
                print(f"   cosine={dense.raw_score:.2f} rank={dense.rank} text: {top.display_text}")
    finally:
        await drop_collections(url)


if __name__ == "__main__":
    asyncio.run(main())
