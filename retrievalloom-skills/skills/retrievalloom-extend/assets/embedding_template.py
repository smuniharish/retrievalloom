"""An embedding model adapter to adapt to your model.

Copy this file and replace ``_encode`` with your model's blocking batch call
(sentence-transformers, FlagEmbedding, ONNX Runtime ...). Keep the rest:

- ``info`` carries ``model_identity`` and ``dimensions``. Change either and the
  engine treats existing dense generations as stale and rebuilds them.
- The blocking call runs through ``context.offloader`` so the event loop never
  stalls. For an async HTTP API, await the client directly instead.
- Load weights in ``initialize`` from a local path. Never download at runtime
  unless the application asked for it.

Prove it with ``test_embedding_contract.py`` (``EmbeddingModelContract``), then
pass it as ``Components(embedding=HashEmbedding())``; that enables the dense
method.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.models import EmbeddingModelABC
from retrievalloom.errors import LifecycleError
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext, ComponentInfo
    from retrievalloom.runtime.offloader import Offloader

WORD = re.compile(r"\w+")


class HashEmbedding(EmbeddingModelABC):
    """Toy hashed bag-of-words model: deterministic, unit-norm, never all zeros."""

    def __init__(self, *, dimensions: int = 64, model_identity: str = "example/hash-64") -> None:
        self._dimensions = dimensions
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="embedding",
            implementation="hash-embedding",
            version="1",
            model_identity=model_identity,
            dimensions=dimensions,
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def max_batch_size(self) -> int:
        # The engine splits larger inputs into batches of this size.
        return 32

    async def initialize(self, context: ComponentContext) -> None:
        # Load the model here, through context.offloader for blocking loads. Idempotent.
        self._offloader = context.offloader

    async def close(self) -> None:
        self._offloader = None

    def _encode(self, texts: list[str]) -> list[list[float]]:
        """Replace with your model's blocking batch call (runs in a worker thread)."""
        vectors = []
        for text in texts:
            vector = [0.0] * self._dimensions
            for word in WORD.findall(text.casefold()):
                bucket = hashlib.sha256(word.encode()).digest()[0] % self._dimensions
                vector[bucket] += 1.0
            norm = math.sqrt(sum(value * value for value in vector))
            if norm == 0:  # a zero vector fails engine validation
                vectors.append([1.0 / math.sqrt(self._dimensions)] * self._dimensions)
            else:
                vectors.append([value / norm for value in vector])
        return vectors

    def _offload(self) -> Offloader:
        if self._offloader is None:
            raise LifecycleError(f"{self.info.name} is not initialised")
        return self._offloader

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return await self._offload().run(WorkloadKind.EMBEDDING, self._encode, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        vectors = await self._offload().run(WorkloadKind.EMBEDDING, self._encode, [text])
        return vectors[0]
