"""Validated, batched access to an application embedding model."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from retrievalloom.errors import EmbeddingValidationError
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.models import EmbeddingModelABC
    from retrievalloom.runtime.offloader import Offloader

_INLINE_VALUES = 4096


def check_vectors(
    vectors: Sequence[Sequence[float]], expected: int, dimensions: int
) -> list[tuple[float, ...]]:
    """Validate count, dimensionality, finiteness and non-zero norm; return tuples."""
    if len(vectors) != expected:
        raise EmbeddingValidationError(
            "embedding count differs from input count",
            details={"expected": expected, "got": len(vectors)},
        )
    checked: list[tuple[float, ...]] = []
    for position, vector in enumerate(vectors):
        values = tuple(float(value) for value in vector)
        if len(values) != dimensions:
            raise EmbeddingValidationError(
                "embedding has wrong dimensions",
                details={"position": position, "expected": dimensions, "got": len(values)},
            )
        if not all(math.isfinite(value) for value in values):
            raise EmbeddingValidationError(
                "embedding has non-finite values", details={"position": position}
            )
        if not any(values):
            raise EmbeddingValidationError(
                "embedding has zero norm", details={"position": position}
            )
        checked.append(values)
    return checked


class EmbeddingValidator:
    """Calls an ``EmbeddingModelABC`` in ``max_batch_size`` batches and validates results.

    Validation of large outputs runs in the offloader so it never blocks the loop.

    Args:
        model: Application embedding model.
        offloader: Engine offloader.
    """

    def __init__(self, model: EmbeddingModelABC, offloader: Offloader) -> None:
        self._model = model
        self._offloader = offloader

    @property
    def model(self) -> EmbeddingModelABC:
        """Wrapped model."""
        return self._model

    async def _check(
        self, vectors: Sequence[Sequence[float]], expected: int, tenant: str | None
    ) -> list[tuple[float, ...]]:
        dimensions = self._model.dimensions
        if expected * dimensions <= _INLINE_VALUES:
            return check_vectors(vectors, expected, dimensions)
        return await self._offloader.run(
            WorkloadKind.EMBEDDING_VALIDATION,
            check_vectors,
            vectors,
            expected,
            dimensions,
            tenant=tenant,
        )

    async def embed_documents(
        self, texts: Sequence[str], *, tenant: str | None = None
    ) -> list[tuple[float, ...]]:
        """Embed ``texts`` in batches; one validated vector per text, in order."""
        size = max(1, self._model.max_batch_size)
        vectors: list[tuple[float, ...]] = []
        for start in range(0, len(texts), size):
            batch = list(texts[start : start + size])
            produced = await self._model.embed_documents(batch)
            vectors.extend(await self._check(produced, len(batch), tenant))
        return vectors

    async def embed_query(self, text: str, *, tenant: str | None = None) -> tuple[float, ...]:
        """Embed a query and validate it."""
        produced = await self._model.embed_query(text)
        return (await self._check([produced], 1, tenant))[0]
