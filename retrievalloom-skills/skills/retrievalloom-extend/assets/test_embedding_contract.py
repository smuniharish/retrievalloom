"""Run RetrievalLoom's embedding model contract against your model.

Copy this file next to your adapter and change the import. Keep ``tolerance``
at its default unless your model is nondeterministic on the same input (GPU
kernels can be); then raise it as little as possible.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from embedding_template import HashEmbedding

from retrievalloom.testing import EmbeddingModelContract

if TYPE_CHECKING:
    from pathlib import Path


class TestHashEmbedding(EmbeddingModelContract):
    async def make_component(self, tmp_path: Path) -> HashEmbedding:
        return HashEmbedding()
