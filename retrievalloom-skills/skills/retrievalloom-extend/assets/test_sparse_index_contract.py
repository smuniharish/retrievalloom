"""Run RetrievalLoom's sparse index contract against your backend.

Copy this file next to your index and change the import. ``make_component``
must return a new, empty, uninitialized index for every test; for a server
backend, point it at a disposable collection prefix or database here. Each
failing test names the promise the backend breaks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sparse_index_template import TemplateSparseIndex

from retrievalloom.testing import SparseIndexContract

if TYPE_CHECKING:
    from pathlib import Path


class TestTemplateSparseIndex(SparseIndexContract):
    async def make_component(self, tmp_path: Path) -> TemplateSparseIndex:
        return TemplateSparseIndex()
