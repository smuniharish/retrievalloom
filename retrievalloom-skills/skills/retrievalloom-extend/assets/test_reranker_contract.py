"""Run RetrievalLoom's reranker contract against your reranker.

Copy this file next to your adapter and change the import. Keep
``check_relevance`` on for a real model: it requires the passage that answers
the query to outscore an unrelated one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reranker_template import OverlapReranker

from retrievalloom.testing import RerankerContract

if TYPE_CHECKING:
    from pathlib import Path


class TestOverlapReranker(RerankerContract):
    async def make_component(self, tmp_path: Path) -> OverlapReranker:
        return OverlapReranker()
