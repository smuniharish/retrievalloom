"""A reranker adapter to adapt to your cross-encoder.

Copy this file and replace ``_score`` with your model's blocking call. The
contract: ``rerank(query, items)`` returns exactly one finite ``RerankScore``
per ``RerankItem``, matched by ``item_id``, and ``[]`` for no items. Higher
scores rank first. A model that cannot load should raise
``BackendUnavailableError`` from ``rerank`` and report itself unhealthy, so
requests degrade with a ``reranker_unavailable`` warning instead of failing.

Prove it with ``test_reranker_contract.py`` (``RerankerContract``), then pass
it as ``Components(reranker=OverlapReranker())``; that replaces FlashRank.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from retrievalloom.contracts.component import make_info
from retrievalloom.contracts.models import RerankerABC, RerankScore
from retrievalloom.errors import LifecycleError
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext, ComponentInfo
    from retrievalloom.contracts.models import RerankItem
    from retrievalloom.runtime.offloader import Offloader

WORD = re.compile(r"\w+")


class OverlapReranker(RerankerABC):
    """Toy reranker: the share of query words a passage contains."""

    def __init__(self) -> None:
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="reranker",
            implementation="overlap-reranker",
            version="1",
            model_identity="example/word-overlap",
        )

    @property
    def info(self) -> ComponentInfo:
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        # Load the model here from a local directory. Idempotent.
        self._offloader = context.offloader

    async def close(self) -> None:
        self._offloader = None

    def _score(self, query: str, items: list[RerankItem]) -> list[RerankScore]:
        """Replace with your model's blocking call (runs in a worker thread)."""
        words = set(WORD.findall(query.casefold()))
        return [
            RerankScore(
                item_id=item.item_id,
                score=len(words & set(WORD.findall(item.text.casefold()))) / (len(words) or 1),
            )
            for item in items
        ]

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        if not items:
            return []
        if self._offloader is None:
            raise LifecycleError(f"{self.info.name} is not initialised")
        return await self._offloader.run(WorkloadKind.RERANK, self._score, query, list(items))
