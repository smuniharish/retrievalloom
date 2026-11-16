"""FlashRank cross-encoder reranker (ONNX, pairwise models only, no implicit downloads)."""

from __future__ import annotations

import logging
import math
import threading
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flashrank import Ranker, RerankRequest
from flashrank.Ranker import model_file_map

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import RerankerABC, RerankScore
from retrievalloom.domain.reports import HealthReport, HealthStatus
from retrievalloom.errors import (
    BackendError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    LifecycleError,
)
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.models import RerankItem
    from retrievalloom.runtime.offloader import Offloader

DEFAULT_MODEL = "ms-marco-TinyBERT-L-2-v2"
_LISTWISE = frozenset({"rank_zephyr_7b_v1_full"})
_ROOT_GUARD = threading.Lock()


def _load_ranker(model_name: str, cache_dir: Path, max_length: int) -> Any:
    """Construct a Ranker without letting its ``logging.basicConfig`` touch the root logger."""
    root = logging.getLogger()
    guard = logging.NullHandler()
    with _ROOT_GUARD:
        added = not root.handlers
        if added:
            root.addHandler(guard)
        try:
            return Ranker(
                model_name=model_name,
                cache_dir=str(cache_dir),
                max_length=max_length,
                log_level="WARNING",
            )
        finally:
            if added:
                root.removeHandler(guard)


class FlashRankReranker(RerankerABC):
    """Reranks with a FlashRank pairwise cross-encoder.

    The model must already be present in ``cache_dir`` unless ``allow_download`` is set; a
    missing model makes the reranker unavailable (the engine degrades with a warning) rather
    than downloading silently. FlashRank's bundled models are CC-BY-SA licensed.

    Args:
        cache_dir: Directory holding ``<model_name>/<model file>``.
        model_name: A FlashRank pairwise model name.
        max_length: Token limit per (query, passage) pair.
        batch_size: Pairs scored per ONNX call.
        allow_download: Let FlashRank download a missing model from Hugging Face.
    """

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        model_name: str = DEFAULT_MODEL,
        max_length: int = 512,
        batch_size: int = 32,
        allow_download: bool = False,
    ) -> None:
        if model_name not in model_file_map or model_name in _LISTWISE:
            raise ConfigurationError(
                "unsupported FlashRank model (pairwise models only)",
                details={"model_name": model_name},
            )
        if max_length < 16 or batch_size < 1:
            raise ConfigurationError("max_length must be >= 16 and batch_size >= 1")
        self._cache_dir = Path(cache_dir)
        self._model_name = model_name
        self._max_length = max_length
        self._batch_size = batch_size
        self._allow_download = allow_download
        self._ranker: Any = None
        self._unavailable: str | None = None
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="reranker",
            implementation="flashrank",
            version="1",
            config={"model_name": model_name, "max_length": max_length},
            model_identity=f"flashrank:{model_name}",
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (model identity ``flashrank:<model>``)."""
        return self._info

    @property
    def model_path(self) -> Path:
        """Expected model file."""
        return self._cache_dir / self._model_name / model_file_map[self._model_name]

    async def initialize(self, context: ComponentContext) -> None:
        """Load the model off the event loop; a missing model marks it unavailable."""
        if self._offloader is not None:
            return
        self._offloader = context.offloader
        if not self._allow_download and not self.model_path.is_file():
            self._unavailable = "model files not found and downloads are disabled"
            context.logger.warning(
                "reranker_unavailable",
                component=self._info.name,
                model=self._model_name,
                reason=self._unavailable,
            )
            return
        load = partial(_load_ranker, self._model_name, self._cache_dir, self._max_length)
        try:
            self._ranker = await self._offloader.run(WorkloadKind.RERANK, load)
        except Exception as exc:
            self._unavailable = f"model failed to load: {type(exc).__name__}"
            context.logger.warning(
                "reranker_unavailable",
                component=self._info.name,
                model=self._model_name,
                reason=self._unavailable,
                exc_info=exc,
            )

    async def close(self) -> None:
        """Release the ONNX session."""
        self._ranker = None
        self._offloader = None

    async def health(self) -> HealthReport:
        """Unhealthy when the model is missing or failed to load."""
        if self._ranker is None:
            return HealthReport(
                component=self._info.name,
                status=HealthStatus.UNHEALTHY,
                message=self._unavailable or "not initialised",
            )
        return HealthReport(component=self._info.name)

    def _score(self, query: str, items: Sequence[RerankItem]) -> list[RerankScore]:
        scores: dict[str, float] = {}
        for start in range(0, len(items), self._batch_size):
            batch = items[start : start + self._batch_size]
            passages = [{"id": item.item_id, "text": item.text} for item in batch]
            try:
                ranked = self._ranker.rerank(RerankRequest(query=query, passages=passages))
            except Exception as exc:
                raise BackendError(
                    "flashrank rerank failed", details={"error": type(exc).__name__}
                ) from exc
            for passage in ranked:
                scores[str(passage["id"])] = float(passage["score"])
        result: list[RerankScore] = []
        for item in items:
            score = scores.get(item.item_id)
            if score is None or not math.isfinite(score):
                raise ComponentContractError(
                    "reranker returned a missing or non-finite score",
                    details={"item_id": item.item_id},
                )
            result.append(RerankScore(item_id=item.item_id, score=score))
        return result

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        """Score every item once, aligned to the input order.

        Raises:
            BackendUnavailableError: The model is not available.
            LifecycleError: The reranker is not initialised.
        """
        if self._offloader is None:
            raise LifecycleError("reranker is not initialised")
        if self._ranker is None:
            raise BackendUnavailableError(
                "reranker model unavailable", details={"reason": self._unavailable}
            )
        if len({item.item_id for item in items}) != len(items):
            raise ComponentContractError("rerank item ids must be unique")
        if not items:
            return []
        return await self._offloader.run(
            WorkloadKind.RERANK, partial(self._score, query, list(items))
        )
