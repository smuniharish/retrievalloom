"""Local-model adapters: bge-m3 dense embeddings, bge cross-encoder reranker, UltraBERT NLI.

Inference is blocking CPU work, so every call goes through ``context.offloader`` with the
matching ``WorkloadKind`` (admission control, per-tenant fairness, bounded executor). The
weights load once per process on ``initialize`` and are never downloaded.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import NLIABC, EmbeddingModelABC, RerankerABC, RerankScore
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.errors import LifecycleError
from retrievalloom.runtime.offloader import WorkloadKind

from ._models import LoadedModel, bge_m3, bge_reranker, sequence_classifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.models import NLIPair, RerankItem
    from retrievalloom.runtime.offloader import Offloader


class _LocalModel:
    """Shared plumbing: offloader binding and a load-once model handle."""

    _offloader: Offloader | None = None
    _loaded: LoadedModel | None = None

    def _ready(self) -> tuple[Offloader, LoadedModel]:
        if self._offloader is None or self._loaded is None:
            raise LifecycleError(f"{type(self).__name__} used before initialize()")
        return self._offloader, self._loaded


class BgeM3Embedding(_LocalModel, EmbeddingModelABC):
    """Dense bge-m3 embeddings (1024-d, L2-normalised) through FlagEmbedding.

    Args:
        model_dir: Local ``bge-m3`` directory.
        max_length: Token limit per text (part of the fingerprint: changing it changes
            the vectors, so the engine rebuilds the dense index).
        batch_size: Largest batch the engine hands to ``embed_documents``.
    """

    DIMENSIONS = 1024

    def __init__(self, model_dir: str | Path, *, max_length: int = 512, batch_size: int = 16):
        self._dir = Path(model_dir)
        self._max_length = max_length
        self._batch_size = batch_size
        self._info = make_info(
            component="embedding",
            implementation="bge-m3-flagembedding",
            version="1",
            config={"max_length": max_length},
            model_identity="BAAI/bge-m3",
            dimensions=self.DIMENSIONS,
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity (model, dimensions and max_length are fingerprinted)."""
        return self._info

    @property
    def dimensions(self) -> int:
        """Vector size."""
        return self.DIMENSIONS

    @property
    def max_batch_size(self) -> int:
        """Largest batch per call."""
        return self._batch_size

    async def initialize(self, context: ComponentContext) -> None:
        """Load (or reuse) the shared bge-m3 model off the event loop."""
        self._offloader = context.offloader
        if self._loaded is None:
            self._loaded = await context.offloader.run(WorkloadKind.EMBEDDING, bge_m3, self._dir)

    async def close(self) -> None:
        """Release the reference (the process-wide model stays cached)."""
        self._loaded = None

    def _encode(self, texts: list[str]) -> list[list[float]]:
        _, loaded = self._ready()
        with loaded.lock:
            output = loaded.model.encode(
                texts,
                batch_size=self._batch_size,
                max_length=self._max_length,
                return_dense=True,
                return_sparse=False,
                return_colbert_vecs=False,
            )
        return [[float(value) for value in vector] for vector in output["dense_vecs"]]

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed document texts in order."""
        offloader, _ = self._ready()
        return await offloader.run(WorkloadKind.EMBEDDING, self._encode, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed one query."""
        offloader, _ = self._ready()
        return (await offloader.run(WorkloadKind.EMBEDDING, self._encode, [text]))[0]


class BgeCrossEncoderReranker(_LocalModel, RerankerABC):
    """bge-reranker-v2-m3 cross-encoder through FlagEmbedding's ``FlagReranker``.

    Scores are raw logits (higher is better); pass ``normalize=True`` for sigmoid
    probabilities in ``[0, 1]``.
    """

    def __init__(
        self,
        model_dir: str | Path,
        *,
        max_length: int = 512,
        batch_size: int = 16,
        normalize: bool = False,
    ) -> None:
        self._dir = Path(model_dir)
        self._max_length = max_length
        self._batch_size = batch_size
        self._normalize = normalize
        self._info = make_info(
            component="reranker",
            implementation="bge-reranker-v2-m3",
            version="1",
            config={"max_length": max_length, "normalize": normalize},
            model_identity="BAAI/bge-reranker-v2-m3",
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        """Load (or reuse) the cross-encoder off the event loop."""
        self._offloader = context.offloader
        if self._loaded is None:
            self._loaded = await context.offloader.run(WorkloadKind.RERANK, bge_reranker, self._dir)

    async def close(self) -> None:
        """Release the reference (the process-wide model stays cached)."""
        self._loaded = None

    def _score(self, query: str, texts: list[str]) -> list[float]:
        _, loaded = self._ready()
        with loaded.lock:
            scores: Any = loaded.model.compute_score(
                [[query, text] for text in texts],
                batch_size=self._batch_size,
                max_length=self._max_length,
                normalize=self._normalize,
            )
        if not isinstance(scores, list):
            scores = [scores]
        return [float(score) for score in scores]

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        """Score every item exactly once."""
        if not items:
            return []
        offloader, _ = self._ready()
        scores = await offloader.run(
            WorkloadKind.RERANK, self._score, query, [item.text for item in items]
        )
        return [
            RerankScore(item_id=item.item_id, score=score)
            for item, score in zip(items, scores, strict=True)
        ]


class UltraBertNLI(_LocalModel, NLIABC):
    """UltraBERT-150M-nli premise/hypothesis classifier (labels from ``config.id2label``)."""

    MODEL_IDENTITY = "UltraBERT-150M-nli"
    _LABELS: dict[str, NLILabel] = {  # noqa: RUF012
        "entailment": NLILabel.ENTAILMENT,
        "neutral": NLILabel.NEUTRAL,
        "contradiction": NLILabel.CONTRADICTION,
    }

    def __init__(self, model_dir: str | Path, *, max_length: int = 512) -> None:
        self._dir = Path(model_dir)
        self._max_length = max_length
        self._info = make_info(
            component="nli",
            implementation="ultrabert",
            version="1",
            config={"max_length": max_length},
            model_identity=self.MODEL_IDENTITY,
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    async def initialize(self, context: ComponentContext) -> None:
        """Load (or reuse) the classifier off the event loop."""
        self._offloader = context.offloader
        if self._loaded is None:
            self._loaded = await context.offloader.run(
                WorkloadKind.NLI, lambda: sequence_classifier(self._dir, trust_remote_code=True)
            )

    async def close(self) -> None:
        """Release the reference (the process-wide model stays cached)."""
        self._loaded = None

    def _classify(self, premises: list[str], hypotheses: list[str]) -> list[dict[str, float]]:
        import torch

        _, loaded = self._ready()
        tokenizer, model = loaded.model
        with loaded.lock, torch.inference_mode():
            batch = tokenizer(
                premises,
                hypotheses,
                padding=True,
                truncation=True,
                max_length=self._max_length,
                return_tensors="pt",
            )
            probabilities = torch.softmax(model(**batch).logits, dim=-1)
        labels = model.config.id2label
        return [
            {str(labels[index]).casefold(): float(value) for index, value in enumerate(row)}
            for row in probabilities
        ]

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        """One judgement per pair, in order."""
        if not pairs:
            return []
        offloader, _ = self._ready()
        rows = await offloader.run(
            WorkloadKind.NLI,
            self._classify,
            [pair.premise for pair in pairs],
            [pair.hypothesis for pair in pairs],
        )
        judgements: list[NLIJudgement] = []
        for pair, row in zip(pairs, rows, strict=True):
            scores = {self._LABELS[name]: value for name, value in row.items()}
            label = max(scores, key=scores.__getitem__)
            judgements.append(
                NLIJudgement(
                    label=label,
                    confidence=min(1.0, max(0.0, scores[label])),
                    scores=scores,
                    model_identity=self.MODEL_IDENTITY,
                    hypothesis_ref=pair.hypothesis_ref,
                )
            )
        return judgements
