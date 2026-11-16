"""Real-model adapters used by the live suite (bge-m3, bge-reranker-v2-m3, UltraBERT NLI).

Inference is blocking, so every call runs through the engine offloader (admission control,
bounded executor). Models load on ``initialize`` from ``RL_TEST_MODELS_DIR``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import NLIABC, EmbeddingModelABC, RerankerABC, RerankScore
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.models import NLIPair, RerankItem
    from retrievalloom.runtime.offloader import Offloader


_LOADED: dict[tuple[str, Path], Any] = {}


def _cached(kind: str, model_dir: Path, load: Callable[[], Any]) -> Any:
    """Load each model once per process; the suite reuses the weights across tests."""
    key = (kind, model_dir)
    if key not in _LOADED:
        _LOADED[key] = load()
    return _LOADED[key]


class _Blocking:
    """Shared plumbing: keep the context offloader and require initialisation."""

    _offloader: Offloader | None = None

    def _require(self) -> Offloader:
        if self._offloader is None:
            message = "model adapter used before initialize()"
            raise RuntimeError(message)
        return self._offloader


class BgeM3OnnxEmbedding(_Blocking, EmbeddingModelABC):
    """bge-m3 dense embeddings (1024-d, CLS-pooled, L2-normalised) via ONNX Runtime."""

    def __init__(self, model_dir: Path, *, batch_size: int = 8, max_length: int = 512) -> None:
        self._dir = model_dir
        self._batch_size = batch_size
        self._max_length = max_length
        self._session: Any = None
        self._tokenizer: Any = None
        self._info = make_info(
            component="embedding",
            implementation="bge-m3-onnx",
            version="1",
            model_identity="BAAI/bge-m3",
            dimensions=1024,
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def dimensions(self) -> int:
        """Vector size."""
        return 1024

    @property
    def max_batch_size(self) -> int:
        """Largest batch."""
        return self._batch_size

    def _load(self) -> None:
        import onnxruntime
        from tokenizers import Tokenizer

        tokenizer = Tokenizer.from_file(str(self._dir / "onnx" / "tokenizer.json"))
        tokenizer.enable_truncation(self._max_length)
        tokenizer.enable_padding()
        self._tokenizer = tokenizer
        self._session = _cached(
            "onnx",
            self._dir,
            lambda: onnxruntime.InferenceSession(
                str(self._dir / "onnx" / "model.onnx"), providers=["CPUExecutionProvider"]
            ),
        )

    async def initialize(self, context: ComponentContext) -> None:
        """Load the tokenizer and ONNX session off the event loop."""
        self._offloader = context.offloader
        if self._session is None:
            await context.offloader.run(WorkloadKind.EMBEDDING, self._load)

    def _embed(self, texts: Sequence[str]) -> list[list[float]]:
        import numpy as np

        encodings = self._tokenizer.encode_batch(list(texts))
        ids = np.array([encoding.ids for encoding in encodings], dtype=np.int64)
        mask = np.array([encoding.attention_mask for encoding in encodings], dtype=np.int64)
        (vectors,) = self._session.run(
            ["sentence_embedding"], {"input_ids": ids, "attention_mask": mask}
        )
        return [[float(value) for value in vector] for vector in vectors]

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed documents."""
        return await self._require().run(WorkloadKind.EMBEDDING, self._embed, list(texts))

    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed a query."""
        return (await self._require().run(WorkloadKind.EMBEDDING, self._embed, [text]))[0]


class BgeRerankerV2(_Blocking, RerankerABC):
    """bge-reranker-v2-m3 cross-encoder through transformers (CPU)."""

    def __init__(self, model_dir: Path, *, max_length: int = 512) -> None:
        self._dir = model_dir
        self._max_length = max_length
        self._model: Any = None
        self._tokenizer: Any = None
        self._info = make_info(
            component="reranker",
            implementation="bge-reranker-v2-m3",
            version="1",
            model_identity="BAAI/bge-reranker-v2-m3",
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _load(self) -> None:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self._tokenizer = _cached(
            "tok", self._dir, lambda: AutoTokenizer.from_pretrained(self._dir)
        )
        self._model = _cached(
            "seq",
            self._dir,
            lambda: AutoModelForSequenceClassification.from_pretrained(self._dir).eval(),
        )

    async def initialize(self, context: ComponentContext) -> None:
        """Load weights off the event loop."""
        self._offloader = context.offloader
        if self._model is None:
            await context.offloader.run(WorkloadKind.RERANK, self._load)

    def _score(self, query: str, texts: list[str]) -> list[float]:
        import torch

        with torch.inference_mode():
            batch = self._tokenizer(
                [query] * len(texts),
                texts,
                padding=True,
                truncation=True,
                max_length=self._max_length,
                return_tensors="pt",
            )
            logits = self._model(**batch).logits.view(-1)
        return [float(value) for value in logits]

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        """Score each item against ``query``."""
        if not items:
            return []
        scores = await self._require().run(
            WorkloadKind.RERANK, self._score, query, [item.text for item in items]
        )
        return [
            RerankScore(item_id=item.item_id, score=score)
            for item, score in zip(items, scores, strict=True)
        ]


class UltraBertNLI(_Blocking, NLIABC):
    """UltraBERT-150M-nli (custom modeling code, loaded with ``trust_remote_code``)."""

    _LABELS: dict[str, NLILabel] = {  # noqa: RUF012
        "entailment": NLILabel.ENTAILMENT,
        "neutral": NLILabel.NEUTRAL,
        "contradiction": NLILabel.CONTRADICTION,
    }

    def __init__(self, model_dir: Path, *, max_length: int = 512) -> None:
        self._dir = model_dir
        self._max_length = max_length
        self._model: Any = None
        self._tokenizer: Any = None
        self._info = make_info(
            component="nli",
            implementation="ultrabert",
            version="1",
            model_identity="UltraBERT-150M-nli",
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def _load(self) -> None:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self._tokenizer = _cached(
            "tok", self._dir, lambda: AutoTokenizer.from_pretrained(self._dir)
        )
        # The weights ship custom modeling code; it is local and was reviewed.
        self._model = _cached(
            "seq",
            self._dir,
            lambda: AutoModelForSequenceClassification.from_pretrained(
                self._dir, trust_remote_code=True
            ).eval(),
        )

    async def initialize(self, context: ComponentContext) -> None:
        """Load weights off the event loop."""
        self._offloader = context.offloader
        if self._model is None:
            await context.offloader.run(WorkloadKind.NLI, self._load)

    def _judge(self, premises: list[str], hypotheses: list[str]) -> list[dict[str, float]]:
        import torch

        with torch.inference_mode():
            batch = self._tokenizer(
                premises,
                hypotheses,
                padding=True,
                truncation=True,
                max_length=self._max_length,
                return_tensors="pt",
            )
            probabilities = torch.softmax(self._model(**batch).logits, dim=-1)
        labels = self._model.config.id2label
        return [
            {labels[index]: float(value) for index, value in enumerate(row)}
            for row in probabilities
        ]

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        """Judge each (premise, hypothesis) pair."""
        if not pairs:
            return []
        rows = await self._require().run(
            WorkloadKind.NLI,
            self._judge,
            [pair.premise for pair in pairs],
            [pair.hypothesis for pair in pairs],
        )
        judgements = []
        for pair, row in zip(pairs, rows, strict=True):
            scores = {self._LABELS[name]: value for name, value in row.items()}
            label = max(scores, key=lambda key: scores[key])
            judgements.append(
                NLIJudgement(
                    label=label,
                    confidence=scores[label],
                    scores=scores,
                    model_identity="UltraBERT-150M-nli",
                    hypothesis_ref=pair.hypothesis_ref,
                )
            )
        return judgements
