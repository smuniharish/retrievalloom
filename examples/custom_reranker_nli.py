"""Custom reranker and NLI models: bge-reranker-v2-m3 and UltraBERT-150M-nli.

The application implements ``RerankerABC`` and ``NLIABC`` itself (here with transformers
and torch) and passes them as components. Blocking inference runs through the engine's
offloader with the matching workload kind (``RERANK``, ``NLI``), so it never blocks the
event loop and is subject to the engine's admission control.

The cross-encoder reorders the lexically fused candidates. The NLI model judges the top
candidates against the query and against application-supplied ``claims``. When one
candidate entails a claim and another contradicts it, consolidation links the two
evidence items (``contradicts``) and keeps the contradicting item in the result even
beyond ``top_k`` (contradiction-closed selection).

Requires local model weights (no Qdrant): ``bge-reranker-v2-m3`` and
``UltraBERT-150M-nli`` under ``RL_EXAMPLES_MODELS_DIR`` (fallback ``RL_TEST_MODELS_DIR``,
default ``models``), plus ``torch`` and ``transformers``.

Run from the repository root::

    python examples/custom_reranker_nli.py
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from _shared import models_dir, quiet_logs

from retrievalloom import (
    Components,
    DocumentInput,
    LifecycleConfig,
    NLIConfig,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    TextRepresentation,
    create_retrieval_engine,
)
from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import NLIABC, RerankerABC, RerankScore
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.runtime.offloader import WorkloadKind

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.models import NLIPair, RerankItem
    from retrievalloom.runtime.offloader import Offloader


class _Model:
    """Shared plumbing: load a sequence-classification model through the offloader."""

    kind: WorkloadKind
    trust_remote_code = False

    def __init__(self, model_dir: Path) -> None:
        self._dir = model_dir
        self._model: Any = None
        self._tokenizer: Any = None
        self._offloader: Offloader | None = None

    def _load(self) -> None:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        from transformers.utils import logging as hf_logging

        hf_logging.disable_progress_bar()
        self._tokenizer = AutoTokenizer.from_pretrained(self._dir)
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._dir, trust_remote_code=self.trust_remote_code
        ).eval()

    async def initialize(self, context: ComponentContext) -> None:
        self._offloader = context.offloader
        await context.offloader.run(self.kind, self._load)

    def _logits(self, first: list[str], second: list[str]) -> Any:
        import torch

        with torch.inference_mode():
            batch = self._tokenizer(
                first, second, padding=True, truncation=True, max_length=512, return_tensors="pt"
            )
            return self._model(**batch).logits

    async def _run(self, fn: Any, *args: Any) -> Any:
        if self._offloader is None:
            raise RuntimeError("model used before initialize()")
        return await self._offloader.run(self.kind, fn, *args)


class BgeReranker(_Model, RerankerABC):
    kind = WorkloadKind.RERANK

    @property
    def info(self) -> ComponentInfo:
        return make_info(
            component="reranker",
            implementation="bge-reranker-v2-m3",
            version="1",
            model_identity="BAAI/bge-reranker-v2-m3",
        )

    def _score(self, query: str, texts: list[str]) -> list[float]:
        return self._logits([query] * len(texts), texts).view(-1).tolist()

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        scores = await self._run(self._score, query, [item.text for item in items])
        return [
            RerankScore(item_id=item.item_id, score=score)
            for item, score in zip(items, scores, strict=True)
        ]


class UltraBertNLI(_Model, NLIABC):
    kind = WorkloadKind.NLI
    trust_remote_code = True  # the local weights ship their own modeling code

    @property
    def info(self) -> ComponentInfo:
        return make_info(
            component="nli",
            implementation="ultrabert",
            version="1",
            model_identity="UltraBERT-150M-nli",
        )

    def _judge(self, premises: list[str], hypotheses: list[str]) -> list[dict[str, float]]:
        import torch

        probabilities = torch.softmax(self._logits(premises, hypotheses), dim=-1)
        labels = self._model.config.id2label
        return [{labels[i]: float(p) for i, p in enumerate(row)} for row in probabilities]

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        rows = await self._run(
            self._judge, [pair.premise for pair in pairs], [pair.hypothesis for pair in pairs]
        )
        judgements = []
        for pair, row in zip(pairs, rows, strict=True):
            scores = {NLILabel(name.lower()): value for name, value in row.items()}
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


CORPUS = {
    "history": "Construction of the Eiffel Tower finished in March 1889, in time for the "
    "World's Fair in Paris.",
    "rumour": "A travel blog claims the Eiffel Tower was completed in 1925.",
    "pyramid": "When was the Louvre pyramid completed? The glass pyramid was completed in 1989.",
    "height": "The Eiffel Tower is 330 metres tall.",
}


async def main() -> None:
    quiet_logs()
    root = models_dir()
    reranker_dir, nli_dir = root / "bge-reranker-v2-m3", root / "UltraBERT-150M-nli"
    if not (reranker_dir.is_dir() and nli_dir.is_dir()):
        print(f"models not found under {root}; set RL_EXAMPLES_MODELS_DIR. Skipping.")
        return
    reranker, nli = BgeReranker(reranker_dir), UltraBertNLI(nli_dir)
    engine = create_retrieval_engine(
        retrieval=RetrievalConfig(nli=NLIConfig(enabled=True, nli_k=4), request_timeout=180.0),
        components=Components(reranker=reranker, nli=nli),
        lifecycle=LifecycleConfig(background=False),
    )
    scope = Scope(tenant_id="acme")
    async with engine:
        await engine.ingest(
            [
                DocumentInput(_id=key, representations=(TextRepresentation(text=text),))
                for key, text in CORPUS.items()
            ],
            scope=scope,
        )
        result = await engine.retrieve(
            "When was the Eiffel Tower completed?",
            scope=scope,
            top_k=1,
            claims=["The Eiffel Tower was completed in 1889."],
            config=RetrievalExecutionConfig(reranking=True, nli=True, include_candidates=True),
        )
        candidates = result.candidates or ()
        fused = sorted(candidates, key=lambda c: -(c.scores.channel_fusion or 0.0))
        reranked = sorted(candidates, key=lambda c: -(c.scores.rerank or float("-inf")))
        print(f"fused order:    {[c.sources[0].document_id for c in fused]}")
        print(f"reranked order: {[c.sources[0].document_id for c in reranked]}")
        for c in reranked:
            labels = {j.hypothesis_ref: j.label.value for j in c.nli}
            print(f"  {c.sources[0].document_id}: rerank={c.scores.rerank:.2f} nli={labels}")

        # Selection is contradiction-closed: contradicting items are added beyond top_k, up
        # to ConsolidationConfig.contradiction_overflow (default ceil(top_k / 2)).
        print(f"top_k=1 returned {len(result.evidence)} evidence item(s):")
        ids = {item.evidence_id: item.sources[0].document_id for item in result.evidence}
        for item in result.evidence:
            linked = [ids[i] for i in item.linked_evidence_ids if i in ids]
            outside = len(item.linked_evidence_ids) - len(linked)
            print(
                f"  {item.sources[0].document_id}: "
                f"has_contradiction={item.nli_summary.has_contradiction} "
                f"contradicts={linked} (+{outside} not selected)"
            )


if __name__ == "__main__":
    asyncio.run(main())
