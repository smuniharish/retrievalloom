"""Contract-kit switches and default hooks not exercised by the built-in adapter suite."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.models import NLIABC, RerankerABC, RerankScore
from retrievalloom.domain.retrieval import NLIJudgement, NLILabel
from retrievalloom.evidence.consolidation import HierarchicalConsolidator
from retrievalloom.infra import NoOpProvenanceRecorder
from retrievalloom.testing import EvidenceConsolidatorContract, NLIContract, RerankerContract
from retrievalloom.testing._base import ComponentContract
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from retrievalloom.contracts.evidence import ConsolidationContext
    from retrievalloom.contracts.models import NLIPair, RerankItem
    from retrievalloom.domain.retrieval import Candidate, Evidence


class ConstantReranker(RerankerABC):
    """Valid but uninformative: every item scores the same."""

    _INFO = make_info(component="reranker", implementation="constant", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._INFO

    async def rerank(self, query: str, items: Sequence[RerankItem]) -> Sequence[RerankScore]:
        return [RerankScore(item_id=item.item_id, score=0.5) for item in items]


class NeutralNLI(NLIABC):
    """Valid but uninformative: every pair is neutral."""

    _INFO = make_info(component="nli", implementation="neutral", version="1")

    @property
    def info(self) -> ComponentInfo:
        return self._INFO

    async def judge(self, pairs: Sequence[NLIPair]) -> Sequence[NLIJudgement]:
        return [
            NLIJudgement(
                label=NLILabel.NEUTRAL,
                confidence=0.5,
                model_identity="neutral",
                hypothesis_ref=pair.hypothesis_ref,
            )
            for pair in pairs
        ]


class TestRerankerKitWithoutRelevanceCheck(SharedResources, RerankerContract):
    check_relevance: ClassVar[bool] = False

    async def make_component(self, tmp_path: Path) -> ConstantReranker:
        return ConstantReranker()


class TestNLIKitWithoutSemanticsCheck(SharedResources, NLIContract):
    check_semantics: ClassVar[bool] = False

    async def make_component(self, tmp_path: Path) -> NeutralNLI:
        return NeutralNLI()


class TestBaseKitWithOwnResourceManager(ComponentContract):
    """No ``resource_manager`` override: the kit starts a private manager per test."""

    async def make_component(self, tmp_path: Path) -> NoOpProvenanceRecorder:
        assert self.resource_manager() is None
        return NoOpProvenanceRecorder()


class FirstOnlyConsolidator(HierarchicalConsolidator):
    """Consolidates only the best candidate (drops the rest, which the kit must tolerate)."""

    async def consolidate(
        self, candidates: Sequence[Candidate], context: ConsolidationContext
    ) -> tuple[Sequence[Evidence], Sequence[str]]:
        return await super().consolidate(candidates[:1], context)


class _ConsolidatorKit(EvidenceConsolidatorContract):
    async def make_component(self, tmp_path: Path) -> FirstOnlyConsolidator:
        return FirstOnlyConsolidator()


async def test_provenance_check_ignores_candidates_left_out_of_evidence(tmp_path: Path) -> None:
    kit = _ConsolidatorKit()
    component = await kit.make_component(tmp_path)
    await kit.test_provenance_is_preserved(component)
