"""Real models: contract kits and an end-to-end evidence and provenance check."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from retrievalloom import (
    Components,
    DocumentInput,
    RetrievalConfig,
    RetrievalExecutionConfig,
    Scope,
    SourceDescriptor,
    TextRepresentation,
)
from retrievalloom.config import NLIConfig
from retrievalloom.domain.retrieval import Channel, NLILabel, ResultStatus
from retrievalloom.evidence.rerank import FlashRankReranker
from retrievalloom.index.vector import QdrantVectorStore
from retrievalloom.storage.sqlite import SQLiteMetadataStore
from retrievalloom.testing import EmbeddingModelContract, NLIContract, RerankerContract
from tests.live.adapters import BgeM3OnnxEmbedding, BgeRerankerV2, UltraBertNLI
from tests.live.conftest import drop_collections, running_engine
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

pytestmark = pytest.mark.models

FLASHRANK_MODEL = "ms-marco-TinyBERT-L-2-v2"


class _ModelKit(SharedResources):
    @pytest.fixture(autouse=True)
    def _models(self, models_dir: Path) -> None:
        self.models = models_dir


class TestBgeM3(_ModelKit, EmbeddingModelContract):
    async def make_component(self, tmp_path: Path) -> BgeM3OnnxEmbedding:
        return BgeM3OnnxEmbedding(self.models / "bge-m3")


class TestBgeRerankerV2(_ModelKit, RerankerContract):
    async def make_component(self, tmp_path: Path) -> BgeRerankerV2:
        return BgeRerankerV2(self.models / "bge-reranker-v2-m3")


class TestUltraBertNLI(_ModelKit, NLIContract):
    async def make_component(self, tmp_path: Path) -> UltraBertNLI:
        return UltraBertNLI(self.models / "UltraBERT-150M-nli")


class TestFlashRank(_ModelKit, RerankerContract):
    """FlashRank never downloads here: the model must be pre-provisioned under models/."""

    async def make_component(self, tmp_path: Path) -> FlashRankReranker:
        reranker = FlashRankReranker(self.models / "flashrank", model_name=FLASHRANK_MODEL)
        if not reranker.model_path.is_file():
            pytest.skip(f"FlashRank model not provisioned at {reranker.model_path}")
        return reranker


CORPUS = {
    "eiffel": "The Eiffel Tower is a wrought-iron lattice tower in Paris, France, "
    "completed in 1889 for the World's Fair.",
    "photosynthesis": "Photosynthesis lets plants convert sunlight, water and carbon dioxide "
    "into glucose and oxygen.",
    "asyncio": "Python's asyncio library runs coroutines on a single-threaded event loop.",
    "everest": "Mount Everest, on the border of Nepal and China, is the highest mountain "
    "above sea level.",
}


@pytest.fixture
async def model_engine(
    tmp_path: Path, models_dir: Path, qdrant_url: str, prefix: str
) -> AsyncIterator[Any]:
    """bge-m3 on the Qdrant server, the bge cross-encoder reranker and UltraBERT NLI."""
    retrieval = RetrievalConfig(
        nli=NLIConfig(enabled=True, nli_k=4, on_error="fail"),
        request_timeout=180.0,
        method_timeout=120.0,
    )
    components = Components(
        metadata_store=SQLiteMetadataStore(tmp_path / "meta.db"),
        embedding=BgeM3OnnxEmbedding(models_dir / "bge-m3"),
        vector_store=QdrantVectorStore(url=qdrant_url, collection_prefix=prefix),
        reranker=BgeRerankerV2(models_dir / "bge-reranker-v2-m3"),
        nli=UltraBertNLI(models_dir / "UltraBERT-150M-nli"),
    )
    try:
        async with running_engine(components, retrieval=retrieval) as engine:
            yield engine
    finally:
        await drop_collections(qdrant_url, prefix)


@pytest.mark.qdrant
async def test_end_to_end_evidence_and_provenance(model_engine: Any) -> None:
    """Real embeddings, reranking and NLI produce grounded, fully attributed evidence."""
    scope = Scope(tenant_id="acme")
    report = await model_engine.ingest(
        [
            DocumentInput(
                _id=key,
                source=SourceDescriptor(uri=f"kb://{key}"),
                representations=(TextRepresentation(text=text),),
            )
            for key, text in CORPUS.items()
        ],
        scope=scope,
    )
    assert not report.warnings
    versions = {item.document_id: item.version for item in report.documents}

    result = await model_engine.retrieve(
        "When was the tower in Paris built?",
        scope=scope,
        top_k=3,
        claims=["The Eiffel Tower was completed in 1889.", "The Eiffel Tower is in Berlin."],
        config=RetrievalExecutionConfig(
            dense=True, reranking=True, nli=True, include_candidates=True
        ),
    )
    assert result.status is ResultStatus.COMPLETE, result.warnings
    assert not result.warnings
    top = result.evidence[0]
    assert top.sources[0].document_id == "eiffel"
    assert top.scores.rerank is not None
    assert top.scores.rerank == max(item.scores.rerank or float("-inf") for item in result.evidence)

    # provenance: every hit, source and stage is real and traceable to the ingested input
    stages = [record.stage for record in top.provenance.stage_trail]
    assert {"rerank", "nli"} <= set(stages)
    methods = {(hit.method.channel, hit.method.method) for hit in top.provenance.hits}
    assert (Channel.INDEX, "dense") in methods
    for item in result.evidence:
        assert item.scope == scope
        for source in item.sources:
            assert source.document_version == versions[source.document_id]
            assert source.source_uri == f"kb://{source.document_id}"
        text = " ".join(segment.text or "" for segment in item.segments)
        assert text in CORPUS[item.sources[0].document_id]

    # NLI: per hypothesis, the true claim is entailed and the false one contradicted
    assert top.nli_summary.entailment >= 1
    assert top.nli_summary.contradiction >= 1
    assert result.candidates is not None
    judged = next(c for c in result.candidates if c.candidate_id in top.member_candidate_ids)
    labels = {judgement.hypothesis_ref: judgement.label for judgement in judged.nli}
    assert labels["claim:0"] is NLILabel.ENTAILMENT
    assert labels["claim:1"] is NLILabel.CONTRADICTION
    assert all(j.model_identity == "UltraBERT-150M-nli" for j in judged.nli)
