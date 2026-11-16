"""Contract kits and behaviour tests for the offline example adapters (always run)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from examples.adapters import (
    AliasGraphNormalizer,
    CapitalizedEntityExtractor,
    DocumentDiversityConsolidator,
    GlossaryKeywordDictionary,
    JsonlProvenanceRecorder,
    NearDuplicateDeduplicator,
    PercentileNormalizer,
    StemmingTokenizer,
    TenantRoleAccessPolicy,
    TTLCache,
    WeightedCombSUMFusion,
)
from examples.adapters.evidence import percentiles
from retrievalloom.contracts.infra import Action
from retrievalloom.contracts.models import GraphExtraction
from retrievalloom.domain.filters import In
from retrievalloom.domain.inputs import GraphEntityInput, GraphRelationshipInput, Scope
from retrievalloom.domain.reports import DeleteReport
from retrievalloom.testing import (
    CacheContract,
    ComponentContract,
    DeduplicatorContract,
    EvidenceConsolidatorContract,
    FusionContract,
    GraphExtractorContract,
    ScoreNormalizerContract,
)
from retrievalloom.testing.evidence import make_candidate, score_reference
from tests.support import SharedResources

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from retrievalloom.contracts.evidence import RankedEntry

pytestmark = pytest.mark.examples

ACME = Scope(tenant_id="acme")


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class TestTTLCache(SharedResources, CacheContract):
    async def make_component(self, tmp_path: Path) -> TTLCache:
        return TTLCache(ttl=60)

    async def test_entries_expire_and_lru_evicts(self) -> None:
        clock = FakeClock()
        cache = TTLCache(ttl=10, max_entries=2, clock=clock)
        await cache.set("ns", "a", 1)
        clock.now = 5
        await cache.set("ns", "b", 2)
        assert await cache.get("ns", "a") == 1
        await cache.set("ns", "c", 3)
        assert await cache.get("ns", "b") is None
        clock.now = 10
        assert await cache.get("ns", "a") is None
        assert await cache.get("ns", "c") == 3
        assert (cache.hits, cache.misses) == (2, 2)


class TestPercentileNormalizer(SharedResources, ScoreNormalizerContract):
    async def make_component(self, tmp_path: Path) -> PercentileNormalizer:
        return PercentileNormalizer()


class TestWeightedCombSUMFusion(SharedResources, FusionContract):
    async def make_component(self, tmp_path: Path) -> WeightedCombSUMFusion:
        return WeightedCombSUMFusion()

    def reference(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> dict[str, float]:
        normalized = {source: percentiles(entries) for source, entries in lists.items()}
        return score_reference(lists, normalized, weights=weights)


class TestNearDuplicateDeduplicator(SharedResources, DeduplicatorContract):
    async def make_component(self, tmp_path: Path) -> NearDuplicateDeduplicator:
        return NearDuplicateDeduplicator(threshold=0.6)

    async def test_merges_lightly_edited_copies(self, component: Any) -> None:
        text = "Refunds are issued to the original payment method within five business days"
        original = make_candidate("faq", document="faq", final=0.9, text=text)
        copy = make_candidate(
            "release-note", document="notes", final=0.7, text=text + " after approval"
        )
        other = make_candidate("other", document="other", final=0.5, text="Reset your password")
        result = await component.deduplicate([original, copy, other])
        assert [c.candidate_id for c in result.kept] == [
            original.candidate_id,
            other.candidate_id,
        ]
        assert result.merged == {original.candidate_id: (copy.candidate_id,)}
        assert set(copy.sources) <= set(result.kept[0].sources)


class TestDocumentDiversityConsolidator(SharedResources, EvidenceConsolidatorContract):
    async def make_component(self, tmp_path: Path) -> DocumentDiversityConsolidator:
        return DocumentDiversityConsolidator(max_per_document=1)

    async def test_caps_candidates_per_document(self, component: Any) -> None:
        manual = [
            make_candidate(
                f"m{i}", document="manual", final=0.9 - i / 10, span=(i * 100, i * 100 + 50)
            )
            for i in range(3)
        ]
        faq = make_candidate("faq", document="faq", final=0.4)
        evidence, _ = await component.consolidate([*manual, faq], self.context())
        documents = [item.sources[0].document_id for item in evidence]
        assert documents == ["manual", "faq"]


class TestCapitalizedEntityExtractor(SharedResources, GraphExtractorContract):
    async def make_component(self, tmp_path: Path) -> CapitalizedEntityExtractor:
        return CapitalizedEntityExtractor(entity_types={"Python": "language"})

    async def test_extracts_people_products_and_relations(self, component: Any) -> None:
        extraction = await component.extract(self.source())
        names = {(entity.name, entity.entity_type) for entity in extraction.entities}
        assert names == {
            ("CPython", "entity"),
            ("Guido van Rossum", "entity"),
            ("Python", "language"),
        }
        relations = {
            (r.source_key, r.relation_type, r.target_key) for r in extraction.relationships
        }
        assert relations == {
            ("Guido van Rossum", "CREATED", "Python"),
            ("Python", "RUNS_ON", "CPython"),
        }


class TestAliasGraphNormalizer(SharedResources, ComponentContract):
    async def make_component(self, tmp_path: Path) -> AliasGraphNormalizer:
        return AliasGraphNormalizer(
            {"Postgres": "PostgreSQL", "PG": "PostgreSQL"},
            entity_types={"PostgreSQL": "database"},
        )

    async def test_aliases_merge_into_one_entity(self, component: Any) -> None:
        extraction = GraphExtraction(
            entities=(
                GraphEntityInput(name="Postgres"),
                GraphEntityInput(name="PostgreSQL"),
                GraphEntityInput(name="Billing API"),
            ),
            relationships=(
                GraphRelationshipInput(
                    source_key="Billing API", target_key="PG", relation_type="STORES_DATA_IN"
                ),
            ),
        )
        graph = await component.normalize(extraction, ACME)
        by_key = {entity.canonical_key: entity for entity in graph.entities}
        assert set(by_key) == {"postgresql", "billing api"}
        assert by_key["postgresql"].entity_type == "database"
        assert "Postgres" in by_key["postgresql"].aliases
        (relationship,) = graph.relationships
        assert (relationship.source_key, relationship.target_key) == ("billing api", "postgresql")


class TestStemmingTokenizer(SharedResources, ComponentContract):
    async def make_component(self, tmp_path: Path) -> StemmingTokenizer:
        return StemmingTokenizer()

    async def test_stems_and_drops_stopwords(self, component: Any) -> None:
        tokens = await component.tokenize(
            ["How do I get REFUNDS for cancelled orders?", "Refunded policies"]
        )
        assert tokens == [["get", "refund", "cancell", "order"], ["refund", "policy"]]


class TestGlossaryKeywordDictionary(SharedResources, ComponentContract):
    async def make_component(self, tmp_path: Path) -> GlossaryKeywordDictionary:
        path = tmp_path / "glossary.json"
        path.write_text(
            json.dumps(
                {
                    "*": [{"canonical": "two-factor authentication", "aliases": ["2fa", "mfa"]}],
                    "acme": [{"canonical": "AcmeSync", "aliases": ["acme sync"]}],
                }
            ),
            encoding="utf-8",
        )
        return GlossaryKeywordDictionary(path)

    async def test_entries_are_shared_plus_tenant(self, component: Any) -> None:
        acme = [entry.canonical for entry in await component.entries(ACME)]
        other = [entry.canonical for entry in await component.entries(Scope(tenant_id="globex"))]
        assert acme == ["two-factor authentication", "AcmeSync"]
        assert other == ["two-factor authentication"]


class TestTenantRoleAccessPolicy(SharedResources, ComponentContract):
    async def make_component(self, tmp_path: Path) -> TenantRoleAccessPolicy:
        return TenantRoleAccessPolicy(service_principals=frozenset({"svc:lifecycle"}))

    @pytest.mark.parametrize(
        ("principal", "action", "allowed"),
        [
            ("acme:viewer", Action.RETRIEVE, True),
            ("acme:viewer", Action.INGEST, False),
            ("acme:editor", Action.DELETE, True),
            ("acme:editor", Action.REBUILD, False),
            ("acme:admin:alice", Action.REBUILD, True),
            ("globex:admin", Action.RETRIEVE, False),
            ("acme:root", Action.RETRIEVE, False),
            (None, Action.RETRIEVE, False),
            ("svc:lifecycle", Action.BACKFILL, True),
        ],
    )
    async def test_decisions(
        self, component: Any, principal: str | None, action: Action, allowed: bool
    ) -> None:
        decision = await component.authorize(principal, ACME, action)
        assert decision.allowed is allowed, decision.reason

    async def test_row_filters_follow_role(self, component: Any) -> None:
        assert await component.row_filter("acme:viewer", ACME) == In(
            field="visibility", values=("public",)
        )
        assert await component.row_filter("acme:editor", ACME) == In(
            field="visibility", values=("public", "internal")
        )
        assert await component.row_filter("acme:admin", ACME) is None


class TestJsonlProvenanceRecorder(SharedResources, ComponentContract):
    async def make_component(self, tmp_path: Path) -> JsonlProvenanceRecorder:
        return JsonlProvenanceRecorder(tmp_path / "audit" / "provenance.jsonl")

    async def test_appends_one_line_per_event(self, component: Any) -> None:
        await component.record_ingestion(
            DeleteReport(scope=ACME, deleted=("faq-1",), not_found=("faq-9",))
        )
        await component.record_ingestion(DeleteReport(scope=ACME, deleted=("faq-2",)))
        lines = component.path.read_text(encoding="utf-8").splitlines()
        records = [json.loads(line) for line in lines]
        assert [record["deleted"] for record in records] == [["faq-1"], ["faq-2"]]
        assert records[0]["not_found"] == ["faq-9"]
        assert all(record["event"] == "delete" and record["at"] for record in records)
