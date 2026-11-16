"""Concrete helpers and default behaviour of the component contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from pydantic import ValidationError as PydanticValidationError

from retrievalloom.contracts.component import (
    ComponentABC,
    ComponentContext,
    ComponentInfo,
    make_info,
)
from retrievalloom.contracts.evidence import (
    ConsolidationContext,
    FusedEntry,
    FusionABC,
    RankedEntry,
)
from retrievalloom.contracts.indexes import GenerationSpec
from retrievalloom.contracts.models import EmbeddingModelABC, KeywordEntry, NormalizedEntity
from retrievalloom.domain.hashing import digest_json
from retrievalloom.domain.reports import HealthStatus

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

INFO = make_info(component="fusion", implementation="dummy", version="1")


class _Fusion(FusionABC):
    @property
    def info(self) -> ComponentInfo:
        return INFO

    async def fuse(
        self, lists: Mapping[str, Sequence[RankedEntry]], weights: Mapping[str, float]
    ) -> Sequence[FusedEntry]:
        return []


class _Embedding(EmbeddingModelABC):
    @property
    def info(self) -> ComponentInfo:
        return make_info(
            component="embedding",
            implementation="dummy",
            version="1",
            model_identity="m",
            dimensions=2,
        )

    @property
    def dimensions(self) -> int:
        return 2

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return [[1.0, 0.0] for _ in texts]

    async def embed_query(self, text: str) -> Sequence[float]:
        return [1.0, 0.0]


def test_make_info_fingerprint_tracks_config() -> None:
    default = make_info(component="c", implementation="impl", version="1")
    assert default.name == "impl"
    assert default.fingerprint.config_digest == digest_json({})
    assert make_info(component="c", implementation="impl", version="1", config={}) == default
    named = make_info(component="c", implementation="impl", version="1", name="custom")
    assert named.name == "custom"
    assert named.fingerprint.digest == default.fingerprint.digest

    configured = make_info(component="c", implementation="impl", version="1", config={"k": 1})
    reordered = make_info(
        component="c", implementation="impl", version="1", config={"k": 1, "j": 2}
    )
    assert configured.fingerprint.digest != default.fingerprint.digest
    assert (
        reordered.fingerprint.digest
        == make_info(
            component="c", implementation="impl", version="1", config={"j": 2, "k": 1}
        ).fingerprint.digest
    )
    for field, value in (("version", "2"), ("model_identity", "m"), ("dimensions", 3)):
        kwargs: dict[str, Any] = {"component": "c", "implementation": "impl", "version": "1"}
        kwargs[field] = value
        assert make_info(**kwargs).fingerprint.digest != default.fingerprint.digest


async def test_component_defaults() -> None:
    fusion = _Fusion()
    await fusion.initialize(cast("ComponentContext", None))
    await fusion.close()
    health = await fusion.health()
    assert health.component == "dummy"
    assert health.status is HealthStatus.HEALTHY
    assert fusion.needs_scores is False
    assert await fusion.fuse({}, {}) == []


async def test_embedding_default_batch_size() -> None:
    model = _Embedding()
    assert model.max_batch_size == 64
    assert model.info.fingerprint.dimensions == model.dimensions
    assert await model.embed_documents(["a", "b"]) == [[1.0, 0.0], [1.0, 0.0]]
    assert await model.embed_query("a") == [1.0, 0.0]


def test_abstract_components_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError):
        cast("Any", ComponentABC)()


def test_keyword_entry_surface_forms() -> None:
    entry = KeywordEntry(canonical="New York", aliases=("NYC", "Big Apple"))
    assert entry.surface_forms == ("New York", "NYC", "Big Apple")
    assert KeywordEntry(canonical="x").surface_forms == ("x",)
    with pytest.raises(PydanticValidationError):
        KeywordEntry(canonical="")
    spec = GenerationSpec(
        generation_id="g",
        method="keyword",
        dependency_fingerprint=digest_json({}),
        keyword_entries=(entry,),
    )
    assert spec.keyword_entries[0].surface_forms[0] == "New York"


def test_contract_models_validate() -> None:
    with pytest.raises(PydanticValidationError):
        ConsolidationContext(top_k=0, contradiction_overflow=0)
    with pytest.raises(PydanticValidationError):
        ConsolidationContext(top_k=1, contradiction_overflow=0, span_iou_threshold=0)
    with pytest.raises(PydanticValidationError):
        RankedEntry(item_id="a", rank=0)
    with pytest.raises(PydanticValidationError):
        NormalizedEntity(canonical_key="", entity_type="t", name="n")
