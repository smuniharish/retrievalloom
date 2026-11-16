"""FlashRankReranker with a fake ``flashrank.Ranker`` (no model files, no network)."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

from retrievalloom.contracts.component import ComponentContext
from retrievalloom.contracts.models import RerankItem
from retrievalloom.domain.base import utc_now
from retrievalloom.domain.hashing import random_id
from retrievalloom.domain.reports import HealthStatus
from retrievalloom.errors import (
    BackendError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    LifecycleError,
)
from retrievalloom.evidence import rerank as rerank_module
from retrievalloom.evidence.rerank import DEFAULT_MODEL, FlashRankReranker
from retrievalloom.infra.metrics import NullMetrics
from retrievalloom.runtime.offloader import Offloader, OffloaderConfig
from tests.support import shared_manager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


class RecordingLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **values: Any) -> None:
        self.warnings.append((event, values))


class FakeRanker:
    """Stands in for ``flashrank.Ranker``: scores passages by query-word overlap."""

    instances: ClassVar[list[FakeRanker]] = []
    fail_init: ClassVar[Exception | None] = None

    def __init__(self, *, model_name: str, cache_dir: str, max_length: int, log_level: str) -> None:
        if FakeRanker.fail_init is not None:
            raise FakeRanker.fail_init
        self.args = {
            "model_name": model_name,
            "cache_dir": cache_dir,
            "max_length": max_length,
            "log_level": log_level,
        }
        self.root_handlers = list(logging.getLogger().handlers)
        self.batches: list[list[str]] = []
        self.error: Exception | None = None
        self.override: dict[str, Any] = {}
        FakeRanker.instances.append(self)

    def rerank(self, request: Any) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        self.batches.append([passage["id"] for passage in request.passages])
        terms = set(request.query.lower().split())
        ranked = [
            {
                **passage,
                "score": self.override.get(
                    passage["id"], len(terms & set(passage["text"].lower().split())) / len(terms)
                ),
            }
            for passage in request.passages
            if self.override.get(passage["id"], 0.0) is not None
        ]
        return sorted(ranked, key=lambda passage: -passage["score"])


@pytest.fixture(autouse=True)
def fake_ranker(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeRanker.instances = []
    FakeRanker.fail_init = None
    monkeypatch.setattr(rerank_module, "Ranker", FakeRanker)


@pytest.fixture
async def offloader() -> AsyncIterator[Offloader]:
    instance = Offloader(OffloaderConfig(max_workers=2), resource_manager=shared_manager())
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
def logger() -> RecordingLogger:
    return RecordingLogger()


@pytest.fixture
def context(offloader: Offloader, logger: RecordingLogger, tmp_path: Path) -> ComponentContext:
    return ComponentContext(
        engine_id=random_id("engine"),
        offloader=offloader,
        clock=utc_now,
        logger=logger,
        metrics=NullMetrics(),
        data_dir=tmp_path,
    )


def with_model(tmp_path: Path, **options: Any) -> FlashRankReranker:
    reranker = FlashRankReranker(tmp_path, **options)
    reranker.model_path.parent.mkdir(parents=True)
    reranker.model_path.write_bytes(b"onnx")
    return reranker


def items(*texts: str) -> list[RerankItem]:
    return [RerankItem(item_id=f"i{index}", text=text) for index, text in enumerate(texts)]


@pytest.mark.parametrize(
    "options",
    [
        {"model_name": "no-such-model"},
        {"model_name": "rank_zephyr_7b_v1_full"},
        {"max_length": 15},
        {"batch_size": 0},
    ],
)
def test_rejects_invalid_configuration(tmp_path: Path, options: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        FlashRankReranker(tmp_path, **options)


def test_identity_declares_model(tmp_path: Path) -> None:
    reranker = FlashRankReranker(tmp_path, model_name="ms-marco-MiniLM-L-12-v2")
    assert reranker.info.fingerprint.model_identity == "flashrank:ms-marco-MiniLM-L-12-v2"
    assert reranker.model_path.parent == tmp_path / "ms-marco-MiniLM-L-12-v2"
    other = FlashRankReranker(tmp_path, model_name="ms-marco-MiniLM-L-12-v2", max_length=256)
    assert other.info.fingerprint.digest != reranker.info.fingerprint.digest


async def test_missing_model_is_unavailable_without_download(
    tmp_path: Path, context: ComponentContext, logger: RecordingLogger
) -> None:
    reranker = FlashRankReranker(tmp_path)
    assert (await reranker.health()).message == "not initialised"
    with pytest.raises(LifecycleError):
        await reranker.rerank("q", items("a"))
    await reranker.initialize(context)
    assert FakeRanker.instances == []
    health = await reranker.health()
    assert health.status is HealthStatus.UNHEALTHY
    assert health.message == "model files not found and downloads are disabled"
    assert [event for event, _ in logger.warnings] == ["reranker_unavailable"]
    assert logger.warnings[0][1]["model"] == DEFAULT_MODEL
    with pytest.raises(BackendUnavailableError) as caught:
        await reranker.rerank("q", items("a"))
    assert caught.value.details == {"reason": health.message}
    await reranker.close()


async def test_download_allowed_loads_without_local_files(
    tmp_path: Path, context: ComponentContext
) -> None:
    reranker = FlashRankReranker(tmp_path, allow_download=True, max_length=128)
    await reranker.initialize(context)
    (ranker,) = FakeRanker.instances
    assert ranker.args == {
        "model_name": DEFAULT_MODEL,
        "cache_dir": str(tmp_path),
        "max_length": 128,
        "log_level": "WARNING",
    }
    assert (await reranker.health()).status is HealthStatus.HEALTHY
    await reranker.close()


async def test_load_failure_marks_unavailable(
    tmp_path: Path, context: ComponentContext, logger: RecordingLogger
) -> None:
    FakeRanker.fail_init = RuntimeError("corrupt model")
    reranker = with_model(tmp_path)
    await reranker.initialize(context)
    health = await reranker.health()
    assert health.status is HealthStatus.UNHEALTHY
    assert health.message == "model failed to load: RuntimeError"
    (event, values) = logger.warnings[0]
    assert event == "reranker_unavailable"
    assert isinstance(values["exc_info"], RuntimeError)
    with pytest.raises(BackendUnavailableError):
        await reranker.rerank("q", items("a"))


async def test_scores_in_batches_aligned_to_input(
    tmp_path: Path, context: ComponentContext
) -> None:
    reranker = with_model(tmp_path, batch_size=2)
    await reranker.initialize(context)
    await reranker.initialize(context)
    (ranker,) = FakeRanker.instances
    texts = ("paris france", "bananas", "capital of france", "paris", "nothing here")
    scores = await reranker.rerank("capital france paris", items(*texts))
    assert ranker.batches == [["i0", "i1"], ["i2", "i3"], ["i4"]]
    assert [score.item_id for score in scores] == ["i0", "i1", "i2", "i3", "i4"]
    assert [score.score for score in scores] == pytest.approx([2 / 3, 0, 2 / 3, 1 / 3, 0])
    assert await reranker.rerank("q", []) == []
    assert len(ranker.batches) == 3


async def test_rejects_duplicate_item_ids(tmp_path: Path, context: ComponentContext) -> None:
    reranker = with_model(tmp_path)
    await reranker.initialize(context)
    duplicate = [RerankItem(item_id="x", text="a"), RerankItem(item_id="x", text="b")]
    with pytest.raises(ComponentContractError):
        await reranker.rerank("q", duplicate)


async def test_backend_failure_is_wrapped(tmp_path: Path, context: ComponentContext) -> None:
    reranker = with_model(tmp_path)
    await reranker.initialize(context)
    FakeRanker.instances[0].error = ValueError("onnx exploded")
    with pytest.raises(BackendError) as caught:
        await reranker.rerank("q", items("a"))
    assert caught.value.details == {"error": "ValueError"}
    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.parametrize("bad", [None, math.nan, math.inf])
async def test_missing_or_non_finite_scores_break_the_contract(
    tmp_path: Path, context: ComponentContext, bad: float | None
) -> None:
    reranker = with_model(tmp_path)
    await reranker.initialize(context)
    FakeRanker.instances[0].override = {"i1": bad}
    with pytest.raises(ComponentContractError) as caught:
        await reranker.rerank("q", items("a", "b"))
    assert caught.value.details == {"item_id": "i1"}


async def test_close_releases_model(tmp_path: Path, context: ComponentContext) -> None:
    reranker = with_model(tmp_path)
    await reranker.initialize(context)
    await reranker.close()
    await reranker.close()
    assert (await reranker.health()).status is HealthStatus.UNHEALTHY
    with pytest.raises(LifecycleError):
        await reranker.rerank("q", items("a"))
    await reranker.initialize(context)
    assert len(FakeRanker.instances) == 2
    assert (await reranker.health()).status is HealthStatus.HEALTHY


def test_loader_shields_an_unconfigured_root_logger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    ranker = rerank_module._load_ranker(DEFAULT_MODEL, tmp_path, 64)
    assert [type(handler) for handler in ranker.root_handlers] == [logging.NullHandler]
    assert root.handlers == []

    existing = logging.NullHandler()
    monkeypatch.setattr(root, "handlers", [existing])
    ranker = rerank_module._load_ranker(DEFAULT_MODEL, tmp_path, 64)
    assert ranker.root_handlers == [existing]
    assert root.handlers == [existing]
