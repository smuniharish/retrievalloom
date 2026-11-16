"""Default infrastructure: allow-all access, caches, no-op recorder and null metrics."""

from __future__ import annotations

import pytest

from retrievalloom.contracts.infra import Action
from retrievalloom.domain.reports import DeleteReport, HealthStatus
from retrievalloom.domain.retrieval import (
    ResultStatus,
    RetrievalRequest,
    RetrievalResult,
    RetrievalSnapshot,
)
from retrievalloom.errors import ConfigurationError
from retrievalloom.infra import (
    AllowAllAccessPolicy,
    InMemoryLRUCache,
    NoCache,
    NoOpProvenanceRecorder,
    NullMetrics,
)
from tests.unit.evidence.helpers import SCOPE


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


@pytest.mark.parametrize("action", list(Action))
async def test_allow_all_policy(action: Action) -> None:
    policy = AllowAllAccessPolicy()
    assert (await policy.authorize(None, SCOPE, action)).allowed
    assert (await policy.authorize("alice", SCOPE, action)).allowed
    assert await policy.row_filter("alice", SCOPE) is None


@pytest.mark.parametrize(("max_entries", "ttl"), [(-1, 1.0), (10, 0.0), (10, -5.0)])
def test_lru_rejects_invalid_configuration(max_entries: int, ttl: float) -> None:
    with pytest.raises(ConfigurationError):
        InMemoryLRUCache(max_entries, ttl)


async def test_lru_evicts_least_recently_used_across_namespaces() -> None:
    cache = InMemoryLRUCache(max_entries=2)
    await cache.set("ns1", "a", 1)
    await cache.set("ns2", "b", 2)
    assert await cache.get("ns1", "a") == 1  # "a" becomes most recently used
    await cache.set("ns1", "c", 3)
    assert len(cache) == 2
    assert await cache.get("ns2", "b") is None
    assert await cache.get("ns1", "a") == 1
    await cache.set("ns1", "c", 30)  # overwriting refreshes recency
    await cache.set("ns1", "d", 4)
    assert await cache.get("ns1", "a") is None
    assert await cache.get("ns1", "c") == 30
    await cache.close()
    assert len(cache) == 0


async def test_lru_ttl_with_injected_timer() -> None:
    clock = FakeClock()
    cache = InMemoryLRUCache(max_entries=4, ttl=10.0, timer=clock)
    await cache.set("ns", "k", "v")
    clock.now = 109.9
    assert await cache.get("ns", "k") == "v"
    clock.now = 110.0
    assert await cache.get("ns", "k") is None
    assert len(cache) == 0
    await cache.set("ns", "k", "fresh")
    clock.now = 119.0
    assert await cache.get("ns", "k") == "fresh"


async def test_lru_with_zero_entries_stores_nothing() -> None:
    cache = InMemoryLRUCache(max_entries=0)
    await cache.set("ns", "k", "v")
    assert len(cache) == 0
    assert await cache.get("ns", "k") is None
    assert await cache.invalidate("ns") == 0


async def test_lru_fingerprint_reflects_configuration() -> None:
    assert InMemoryLRUCache(10).info != InMemoryLRUCache(20).info
    assert InMemoryLRUCache(10).info == InMemoryLRUCache(10).info
    assert NoCache().info.name != InMemoryLRUCache().info.name
    assert AllowAllAccessPolicy().info.name != NoCache().info.name


async def test_lru_invalidation_is_per_namespace_and_prefix() -> None:
    cache = InMemoryLRUCache()
    for key in ("q:1", "q:2", "r:1"):
        await cache.set("ns", key, key)
    await cache.set("other", "q:1", "kept")
    assert await cache.invalidate("ns", "q:") == 2
    assert await cache.get("ns", "r:1") == "r:1"
    assert await cache.invalidate("ns") == 1
    assert await cache.get("other", "q:1") == "kept"
    assert len(cache) == 1


async def test_no_cache_always_misses() -> None:
    cache = NoCache()
    await cache.set("ns", "k", "v")
    assert await cache.get("ns", "k") is None
    assert await cache.invalidate("ns", "k") == 0


async def test_noop_recorder_and_null_metrics_accept_everything() -> None:
    recorder = NoOpProvenanceRecorder()
    request = RetrievalRequest(request_id="r1", scope=SCOPE, query="q", top_k=3)
    result = RetrievalResult(
        request_id="r1",
        scope=SCOPE,
        snapshot=RetrievalSnapshot(snapshot_seq=0),
        status=ResultStatus.EMPTY,
    )
    assert await recorder.record_retrieval(request, result) is None
    assert await recorder.record_ingestion(DeleteReport(scope=SCOPE)) is None
    assert (await recorder.health()).status is HealthStatus.HEALTHY
    assert (await recorder.health()).component == recorder.info.name
    metrics = NullMetrics()
    assert await metrics.increment("calls") is None
    assert await metrics.increment("calls", 2, {"stage": "fusion"}) is None
    assert await metrics.observe("latency", 1.5, {"stage": "fusion"}) is None
