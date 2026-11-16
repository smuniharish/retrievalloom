"""Default graph traversal: entity name matching followed by a k-hop neighbourhood."""

from __future__ import annotations

from typing import TYPE_CHECKING

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import GraphRetrieverABC
from retrievalloom.domain.records import Channel
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import ConfigurationError
from retrievalloom.index.text import SimpleTokenizer

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retrievalloom.contracts.indexes import GraphStoreABC
    from retrievalloom.domain.inputs import Scope

NEIGHBORHOOD_METHOD = MethodRef(channel=Channel.GRAPH, method="graph.neighborhood")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNIT_INTERVAL)


class NeighborhoodGraphRetriever(GraphRetrieverABC):
    """Seeds are entities named in the query; results are seeds plus their neighbourhood.

    A seed is an entity whose name or alias equals a query n-gram; the neighbourhood is
    everything within ``depth`` entity hops.

    Seeds score ``1.0``; a node at distance ``d`` scores ``1 / (1 + d)``. Each hit records
    its traversal path (object ids) in ``path_refs`` and a ``graph_path`` match detail.

    Args:
        depth: Entity hops to expand.
        max_ngram: Longest query n-gram tried as an entity name.
        seed_limit: Most seed entities.
    """

    def __init__(self, *, depth: int = 1, max_ngram: int = 4, seed_limit: int = 20) -> None:
        if depth < 1 or max_ngram < 1 or seed_limit < 1:
            raise ConfigurationError("depth, max_ngram and seed_limit must be positive")
        self._depth = depth
        self._max_ngram = max_ngram
        self._seed_limit = seed_limit
        self._tokenizer = SimpleTokenizer()
        self._info = make_info(
            component="graph_retriever",
            implementation="neighborhood",
            version="1",
            config={"depth": depth, "max_ngram": max_ngram, "seed_limit": seed_limit},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    def terms(self, query: str) -> list[str]:
        """Query n-grams (1..``max_ngram`` tokens), longest first."""
        tokens = self._tokenizer.tokenize_one(query)
        grams: dict[str, None] = {}
        for size in range(min(self._max_ngram, len(tokens)), 0, -1):
            for start in range(len(tokens) - size + 1):
                grams.setdefault(" ".join(tokens[start : start + size]), None)
        return list(grams)

    async def retrieve(
        self,
        scope: Scope,
        generation_id: str,
        query: str,
        store: GraphStoreABC,
        limit: int,
    ) -> Sequence[RetrievalHit]:
        """Return seed entities followed by their neighbourhood, nearest first."""
        terms = self.terms(query)
        if not terms or limit < 1:
            return []
        seeds = await store.find_entities(scope, generation_id, terms, self._seed_limit)
        if not seeds:
            return []
        subgraph = await store.neighborhood(
            scope,
            generation_id,
            [hit.object_ref.object_id for hit in seeds],
            self._depth,
            limit,
        )
        ranked: list[tuple[int, str, tuple[str, ...], tuple[MatchDetail, ...]]] = [
            (0, hit.object_ref.object_id, (hit.object_ref.object_id,), hit.matched) for hit in seeds
        ]
        seen = {object_id for _, object_id, _, _ in ranked}
        for node in sorted(
            (*subgraph.nodes, *subgraph.relationships),
            key=lambda value: (value.distance, value.object_id),
        ):
            if node.object_id in seen:
                continue
            seen.add(node.object_id)
            detail = MatchDetail(
                kind="graph_path", path=node.path, details={"distance": node.distance}
            )
            ranked.append((node.distance, node.object_id, node.path, (detail,)))
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key, object_id=object_id, generation_id=generation_id
                ),
                method=NEIGHBORHOOD_METHOD,
                backend_id=store.info.implementation,
                raw_score=1.0 / (1.0 + distance),
                score_semantics=_SEMANTICS,
                rank=rank,
                matched=matched,
                path_refs=path,
            )
            for rank, (distance, object_id, path, matched) in enumerate(ranked[:limit], start=1)
        ]
