# ruff: noqa: S608 - identifiers are validated at construction; values are bound parameters.
"""Property graph store on Neo4j (official async driver), mirroring ``LadybugGraphStore``.

Entities and relationships are both ``:<label>`` nodes keyed by
``scope_key:generation_id:object_id``; a relationship node links its endpoints through
``RL_LINK`` edges (``source -> relationship -> target``), so one entity hop is two edge hops
and relationships are first-class, filterable objects. Every query is parameterised with
``$scope_key`` and ``$generation_id``; seeds are resolved to node ids of the same scope and
generation, so traversal can never leave a tenant's graph.

The driver is natively async (non-blocking network I/O), so no offloading is needed.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, LiteralString, cast

from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    GraphNode,
    GraphStoreABC,
    GraphSubgraph,
    IndexCapabilities,
    WriteReport,
)
from retrievalloom.domain.records import Channel, ObjectKind
from retrievalloom.domain.reports import HealthReport, HealthStatus
from retrievalloom.domain.retrieval import (
    MatchDetail,
    MethodRef,
    ObjectRef,
    RetrievalHit,
    ScoreRange,
    ScoreSemantics,
)
from retrievalloom.errors import (
    BackendError,
    BackendUnavailableError,
    ComponentContractError,
    ConfigurationError,
    LifecycleError,
)
from retrievalloom.graph.normalizer import canonical_key

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from neo4j import AsyncDriver, AsyncManagedTransaction

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.inputs import Scope

ENTITY_METHOD = MethodRef(channel=Channel.GRAPH, method="graph.entity")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_MAX_DEPTH = 5
_LABEL = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


def _node_id(scope_key: str, generation_id: str, object_id: str) -> str:
    return f"{scope_key}:{generation_id}:{object_id}"


def _cypher(text: str) -> LiteralString:
    # Only the validated label and integer hop bounds are interpolated; values are parameters.
    return cast("LiteralString", text)


class Neo4jGraphStore(GraphStoreABC):
    """Generation-aware graph aggregates in Neo4j.

    Args:
        url: Bolt URL, e.g. ``bolt://localhost:7687``.
        user: User name.
        password: Password.
        database: Database name (``None`` = server default).
        label: Node label owning this store's data; use one label per deployment (or per
            test) to share a Neo4j server. Must be a plain identifier.
        driver: Application-owned ``AsyncDriver`` (never closed by the store).
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        user: str = "neo4j",
        password: str | None = None,
        database: str | None = None,
        label: str = "RLNode",
        driver: AsyncDriver | None = None,
    ) -> None:
        if (url is None) == (driver is None):
            raise ConfigurationError("pass exactly one of url or driver")
        if not _LABEL.match(label):
            raise ConfigurationError("label must be a plain identifier")
        self._url = url
        self._auth = (user, password or "")
        self._database = database
        self._label = label
        self._driver: AsyncDriver | None = driver
        self._owns_driver = driver is None
        self._schema_ready = False
        self._info = make_info(
            component="graph_store",
            implementation="neo4j",
            version="1",
            config={"label": label, "database": database},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place, durable (shared server), no filter pushdown."""
        return IndexCapabilities(mutable_in_place=True, durable=True, filter_pushdown=False)

    async def initialize(self, context: ComponentContext) -> None:
        """Connect and create the uniqueness constraint and lookup index (idempotent)."""
        del context
        if self._driver is None:
            assert self._url is not None
            self._driver = AsyncGraphDatabase.driver(self._url, auth=self._auth)
        if not self._schema_ready:
            label = self._label
            await self._query(
                f"CREATE CONSTRAINT {label}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE"
            )
            await self._query(
                f"CREATE INDEX {label}_scope IF NOT EXISTS "
                f"FOR (n:{label}) ON (n.scope_key, n.generation_id)"
            )
            self._schema_ready = True

    async def close(self) -> None:
        """Close an owned driver (idempotent)."""
        driver, self._driver = self._driver, None
        self._schema_ready = False
        if driver is not None and self._owns_driver:
            await driver.close()

    async def drop_label(self) -> None:
        """Delete every node, the constraint and the index of this store's label."""
        label = self._label
        await self._query(f"MATCH (n:{label}) DETACH DELETE n")
        await self._query(f"DROP CONSTRAINT {label}_id IF EXISTS")
        await self._query(f"DROP INDEX {label}_scope IF EXISTS")

    async def health(self) -> HealthReport:
        """Healthy when the server answers ``RETURN 1``."""
        try:
            await self._query("RETURN 1 AS ok")
        except (BackendError, LifecycleError) as exc:
            return HealthReport(
                component=self.info.name, status=HealthStatus.UNHEALTHY, message=str(exc)
            )
        return HealthReport(component=self.info.name)

    def _require(self) -> AsyncDriver:
        if self._driver is None:
            raise LifecycleError("neo4j graph store is not initialised")
        return self._driver

    async def _query(self, query: str, **parameters: Any) -> list[dict[str, Any]]:
        driver = self._require()
        try:
            result = await driver.execute_query(
                _cypher(query), parameters, database_=self._database
            )
        except (ServiceUnavailable, SessionExpired) as exc:
            raise BackendUnavailableError("neo4j is unreachable") from exc
        except Neo4jError as exc:
            raise BackendError("neo4j query failed", details={"code": exc.code}) from exc
        return [record.data() for record in result.records]

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Generations are node properties; nothing to create."""
        del scope, spec

    async def _write_tx(
        self,
        tx: AsyncManagedTransaction,
        scope: Scope,
        generation_id: str,
        items: Sequence[IndexItem],
    ) -> None:
        label = self._label
        for item in items:
            node_id = _node_id(scope.key, generation_id, item.object_id)
            base = {
                "id": node_id,
                "scope_key": scope.key,
                "generation_id": generation_id,
                "object_id": item.object_id,
                "meta": json.dumps(item.metadata, sort_keys=True),
            }
            payload = item.payload
            if item.kind == ObjectKind.GRAPH_ENTITY:
                name = str(payload.get("name") or item.text)
                aliases = [str(alias) for alias in payload.get("aliases", ())]
                names = sorted({canonical_key(value) for value in (name, *aliases)} - {""})
                await tx.run(
                    _cypher(
                        f"MERGE (n:{label} {{id: $id}}) SET n.scope_key = $scope_key, "
                        "n.generation_id = $generation_id, n.object_id = $object_id, "
                        "n.kind = 'entity', n.entity_type = $entity_type, n.name = $name, "
                        "n.names = $names, n.relation_type = '', n.meta = $meta"
                    ),
                    {
                        **base,
                        "entity_type": str(payload.get("entity_type", "entity")),
                        "name": name,
                        "names": names,
                    },
                )
                continue
            if item.kind != ObjectKind.GRAPH_RELATIONSHIP:
                raise ComponentContractError(
                    "graph items must be graph entities or relationships",
                    details={"object_id": item.object_id, "kind": item.kind},
                )
            ends = {
                "source": _node_id(scope.key, generation_id, str(payload.get("source_entity_id"))),
                "target": _node_id(scope.key, generation_id, str(payload.get("target_entity_id"))),
            }
            found = await tx.run(
                _cypher(
                    f"MATCH (n:{label}) WHERE n.id IN $ids AND n.kind = 'entity' RETURN n.id AS id"
                ),
                {"ids": list(ends.values())},
            )
            if {record["id"] async for record in found} != set(ends.values()):
                raise ComponentContractError(
                    "relationship endpoints must be written first",
                    details={"object_id": item.object_id},
                )
            await tx.run(
                _cypher(
                    f"MERGE (r:{label} {{id: $id}}) SET r.scope_key = $scope_key, "
                    "r.generation_id = $generation_id, r.object_id = $object_id, "
                    "r.kind = 'relationship', r.entity_type = '', r.name = $name, r.names = [], "
                    "r.relation_type = $relation_type, r.meta = $meta "
                    "WITH r OPTIONAL MATCH (r)-[old:RL_LINK]-() DELETE old "
                    f"WITH DISTINCT r MATCH (s:{label} {{id: $source}}), "
                    f"(t:{label} {{id: $target}}) CREATE (s)-[:RL_LINK {{role: 'source'}}]->(r), "
                    "(r)-[:RL_LINK {role: 'target'}]->(t)"
                ),
                {
                    **base,
                    **ends,
                    "name": item.text,
                    "relation_type": str(payload.get("relation_type", "")),
                },
            )

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Upsert entities and relationships in one transaction (entities first)."""
        if not items:
            return WriteReport(written=0)
        ordered = sorted(items, key=lambda item: item.kind != ObjectKind.GRAPH_ENTITY)
        driver = self._require()
        try:
            async with driver.session(database=self._database) as session:
                await session.execute_write(self._write_tx, scope, generation_id, ordered)
        except (ServiceUnavailable, SessionExpired) as exc:
            raise BackendUnavailableError("neo4j is unreachable") from exc
        except Neo4jError as exc:
            raise BackendError("neo4j write failed", details={"code": exc.code}) from exc
        return WriteReport(written=len(ordered))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Detach-delete nodes; returns how many existed."""
        ids = [_node_id(scope.key, generation_id, object_id) for object_id in object_ids]
        if not ids:
            return 0
        rows = await self._query(
            f"MATCH (n:{self._label}) WHERE n.id IN $ids "
            "WITH collect(n) AS nodes, count(n) AS present "
            "FOREACH (n IN nodes | DETACH DELETE n) RETURN present",
            ids=ids,
        )
        return int(rows[0]["present"]) if rows else 0

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace stored filter metadata of present nodes."""
        if not updates:
            return 0
        rows = await self._query(
            "UNWIND $updates AS update "
            f"MATCH (n:{self._label} {{id: update.id}}) SET n.meta = update.meta "
            "RETURN count(n) AS updated",
            updates=[
                {
                    "id": _node_id(scope.key, generation_id, object_id),
                    "meta": json.dumps(metadata, sort_keys=True),
                }
                for object_id, metadata in updates.items()
            ],
        )
        return int(rows[0]["updated"]) if rows else 0

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Count the generation's nodes by kind (no build step)."""
        rows = await self._query(
            f"MATCH (n:{self._label}) WHERE n.scope_key = $scope_key "
            "AND n.generation_id = $generation_id RETURN n.kind AS kind, count(n) AS count",
            scope_key=scope.key,
            generation_id=generation_id,
        )
        counts = {str(row["kind"]): int(row["count"]) for row in rows}
        return GenerationStats(object_count=sum(counts.values()), details=counts)

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete every node of a generation (idempotent)."""
        await self._query(
            f"MATCH (n:{self._label}) WHERE n.scope_key = $scope_key "
            "AND n.generation_id = $generation_id DETACH DELETE n",
            scope_key=scope.key,
            generation_id=generation_id,
        )

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Distinct generations stored for ``scope``."""
        rows = await self._query(
            f"MATCH (n:{self._label}) WHERE n.scope_key = $scope_key "
            "RETURN DISTINCT n.generation_id AS generation_id",
            scope_key=scope.key,
        )
        return sorted(str(row["generation_id"]) for row in rows)

    async def find_entities(
        self,
        scope: Scope,
        generation_id: str,
        terms: Sequence[str],
        limit: int,
    ) -> Sequence[RetrievalHit]:
        """Entities whose canonical name or alias equals a term; score = matched terms."""
        folded = sorted({canonical_key(term) for term in terms} - {""})
        if not folded or limit < 1:
            return []
        rows = await self._query(
            f"MATCH (n:{self._label}) WHERE n.scope_key = $scope_key "
            "AND n.generation_id = $generation_id AND n.kind = 'entity' "
            "AND any(name IN n.names WHERE name IN $terms) "
            "RETURN n.object_id AS object_id, n.names AS names",
            scope_key=scope.key,
            generation_id=generation_id,
            terms=folded,
        )
        wanted = set(folded)
        scored = sorted(
            ((str(row["object_id"]), sorted(wanted.intersection(row["names"]))) for row in rows),
            key=lambda pair: (-len(pair[1]), pair[0]),
        )[:limit]
        return [
            RetrievalHit(
                object_ref=ObjectRef(
                    scope_key=scope.key, object_id=object_id, generation_id=generation_id
                ),
                method=ENTITY_METHOD,
                backend_id=self._info.implementation,
                raw_score=float(len(matched)),
                score_semantics=_SEMANTICS,
                rank=rank,
                matched=tuple(MatchDetail(kind="entity_name", term=term) for term in matched),
            )
            for rank, (object_id, matched) in enumerate(scored, start=1)
        ]

    async def neighborhood(
        self,
        scope: Scope,
        generation_id: str,
        seeds: Sequence[str],
        depth: int,
        limit: int,
    ) -> GraphSubgraph:
        """Entities and relationships within ``depth`` entity hops of ``seeds`` (shortest)."""
        if not 1 <= depth <= _MAX_DEPTH:
            raise ConfigurationError(f"depth must be within [1, {_MAX_DEPTH}]")
        seed_ids = [_node_id(scope.key, generation_id, seed) for seed in seeds]
        if not seed_ids or limit < 1:
            return GraphSubgraph()
        label = self._label
        # The hop bound must be a literal; it is a validated int, never user text.
        rows = await self._query(
            f"MATCH p = SHORTEST 1 (s:{label})-[:RL_LINK]-{{1,{2 * depth}}}(n:{label}) "
            "WHERE s.id IN $seeds AND n.scope_key = $scope_key "
            "AND n.generation_id = $generation_id AND NOT n.id IN $seeds "
            "RETURN n.object_id AS object_id, n.kind AS kind, length(p) AS length, "
            "[x IN nodes(p) | x.object_id] AS path",
            seeds=seed_ids,
            scope_key=scope.key,
            generation_id=generation_id,
        )
        best: dict[str, tuple[int, str, tuple[str, ...]]] = {}
        for row in rows:
            object_id = str(row["object_id"])
            candidate = (int(row["length"]), str(row["kind"]), tuple(map(str, row["path"])))
            current = best.get(object_id)
            if current is None or (candidate[0], candidate[2]) < (current[0], current[2]):
                best[object_id] = candidate
        entities: list[GraphNode] = []
        relationships: list[GraphNode] = []
        for object_id, (length, kind, path) in sorted(
            best.items(), key=lambda pair: (pair[1][0], pair[0])
        ):
            node = GraphNode(object_id=object_id, distance=(length + 1) // 2, path=path)
            (entities if kind == "entity" else relationships).append(node)
        return GraphSubgraph(
            nodes=tuple(entities[:limit]), relationships=tuple(relationships[:limit])
        )
