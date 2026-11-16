"""Property graph store on LadybugDB (embedded, in-memory or on-disk)."""

from __future__ import annotations

import asyncio
import contextlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
from retrievalloom.runtime.offloader import WorkloadKind

try:
    import ladybug
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError("LadybugGraphStore needs the 'graph' extra: retrievalloom[graph]") from exc

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

ENTITY_METHOD = MethodRef(channel=Channel.GRAPH, method="graph.entity")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_MAX_DEPTH = 5
# LadybugDB's message when another process holds the database file's lock.
_LOCK_FAILURE = "Could not set lock on file"
_SCHEMA = (
    "CREATE NODE TABLE IF NOT EXISTS RLNode("
    "id STRING, scope_key STRING, generation_id STRING, object_id STRING, kind STRING, "
    "entity_type STRING, name STRING, names STRING[], relation_type STRING, meta STRING, "
    "PRIMARY KEY(id))",
    "CREATE REL TABLE IF NOT EXISTS RLLink(FROM RLNode TO RLNode, role STRING)",
)


def _node_id(scope_key: str, generation_id: str, object_id: str) -> str:
    return f"{scope_key}:{generation_id}:{object_id}"


def _rows(connection: Any, query: str, parameters: dict[str, Any] | None = None) -> list[list]:
    result = connection.execute(query, parameters or {})
    rows: list[list] = []
    while result.has_next():
        rows.append(result.get_next())
    return rows


class LadybugGraphStore(GraphStoreABC):
    """Graph aggregates in LadybugDB.

    Entities and relationships are both nodes of ``RLNode`` (primary key
    ``scope_key:generation_id:object_id``); a relationship node links to its endpoint
    entities through ``RLLink`` edges, so an entity hop is two edge hops. Every query is
    parameterised with ``$scope_key`` and ``$generation_id``. All access goes through one
    connection on a dedicated single-thread executor (LadybugDB has a single writer).

    Args:
        path: Database file; ``None`` keeps the graph in memory (not durable).
        buffer_pool_size: LadybugDB buffer pool bytes (``0`` = library default).
    """

    def __init__(self, path: str | Path | None = None, *, buffer_pool_size: int = 0) -> None:
        if buffer_pool_size < 0:
            raise ConfigurationError("buffer_pool_size must be non-negative")
        self._path = Path(path) if path is not None else None
        self._buffer_pool_size = buffer_pool_size
        self._database: Any = None
        self._connection: Any = None
        self._executor: ThreadPoolExecutor | None = None
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="graph_store",
            implementation="ladybug",
            version="1",
            config={"durable": self._path is not None},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place; durable when file-backed."""
        return IndexCapabilities(
            mutable_in_place=True, durable=self._path is not None, filter_pushdown=False
        )

    def _open(self) -> None:
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            try:
                self._database = ladybug.Database(
                    str(self._path) if self._path is not None else None,
                    buffer_pool_size=self._buffer_pool_size,
                )
            except RuntimeError as exc:
                if _LOCK_FAILURE not in str(exc):
                    raise
                raise BackendUnavailableError(
                    "the LadybugDB file is locked by another process; one process may own "
                    "a LadybugDB database at a time (use a single owner process, or a "
                    "server-backed GraphStoreABC such as one on Neo4j, to share a graph)",
                    details={"path": str(self._path), "error": str(exc)},
                ) from exc
            self._connection = ladybug.Connection(self._database)
            for statement in _SCHEMA:
                self._connection.execute(statement)
        except BaseException:
            self._close_sync()
            raise

    async def initialize(self, context: ComponentContext) -> None:
        """Open the database and create the schema (idempotent).

        A failed or cancelled initialisation releases the database and the worker thread.
        """
        if self._connection is not None:
            return
        self._offloader = context.offloader
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="retrievalloom-graph")
        try:
            await self._run(None, lambda _connection: self._open(), require_open=False)
        except BaseException:
            await self.close()
            raise

    def _close_sync(self) -> None:
        if self._connection is not None:
            self._connection.close()
        if self._database is not None:
            self._database.close()
        self._connection = None
        self._database = None

    async def close(self) -> None:
        """Close the database (idempotent; works after the engine's offloader stopped)."""
        executor = self._executor
        if executor is None:
            return

        def call() -> None:
            try:
                self._close_sync()
            except RuntimeError as exc:
                raise BackendError("ladybug close failed", details={"error": str(exc)}) from exc

        try:
            # Queued on the single worker, so it also runs after an open still in flight.
            # Closing only releases resources, so it bypasses offloader admission.
            await asyncio.get_running_loop().run_in_executor(executor, call)
        finally:
            self._executor = None
            executor.shutdown(wait=False)

    async def health(self) -> HealthReport:
        """Healthy when a trivial query succeeds."""
        try:
            await self._run(None, lambda connection: _rows(connection, "RETURN 1"))
        except (BackendError, LifecycleError) as exc:
            return HealthReport(
                component=self.info.name, status=HealthStatus.UNHEALTHY, message=str(exc)
            )
        return HealthReport(component=self.info.name)

    async def _run[T](
        self, tenant: str | None, fn: Callable[[Any], T], *, require_open: bool = True
    ) -> T:
        if (
            self._offloader is None
            or self._executor is None
            or (require_open and self._connection is None)
        ):
            raise LifecycleError("graph store is not initialised")

        def call() -> T:
            try:
                return fn(self._connection)
            except RuntimeError as exc:
                raise BackendError("ladybug query failed", details={"error": str(exc)}) from exc

        return await self._offloader.run(
            WorkloadKind.GRAPH_QUERY, call, tenant=tenant, executor=self._executor
        )

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Generations are implicit (node properties); nothing to create."""
        del scope, spec

    def _write_sync(
        self, connection: Any, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> int:
        connection.execute("BEGIN TRANSACTION")
        try:
            for item in items:
                self._write_one(connection, scope, generation_id, item)
        except BaseException:
            # A failed statement has already rolled the transaction back.
            with contextlib.suppress(RuntimeError):
                connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return len(items)

    def _write_one(
        self, connection: Any, scope: Scope, generation_id: str, item: IndexItem
    ) -> None:
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
            connection.execute(
                "MERGE (n:RLNode {id: $id}) SET n.scope_key = $scope_key, "
                "n.generation_id = $generation_id, n.object_id = $object_id, "
                "n.kind = 'entity', n.entity_type = $entity_type, n.name = $name, "
                "n.names = $names, n.relation_type = '', n.meta = $meta",
                {
                    **base,
                    "entity_type": str(payload.get("entity_type", "entity")),
                    "name": name,
                    "names": names,
                },
            )
            return
        if item.kind != ObjectKind.GRAPH_RELATIONSHIP:
            raise ComponentContractError(
                "graph items must be graph entities or relationships",
                details={"object_id": item.object_id, "kind": item.kind},
            )
        ends = {
            "source": _node_id(scope.key, generation_id, str(payload.get("source_entity_id"))),
            "target": _node_id(scope.key, generation_id, str(payload.get("target_entity_id"))),
        }
        found = _rows(
            connection,
            "MATCH (n:RLNode) WHERE n.id IN $ids AND n.kind = 'entity' RETURN n.id",
            {"ids": list(ends.values())},
        )
        if {row[0] for row in found} != set(ends.values()):
            raise ComponentContractError(
                "relationship endpoints must be written first",
                details={"object_id": item.object_id},
            )
        connection.execute(
            "MERGE (n:RLNode {id: $id}) SET n.scope_key = $scope_key, "
            "n.generation_id = $generation_id, n.object_id = $object_id, "
            "n.kind = 'relationship', n.entity_type = '', n.name = $name, n.names = [], "
            "n.relation_type = $relation_type, n.meta = $meta",
            {
                **base,
                "name": item.text,
                "relation_type": str(payload.get("relation_type", "")),
            },
        )
        connection.execute("MATCH (n:RLNode {id: $id})-[l:RLLink]->() DELETE l", {"id": node_id})
        connection.execute("MATCH ()-[l:RLLink]->(n:RLNode {id: $id}) DELETE l", {"id": node_id})
        connection.execute(
            "MATCH (s:RLNode {id: $source}), (r:RLNode {id: $id}), (t:RLNode {id: $target}) "
            "CREATE (s)-[:RLLink {role: 'source'}]->(r), (r)-[:RLLink {role: 'target'}]->(t)",
            {"id": node_id, **ends},
        )

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Upsert entities and relationships atomically (endpoints first)."""
        if not items:
            return WriteReport(written=0)
        ordered = sorted(items, key=lambda item: item.kind != ObjectKind.GRAPH_ENTITY)
        written = await self._run(
            scope.tenant_id,
            lambda connection: self._write_sync(connection, scope, generation_id, ordered),
        )
        return WriteReport(written=written)

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Delete nodes and their links; returns how many existed."""
        ids = [_node_id(scope.key, generation_id, object_id) for object_id in object_ids]
        if not ids:
            return 0

        def remove(connection: Any) -> int:
            present = _rows(
                connection, "MATCH (n:RLNode) WHERE n.id IN $ids RETURN count(n)", {"ids": ids}
            )[0][0]
            connection.execute("MATCH (n:RLNode) WHERE n.id IN $ids DETACH DELETE n", {"ids": ids})
            return int(present)

        return await self._run(scope.tenant_id, remove)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace stored filter metadata."""

        def update(connection: Any) -> int:
            updated = 0
            for object_id, metadata in updates.items():
                rows = _rows(
                    connection,
                    "MATCH (n:RLNode {id: $id}) SET n.meta = $meta RETURN count(n)",
                    {
                        "id": _node_id(scope.key, generation_id, object_id),
                        "meta": json.dumps(metadata, sort_keys=True),
                    },
                )
                updated += int(rows[0][0]) if rows else 0
            return updated

        return await self._run(scope.tenant_id, update)

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Count the generation's nodes (no build step)."""
        rows = await self._run(
            scope.tenant_id,
            lambda connection: _rows(
                connection,
                "MATCH (n:RLNode) WHERE n.scope_key = $scope_key "
                "AND n.generation_id = $generation_id RETURN n.kind, count(n)",
                {"scope_key": scope.key, "generation_id": generation_id},
            ),
        )
        counts = {str(kind): int(count) for kind, count in rows}
        return GenerationStats(object_count=sum(counts.values()), details=counts)

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete every node of a generation (idempotent)."""
        await self._run(
            scope.tenant_id,
            lambda connection: connection.execute(
                "MATCH (n:RLNode) WHERE n.scope_key = $scope_key "
                "AND n.generation_id = $generation_id DETACH DELETE n",
                {"scope_key": scope.key, "generation_id": generation_id},
            ),
        )

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Distinct generations stored for ``scope``."""
        rows = await self._run(
            scope.tenant_id,
            lambda connection: _rows(
                connection,
                "MATCH (n:RLNode) WHERE n.scope_key = $scope_key RETURN DISTINCT n.generation_id",
                {"scope_key": scope.key},
            ),
        )
        return sorted(str(row[0]) for row in rows)

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
        rows = await self._run(
            scope.tenant_id,
            lambda connection: _rows(
                connection,
                "MATCH (n:RLNode) WHERE n.scope_key = $scope_key "
                "AND n.generation_id = $generation_id AND n.kind = 'entity' "
                "AND list_has_any(n.names, $terms) RETURN n.object_id, n.names",
                {"scope_key": scope.key, "generation_id": generation_id, "terms": folded},
            ),
        )
        wanted = set(folded)
        scored = sorted(
            ((str(object_id), sorted(wanted.intersection(names))) for object_id, names in rows),
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
        # The bound must be a literal; it is a validated int, never user text.
        query = (
            f"MATCH p = (s:RLNode)-[:RLLink* SHORTEST 1..{2 * depth}]-(n:RLNode) "
            "WHERE s.id IN $seeds AND n.scope_key = $scope_key "
            "AND n.generation_id = $generation_id AND NOT n.id IN $seeds "
            "RETURN n.object_id, n.kind, length(p), properties(nodes(p), 'object_id')"
        )
        rows = await self._run(
            scope.tenant_id,
            lambda connection: _rows(
                connection,
                query,
                {"seeds": seed_ids, "scope_key": scope.key, "generation_id": generation_id},
            ),
        )
        best: dict[str, tuple[int, str, tuple[str, ...]]] = {}
        for object_id, kind, length, path in rows:
            current = best.get(object_id)
            candidate = (int(length), str(kind), tuple(str(value) for value in path))
            if (
                current is None
                or candidate[0] < current[0]
                or (candidate[0] == current[0] and candidate[2] < current[2])
            ):
                best[str(object_id)] = candidate
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
