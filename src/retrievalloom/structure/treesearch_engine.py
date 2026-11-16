"""Document-structure search on TreeSearch FTS5 indexes (in-memory or on-disk)."""

from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    IndexCapabilities,
    NavigationDirection,
    StructureEngineABC,
    WriteReport,
)
from retrievalloom.domain.filters import evaluate
from retrievalloom.domain.records import Channel, ObjectKind
from retrievalloom.domain.reports import HealthReport
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
    ComponentContractError,
    GenerationError,
    LifecycleError,
)
from retrievalloom.runtime.offloader import WorkloadKind

try:
    from treesearch import Document, FTS5Index
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "TreeSearchStructureEngine needs the 'structure' extra: retrievalloom[structure]"
    ) from exc

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope
    from retrievalloom.runtime.offloader import Offloader

STRUCTURE_METHOD = MethodRef(channel=Channel.STRUCTURE, method="structure.fts")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.UNBOUNDED)
_META_KEY = "retrievalloom_meta"
_OVERFETCH = 4


@dataclass(slots=True)
class _Generation:
    index: Any
    documents: dict[str, Any] = field(default_factory=dict)
    node_document: dict[str, str] = field(default_factory=dict)


def _tree(items: Sequence[IndexItem]) -> list[dict[str, Any]]:
    """Build a TreeSearch structure (nested ``nodes``) from parent links and ordinals."""
    nodes: dict[str, dict[str, Any]] = {}
    order: dict[str, tuple[int, str]] = {}
    for item in items:
        nodes[item.object_id] = {
            "node_id": item.object_id,
            "title": str(item.payload.get("title") or ""),
            "text": item.text,
            "nodes": [],
        }
        order[item.object_id] = (int(item.payload.get("ordinal", 0)), item.object_id)
    roots: list[str] = []
    for item in items:
        parent = item.payload.get("parent_object_id")
        if parent is not None and parent in nodes and parent != item.object_id:
            nodes[parent]["nodes"].append(item.object_id)
        else:
            roots.append(item.object_id)

    reached: set[str] = set()

    def resolve(object_id: str) -> dict[str, Any]:
        reached.add(object_id)
        node = nodes[object_id]
        children = sorted(node["nodes"], key=order.__getitem__)
        return {**node, "nodes": [resolve(child) for child in children]}

    tree = [resolve(root) for root in sorted(roots, key=order.__getitem__)]
    # Every node has one parent, so nodes not reachable from a root sit on a parent cycle.
    stranded = sorted(set(nodes) - reached)
    if stranded:
        raise ComponentContractError("structure nodes form a cycle", details={"nodes": stranded})
    return tree


def _flatten(structure: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat: list[dict[str, Any]] = []
    stack = list(reversed(structure))
    while stack:
        node = stack.pop()
        flat.append(node)
        stack.extend(reversed(node.get("nodes", [])))
    return flat


class TreeSearchStructureEngine(StructureEngineABC):
    """Structure search over application-provided trees with TreeSearch's FTS5 index.

    Each ``(scope, generation)`` gets its own FTS5 database (physical isolation). Nodes of
    one document version form one TreeSearch ``Document``; filter metadata is stored in the
    document and applied after ranking. Every SQLite call runs on one dedicated thread.

    Args:
        directory: Root directory of on-disk indexes; ``None`` keeps them in memory.
    """

    def __init__(self, directory: str | Path | None = None) -> None:
        self._directory = Path(directory) if directory is not None else None
        self._generations: dict[tuple[str, str], _Generation] = {}
        self._executor: ThreadPoolExecutor | None = None
        self._offloader: Offloader | None = None
        self._info = make_info(
            component="structure_engine",
            implementation="treesearch",
            version="1",
            config={"durable": self._directory is not None},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place; durable when file-backed; filters applied after ranking."""
        return IndexCapabilities(
            mutable_in_place=True, durable=self._directory is not None, filter_pushdown=False
        )

    async def initialize(self, context: ComponentContext) -> None:
        """Bind the offloader and start the SQLite thread (idempotent)."""
        if self._executor is not None:
            return
        if self._directory is not None:
            self._directory.mkdir(parents=True, exist_ok=True)
        self._offloader = context.offloader
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="retrievalloom-structure"
        )

    async def close(self) -> None:
        """Close every index (idempotent; works after the engine's offloader stopped)."""
        if self._executor is None:
            return
        generations = list(self._generations.values())
        self._generations.clear()
        # Closing only releases resources, so it bypasses offloader admission and runs
        # on this component's own SQLite thread directly.
        await asyncio.get_running_loop().run_in_executor(
            self._executor, lambda: [generation.index.close() for generation in generations]
        )
        self._executor.shutdown(wait=True)
        self._executor = None

    async def health(self) -> HealthReport:
        """Healthy when initialised."""
        if self._executor is None:
            return HealthReport(component=self.info.name, message="not initialised")
        return HealthReport(component=self.info.name)

    async def _run[T](self, tenant: str | None, fn: Callable[[], T]) -> T:
        if self._offloader is None or self._executor is None:
            raise LifecycleError("structure engine is not initialised")

        def call() -> T:
            try:
                return fn()
            except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
                raise BackendError("treesearch call failed", details={"error": str(exc)}) from exc

        return await self._offloader.run(
            WorkloadKind.STRUCTURE_INDEX, call, tenant=tenant, executor=self._executor
        )

    def _path(self, scope_key: str, generation_id: str) -> Path | None:
        if self._directory is None:
            return None
        return self._directory / scope_key / f"{generation_id}.db"

    def _open_sync(self, scope_key: str, generation_id: str, create: bool) -> _Generation | None:
        key = (scope_key, generation_id)
        found = self._generations.get(key)
        if found is not None:
            return found
        path = self._path(scope_key, generation_id)
        if path is None:
            if not create:
                return None
            generation = _Generation(index=FTS5Index(None))
        else:
            if not path.exists() and not create:
                return None
            path.parent.mkdir(parents=True, exist_ok=True)
            generation = _Generation(index=FTS5Index(str(path)))
            for document in generation.index.load_all_documents():
                generation.documents[document.doc_id] = document
                for node in _flatten(document.structure):
                    generation.node_document[node["node_id"]] = document.doc_id
        self._generations[key] = generation
        return generation

    async def _generation(self, scope: Scope, generation_id: str) -> _Generation:
        found = await self._run(
            scope.tenant_id, lambda: self._open_sync(scope.key, generation_id, create=False)
        )
        if found is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        return found

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Create (or open) the generation's FTS5 index."""
        await self._run(
            scope.tenant_id, lambda: self._open_sync(scope.key, spec.generation_id, create=True)
        )

    @staticmethod
    def _save(generation: _Generation, document_id: str, structure: list, meta: dict) -> None:
        old = generation.documents.get(document_id)
        if old is not None:
            for node in _flatten(old.structure):
                generation.node_document.pop(node["node_id"], None)
        if not structure:
            generation.documents.pop(document_id, None)
            generation.index.delete_document(document_id)
            return
        document = Document(
            doc_id=document_id,
            doc_name=structure[0]["title"] or document_id,
            structure=structure,
            metadata={_META_KEY: meta},
        )
        generation.index.index_document(document, force=True)
        generation.documents[document_id] = document
        for node in _flatten(structure):
            generation.node_document[node["node_id"]] = document_id

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Index the nodes of whole document versions (one TreeSearch document each)."""
        groups: dict[str, list[IndexItem]] = {}
        for item in items:
            if item.kind != ObjectKind.STRUCTURE_NODE:
                raise ComponentContractError(
                    "structure items must be structure nodes",
                    details={"object_id": item.object_id, "kind": item.kind},
                )
            document_id = (
                f"{item.document_uid}:{item.version}"
                if item.document_uid is not None
                else item.object_id
            )
            groups.setdefault(document_id, []).append(item)
        generation = await self._generation(scope, generation_id)
        trees = {document_id: _tree(group) for document_id, group in groups.items()}

        def write() -> int:
            for document_id, group in groups.items():
                meta = {item.object_id: dict(item.metadata) for item in group}
                self._save(generation, document_id, trees[document_id], meta)
            return len(items)

        return WriteReport(written=await self._run(scope.tenant_id, write))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Remove nodes; children of a removed node move to its parent."""
        try:
            generation = await self._generation(scope, generation_id)
        except GenerationError:
            return 0

        def remove() -> int:
            removed: dict[str, set[str]] = {}
            for object_id in object_ids:
                document_id = generation.node_document.get(object_id)
                if document_id is not None:
                    removed.setdefault(document_id, set()).add(object_id)
            for document_id, gone in removed.items():
                document = generation.documents[document_id]
                meta = dict(document.metadata.get(_META_KEY, {}))
                structure = _prune(document.structure, gone)
                for object_id in gone:
                    meta.pop(object_id, None)
                self._save(generation, document_id, structure, meta)
            return sum(len(gone) for gone in removed.values())

        return await self._run(scope.tenant_id, remove)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace stored filter metadata of present nodes."""
        generation = await self._generation(scope, generation_id)

        def update() -> int:
            touched: dict[str, dict[str, Metadata]] = {}
            for object_id, metadata in updates.items():
                document_id = generation.node_document.get(object_id)
                if document_id is not None:
                    touched.setdefault(document_id, {})[object_id] = dict(metadata)
            for document_id, changes in touched.items():
                document = generation.documents[document_id]
                document.metadata[_META_KEY] = {**document.metadata.get(_META_KEY, {}), **changes}
                generation.index.save_document(document)
            return sum(len(changes) for changes in touched.values())

        return await self._run(scope.tenant_id, update)

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Report node and document counts (no build step)."""
        generation = await self._generation(scope, generation_id)
        return GenerationStats(
            object_count=len(generation.node_document),
            details={"documents": len(generation.documents)},
        )

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Close and delete the generation's index (idempotent)."""
        path = self._path(scope.key, generation_id)

        def drop() -> None:
            generation = self._generations.pop((scope.key, generation_id), None)
            if generation is not None:
                generation.index.close()
            if path is not None:
                for suffix in ("", "-wal", "-shm", "-journal"):
                    Path(f"{path}{suffix}").unlink(missing_ok=True)

        await self._run(scope.tenant_id, drop)

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Generations held for ``scope`` (open ones and files on disk)."""
        found = {gid for key, gid in self._generations if key == scope.key}
        if self._directory is not None:
            folder = self._directory / scope.key
            if folder.is_dir():
                found.update(path.stem for path in folder.glob("*.db"))
        return sorted(found)

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        query: str,
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """FTS5 BM25 ranking over node titles and texts."""
        generation = await self._generation(scope, generation_id)
        if limit < 1 or not query.strip():
            return []
        fetch = limit if filters is None else limit * _OVERFETCH
        rows = await self._run(scope.tenant_id, lambda: generation.index.search(query, top_k=fetch))
        hits: list[RetrievalHit] = []
        for row in rows:
            object_id = str(row["node_id"])
            document_id = generation.node_document.get(object_id)
            if document_id is None:
                continue
            meta = generation.documents[document_id].metadata.get(_META_KEY, {})
            if not evaluate(filters, meta.get(object_id, {})):
                continue
            hits.append(
                RetrievalHit(
                    object_ref=ObjectRef(
                        scope_key=scope.key, object_id=object_id, generation_id=generation_id
                    ),
                    method=STRUCTURE_METHOD,
                    backend_id=self._info.implementation,
                    raw_score=float(row.get("fts_score", 0.0)),
                    score_semantics=_SEMANTICS,
                    rank=len(hits) + 1,
                    matched=(
                        MatchDetail(
                            kind="structure_node",
                            matched=str(row.get("title") or ""),
                            details={"depth": int(row.get("depth", 0))},
                        ),
                    ),
                )
            )
            if len(hits) == limit:
                break
        return hits

    async def navigate(
        self,
        scope: Scope,
        generation_id: str,
        object_id: str,
        direction: NavigationDirection,
    ) -> Sequence[str]:
        """Related node ids within the node's document tree."""
        generation = await self._generation(scope, generation_id)
        document_id = generation.node_document.get(object_id)
        if document_id is None:
            return []
        document = generation.documents[document_id]
        if direction is NavigationDirection.PARENT:
            parent = document.get_parent_id(object_id)
            return [parent] if parent else []
        if direction is NavigationDirection.CHILDREN:
            return list(document.get_children_ids(object_id))
        if direction is NavigationDirection.SIBLINGS:
            return list(document.get_sibling_ids(object_id))
        return list(document.get_path_to_root(object_id))


def _prune(structure: list[dict[str, Any]], gone: set[str]) -> list[dict[str, Any]]:
    """Remove ``gone`` nodes, lifting their children into the removed node's place."""
    kept: list[dict[str, Any]] = []
    for node in structure:
        children = _prune(node.get("nodes", []), gone)
        if node["node_id"] in gone:
            kept.extend(children)
        else:
            kept.append({**node, "nodes": children})
    return kept
