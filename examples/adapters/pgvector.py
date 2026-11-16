# ruff: noqa: S608 - identifiers are validated at construction; values are bound parameters.
"""Dense vector store on PostgreSQL + pgvector (asyncpg).

One table per dimensionality (``{table_prefix}_items_{dims}``) holds every scope; rows are
keyed by ``(scope_key, generation_id, object_id)`` and carry the filter metadata as
``jsonb``. A ``{table_prefix}_generations`` table records generations (so empty and
restarted generations are known), and an HNSW index (``vector_cosine_ops``) serves the
nearest-neighbour search with pgvector's iterative scan, so ``WHERE`` filters on scope,
generation and metadata never starve the result list.

asyncpg is natively async (non-blocking network I/O), so no offloading is needed.
"""

from __future__ import annotations

import json
import math
import re
from typing import TYPE_CHECKING, Any

import asyncpg

from retrievalloom.contracts.component import ComponentInfo, make_info
from retrievalloom.contracts.indexes import (
    GenerationStats,
    IndexCapabilities,
    VectorStoreABC,
    WriteReport,
)
from retrievalloom.domain.filters import And, Eq, In, Not, Or, evaluate
from retrievalloom.domain.records import Channel
from retrievalloom.domain.reports import HealthReport, HealthStatus
from retrievalloom.domain.retrieval import (
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
    GenerationError,
    LifecycleError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from retrievalloom.contracts.component import ComponentContext
    from retrievalloom.contracts.indexes import GenerationSpec, IndexItem
    from retrievalloom.domain.base import Metadata
    from retrievalloom.domain.filters import FilterExpression
    from retrievalloom.domain.inputs import Scope

_METHOD = MethodRef(channel=Channel.INDEX, method="dense")
_SEMANTICS = ScoreSemantics(higher_is_better=True, range=ScoreRange.SIMILARITY)
_PREFIX = re.compile(r"^[a-z][a-z0-9_]{0,40}$")


def _vector_literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


class _SQLFilter:
    """Translate a filter AST into a SQL predicate over ``meta`` (jsonb)."""

    def __init__(self, first_parameter: int) -> None:
        self.parameters: list[Any] = []
        self._next = first_parameter

    def _param(self, value: Any) -> str:
        self.parameters.append(value)
        placeholder = f"${self._next}"
        self._next += 1
        return placeholder

    def _equals(self, field: str, value: Any) -> str | None:
        if isinstance(value, list | dict) or value is None:
            return None
        path = self._param(field.split("."))
        literal = self._param(json.dumps(value))
        found = f"meta #> {path}::text[]"
        return (
            f"COALESCE(({found} = {literal}::jsonb) OR (jsonb_typeof({found}) = 'array' "
            f"AND {found} @> jsonb_build_array({literal}::jsonb)), false)"
        )

    def translate(self, expression: FilterExpression | None) -> tuple[str | None, bool]:
        """Return ``(predicate, exact)``; ``None`` means "no constraint" (broader)."""
        if expression is None:
            return None, True
        if isinstance(expression, And | Or):
            parts = [self.translate(clause) for clause in expression.clauses]
            exact = all(part_exact for _, part_exact in parts)
            sql = [part for part, _ in parts if part is not None]
            if isinstance(expression, And):
                return (f"({' AND '.join(sql)})" if sql else None), exact
            if len(sql) < len(parts):
                return None, False
            return f"({' OR '.join(sql)})", exact
        if isinstance(expression, Not):
            inner, inner_exact = self.translate(expression.clause)
            if inner is None or not inner_exact:
                return None, False
            return f"(NOT {inner})", True
        if isinstance(expression, Eq):
            sql = self._equals(expression.field, expression.value)
            return sql, sql is not None
        if isinstance(expression, In):
            options = [self._equals(expression.field, value) for value in expression.values]
            if not options or any(option is None for option in options):
                return None, False
            return f"({' OR '.join(str(option) for option in options)})", True
        return None, False


class PgVectorStore(VectorStoreABC):
    """Generation-aware dense vector store on pgvector.

    Args:
        dsn: PostgreSQL DSN, e.g. ``postgresql://user:pass@host:5432/db``.
        table_prefix: Table name prefix (lowercase identifier).
        pool: Application-owned ``asyncpg.Pool`` (never closed by the store).
        min_pool_size: Pool minimum size (owned pool only).
        max_pool_size: Pool maximum size (owned pool only).
        overfetch_factor: Candidate multiplier when a filter is only partly pushed down.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        table_prefix: str = "retrievalloom",
        pool: asyncpg.Pool | None = None,
        min_pool_size: int = 1,
        max_pool_size: int = 8,
        overfetch_factor: int = 4,
    ) -> None:
        if (dsn is None) == (pool is None):
            raise ConfigurationError("pass exactly one of dsn or pool")
        if not _PREFIX.match(table_prefix):
            raise ConfigurationError("table_prefix must be a lowercase identifier")
        if overfetch_factor < 1:
            raise ConfigurationError("overfetch_factor must be at least 1")
        self._dsn = dsn
        self._prefix = table_prefix
        self._pool: asyncpg.Pool | None = pool
        self._owns_pool = pool is None
        self._pool_sizes = (min_pool_size, max_pool_size)
        self._overfetch = overfetch_factor
        self._schema_ready = False
        self._tables: set[int] = set()
        self._info = make_info(
            component="vector_store",
            implementation="pgvector",
            version="1",
            config={"prefix": table_prefix, "distance": "cosine"},
        )

    @property
    def info(self) -> ComponentInfo:
        """Component identity."""
        return self._info

    @property
    def capabilities(self) -> IndexCapabilities:
        """Mutable in place, durable, with (partial) filter pushdown."""
        return IndexCapabilities(mutable_in_place=True, durable=True, filter_pushdown=True)

    @property
    def generations_table(self) -> str:
        """Name of the generation registry table."""
        return f"{self._prefix}_generations"

    def _items(self, dimensions: int) -> str:
        return f"{self._prefix}_items_{int(dimensions)}"

    @staticmethod
    async def _session(connection: asyncpg.Connection) -> None:
        # Keep filtering HNSW scans going until LIMIT rows match (pgvector >= 0.8).
        await connection.execute("SET hnsw.iterative_scan = strict_order")

    async def initialize(self, context: ComponentContext) -> None:
        """Create the pool, the ``vector`` extension and the registry table (idempotent)."""
        del context
        if self._pool is None:
            assert self._dsn is not None
            try:
                self._pool = await asyncpg.create_pool(
                    self._dsn,
                    min_size=self._pool_sizes[0],
                    max_size=self._pool_sizes[1],
                    setup=self._session,
                )
            except (OSError, asyncpg.PostgresError) as exc:
                raise BackendUnavailableError("postgres is unreachable") from exc
        if not self._schema_ready:
            await self._execute("CREATE EXTENSION IF NOT EXISTS vector")
            await self._execute(
                f"CREATE TABLE IF NOT EXISTS {self.generations_table} ("
                "scope_key text NOT NULL, generation_id text NOT NULL, "
                "dimensions integer NOT NULL, PRIMARY KEY (scope_key, generation_id))"
            )
            self._schema_ready = True

    async def close(self) -> None:
        """Close an owned pool (idempotent)."""
        pool, self._pool = self._pool, None
        self._schema_ready = False
        self._tables.clear()
        if pool is not None and self._owns_pool:
            await pool.close()

    async def drop_tables(self) -> None:
        """Drop every table of this prefix (test and demo cleanup)."""
        rows = await self._fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() "
            "AND tablename LIKE $1",
            f"{self._prefix}\\_%",
        )
        for row in rows:
            name = str(row["tablename"])
            if name == self.generations_table or name.startswith(f"{self._prefix}_items_"):
                await self._execute(f'DROP TABLE IF EXISTS "{name}"')
        self._tables.clear()

    async def health(self) -> HealthReport:
        """Healthy when PostgreSQL answers ``SELECT 1``."""
        try:
            await self._fetch("SELECT 1")
        except (BackendError, LifecycleError) as exc:
            return HealthReport(
                component=self.info.name, status=HealthStatus.UNHEALTHY, message=str(exc)
            )
        return HealthReport(component=self.info.name)

    def _require(self) -> asyncpg.Pool:
        if self._pool is None:
            raise LifecycleError("pgvector store is not initialised")
        return self._pool

    async def _execute(self, query: str, *args: Any) -> str:
        try:
            return await self._require().execute(query, *args)
        except (OSError, asyncpg.InterfaceError) as exc:
            raise BackendUnavailableError("postgres is unreachable") from exc
        except asyncpg.PostgresError as exc:
            raise BackendError("postgres query failed", details={"error": str(exc)}) from exc

    async def _fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        try:
            return await self._require().fetch(query, *args)
        except (OSError, asyncpg.InterfaceError) as exc:
            raise BackendUnavailableError("postgres is unreachable") from exc
        except asyncpg.PostgresError as exc:
            raise BackendError("postgres query failed", details={"error": str(exc)}) from exc

    async def _ensure_table(self, dimensions: int) -> str:
        name = self._items(dimensions)
        if dimensions in self._tables:
            return name
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {name} ("
            "scope_key text NOT NULL, generation_id text NOT NULL, object_id text NOT NULL, "
            f"meta jsonb NOT NULL DEFAULT '{{}}', embedding vector({int(dimensions)}) NOT NULL, "
            "PRIMARY KEY (scope_key, generation_id, object_id))"
        )
        await self._execute(
            f"CREATE INDEX IF NOT EXISTS {name}_hnsw ON {name} "
            "USING hnsw (embedding vector_cosine_ops)"
        )
        self._tables.add(dimensions)
        return name

    async def _dimensions(self, scope: Scope, generation_id: str) -> int | None:
        rows = await self._fetch(
            f"SELECT dimensions FROM {self.generations_table} "
            "WHERE scope_key = $1 AND generation_id = $2",
            scope.key,
            generation_id,
        )
        return int(rows[0]["dimensions"]) if rows else None

    async def _register(self, scope: Scope, generation_id: str, dimensions: int) -> str:
        name = await self._ensure_table(dimensions)
        await self._execute(
            f"INSERT INTO {self.generations_table} (scope_key, generation_id, dimensions) "
            "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            scope.key,
            generation_id,
            dimensions,
        )
        return name

    async def create_generation(self, scope: Scope, spec: GenerationSpec) -> None:
        """Register the generation and ensure its table exists (idempotent)."""
        if spec.dimensions is None:
            raise ComponentContractError(
                "vector generations need dimensions", details={"generation_id": spec.generation_id}
            )
        await self._register(scope, spec.generation_id, spec.dimensions)

    @staticmethod
    def _vector(item: IndexItem, dimensions: int) -> str:
        vector = item.vector
        if not vector:
            raise ComponentContractError(
                "dense items need a vector", details={"object_id": item.object_id}
            )
        if len(vector) != dimensions:
            raise ComponentContractError(
                "vector dimensions differ from the generation",
                details={"object_id": item.object_id, "expected": dimensions, "got": len(vector)},
            )
        if not all(math.isfinite(value) for value in vector):
            raise ComponentContractError(
                "vector has non-finite values", details={"object_id": item.object_id}
            )
        return _vector_literal(vector)

    async def write(
        self, scope: Scope, generation_id: str, items: Sequence[IndexItem]
    ) -> WriteReport:
        """Upsert rows (idempotent by object id)."""
        if not items:
            return WriteReport(written=0)
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is None:
            dimensions = len(items[0].vector or ())
            name = await self._register(scope, generation_id, dimensions)
        else:
            name = await self._ensure_table(dimensions)
        rows = [
            (
                scope.key,
                generation_id,
                item.object_id,
                json.dumps(item.metadata),
                self._vector(item, dimensions),
            )
            for item in items
        ]
        pool = self._require()
        try:
            async with pool.acquire() as connection, connection.transaction():
                await connection.executemany(
                    f"INSERT INTO {name} (scope_key, generation_id, object_id, meta, embedding) "
                    "VALUES ($1, $2, $3, $4::jsonb, $5::vector) "
                    "ON CONFLICT (scope_key, generation_id, object_id) "
                    "DO UPDATE SET meta = EXCLUDED.meta, embedding = EXCLUDED.embedding",
                    rows,
                )
        except (OSError, asyncpg.InterfaceError) as exc:
            raise BackendUnavailableError("postgres is unreachable") from exc
        except asyncpg.PostgresError as exc:
            raise BackendError("postgres write failed", details={"error": str(exc)}) from exc
        return WriteReport(written=len(rows))

    async def remove(self, scope: Scope, generation_id: str, object_ids: Sequence[str]) -> int:
        """Delete rows; returns how many existed."""
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is None or not object_ids:
            return 0
        rows = await self._fetch(
            f"DELETE FROM {await self._ensure_table(dimensions)} "
            "WHERE scope_key = $1 AND generation_id = $2 AND object_id = ANY($3::text[]) "
            "RETURNING object_id",
            scope.key,
            generation_id,
            list(object_ids),
        )
        return len(rows)

    async def update_metadata(
        self, scope: Scope, generation_id: str, updates: Mapping[str, Metadata]
    ) -> int:
        """Replace the metadata of present rows."""
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is None or not updates:
            return 0
        name = await self._ensure_table(dimensions)
        updated = 0
        for object_id, metadata in updates.items():
            rows = await self._fetch(
                f"UPDATE {name} SET meta = $4::jsonb "
                "WHERE scope_key = $1 AND generation_id = $2 AND object_id = $3 "
                "RETURNING object_id",
                scope.key,
                generation_id,
                object_id,
                json.dumps(metadata),
            )
            updated += len(rows)
        return updated

    async def seal(self, scope: Scope, generation_id: str) -> GenerationStats:
        """Count the generation's rows (the HNSW index is maintained on write)."""
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        rows = await self._fetch(
            f"SELECT count(*) AS n FROM {await self._ensure_table(dimensions)} "
            "WHERE scope_key = $1 AND generation_id = $2",
            scope.key,
            generation_id,
        )
        return GenerationStats(object_count=int(rows[0]["n"]))

    async def drop_generation(self, scope: Scope, generation_id: str) -> None:
        """Delete the generation's rows and registry entry (idempotent)."""
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is not None:
            await self._execute(
                f"DELETE FROM {await self._ensure_table(dimensions)} "
                "WHERE scope_key = $1 AND generation_id = $2",
                scope.key,
                generation_id,
            )
        await self._execute(
            f"DELETE FROM {self.generations_table} WHERE scope_key = $1 AND generation_id = $2",
            scope.key,
            generation_id,
        )

    async def list_generations(self, scope: Scope) -> Sequence[str]:
        """Registered generations of ``scope``."""
        rows = await self._fetch(
            f"SELECT generation_id FROM {self.generations_table} WHERE scope_key = $1",
            scope.key,
        )
        return sorted(str(row["generation_id"]) for row in rows)

    async def search(
        self,
        scope: Scope,
        generation_id: str,
        vector: Sequence[float],
        limit: int,
        filters: FilterExpression | None,
    ) -> Sequence[RetrievalHit]:
        """Cosine nearest neighbours within the scope and generation."""
        dimensions = await self._dimensions(scope, generation_id)
        if dimensions is None:
            raise GenerationError("generation not found", details={"generation_id": generation_id})
        if len(vector) != dimensions:
            raise ComponentContractError(
                "query vector dimensions differ from the generation",
                details={"expected": dimensions, "got": len(vector)},
            )
        if limit < 1:
            return []
        translator = _SQLFilter(first_parameter=5)
        predicate, exact = translator.translate(filters)
        where = "scope_key = $1 AND generation_id = $2"
        if predicate is not None:
            where += f" AND {predicate}"
        rows = await self._fetch(
            "SELECT object_id, meta, 1 - (embedding <=> $3::vector) AS similarity "
            f"FROM {await self._ensure_table(dimensions)} WHERE {where} "
            "ORDER BY embedding <=> $3::vector LIMIT $4",
            scope.key,
            generation_id,
            _vector_literal(vector),
            limit if exact else limit * self._overfetch,
            *translator.parameters,
        )
        hits: list[RetrievalHit] = []
        for row in rows:
            if not evaluate(filters, json.loads(row["meta"])):
                continue
            hits.append(
                RetrievalHit(
                    object_ref=ObjectRef(
                        scope_key=scope.key,
                        object_id=str(row["object_id"]),
                        generation_id=generation_id,
                    ),
                    method=_METHOD,
                    backend_id=self._info.implementation,
                    raw_score=float(row["similarity"]),
                    score_semantics=_SEMANTICS,
                    rank=len(hits) + 1,
                )
            )
            if len(hits) == limit:
                break
        return hits
