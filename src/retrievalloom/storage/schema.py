"""SQLAlchemy Core schema of the metadata database (``rl_`` prefix).

The Alembic revisions under ``retrievalloom/storage/migrations`` own the DDL; this
module mirrors the head revision for query construction and drift tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import TypeDecorator


class UTCDateTime(TypeDecorator[datetime]):
    """Aware UTC datetime.

    Stored as sortable ISO-8601 text on SQLite and as native ``timestamptz`` on
    PostgreSQL; naive datetimes are rejected on every dialect.
    """

    impl = sa.String(32)
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        """Native ``timestamptz`` on PostgreSQL, ISO text elsewhere."""
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.TIMESTAMP(timezone=True))
        return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value: datetime | None, dialect: Any) -> str | datetime | None:
        """Serialise to UTC (ISO text unless the dialect stores timestamps natively)."""
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetimes are not accepted")
        value = value.astimezone(UTC)
        if _native(dialect):
            return value
        return value.isoformat(timespec="microseconds")

    def process_result_value(self, value: str | datetime | None, dialect: Any) -> datetime | None:  # noqa: ARG002 - SQLAlchemy hook signature
        """Parse into an aware UTC datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        return datetime.fromisoformat(value)


def _native(dialect: Any) -> bool:
    return dialect is not None and dialect.name == "postgresql"


metadata = sa.MetaData()

scopes = sa.Table(
    "rl_scopes",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("tenant_id", sa.String(256), nullable=False),
    sa.Column("namespace", sa.String(256), nullable=False),
    sa.Column("collection", sa.String(256), nullable=False),
)

documents = sa.Table(
    "rl_documents",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("document_id", sa.String(256), primary_key=True),
    sa.Column("document_uid", sa.String(64), nullable=False, unique=True),
    sa.Column("counter", sa.Integer, nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("lifecycle_revision", sa.Integer, nullable=False),
    sa.Column("metadata_revision", sa.Integer, nullable=False),
    sa.Column("content_hash", sa.String(80), nullable=False),
    sa.Column("hash_profile", sa.String(32), nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("created_at", UTCDateTime, nullable=False),
    sa.Column("updated_at", UTCDateTime, nullable=False),
    sa.Column("deleted_at", UTCDateTime),
    sa.Column("change_seq", sa.Integer, nullable=False),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("dataset", sa.String(256)),
    sa.Column("source", sa.JSON),
    sa.Column("representation_digests", sa.JSON, nullable=False),
    sa.Column("representation_kinds", sa.JSON, nullable=False),
    sa.Column("row_revision", sa.Integer, nullable=False),
)
sa.Index("ix_rl_documents_scope_state", documents.c.scope_key, documents.c.state)
sa.Index("ix_rl_documents_scope_change", documents.c.scope_key, documents.c.change_seq)

document_versions = sa.Table(
    "rl_document_versions",
    metadata,
    sa.Column("document_uid", sa.String(64), primary_key=True),
    sa.Column("version", sa.Integer, primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("content_hash", sa.String(80), nullable=False),
    sa.Column("hash_profile", sa.String(32), nullable=False),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("dataset", sa.String(256)),
    sa.Column("source", sa.JSON),
    sa.Column("representation_digests", sa.JSON, nullable=False),
    sa.Column("event_id", sa.String(128), nullable=False),
    sa.Column("created_at", UTCDateTime, nullable=False),
)
sa.Index(
    "ix_rl_versions_scope_document",
    document_versions.c.scope_key,
    document_versions.c.document_uid,
)

representations = sa.Table(
    "rl_representations",
    metadata,
    sa.Column("document_uid", sa.String(64), primary_key=True),
    sa.Column("version", sa.Integer, primary_key=True),
    sa.Column("representation_id", sa.String(256), primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("kind", sa.String(16), nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("transformation", sa.JSON),
    sa.Column("hash_profile", sa.String(32), nullable=False),
)

ingestion_events = sa.Table(
    "rl_ingestion_events",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("event_id", sa.String(128), primary_key=True),
    sa.Column("document_id", sa.String(256), nullable=False),
    sa.Column("document_uid", sa.String(64), nullable=False),
    sa.Column("status", sa.String(16), nullable=False),
    sa.Column("outcome", sa.String(32)),
    sa.Column("counter_after", sa.Integer),
    sa.Column("version_after", sa.Integer),
    sa.Column("content_hash", sa.String(80), nullable=False),
    sa.Column("appearance_fingerprint", sa.String(80), nullable=False),
    sa.Column("possible_redelivery_of", sa.String(128)),
    sa.Column("delivery", sa.JSON),
    sa.Column("staged_payload", sa.JSON),
    sa.Column("error", sa.JSON),
    sa.Column("received_at", UTCDateTime, nullable=False),
)
sa.Index(
    "ix_rl_events_fingerprint",
    ingestion_events.c.scope_key,
    ingestion_events.c.appearance_fingerprint,
)
sa.Index("ix_rl_events_status", ingestion_events.c.scope_key, ingestion_events.c.status)
sa.Index(
    "ix_rl_events_document",
    ingestion_events.c.scope_key,
    ingestion_events.c.document_uid,
)

objects = sa.Table(
    "rl_objects",
    metadata,
    sa.Column("object_id", sa.String(64), primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("kind", sa.String(32), nullable=False),
    sa.Column("document_uid", sa.String(64)),
    sa.Column("version", sa.Integer),
    sa.Column("representation_id", sa.String(256)),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("text", sa.Text),
    sa.Column("content_hash", sa.String(80), nullable=False),
    sa.Column("metadata", sa.JSON, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.Column("sources", sa.JSON, nullable=False),
    sa.Column("transformation", sa.JSON),
)
sa.Index(
    "ix_rl_objects_document",
    objects.c.scope_key,
    objects.c.document_uid,
    objects.c.version,
)
sa.Index("ix_rl_objects_scope_kind", objects.c.scope_key, objects.c.kind, objects.c.state)

object_contributions = sa.Table(
    "rl_object_contributions",
    metadata,
    sa.Column("object_id", sa.String(64), primary_key=True),
    sa.Column("document_uid", sa.String(64), primary_key=True),
    sa.Column("version", sa.Integer, primary_key=True),
    sa.Column("field_path", sa.String(256), primary_key=True),
    sa.Column("ordinal", sa.Integer, primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("value", sa.JSON),
)
sa.Index(
    "ix_rl_contributions_document",
    object_contributions.c.scope_key,
    object_contributions.c.document_uid,
)

lineage = sa.Table(
    "rl_lineage",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("from_id", sa.String(64), primary_key=True),
    sa.Column("to_id", sa.String(64), primary_key=True),
    sa.Column("relation", sa.String(32), primary_key=True),
)
sa.Index("ix_rl_lineage_to", lineage.c.scope_key, lineage.c.to_id)

visibility = sa.Table(
    "rl_visibility",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("document_uid", sa.String(64), primary_key=True),
    sa.Column("version", sa.Integer, primary_key=True),
    sa.Column("channel", sa.String(16), primary_key=True),
    sa.Column("method", sa.String(64), primary_key=True),
    sa.Column("instance", sa.String(64), primary_key=True),
    sa.Column("visible_from_seq", sa.Integer, primary_key=True),
    sa.Column("visible_until_seq", sa.Integer),
)

commit_seq = sa.Table(
    "rl_commit_seq",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("seq", sa.Integer, nullable=False),
)

generations = sa.Table(
    "rl_generations",
    metadata,
    sa.Column("generation_id", sa.String(64), primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("channel", sa.String(16), nullable=False),
    sa.Column("method", sa.String(64), nullable=False),
    sa.Column("backend_id", sa.String(128), nullable=False),
    sa.Column("backend_instance_id", sa.String(64)),
    sa.Column("dependency_fingerprint", sa.String(80), nullable=False),
    sa.Column("build_watermark_seq", sa.Integer, nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("owner_id", sa.String(64)),
    sa.Column("lease_epoch", sa.Integer, nullable=False),
    sa.Column("heartbeat_at", UTCDateTime),
    sa.Column("created_at", UTCDateTime, nullable=False),
    sa.Column("published_at", UTCDateTime),
    sa.Column("retired_at", UTCDateTime),
    sa.Column("predecessor_id", sa.String(64)),
    sa.Column("stats", sa.JSON, nullable=False),
    sa.Column("row_revision", sa.Integer, nullable=False),
)
sa.Index(
    "ix_rl_generations_unit",
    generations.c.scope_key,
    generations.c.channel,
    generations.c.method,
    generations.c.state,
)

generation_pointers = sa.Table(
    "rl_generation_pointers",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("channel", sa.String(16), primary_key=True),
    sa.Column("method", sa.String(64), primary_key=True),
    sa.Column("instance", sa.String(64), primary_key=True),
    sa.Column("active_generation_id", sa.String(64), nullable=False),
    sa.Column("row_revision", sa.Integer, nullable=False),
)

reader_leases = sa.Table(
    "rl_reader_leases",
    metadata,
    sa.Column("generation_id", sa.String(64), primary_key=True),
    sa.Column("engine_id", sa.String(64), primary_key=True),
    sa.Column("expires_at", UTCDateTime, nullable=False),
    sa.Column("heartbeat_at", UTCDateTime, nullable=False),
)

coverage = sa.Table(
    "rl_coverage",
    metadata,
    sa.Column("scope_key", sa.String(64), primary_key=True),
    sa.Column("document_uid", sa.String(64), primary_key=True),
    sa.Column("channel", sa.String(16), primary_key=True),
    sa.Column("method", sa.String(64), primary_key=True),
    sa.Column("instance", sa.String(64), primary_key=True),
    sa.Column("generation_id", sa.String(64)),
    sa.Column("target_version", sa.Integer, nullable=False),
    sa.Column("indexed_version", sa.Integer),
    sa.Column("indexed_metadata_revision", sa.Integer),
    sa.Column("dependency_fingerprint", sa.String(80)),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("attempts", sa.Integer, nullable=False),
    sa.Column("last_error", sa.JSON),
    sa.Column("lease_owner", sa.String(64)),
    sa.Column("lease_epoch", sa.Integer, nullable=False),
    sa.Column("lease_expires_at", UTCDateTime),
    sa.Column("updated_at", UTCDateTime, nullable=False),
    sa.Column("row_revision", sa.Integer, nullable=False),
)
sa.Index(
    "ix_rl_coverage_unit_state",
    coverage.c.scope_key,
    coverage.c.channel,
    coverage.c.method,
    coverage.c.instance,
    coverage.c.state,
)

tasks = sa.Table(
    "rl_tasks",
    metadata,
    sa.Column("task_id", sa.String(64), primary_key=True),
    sa.Column("scope_key", sa.String(64), nullable=False),
    sa.Column("kind", sa.String(32), nullable=False),
    sa.Column("target", sa.JSON, nullable=False),
    sa.Column("state", sa.String(16), nullable=False),
    sa.Column("cursor", sa.JSON, nullable=False),
    sa.Column("attempts", sa.Integer, nullable=False),
    sa.Column("owner_id", sa.String(64)),
    sa.Column("lease_epoch", sa.Integer, nullable=False),
    sa.Column("heartbeat_at", UTCDateTime),
    sa.Column("refresh_correlation_id", sa.String(128)),
    sa.Column("created_at", UTCDateTime, nullable=False),
    sa.Column("updated_at", UTCDateTime, nullable=False),
    sa.Column("error", sa.JSON),
    sa.Column("row_revision", sa.Integer, nullable=False),
)
sa.Index("ix_rl_tasks_scope_state", tasks.c.scope_key, tasks.c.state)

engine_instances = sa.Table(
    "rl_engine_instances",
    metadata,
    sa.Column("instance_id", sa.String(64), primary_key=True),
    sa.Column("engine_id", sa.String(64), nullable=False),
    sa.Column("started_at", UTCDateTime, nullable=False),
    sa.Column("expires_at", UTCDateTime, nullable=False),
)

refresh_state = sa.Table(
    "rl_refresh_state",
    metadata,
    sa.Column("namespace", sa.String(64), primary_key=True),
    sa.Column("resource_id", sa.String(512), primary_key=True),
    sa.Column("state", sa.JSON, nullable=False),
)
