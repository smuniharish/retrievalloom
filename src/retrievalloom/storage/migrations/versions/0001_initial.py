"""Initial metadata schema.

Revision ID: 0001_initial
Revises:
Create Date: 2025-01-01 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None

# ISO-8601 text on SQLite, native ``timestamptz`` on PostgreSQL (see ``schema.UTCDateTime``).
_TIMESTAMP = sa.String(length=32).with_variant(postgresql.TIMESTAMP(timezone=True), "postgresql")


def upgrade() -> None:
    op.create_table(
        "rl_commit_seq",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "rl_coverage",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("instance", sa.String(length=64), nullable=False),
        sa.Column("generation_id", sa.String(length=64), nullable=True),
        sa.Column("target_version", sa.Integer(), nullable=False),
        sa.Column("indexed_version", sa.Integer(), nullable=True),
        sa.Column("indexed_metadata_revision", sa.Integer(), nullable=True),
        sa.Column("dependency_fingerprint", sa.String(length=80), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.JSON(), nullable=True),
        sa.Column("lease_owner", sa.String(length=64), nullable=True),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("lease_expires_at", _TIMESTAMP, nullable=True),
        sa.Column("updated_at", _TIMESTAMP, nullable=False),
        sa.Column("row_revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "document_uid", "channel", "method", "instance"),
    )
    op.create_index(
        "ix_rl_coverage_unit_state",
        "rl_coverage",
        ["scope_key", "channel", "method", "instance", "state"],
        unique=False,
    )
    op.create_table(
        "rl_document_versions",
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("content_hash", sa.String(length=80), nullable=False),
        sa.Column("hash_profile", sa.String(length=32), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("dataset", sa.String(length=256), nullable=True),
        sa.Column("source", sa.JSON(), nullable=True),
        sa.Column("representation_digests", sa.JSON(), nullable=False),
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.PrimaryKeyConstraint("document_uid", "version"),
    )
    op.create_table(
        "rl_documents",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("document_id", sa.String(length=256), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("counter", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("lifecycle_revision", sa.Integer(), nullable=False),
        sa.Column("metadata_revision", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=80), nullable=False),
        sa.Column("hash_profile", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.Column("updated_at", _TIMESTAMP, nullable=False),
        sa.Column("deleted_at", _TIMESTAMP, nullable=True),
        sa.Column("change_seq", sa.Integer(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("dataset", sa.String(length=256), nullable=True),
        sa.Column("source", sa.JSON(), nullable=True),
        sa.Column("representation_digests", sa.JSON(), nullable=False),
        sa.Column("representation_kinds", sa.JSON(), nullable=False),
        sa.Column("row_revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "document_id"),
        sa.UniqueConstraint("document_uid"),
    )
    op.create_index(
        "ix_rl_documents_scope_change", "rl_documents", ["scope_key", "change_seq"], unique=False
    )
    op.create_index(
        "ix_rl_documents_scope_state", "rl_documents", ["scope_key", "state"], unique=False
    )
    op.create_table(
        "rl_engine_instances",
        sa.Column("instance_id", sa.String(length=64), nullable=False),
        sa.Column("engine_id", sa.String(length=64), nullable=False),
        sa.Column("started_at", _TIMESTAMP, nullable=False),
        sa.Column("expires_at", _TIMESTAMP, nullable=False),
        sa.PrimaryKeyConstraint("instance_id"),
    )
    op.create_table(
        "rl_generation_pointers",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("instance", sa.String(length=64), nullable=False),
        sa.Column("active_generation_id", sa.String(length=64), nullable=False),
        sa.Column("row_revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "channel", "method", "instance"),
    )
    op.create_table(
        "rl_generations",
        sa.Column("generation_id", sa.String(length=64), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("backend_id", sa.String(length=128), nullable=False),
        sa.Column("backend_instance_id", sa.String(length=64), nullable=True),
        sa.Column("dependency_fingerprint", sa.String(length=80), nullable=False),
        sa.Column("build_watermark_seq", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("owner_id", sa.String(length=64), nullable=True),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("heartbeat_at", _TIMESTAMP, nullable=True),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.Column("published_at", _TIMESTAMP, nullable=True),
        sa.Column("retired_at", _TIMESTAMP, nullable=True),
        sa.Column("predecessor_id", sa.String(length=64), nullable=True),
        sa.Column("stats", sa.JSON(), nullable=False),
        sa.Column("row_revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("generation_id"),
    )
    op.create_index(
        "ix_rl_generations_unit",
        "rl_generations",
        ["scope_key", "channel", "method", "state"],
        unique=False,
    )
    op.create_table(
        "rl_ingestion_events",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("event_id", sa.String(length=128), nullable=False),
        sa.Column("document_id", sa.String(length=256), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=True),
        sa.Column("counter_after", sa.Integer(), nullable=True),
        sa.Column("version_after", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(length=80), nullable=False),
        sa.Column("appearance_fingerprint", sa.String(length=80), nullable=False),
        sa.Column("possible_redelivery_of", sa.String(length=128), nullable=True),
        sa.Column("delivery", sa.JSON(), nullable=True),
        sa.Column("staged_payload", sa.JSON(), nullable=True),
        sa.Column("error", sa.JSON(), nullable=True),
        sa.Column("received_at", _TIMESTAMP, nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "event_id"),
    )
    op.create_index(
        "ix_rl_events_fingerprint",
        "rl_ingestion_events",
        ["scope_key", "appearance_fingerprint"],
        unique=False,
    )
    op.create_index(
        "ix_rl_events_status", "rl_ingestion_events", ["scope_key", "status"], unique=False
    )
    op.create_table(
        "rl_lineage",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("from_id", sa.String(length=64), nullable=False),
        sa.Column("to_id", sa.String(length=64), nullable=False),
        sa.Column("relation", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "from_id", "to_id", "relation"),
    )
    op.create_index("ix_rl_lineage_to", "rl_lineage", ["scope_key", "to_id"], unique=False)
    op.create_table(
        "rl_object_contributions",
        sa.Column("object_id", sa.String(length=64), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("field_path", sa.String(length=256), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("value", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("object_id", "document_uid", "version", "field_path", "ordinal"),
    )
    op.create_index(
        "ix_rl_contributions_document",
        "rl_object_contributions",
        ["scope_key", "document_uid"],
        unique=False,
    )
    op.create_table(
        "rl_objects",
        sa.Column("object_id", sa.String(length=64), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=True),
        sa.Column("version", sa.Integer(), nullable=True),
        sa.Column("representation_id", sa.String(length=256), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(length=80), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("sources", sa.JSON(), nullable=False),
        sa.Column("transformation", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("object_id"),
    )
    op.create_index(
        "ix_rl_objects_document",
        "rl_objects",
        ["scope_key", "document_uid", "version"],
        unique=False,
    )
    op.create_index(
        "ix_rl_objects_scope_kind", "rl_objects", ["scope_key", "kind", "state"], unique=False
    )
    op.create_table(
        "rl_reader_leases",
        sa.Column("generation_id", sa.String(length=64), nullable=False),
        sa.Column("engine_id", sa.String(length=64), nullable=False),
        sa.Column("expires_at", _TIMESTAMP, nullable=False),
        sa.Column("heartbeat_at", _TIMESTAMP, nullable=False),
        sa.PrimaryKeyConstraint("generation_id", "engine_id"),
    )
    op.create_table(
        "rl_refresh_state",
        sa.Column("namespace", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=512), nullable=False),
        sa.Column("state", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("namespace", "resource_id"),
    )
    op.create_table(
        "rl_representations",
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("representation_id", sa.String(length=256), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("transformation", sa.JSON(), nullable=True),
        sa.Column("hash_profile", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("document_uid", "version", "representation_id"),
    )
    op.create_table(
        "rl_scopes",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=256), nullable=False),
        sa.Column("namespace", sa.String(length=256), nullable=False),
        sa.Column("collection", sa.String(length=256), nullable=False),
        sa.PrimaryKeyConstraint("scope_key"),
    )
    op.create_table(
        "rl_tasks",
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("target", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("cursor", sa.JSON(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("owner_id", sa.String(length=64), nullable=True),
        sa.Column("lease_epoch", sa.Integer(), nullable=False),
        sa.Column("heartbeat_at", _TIMESTAMP, nullable=True),
        sa.Column("refresh_correlation_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", _TIMESTAMP, nullable=False),
        sa.Column("updated_at", _TIMESTAMP, nullable=False),
        sa.Column("error", sa.JSON(), nullable=True),
        sa.Column("row_revision", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("task_id"),
    )
    op.create_index("ix_rl_tasks_scope_state", "rl_tasks", ["scope_key", "state"], unique=False)
    op.create_table(
        "rl_visibility",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("instance", sa.String(length=64), nullable=False),
        sa.Column("visible_from_seq", sa.Integer(), nullable=False),
        sa.Column("visible_until_seq", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint(
            "scope_key",
            "document_uid",
            "version",
            "channel",
            "method",
            "instance",
            "visible_from_seq",
        ),
    )


def downgrade() -> None:
    op.drop_table("rl_visibility")
    op.drop_index("ix_rl_tasks_scope_state", table_name="rl_tasks")
    op.drop_table("rl_tasks")
    op.drop_table("rl_scopes")
    op.drop_table("rl_representations")
    op.drop_table("rl_refresh_state")
    op.drop_table("rl_reader_leases")
    op.drop_index("ix_rl_objects_scope_kind", table_name="rl_objects")
    op.drop_index("ix_rl_objects_document", table_name="rl_objects")
    op.drop_table("rl_objects")
    op.drop_index("ix_rl_contributions_document", table_name="rl_object_contributions")
    op.drop_table("rl_object_contributions")
    op.drop_index("ix_rl_lineage_to", table_name="rl_lineage")
    op.drop_table("rl_lineage")
    op.drop_index("ix_rl_events_status", table_name="rl_ingestion_events")
    op.drop_index("ix_rl_events_fingerprint", table_name="rl_ingestion_events")
    op.drop_table("rl_ingestion_events")
    op.drop_index("ix_rl_generations_unit", table_name="rl_generations")
    op.drop_table("rl_generations")
    op.drop_table("rl_generation_pointers")
    op.drop_table("rl_engine_instances")
    op.drop_index("ix_rl_documents_scope_state", table_name="rl_documents")
    op.drop_index("ix_rl_documents_scope_change", table_name="rl_documents")
    op.drop_table("rl_documents")
    op.drop_table("rl_document_versions")
    op.drop_index("ix_rl_coverage_unit_state", table_name="rl_coverage")
    op.drop_table("rl_coverage")
    op.drop_table("rl_commit_seq")
