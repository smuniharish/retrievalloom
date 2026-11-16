"""Indexes for physical erasure and retention.

Revision ID: 0002_retention_indexes
Revises: 0001_initial
Create Date: 2026-11-16 00:00:00
"""

from __future__ import annotations

from alembic import op

revision = "0002_retention_indexes"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_rl_versions_scope_document",
        "rl_document_versions",
        ["scope_key", "document_uid"],
        unique=False,
    )
    op.create_index(
        "ix_rl_events_document",
        "rl_ingestion_events",
        ["scope_key", "document_uid"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_rl_events_document", table_name="rl_ingestion_events")
    op.drop_index("ix_rl_versions_scope_document", table_name="rl_document_versions")
