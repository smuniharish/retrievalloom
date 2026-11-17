"""Purge log: erasures other engines evict from their in-memory indexes.

Revision ID: 0003_purge_log
Revises: 0002_retention_indexes
Create Date: 2026-11-17 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003_purge_log"
down_revision = "0002_retention_indexes"
branch_labels = None
depends_on = None

# ISO-8601 text on SQLite, native ``timestamptz`` on PostgreSQL (see ``schema.UTCDateTime``).
_TIMESTAMP = sa.String(length=32).with_variant(postgresql.TIMESTAMP(timezone=True), "postgresql")


def upgrade() -> None:
    op.create_table(
        "rl_purge_log",
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False, autoincrement=False),
        sa.Column("document_uid", sa.String(length=64), nullable=False),
        sa.Column("object_ids", sa.JSON(), nullable=False),
        sa.Column("purged_at", _TIMESTAMP, nullable=False),
        sa.PrimaryKeyConstraint("scope_key", "seq"),
    )


def downgrade() -> None:
    op.drop_table("rl_purge_log")
