"""Coverage watermarks and incrementally maintained metadata keyword dictionaries.

Revision ID: 0004_incremental_indexing
Revises: 0003_purge_log
Create Date: 2026-11-17 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_incremental_indexing"
down_revision = "0003_purge_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rl_coverage_watermarks",
        sa.Column("scope_key", sa.String(64), primary_key=True),
        sa.Column("channel", sa.String(16), primary_key=True),
        sa.Column("method", sa.String(64), primary_key=True),
        sa.Column("instance", sa.String(64), primary_key=True),
        sa.Column("change_seq", sa.Integer, nullable=False),
    )
    op.create_table(
        "rl_keyword_dictionaries",
        sa.Column("scope_key", sa.String(64), primary_key=True),
        sa.Column("dictionary_id", sa.String(80), primary_key=True),
        sa.Column("fields", sa.JSON, nullable=False),
        sa.Column("revision", sa.Integer, nullable=False),
    )
    op.create_table(
        "rl_keyword_terms",
        sa.Column("scope_key", sa.String(64), primary_key=True),
        sa.Column("dictionary_id", sa.String(80), primary_key=True),
        sa.Column("term_hash", sa.String(80), primary_key=True),
        sa.Column("term", sa.Text, nullable=False),
        sa.Column("documents", sa.Integer, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("rl_keyword_terms")
    op.drop_table("rl_keyword_dictionaries")
    op.drop_table("rl_coverage_watermarks")
