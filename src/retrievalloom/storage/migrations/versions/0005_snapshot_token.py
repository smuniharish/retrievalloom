"""Per-scope snapshot token, changed by every write that affects retrieval snapshots.

Revision ID: 0005_snapshot_token
Revises: 0004_incremental_indexing
Create Date: 2026-11-17 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_snapshot_token"
down_revision = "0004_incremental_indexing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("rl_scopes", sa.Column("snapshot_token", sa.String(64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("rl_scopes") as batch:
        batch.drop_column("snapshot_token")
