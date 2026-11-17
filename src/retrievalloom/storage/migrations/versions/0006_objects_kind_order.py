"""Order live objects of a kind by id, so full-build pages seek instead of sort.

Revision ID: 0006_objects_kind_order
Revises: 0005_snapshot_token
Create Date: 2026-11-17 00:00:00
"""

from __future__ import annotations

from alembic import op

revision = "0006_objects_kind_order"
down_revision = "0005_snapshot_token"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_rl_objects_scope_kind_id",
        "rl_objects",
        ["scope_key", "kind", "state", "object_id"],
        unique=False,
    )
    op.drop_index("ix_rl_objects_scope_kind", table_name="rl_objects")


def downgrade() -> None:
    op.create_index(
        "ix_rl_objects_scope_kind", "rl_objects", ["scope_key", "kind", "state"], unique=False
    )
    op.drop_index("ix_rl_objects_scope_kind_id", table_name="rl_objects")
