"""Alembic environment for the RetrievalLoom metadata schema.

Migrations run on a connection supplied by the metadata store's ``migrate`` through
``config.attributes["connection"]``; this environment never opens its own engine.
Batch (copy-and-move) operations are only used on SQLite, whose ``ALTER TABLE`` is limited.
"""

from __future__ import annotations

from alembic import context

VERSION_TABLE = "rl_alembic_version"


def run_migrations() -> None:
    """Run migrations on the supplied connection."""
    connection = context.config.attributes.get("connection")
    if connection is None:
        raise RuntimeError("retrievalloom migrations require a supplied connection")
    context.configure(
        connection=connection,
        target_metadata=None,
        render_as_batch=connection.dialect.name == "sqlite",
        version_table=VERSION_TABLE,
        transaction_per_migration=False,
    )
    with context.begin_transaction():
        context.run_migrations()


run_migrations()
