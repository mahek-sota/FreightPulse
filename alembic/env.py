"""Alembic environment. Migrations are hand-written SQL (partitioned tables need explicit DDL)."""

from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine, pool

from freightpulse.config import get_settings

config = context.config


def _url() -> str:
    # Allow `alembic -x url=...` (used by tests and the perf database), else settings.
    x_args = context.get_x_argument(as_dictionary=True)
    return (
        x_args.get("url") or config.get_main_option("sqlalchemy.url") or get_settings().database_url
    )


def run_migrations_offline() -> None:
    context.configure(url=_url(), literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = config.attributes.get("connection")
    if connectable is not None:
        context.configure(connection=connectable)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
