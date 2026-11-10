"""Daily range-partition management for ``shipment_events`` and ``gps_pings`` (ADR 0004).

* ``run_maintenance`` creates partitions from yesterday through ``days_ahead`` days ahead and
  drops partitions entirely older than the retention window. The jobs service runs it hourly.
* ``ensure_partitions_for`` is called by the processor before each batch for the event days it
  contains (cached in memory), so late or accelerated-clock events never hit a missing partition.

DDL is serialised with a transaction-scoped advisory lock so several processor instances and the
jobs service never race on CREATE/DROP.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Final

from sqlalchemy import Connection, Engine, text

from freightpulse.config import get_settings
from freightpulse.storage.db import make_engine

log = logging.getLogger(__name__)

PARTITIONED_TABLES: Final = ("shipment_events", "gps_pings")
_LOCK_KEY: Final = 774_211_001  # arbitrary, unique to partition DDL
_NAME_RE: Final = re.compile(r"^(?P<table>\w+)_p(?P<day>\d{8})$")


def partition_name(table: str, day: date) -> str:
    return f"{table}_p{day:%Y%m%d}"


def _lock(conn: Connection) -> None:
    conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _LOCK_KEY})


def list_partitions(conn: Connection, table: str) -> list[tuple[str, date]]:
    rows = conn.execute(
        text(
            """
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = :table
            ORDER BY c.relname
            """
        ),
        {"table": table},
    ).scalars()
    out: list[tuple[str, date]] = []
    for name in rows:
        m = _NAME_RE.match(name)
        if m and m["table"] == table:
            out.append((name, datetime.strptime(m["day"], "%Y%m%d").date()))
    return out


def create_partition(conn: Connection, table: str, day: date) -> None:
    if table not in PARTITIONED_TABLES:
        raise ValueError(f"not a partitioned table: {table}")
    name = partition_name(table, day)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF {table} "
            f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
        )
    )


def ensure_partitions(conn: Connection, days: Iterable[date]) -> list[str]:
    """Create any missing partitions for ``days`` on every partitioned table (needs a txn)."""
    wanted = sorted(set(days))
    if not wanted:
        return []
    _lock(conn)
    created: list[str] = []
    for table in PARTITIONED_TABLES:
        existing = {d for _, d in list_partitions(conn, table)}
        for day in wanted:
            if day not in existing:
                create_partition(conn, table, day)
                created.append(partition_name(table, day))
    return created


def drop_old_partitions(conn: Connection, *, older_than: date) -> list[str]:
    """Drop partitions whose whole day is before ``older_than``."""
    _lock(conn)
    dropped: list[str] = []
    for table in PARTITIONED_TABLES:
        for name, day in list_partitions(conn, table):
            if day < older_than:
                conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
                dropped.append(name)
    return dropped


def run_maintenance(
    engine: Engine,
    *,
    today: date | None = None,
    days_ahead: int = 7,
    retention_days: int = 90,
) -> tuple[list[str], list[str]]:
    today = today or datetime.now(UTC).date()
    days = [today + timedelta(days=i) for i in range(-1, days_ahead + 1)]
    with engine.begin() as conn:
        created = ensure_partitions(conn, days)
        dropped = drop_old_partitions(conn, older_than=today - timedelta(days=retention_days))
    if created or dropped:
        log.info("partitions created=%d dropped=%d", len(created), len(dropped))
    return created, dropped


class PartitionCache:
    """Remembers which days already have partitions so the hot path rarely touches the catalog."""

    def __init__(self) -> None:
        self._known: set[date] = set()

    def missing(self, days: Iterable[date]) -> set[date]:
        return set(days) - self._known

    def ensure(self, engine: Engine, days: Iterable[date]) -> None:
        missing = self.missing(days)
        if not missing:
            return
        with engine.begin() as conn:
            ensure_partitions(conn, missing)
        self._known |= missing


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Create/drop daily event partitions")
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--days-ahead", type=int, default=settings.partition_days_ahead)
    parser.add_argument("--retention-days", type=int, default=settings.partition_retention_days)
    args = parser.parse_args()
    engine = make_engine(args.database_url, pool_size=1)
    created, dropped = run_maintenance(
        engine, days_ahead=args.days_ahead, retention_days=args.retention_days
    )
    print(f"partitions created: {len(created)}, dropped: {len(dropped)}")


if __name__ == "__main__":
    main()
