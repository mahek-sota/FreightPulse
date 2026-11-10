from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import Engine, inspect, text

from freightpulse.domain.reference_data import CARRIERS, CUSTOMERS, FACILITIES
from freightpulse.storage import tables as t
from freightpulse.storage.partitions import (
    PartitionCache,
    create_partition,
    drop_old_partitions,
    ensure_partitions,
    list_partitions,
    partition_name,
    run_maintenance,
)
from freightpulse.storage.seed import seed_reference_data

pytestmark = pytest.mark.integration


def test_core_metadata_matches_migrated_schema(engine: Engine) -> None:
    insp = inspect(engine)
    db_tables = set(insp.get_table_names())
    for table in t.metadata.sorted_tables:
        assert table.name in db_tables, table.name
        db_cols = {c["name"] for c in insp.get_columns(table.name)}
        assert db_cols == set(table.columns.keys()), table.name


def test_seed_is_idempotent(engine: Engine) -> None:
    seed_reference_data(engine)
    seed_reference_data(engine)
    with engine.connect() as conn:
        counts = [
            conn.execute(text(f"SELECT count(*) FROM {n}")).scalar_one()
            for n in ("facilities", "carriers", "customers")
        ]
    assert counts == [len(FACILITIES), len(CARRIERS), len(CUSTOMERS)]
    assert len(FACILITIES) >= 40
    assert len(CARRIERS) == 25


def test_event_tables_are_partitioned_with_brin(engine: Engine) -> None:
    with engine.connect() as conn:
        kinds = dict(
            conn.execute(
                text(
                    "SELECT relname, relkind FROM pg_class "
                    "WHERE relname IN ('shipment_events', 'gps_pings')"
                )
            ).all()
        )
        brin = conn.execute(
            text(
                "SELECT count(*) FROM pg_indexes "
                "WHERE tablename = 'gps_pings' AND indexdef ILIKE '%USING brin%'"
            )
        ).scalar_one()
    assert kinds == {"shipment_events": "p", "gps_pings": "p"}
    assert brin == 1


def test_partition_create_list_and_drop(engine: Engine) -> None:
    far = date(2031, 1, 1)
    with engine.begin() as conn:
        created = ensure_partitions(conn, [far, far + timedelta(days=1)])
        assert partition_name("gps_pings", far) in created
        assert ensure_partitions(conn, [far]) == []  # idempotent
        with pytest.raises(ValueError, match="not a partitioned table"):
            create_partition(conn, "alerts", far)
        names = [n for n, _ in list_partitions(conn, "shipment_events")]
        assert partition_name("shipment_events", far) in names
        # Rows route to the right partition.
        conn.execute(
            text(
                "INSERT INTO gps_pings (event_id, shipment_id, event_time, lat, lon, speed_mph, "
                "driver_status) VALUES (gen_random_uuid(), 'SHP-000001', '2031-01-01T12:00Z', "
                "1, 1, 50, 'DRIVING')"
            )
        )
        owner = conn.execute(
            text(
                "SELECT tableoid::regclass::text FROM gps_pings "
                "WHERE event_time = '2031-01-01T12:00Z'"
            )
        ).scalar_one()
        assert owner == partition_name("gps_pings", far)
    with engine.begin() as conn:
        dropped = drop_old_partitions(conn, older_than=date(1999, 1, 1))
        assert dropped == []


def test_run_maintenance_creates_ahead_and_drops_old(engine: Engine) -> None:
    today = date(2032, 6, 15)
    old = today - timedelta(days=40)
    with engine.begin() as conn:
        ensure_partitions(conn, [old])
    created, dropped = run_maintenance(engine, today=today, days_ahead=7, retention_days=30)
    assert partition_name("gps_pings", today + timedelta(days=7)) in created
    assert partition_name("shipment_events", today - timedelta(days=1)) in created
    old_names = {partition_name("gps_pings", old), partition_name("shipment_events", old)}
    assert old_names <= set(dropped)
    # Second run is a no-op.
    assert run_maintenance(engine, today=today, days_ahead=7, retention_days=30) == ([], [])
    # The 2032 run dropped today's real partitions (older than its window): recreate them.
    run_maintenance(engine, days_ahead=7, retention_days=3650)


def test_partition_cache_only_touches_catalog_for_new_days(engine: Engine) -> None:
    cache = PartitionCache()
    day = date(2033, 3, 3)
    assert cache.missing([day]) == {day}
    cache.ensure(engine, [day])
    assert cache.missing([day]) == set()
    cache.ensure(engine, [day])  # no-op
    with engine.connect() as conn:
        assert (partition_name("gps_pings", day), day) in list_partitions(conn, "gps_pings")
