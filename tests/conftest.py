"""Shared fixtures. Containers start lazily, so unit tests never need Docker.

* ``pg_url``: a PostgreSQL 16 testcontainer, migrated to head and seeded with reference data.
* ``kafka_bootstrap``: a Redpanda testcontainer.
* ``settings``: per-test ``Settings`` with a unique topic prefix and consumer groups (topics are
  created), so tests never see each other's messages.
* ``engine``: a SQLAlchemy engine on a database truncated to reference data only.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, text

from freightpulse.config import Settings

# Docker Desktop exposes the daemon at a per-user socket path that cannot be bind-mounted into
# the Ryuk reaper container; inside the Docker VM it is always /var/run/docker.sock.
os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

PG_IMAGE = "postgres:16-alpine"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"

DYNAMIC_TABLES = (
    "alerts",
    "shipment_current_state",
    "shipment_stops",
    "shipments",
    "lanes",
    "shipment_events",
    "gps_pings",
    "lane_metrics_hourly",
    "carrier_scorecard_daily",
    "parked_events",
    "stream_watermarks",
)


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    from testcontainers.community.postgres import PostgresContainer

    from freightpulse.storage.db import make_engine, run_migrations
    from freightpulse.storage.partitions import run_maintenance
    from freightpulse.storage.seed import seed_reference_data

    with PostgresContainer(PG_IMAGE, driver="psycopg") as pg:
        url = pg.get_connection_url()
        run_migrations(url)
        eng = make_engine(url, pool_size=1)
        run_maintenance(eng, days_ahead=7, retention_days=3650)
        seed_reference_data(eng)
        eng.dispose()
        yield url


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    from testcontainers.community.kafka import RedpandaContainer

    # Note: RedpandaContainer.start() takes the startup timeout; using it as a context manager
    # as well would start a *second* container and leak the first.
    rp = RedpandaContainer(REDPANDA_IMAGE)
    rp.start(timeout=120)
    try:
        yield rp.get_bootstrap_server()
    finally:
        rp.stop()


@pytest.fixture(scope="session")
def _session_engine(pg_url: str) -> Iterator[Engine]:
    from freightpulse.storage.db import make_engine

    eng = make_engine(pg_url, pool_size=4)
    yield eng
    eng.dispose()


@pytest.fixture
def engine(_session_engine: Engine) -> Engine:
    with _session_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(DYNAMIC_TABLES)} RESTART IDENTITY CASCADE"))
        conn.execute(text("UPDATE facilities SET avg_dwell_min = NULL"))
    return _session_engine


@pytest.fixture
def db_settings(pg_url: str) -> Settings:
    """Settings for tests that only need Postgres."""
    return Settings(database_url=pg_url, _env_file=None)  # type: ignore[call-arg]


@pytest.fixture
def settings(pg_url: str, kafka_bootstrap: str) -> Iterator[Settings]:
    from freightpulse.processing.topics import delete_topics, ensure_topics

    tag = uuid.uuid4().hex[:8]
    s = Settings(  # type: ignore[call-arg]
        _env_file=None,
        database_url=pg_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        topic_prefix=f"t{tag}.",
        processor_group_id=f"proc-{tag}",
        alert_sink_group_id=f"sink-{tag}",
        processor_batch_timeout_s=0.2,
        unknown_shipment_retry_s=2.0,
        parking_sweep_interval_s=0.2,
    )
    ensure_topics(s)
    yield s
    # Redpanda caps partitions per shard by memory; drop this test's 16 partitions.
    delete_topics(s)
