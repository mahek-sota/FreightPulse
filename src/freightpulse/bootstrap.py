"""One-shot environment bootstrap (the compose ``init`` service and ``make migrate``):

1. create Kafka topics with explicit partition counts,
2. run Alembic migrations,
3. create daily partitions (yesterday .. +7 days),
4. seed reference data (facilities, carriers, customers).

Every step is idempotent.
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from freightpulse.config import get_settings
from freightpulse.processing.topics import ensure_topics
from freightpulse.storage.db import make_engine, run_migrations
from freightpulse.storage.partitions import run_maintenance
from freightpulse.storage.seed import seed_reference_data

log = logging.getLogger("bootstrap")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    created = ensure_topics(settings)
    log.info("topics ready (created: %s)", created or "none")

    engine = make_engine(settings.database_url, pool_size=1)
    for attempt in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except OperationalError:
            log.info("waiting for postgres (attempt %d)", attempt + 1)
            time.sleep(2)
    run_migrations(settings.database_url)
    log.info("migrations at head")
    run_maintenance(
        engine,
        days_ahead=settings.partition_days_ahead,
        retention_days=settings.partition_retention_days,
    )
    seed_reference_data(engine)
    log.info("bootstrap complete")


if __name__ == "__main__":
    main()
