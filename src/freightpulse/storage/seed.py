"""Idempotent reference-data seed: facilities, carriers, customers."""

from __future__ import annotations

import argparse
import logging

from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert

from freightpulse.config import get_settings
from freightpulse.domain.reference_data import CARRIERS, CUSTOMERS, FACILITIES
from freightpulse.storage import tables as t
from freightpulse.storage.db import make_engine

log = logging.getLogger(__name__)


def seed_reference_data(engine: Engine) -> dict[str, int]:
    with engine.begin() as conn:
        fac = insert(t.facilities).values(
            [
                {
                    "facility_id": f.facility_id,
                    "name": f.name,
                    "city": f.city,
                    "state": f.state,
                    "lat": f.lat,
                    "lon": f.lon,
                    "timezone": f.timezone,
                }
                for f in FACILITIES
            ]
        )
        conn.execute(
            fac.on_conflict_do_update(
                index_elements=["facility_id"],
                set_={
                    c: fac.excluded[c] for c in ("name", "city", "state", "lat", "lon", "timezone")
                },
            )
        )
        car = insert(t.carriers).values(
            [
                {
                    "carrier_id": c.carrier_id,
                    "name": c.name,
                    "scac": c.scac,
                    "mc_number": c.mc_number,
                }
                for c in CARRIERS
            ]
        )
        conn.execute(
            car.on_conflict_do_update(
                index_elements=["carrier_id"],
                set_={c: car.excluded[c] for c in ("name", "scac", "mc_number")},
            )
        )
        cus = insert(t.customers).values(
            [{"customer_id": c.customer_id, "name": c.name} for c in CUSTOMERS]
        )
        conn.execute(
            cus.on_conflict_do_update(
                index_elements=["customer_id"], set_={"name": cus.excluded.name}
            )
        )
    counts = {"facilities": len(FACILITIES), "carriers": len(CARRIERS), "customers": len(CUSTOMERS)}
    log.info("reference data seeded: %s", counts)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Seed reference data")
    parser.add_argument("--database-url", default=get_settings().database_url)
    args = parser.parse_args()
    print(seed_reference_data(make_engine(args.database_url, pool_size=1)))


if __name__ == "__main__":
    main()
