"""``make demo``: run the fleet simulator and print live fleet status and alerts from the API.

Runs inside the compose network (``docker compose run demo``). The simulator streams 500
shipments at 60x (one real second = one simulated minute) while this loop polls the API.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from freightpulse.config import get_settings
from freightpulse.simulator.chaos import ChaosConfig
from freightpulse.simulator.cli import RunOptions, default_id_offset, run
from freightpulse.simulator.fleet import SimConfig


def _get(base: str, path: str) -> Any:
    with urllib.request.urlopen(base + path, timeout=5) as resp:
        return json.loads(resp.read())


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--api", default="http://api:8000")
    p.add_argument("--shipments", type=int, default=500)
    p.add_argument("--minutes", type=float, default=4.0, help="real minutes to run")
    p.add_argument("--acceleration", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    start = datetime.now(UTC).replace(second=0, microsecond=0)
    sim = SimConfig(
        shipments=args.shipments,
        seed=args.seed,
        start=start,
        ramp=timedelta(hours=2),
        id_offset=default_id_offset(start),
    )
    opts = RunOptions(
        acceleration=args.acceleration, duration_s=args.minutes * 60, report_every_s=3600
    )
    stats: dict[str, Any] = {}

    def simulate() -> None:
        stats.update(run(get_settings(), sim, ChaosConfig(seed=args.seed), opts))

    t = threading.Thread(target=simulate, daemon=True)
    t.start()
    print(
        f"FreightPulse demo: {args.shipments} shipments at {args.acceleration:.0f}x for "
        f"{args.minutes:g} real minutes (simulated start {start:%Y-%m-%d %H:%M} UTC)"
    )
    port = os.environ.get("API_PORT", "8000")
    print(f"Live alerts page: http://localhost:{port}/   Grafana: http://localhost:3000\n")
    seen: set[str] = set()
    while t.is_alive():
        time.sleep(15)
        try:
            s = _get(args.api, "/metrics/summary")
            alerts = _get(args.api, "/alerts?active=true&limit=200")["items"]
        except OSError as exc:
            print(f"  (API not reachable yet: {exc})")
            continue
        print(
            f"[fleet {s['fleet_time'][11:16]}] shipments={s['shipments_total']} "
            f"in_transit={s['in_transit']} at_risk={s['at_risk']} "
            f"detained={s['detained_now']} detention_today=${s['detention_cost_today_usd']:,.0f} "
            f"delivered_today={s['delivered_today']} active_alerts={s['active_alerts']}"
        )
        for a in reversed(alerts):
            if a["dedupe_key"] not in seen:
                seen.add(a["dedupe_key"])
                print(f"    {a['severity']:<8} {a['alert_type']:<14} {a['message']}")
    t.join()
    print("\nSimulator finished:", dict(sorted(stats.items())))
    print(f"Explore: http://localhost:{port}/docs  |  make dlq-replay  |  make logs")


if __name__ == "__main__":
    main()
