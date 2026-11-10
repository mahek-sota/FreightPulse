"""The jobs service: alert scanners, rollups and partition maintenance on their own schedules."""

from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import UTC, datetime

from sqlalchemy import Engine

from freightpulse.config import Settings, get_settings
from freightpulse.observability import metrics as m
from freightpulse.processing.rollups import run_rollups
from freightpulse.processing.scanners import current_fleet_time, run_scanners
from freightpulse.simulator.producer import EventProducer
from freightpulse.storage.db import make_engine
from freightpulse.storage.partitions import run_maintenance

log = logging.getLogger("jobs")


class Jobs:
    def __init__(
        self,
        settings: Settings,
        engine: Engine | None = None,
        producer: EventProducer | None = None,
    ) -> None:
        self.settings = settings
        self.engine = engine or make_engine(settings.database_url, pool_size=2)
        self.producer = producer or EventProducer(settings, **{"linger.ms": 5})
        self._running = False
        self._next: dict[str, float] = {"scan": 0.0, "rollup": 0.0, "partitions": 0.0}

    def scan(self, now: datetime | None = None) -> int:
        started = time.perf_counter()
        clock, alerts = run_scanners(self.engine, now)
        produced_at = datetime.now(UTC)
        for a in alerts:
            self.producer.send(
                self.settings.alerts_topic,
                a.shipment_id,
                a.model_copy(update={"produced_at": produced_at}).to_json_bytes(),
            )
            m.ALERTS_EMITTED.labels(type=a.alert_type, severity=a.severity).inc()
        self.producer.flush()
        m.FLEET_CLOCK.set(clock.timestamp())
        m.SCANNER_RUN.labels(job="scanners").observe(time.perf_counter() - started)
        if alerts:
            log.info(
                "scanners at fleet time %s emitted %d alert message(s)",
                clock.isoformat(timespec="minutes"),
                len(alerts),
            )
        return len(alerts)

    def rollup(self, now: datetime | None = None) -> dict[str, int]:
        started = time.perf_counter()
        with self.engine.begin() as conn:
            counts = run_rollups(conn, now or current_fleet_time(conn))
        m.SCANNER_RUN.labels(job="rollups").observe(time.perf_counter() - started)
        log.info("rollups: %s", counts)
        return counts

    def partitions(self) -> None:
        started = time.perf_counter()
        run_maintenance(
            self.engine,
            days_ahead=self.settings.partition_days_ahead,
            retention_days=self.settings.partition_retention_days,
        )
        m.SCANNER_RUN.labels(job="partitions").observe(time.perf_counter() - started)

    def tick(self) -> None:
        now = time.monotonic()
        s = self.settings
        for name, interval, fn in (
            ("partitions", s.partition_maintenance_interval_s, self.partitions),
            ("scan", s.scanner_interval_s, self.scan),
            ("rollup", s.rollup_interval_s, self.rollup),
        ):
            if now >= self._next[name]:
                self._next[name] = now + interval
                try:
                    fn()
                except Exception:  # keep the other jobs running; the next tick retries
                    log.exception("job %s failed", name)

    def run(self) -> None:
        self._running = True
        while self._running:
            self.tick()
            time.sleep(0.5)

    def stop(self, *_: object) -> None:
        self._running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="FreightPulse periodic jobs")
    parser.add_argument("--once", action="store_true", help="run every job once and exit")
    args = parser.parse_args()
    settings = get_settings()
    jobs = Jobs(settings)
    if args.once:
        jobs.partitions()
        jobs.scan()
        jobs.rollup()
        return
    m.serve_metrics(settings.jobs_metrics_port)
    signal.signal(signal.SIGTERM, jobs.stop)
    signal.signal(signal.SIGINT, jobs.stop)
    log.info(
        "jobs started: scan every %.0fs, rollups every %.0fs",
        settings.scanner_interval_s,
        settings.rollup_interval_s,
    )
    jobs.run()


if __name__ == "__main__":
    main()
