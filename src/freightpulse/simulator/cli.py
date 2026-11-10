"""Fleet simulator CLI: paces the simulated stream against the wall clock and produces to Kafka.

Example::

    python -m freightpulse.simulator --shipments 500 --acceleration 60 --duration 300
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from freightpulse.config import Settings, get_settings
from freightpulse.simulator.chaos import ChaosConfig, ChaosStream
from freightpulse.simulator.fleet import Fleet, SimConfig
from freightpulse.simulator.producer import EventProducer

log = logging.getLogger("simulator")

EPOCH_2026 = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class RunOptions:
    acceleration: float = 60.0
    """Simulated seconds per wall-clock second; 0 means as fast as possible."""
    duration_s: float | None = None
    max_events: int | None = None
    max_rate: float | None = None
    """Cap on events sent per wall-clock second (token bucket)."""
    report_every_s: float = 10.0


def default_id_offset(start: datetime) -> int:
    """Distinct shipment-id ranges for runs started at different minutes (no collisions)."""
    minutes = int((start - EPOCH_2026).total_seconds() // 60)
    return max(0, minutes) * 10_000


def run(
    settings: Settings,
    sim: SimConfig,
    chaos: ChaosConfig,
    opts: RunOptions,
    producer: EventProducer | None = None,
) -> Counter[str]:
    producer = producer or EventProducer(settings)
    stream = ChaosStream(chaos)
    emissions = stream.emissions(Fleet(sim).events())

    wall_start = time.monotonic()
    sim_start = sim.start
    sent = 0
    tokens = 0.0
    last_refill = wall_start
    last_report = wall_start

    for emission in emissions:
        now = time.monotonic()
        if opts.duration_s is not None and now - wall_start >= opts.duration_s:
            break
        if opts.max_events is not None and sent >= opts.max_events:
            break
        if opts.acceleration > 0:
            due_wall = (emission.emit_at - sim_start).total_seconds() / opts.acceleration
            wait = due_wall - (now - wall_start)
            if wait > 0:
                if opts.duration_s is not None:
                    wait = min(wait, max(0.0, opts.duration_s - (now - wall_start)))
                time.sleep(wait)
                if opts.duration_s is not None and time.monotonic() - wall_start >= opts.duration_s:
                    break
        if opts.max_rate:
            while True:
                t = time.monotonic()
                tokens = min(opts.max_rate, tokens + (t - last_refill) * opts.max_rate)
                last_refill = t
                if tokens >= 1:
                    tokens -= 1
                    break
                time.sleep((1 - tokens) / opts.max_rate)

        topic = (
            settings.telemetry_topic if emission.is_telemetry else settings.shipment_events_topic
        )
        producer.send(topic, emission.key, emission.to_bytes(datetime.now(UTC)))
        sent += 1

        if time.monotonic() - last_report >= opts.report_every_s:
            last_report = time.monotonic()
            sim_now = sim_start + timedelta(seconds=(last_report - wall_start) * opts.acceleration)
            log.info(
                "sent=%d sim_time=%s rate=%.0f/s",
                sent,
                sim_now.strftime("%Y-%m-%d %H:%M") if opts.acceleration > 0 else "-",
                sent / max(1e-9, last_report - wall_start),
            )
    producer.flush()
    stats = Counter(stream.stats)
    stats.update(producer.stats)
    stats["sent"] = sent  # this run's count, even when the producer is shared across runs
    stats["wall_seconds"] = round(time.monotonic() - wall_start)
    return stats


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="freightpulse-sim", description=__doc__.split("\n")[0])
    p.add_argument("--shipments", type=int, default=500, help="concurrent shipments (default 500)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for deterministic output")
    p.add_argument(
        "--start",
        type=datetime.fromisoformat,
        default=None,
        help="simulated start time, ISO-8601 (default: now, UTC, to the minute)",
    )
    p.add_argument(
        "--acceleration",
        type=float,
        default=60.0,
        help="simulated seconds per real second (default 60; 0 = as fast as possible)",
    )
    p.add_argument(
        "--ramp-minutes",
        type=float,
        default=120.0,
        help="spread shipment creation over this many simulated minutes",
    )
    p.add_argument("--duration", type=float, default=None, help="stop after N real seconds")
    p.add_argument("--max-events", type=int, default=None, help="stop after N events")
    p.add_argument("--max-rate", type=float, default=None, help="cap events/sec (wall clock)")
    p.add_argument("--id-offset", type=int, default=None, help="first shipment number - 1")
    p.add_argument("--tender-reject-rate", type=float, default=0.15)
    p.add_argument("--duplicate-rate", type=float, default=0.02)
    p.add_argument("--out-of-order-rate", type=float, default=0.03)
    p.add_argument("--malformed-rate", type=float, default=0.005)
    p.add_argument("--gps-dropout-rate", type=float, default=0.01)
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    start: datetime = args.start or datetime.now(UTC).replace(second=0, microsecond=0)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    sim = SimConfig(
        shipments=args.shipments,
        seed=args.seed,
        start=start,
        ramp=timedelta(minutes=args.ramp_minutes),
        tender_reject_rate=args.tender_reject_rate,
        id_offset=args.id_offset if args.id_offset is not None else default_id_offset(start),
    )
    chaos = ChaosConfig(
        duplicate_rate=args.duplicate_rate,
        out_of_order_rate=args.out_of_order_rate,
        malformed_rate=args.malformed_rate,
        gps_dropout_rate=args.gps_dropout_rate,
        seed=args.seed,
    )
    opts = RunOptions(
        acceleration=args.acceleration,
        duration_s=args.duration,
        max_events=args.max_events,
        max_rate=args.max_rate,
    )
    log.info(
        "simulating %d shipments from %s at %sx",
        sim.shipments,
        start.isoformat(),
        opts.acceleration or "max",
    )
    stats = run(get_settings(), sim, chaos, opts)
    log.info("done: %s", dict(sorted(stats.items())))


if __name__ == "__main__":
    main()
