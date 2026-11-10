"""``make bench``: measure processor throughput and end-to-end latency; write docs/benchmarks.md.

Two experiments, each with 1 and with 3 processor instances in one consumer group. Before any
measurement the processors are started and the harness waits (Kafka admin API) until the group
is **Stable** with N members that all own partitions, so no join or rebalance happens inside a
measured window.

A. **Capacity (backlog drain).** With the group stable, one producer writes a backlog of
   simulator events (default 500,000, default chaos mix) as fast as it can, several times faster
   than the consumers. Throughput = rows committed / time from the first to the last commit.
   This is the sustained throughput ceiling of the group.

B. **Latency vs offered rate.** The simulator produces at increasing fixed rates (token bucket;
   default 1k/2k/4k/6k/8k events/s for 20 s each). Each processor appends one exact latency sample
   per committed event (``produced_at`` -> DB commit), from which p50/p95/p99 and the achieved
   commit rate are computed per step.

Everything runs against the compose Redpanda and Postgres on isolated topics (``bench.*``) and
an isolated database (``freightpulse_bench``). Processors run as host subprocesses. The report
is generated from this run's raw samples: numbers are measured, never estimated.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from confluent_kafka import ConsumerGroupState
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine, text

from freightpulse.config import Settings
from freightpulse.perf.sql_cases import machine_specs
from freightpulse.processing.topics import delete_topics, ensure_topics
from freightpulse.simulator.chaos import ChaosConfig
from freightpulse.simulator.cli import RunOptions, run
from freightpulse.simulator.fleet import SimConfig
from freightpulse.simulator.producer import EventProducer
from freightpulse.storage.db import make_engine, run_migrations
from freightpulse.storage.partitions import run_maintenance
from freightpulse.storage.seed import seed_reference_data
from freightpulse.storage.seed_large import ensure_database

ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs" / "benchmarks.md"
RESULTS = ROOT / "docs" / "benchmarks_results.json"

DYNAMIC = (
    "alerts, shipment_current_state, shipment_stops, shipments, lanes, shipment_events, "
    "gps_pings, lane_metrics_hourly, carrier_scorecard_daily, parked_events, stream_watermarks"
)
RAMP_S = 3.0


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def bench_settings(db_url: str, bootstrap: str, prefix: str) -> Settings:
    return Settings(
        _env_file=None,
        database_url=db_url,
        kafka_bootstrap_servers=bootstrap,
        topic_prefix=prefix,
        processor_group_id=f"bench-{uuid.uuid4().hex[:8]}",
    )


def reset(engine: Engine, settings: Settings) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {DYNAMIC} RESTART IDENTITY CASCADE"))
    delete_topics(settings)
    time.sleep(1)
    ensure_topics(settings)


def committed_rows(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(
            conn.execute(
                text(
                    "SELECT (SELECT count(*) FROM gps_pings) + (SELECT count(*) FROM shipment_events)"
                )
            ).scalar_one()
        )


def spawn(settings: Settings, n: int, sample_dir: Path | None) -> list[subprocess.Popen[bytes]]:
    procs = []
    for i in range(n):
        env = dict(os.environ)
        env.update(
            {
                "FREIGHTPULSE_DATABASE_URL": settings.database_url,
                "FREIGHTPULSE_KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
                "FREIGHTPULSE_TOPIC_PREFIX": settings.topic_prefix,
                "FREIGHTPULSE_PROCESSOR_GROUP_ID": settings.processor_group_id,
                "FREIGHTPULSE_KAFKA_SESSION_TIMEOUT_MS": "10000",
            }
        )
        if sample_dir is not None:
            env["FREIGHTPULSE_LATENCY_SAMPLE_FILE"] = str(sample_dir / f"lat-{i}.txt")
        args = [sys.executable, "-m", "freightpulse.processing.processor", "--metrics-port", "0"]
        procs.append(
            subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        )
    return procs


def wait_for_stable_group(settings: Settings, members: int, timeout_s: float = 120.0) -> float:
    """Block until the group is Stable with ``members`` members that all own partitions, and has
    stayed so for 3 s. Returns the seconds waited."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    group = settings.processor_group_id
    start = time.monotonic()
    stable_since: float | None = None
    while time.monotonic() - start < timeout_s:
        try:
            desc = admin.describe_consumer_groups([group])[group].result(timeout=10)
            ok = (
                desc.state == ConsumerGroupState.STABLE
                and len(desc.members) == members
                and all(m.assignment.topic_partitions for m in desc.members)
            )
        except Exception:  # group not registered yet
            ok = False
        if ok:
            stable_since = stable_since or time.monotonic()
            if time.monotonic() - stable_since >= 3:
                return time.monotonic() - start
        else:
            stable_since = None
        time.sleep(0.5)
    raise TimeoutError(f"group {group} did not stabilise with {members} members")


def stop(procs: list[subprocess.Popen[bytes]]) -> None:
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()


def sim_config(shipments: int, seed: int, id_offset: int) -> SimConfig:
    start = datetime.now(UTC).replace(second=0, microsecond=0)
    return SimConfig(
        shipments=shipments, seed=seed, start=start, ramp=timedelta(hours=1), id_offset=id_offset
    )


def resource_snapshot(procs: list[subprocess.Popen[bytes]]) -> dict[str, str]:
    """CPU usage of the containers and processor processes at one instant (for diagnosis)."""
    snap: dict[str, str] = {}
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        for line in out.splitlines():
            name, cpu = line.rsplit(" ", 1)
            if name.startswith("freightpulse-") and ("postgres" in name or "redpanda" in name):
                snap[name.removeprefix("freightpulse-").removesuffix("-1")] = cpu
    except (OSError, subprocess.SubprocessError):
        pass
    pids = ",".join(str(p.pid) for p in procs)
    try:
        cpus = subprocess.run(
            ["ps", "-o", "%cpu=", "-p", pids], capture_output=True, text=True, timeout=10
        ).stdout.split()
        snap["processors"] = " + ".join(f"{float(x):.0f}%" for x in cpus)
    except (OSError, subprocess.SubprocessError, ValueError):
        pass
    return snap


def start_group(
    db_url: str, bootstrap: str, engine: Engine, prefix: str, n: int, sample_dir: Path | None
) -> tuple[Settings, list[subprocess.Popen[bytes]], float]:
    settings = bench_settings(db_url, bootstrap, prefix)
    reset(engine, settings)
    procs = spawn(settings, n, sample_dir)
    waited = wait_for_stable_group(settings, n)
    return settings, procs, waited


# --- experiment A ------------------------------------------------------------------------------


def capacity(
    db_url: str, bootstrap: str, engine: Engine, instances: list[int], backlog: int
) -> dict[str, Any]:
    out: dict[str, Any] = {"backlog_events": backlog, "runs": {}}
    for n in instances:
        settings, procs, waited = start_group(db_url, bootstrap, engine, f"bench.cap{n}.", n, None)
        print(f"[capacity] {n} processor(s): group stable after {waited:.1f}s; producing backlog")
        produced: dict[str, float] = {}

        def produce(s: Settings = settings, sink: dict[str, float] = produced, k: int = n) -> None:
            t0 = time.monotonic()
            st = run(
                s,
                sim_config(6000, 11, 90_000_000 + k * 1_000_000),
                ChaosConfig(seed=11),
                RunOptions(acceleration=0, max_events=backlog, report_every_s=3600),
            )
            sink.update(sent=st["sent"], seconds=time.monotonic() - t0)

        producer = threading.Thread(target=produce)
        producer.start()
        samples: list[tuple[float, int]] = []
        snapshot: dict[str, str] = {}
        start = time.monotonic()
        while time.monotonic() - start < 900:
            rows = committed_rows(engine)
            samples.append((time.monotonic(), rows))
            if not snapshot and rows > backlog // 3:  # mid-drain
                snapshot = resource_snapshot(procs)
            # Done once production has finished and nothing new was committed for 5 s.
            if (
                not producer.is_alive()
                and rows > 0
                and len(samples) > 10
                and rows == samples[-11][1]
            ):
                break
            time.sleep(0.5)
        producer.join()
        stop(procs)
        final = committed_rows(engine)
        first = next(t for t, r in samples if r > 0)
        last = next(t for t, r in samples if r >= final)
        secs = max(0.001, last - first)
        eps = final / secs
        prod_rate = produced["sent"] / produced["seconds"]
        out["producer_rate_eps"] = round(prod_rate)
        out["runs"][str(n)] = {
            "sent": int(produced["sent"]),
            "committed_rows": final,
            "seconds": round(secs, 2),
            "events_per_sec": round(eps),
            "group_stable_after_s": round(waited, 1),
            "cpu_mid_drain": snapshot,
        }
        print(
            f"[capacity] {n} processor(s): {final:,} rows committed in {secs:.1f}s "
            f"= {eps:,.0f} events/s (producer {prod_rate:,.0f}/s)"
        )
        delete_topics(settings)
    return out


# --- experiment B ------------------------------------------------------------------------------


def latency_sweep(
    db_url: str,
    bootstrap: str,
    engine: Engine,
    instances: list[int],
    rates: list[int],
    step_s: float,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for n in instances:
        tmp = Path(tempfile.mkdtemp(prefix=f"fp-bench-{n}-"))
        settings, procs, waited = start_group(db_url, bootstrap, engine, f"bench.lat{n}.", n, tmp)
        print(f"[latency] {n} processor(s): group stable after {waited:.1f}s")
        # Short warm-up (connections, caches, partitions); excluded from the results.
        run(
            settings,
            sim_config(50, 3, 80_000_000 + n * 1_000_000),
            ChaosConfig(seed=3),
            RunOptions(acceleration=0, max_rate=500, duration_s=5, report_every_s=3600),
        )
        time.sleep(3)
        # One long-lived producer for every step: a fresh producer per step would stamp
        # produced_at on its first messages before its broker connection is even up.
        shared = EventProducer(settings)
        shared.send(settings.alerts_topic, "warmup", b"{}")
        shared.flush()
        steps = []
        for i, rate in enumerate(rates):
            t_start = time.time()
            stats = run(
                settings,
                sim_config(3000, 100 + i, 81_000_000 + n * 1_000_000 + i * 50_000),
                ChaosConfig(seed=100 + i),
                RunOptions(acceleration=0, max_rate=rate, duration_s=step_s, report_every_s=3600),
                producer=shared,
            )
            t_end = time.time()
            time.sleep(4)  # drain before the next step
            steps.append(
                {"target": rate, "sent": stats["sent"], "t_start": t_start, "t_end": t_end}
            )
            print(f"[latency] {n} proc, target {rate}/s: sent {stats['sent']:,}")
        stop(procs)
        samples: list[tuple[float, float]] = []
        for f in tmp.glob("lat-*.txt"):
            for line in f.read_text().splitlines():
                a, b = line.split()
                samples.append((float(a), float(b)))
        rows = []
        for st in steps:
            # Events produced in the step after the ramp; the drain tail is included so that
            # events produced near the end of the step are not dropped from the percentiles.
            lo, hi = st["t_start"] + RAMP_S, st["t_end"] + 4
            lat = [lv for t, lv in samples if lo <= t <= hi]
            duration = st["t_end"] - st["t_start"]
            window = [t for t, _ in samples if lo <= t <= st["t_end"]]
            offered = st["sent"] / max(0.001, duration)
            achieved = len(window) / max(0.001, duration - RAMP_S)
            rows.append(
                {
                    "target_eps": st["target"],
                    "offered_eps": round(offered),
                    "committed_eps": round(achieved),
                    "samples": len(lat),
                    "p50_ms": round(pct(lat, 0.50) * 1000, 1),
                    "p95_ms": round(pct(lat, 0.95) * 1000, 1),
                    "p99_ms": round(pct(lat, 0.99) * 1000, 1),
                    "max_ms": round(max(lat) * 1000, 1) if lat else None,
                }
            )
            print(
                f"[latency] {n} proc @ {offered:,.0f}/s offered: committed {achieved:,.0f}/s, "
                f"p50 {rows[-1]['p50_ms']} ms, p95 {rows[-1]['p95_ms']} ms, "
                f"p99 {rows[-1]['p99_ms']} ms"
            )
        out[str(n)] = rows
        delete_topics(settings)
    return out


def render(res: dict[str, Any]) -> str:
    cap, lat, spec = res["capacity"], res["latency"], res["machine"]
    lines = [
        "# Benchmarks",
        "",
        f"> Generated by `make bench` (`src/freightpulse/perf/bench.py`) on {res['generated_at']}."
        " Every number below was measured in that run. Raw data: `docs/benchmarks_results.json`.",
        "",
        "## Machine",
        "",
        f"- Host: {spec['host']}; {spec['os']}; Python {platform.python_version()}.",
        f"- Docker VM (Redpanda + Postgres): {spec['docker_vm']}.",
        "- Processors run as host subprocesses (one Python process each), talking to Redpanda and "
        "Postgres in Docker over localhost. Everything shares one laptop, so treat the numbers as "
        "a lower bound for dedicated hardware.",
        "",
        "## Workload",
        "",
        "Simulator events with the default chaos mix (2% duplicates, 3% out-of-order, 0.5% "
        "malformed, 1% GPS dropout); about 95% GPS pings and 5% lifecycle events. Processor batch "
        "size 1000, one Postgres transaction per batch, offsets committed after the DB commit. "
        "Before every measurement the harness waits until the consumer group is Stable with all "
        "N members owning partitions, so no rebalance falls inside a measured window.",
        "",
        "## A. Sustained throughput (backlog drain)",
        "",
        f"With the group stable, one producer wrote a backlog of {cap['backlog_events']:,} events "
        f"(~{cap['producer_rate_eps']:,} events/s, well above consumer capacity). Throughput = "
        "rows committed / time from the first to the last commit. Fewer rows are committed than "
        "events sent because duplicates, malformed and dead-lettered events never reach the log.",
        "",
        "| Processors | Events sent | Rows committed | Seconds | **Events/s** |",
        "|---:|---:|---:|---:|---:|",
    ]
    for n, r in cap["runs"].items():
        lines.append(
            f"| {n} | {r['sent']:,} | {r['committed_rows']:,} | {r['seconds']} | "
            f"**{r['events_per_sec']:,}** |"
        )
    lines += ["", "CPU at mid-drain (one `docker stats` / `ps` sample; 100% = one core):", ""]
    for n, r in cap["runs"].items():
        snap = r.get("cpu_mid_drain") or {}
        lines.append(f"- {n} processor(s): " + ", ".join(f"{k} {v}" for k, v in snap.items()))
    one, three = cap["runs"].get("1"), cap["runs"].get("3")
    if one and three:
        ratio = three["events_per_sec"] / one["events_per_sec"]
        lines += ["", f"Scaling from 1 to 3 instances: **{ratio:.2f}x**."]
    lines += [
        "",
        "## B. End-to-end latency vs offered load",
        "",
        "Latency = Postgres commit time minus the producer's `produced_at`. That covers "
        "producer batching, the broker, consumer batching, validation, the reducer, and the DB "
        f"transaction. Percentiles come from exact per-event samples. The first {RAMP_S:.0f} s of "
        "each step are excluded as ramp-up.",
        "",
    ]
    for n, rows in lat.items():
        lines += [
            f"### {n} processor{'s' if n != '1' else ''}",
            "",
            "| Target ev/s | Offered ev/s | Committed ev/s | p50 | p95 | p99 | max | samples |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            lines.append(
                f"| {r['target_eps']:,} | {r['offered_eps']:,} | {r['committed_eps']:,} | "
                f"{r['p50_ms']:,} ms | {r['p95_ms']:,} ms | {r['p99_ms']:,} ms | "
                f"{(r['max_ms'] or 0):,} ms | {r['samples']:,} |"
            )
        lines.append("")
    lines += [
        "## Reading the numbers",
        "",
        "- **The batching window sets the latency floor.** The processor calls `consume()` for up "
        "to 1,000 messages or 0.25 s, whichever comes first, then commits one transaction. At "
        "low rates a batch waits for the timeout; at higher rates it fills sooner. So latency "
        "doesn't rise steadily with load until the group nears saturation. Smaller batches or a "
        "shorter timeout trade throughput for latency.",
        "- When committed ev/s keeps up with offered ev/s and p99 stays flat, the group is below "
        "saturation. Above the capacity measured in A, a backlog builds and latency climbs with it.",
        "- **Per-instance ceiling.** A processor handles one batch at a time: consume, one "
        "Postgres transaction, then a synchronous offset commit. If its CPU (snapshot above) is "
        "well below 100% while throughput plateaus, the instance is bound by those serial round "
        "trips rather than by CPU, and more instances overlap the waits.",
        "- **Group scaling.** Three instances get two partitions of each topic (range "
        "assignor). They all write to one Postgres in the same 4-CPU Docker VM as Redpanda, so "
        "the snapshot's Postgres CPU is the resource to watch as instances are added.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument(
        "--database-url",
        default="postgresql+psycopg://freightpulse:freightpulse@localhost:5433/freightpulse_bench",
    )
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--backlog", type=int, default=500_000)
    p.add_argument("--rates", default="1000,2000,4000,6000,8000")
    p.add_argument("--step-seconds", type=float, default=20.0)
    p.add_argument("--instances", default="1,3")
    p.add_argument(
        "--render-only",
        action="store_true",
        help="re-render docs/benchmarks.md from docs/benchmarks_results.json",
    )
    a = p.parse_args()
    if a.render_only:
        DOC.write_text(render(json.loads(RESULTS.read_text())))
        print(f"re-rendered {DOC.relative_to(ROOT)} from {RESULTS.relative_to(ROOT)}")
        return
    instances = [int(x) for x in a.instances.split(",")]
    rates = [int(x) for x in a.rates.split(",")]

    ensure_database(a.database_url)
    run_migrations(a.database_url)
    engine = make_engine(a.database_url, pool_size=2)
    run_maintenance(engine, days_ahead=14, retention_days=3650)
    seed_reference_data(engine)

    res: dict[str, Any] = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "machine": machine_specs(),
    }
    res["capacity"] = capacity(a.database_url, a.bootstrap, engine, instances, a.backlog)
    res["latency"] = latency_sweep(
        a.database_url, a.bootstrap, engine, instances, rates, a.step_seconds
    )
    RESULTS.write_text(json.dumps(res, indent=2) + "\n")
    DOC.write_text(render(res))
    print(f"wrote {DOC.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
