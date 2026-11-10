"""``make sql-perf``: run the SQL before/after case studies and write ``docs/sql_performance.md``.

Every number in the generated document comes from ``EXPLAIN (ANALYZE, BUFFERS)`` executed by this
script against the ``seed-large`` database: each variant is warmed up once and then run
``--runs`` times, and the median "Execution Time" is reported with the plan of that median run.
At the end the schema is restored to exactly the migration's index set.
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import statistics
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Engine, text

from freightpulse.storage.db import make_engine

ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs" / "sql_performance.md"
RESULTS = ROOT / "docs" / "sql_performance_results.json"

EXEC_RE = re.compile(r"Execution Time: ([\d.]+) ms")
PLAN_RE = re.compile(r"Planning Time: ([\d.]+) ms")
NODE_RE = re.compile(
    r"(Parallel )?(Seq Scan|Index Only Scan|Index Scan|Bitmap Heap Scan|Bitmap Index Scan)"
    r"(?: Backward)? (?:using \S+ )?on (\S+)"
)


@dataclass
class Variant:
    label: str
    query: str
    setup: list[str] = field(default_factory=list)
    session: list[str] = field(default_factory=list)


@dataclass
class Case:
    key: str
    title: str
    question: str
    change: str
    variants: list[Variant]
    discussion: str
    index_sizes: list[str] = field(default_factory=list)


@dataclass
class Measured:
    label: str
    median_ms: float
    runs_ms: list[float]
    planning_ms: float
    plan: str
    scans: dict[str, int]
    partitions_scanned: int


# Indexes as created by the migrations (restored at the end).
CANONICAL_INDEXES = {
    "ix_events_shipment_time": "CREATE INDEX IF NOT EXISTS ix_events_shipment_time "
    "ON shipment_events (shipment_id, event_time)",
    "ix_pings_shipment_time": "CREATE INDEX IF NOT EXISTS ix_pings_shipment_time "
    "ON gps_pings (shipment_id, event_time)",
    "ix_pings_time_brin": "CREATE INDEX IF NOT EXISTS ix_pings_time_brin ON gps_pings "
    "USING brin (event_time) WITH (pages_per_range = 32)",
    "ix_alerts_active_shipment": "CREATE INDEX IF NOT EXISTS ix_alerts_active_shipment "
    "ON alerts (shipment_id) WHERE resolved_at IS NULL",
}
NON_CANONICAL = ["ix_pings_time_btree", "ix_alerts_shipment_full"]


def _sample_ids(conn: Connection) -> dict[str, Any]:
    window_start = conn.execute(
        text(
            "SELECT date_trunc('hour', min(event_time)) + interval '15 days 10 hours' "
            "FROM gps_pings"
        )
    ).scalar_one()
    sid = conn.execute(
        text("SELECT shipment_id FROM shipments ORDER BY shipment_id OFFSET 100000 LIMIT 1")
    ).scalar_one()
    timeline_sid = conn.execute(
        text("SELECT shipment_id FROM shipments ORDER BY shipment_id OFFSET 123456 LIMIT 1")
    ).scalar_one()
    alert_sid = conn.execute(
        text(
            "SELECT shipment_id FROM alerts WHERE resolved_at IS NULL "
            "ORDER BY shipment_id OFFSET 1000 LIMIT 1"
        )
    ).scalar_one()
    return {
        "window_start": window_start,
        "keyset_after": sid,
        "timeline_sid": timeline_sid,
        "alert_sid": alert_sid,
        "carrier": "CAR-007",
    }


def build_cases(p: dict[str, Any]) -> list[Case]:
    ws = p["window_start"].isoformat()
    timeline_q = (
        "SELECT event_id, event_type, event_time, payload FROM shipment_events "
        f"WHERE shipment_id = '{p['timeline_sid']}' ORDER BY event_time"
    )
    gps_q = (
        "SELECT count(*), round(avg(speed_mph)::numeric, 1) FROM gps_pings "
        f"WHERE event_time >= '{ws}' AND event_time < '{ws}'::timestamptz + interval '2 hours'"
    )
    list_cols = (
        "SELECT c.shipment_id, c.status, s.customer_id, s.carrier_id, s.lane_id, "
        "l.origin_facility_id, l.dest_facility_id, c.late_risk, c.eta, c.miles_remaining, "
        "c.last_ping_at, c.updated_at FROM shipment_current_state c "
        "JOIN shipments s ON s.shipment_id = c.shipment_id JOIN lanes l ON l.lane_id = s.lane_id "
    )
    raw_scorecard = f"""
WITH t AS (
  SELECT (event_time AT TIME ZONE 'UTC')::date AS day,
         count(*) FILTER (WHERE event_type = 'shipment.tendered') AS tenders,
         count(*) FILTER (WHERE event_type = 'shipment.tender_accepted') AS accepted,
         count(*) FILTER (WHERE event_type = 'shipment.tender_rejected') AS rejected
  FROM shipment_events
  WHERE event_type IN ('shipment.tendered', 'shipment.tender_accepted', 'shipment.tender_rejected')
    AND payload->>'carrier_id' = '{p["carrier"]}'
    AND event_time >= now() - interval '30 days'
  GROUP BY 1
), d AS (
  SELECT (st.arrived_at AT TIME ZONE 'UTC')::date AS day,
         avg((st.arrived_at <= st.appt_end)::int) AS on_time
  FROM shipment_stops st JOIN shipments s USING (shipment_id)
  WHERE s.carrier_id = '{p["carrier"]}' AND st.stop_type = 'DELIVERY'
    AND st.arrived_at >= now() - interval '30 days'
  GROUP BY 1
)
SELECT t.day, t.tenders, round(100.0 * t.accepted / NULLIF(t.accepted + t.rejected, 0), 2),
       round(100.0 * d.on_time, 2)
FROM t LEFT JOIN d USING (day) ORDER BY t.day""".strip()
    rollup_scorecard = (
        "SELECT day, tenders, acceptance_rate, on_time_delivery_pct FROM carrier_scorecard_daily "
        f"WHERE carrier_id = '{p['carrier']}' AND day >= (now() - interval '30 days')::date "
        "ORDER BY day"
    )
    alerts_q = (
        f"SELECT * FROM alerts WHERE shipment_id = '{p['alert_sid']}' AND resolved_at IS NULL"
    )

    return [
        Case(
            key="timeline",
            title="Shipment timeline: composite index `(shipment_id, event_time)` vs sequential scan",
            question="`GET /shipments/{id}/timeline`: every lifecycle event of one shipment, ordered.",
            change="`CREATE INDEX ix_events_shipment_time ON shipment_events (shipment_id, event_time)`",
            variants=[
                Variant(
                    "before: no index (seq scan of every partition)",
                    timeline_q,
                    setup=["DROP INDEX IF EXISTS ix_events_shipment_time"],
                ),
                Variant(
                    "after: composite index",
                    timeline_q,
                    setup=[CANONICAL_INDEXES["ix_events_shipment_time"]],
                ),
            ],
            discussion=(
                "The timeline filters on `shipment_id` but the table is partitioned on "
                "`event_time`, so there is no pruning: without an index Postgres reads every "
                "daily partition in full. The composite index turns each partition into a short "
                "index range scan that already returns rows in `event_time` order, so no sort is "
                "needed. The remaining cost is one index probe per partition, which is why "
                "retention (ADR 0004) keeps the partition count bounded."
            ),
            index_sizes=["ix_events_shipment_time"],
        ),
        Case(
            key="gps_range",
            title="GPS time-range scan: partition pruning, B-tree vs BRIN on `event_time`",
            question="Fleet-wide analytics over a 2-hour window of pings (rollups, heat maps).",
            change=(
                "Partition pruning (daily partitions) plus `CREATE INDEX ix_pings_time_brin ON "
                "gps_pings USING brin (event_time) WITH (pages_per_range = 32)`, compared with a "
                "B-tree on the same column"
            ),
            variants=[
                Variant(
                    "before: pruning off, no event_time-leading index (planner full-scans the PK)",
                    gps_q,
                    setup=[
                        "DROP INDEX IF EXISTS ix_pings_time_brin",
                        "DROP INDEX IF EXISTS ix_pings_time_btree",
                        # Otherwise the planner full-scans this composite index filtering on its
                        # second column, which is not the "no time index" baseline we want.
                        "DROP INDEX IF EXISTS ix_pings_shipment_time",
                    ],
                    session=["SET enable_partition_pruning = off"],
                ),
                Variant("pruning only (1 partition, seq scan)", gps_q),
                Variant(
                    "pruning + B-tree on event_time",
                    gps_q,
                    setup=[
                        "CREATE INDEX IF NOT EXISTS ix_pings_time_btree ON gps_pings (event_time)"
                    ],
                ),
                Variant(
                    "after: pruning + BRIN on event_time",
                    gps_q,
                    setup=[
                        "DROP INDEX IF EXISTS ix_pings_time_btree",
                        CANONICAL_INDEXES["ix_pings_time_brin"],
                    ],
                ),
            ],
            discussion=(
                "(For a clean baseline the composite `(shipment_id, event_time)` index is dropped "
                "during this case; otherwise the planner full-scans it, filtering on its second "
                "column.) Partition pruning does most of the work: a 2-hour predicate on `event_time` "
                "touches one daily partition instead of all of them. Inside that partition a "
                "time index avoids reading the other ~22 hours. Because pings are appended in "
                "time order, BRIN's per-block-range min/max summaries are tight, so BRIN skips "
                "almost as well as a B-tree while being a tiny fraction of its size (see the "
                "size table). That is why the migration ships BRIN and not a B-tree on "
                "`event_time` for this append-only table."
            ),
            index_sizes=["ix_pings_time_brin", "ix_pings_time_btree"],
        ),
        Case(
            key="keyset",
            title="Shipment list: keyset pagination vs `OFFSET 100000`",
            question="`GET /shipments` deep in the result set (page 2,001 of 50).",
            change="`WHERE c.shipment_id > :cursor ORDER BY c.shipment_id LIMIT 50` instead of `OFFSET`",
            variants=[
                Variant(
                    "before: OFFSET 100000 LIMIT 50",
                    list_cols + "ORDER BY c.shipment_id OFFSET 100000 LIMIT 50",
                ),
                Variant(
                    "keyset, cursor on one join input only",
                    list_cols + f"WHERE c.shipment_id > '{p['keyset_after']}' "
                    "ORDER BY c.shipment_id LIMIT 50",
                ),
                Variant(
                    "after: keyset, cursor on both join inputs (what the API runs)",
                    list_cols + f"WHERE c.shipment_id > '{p['keyset_after']}' "
                    f"AND s.shipment_id > '{p['keyset_after']}' "
                    "ORDER BY c.shipment_id LIMIT 50",
                ),
            ],
            discussion=(
                "`OFFSET n` still has to produce and discard the first n joined rows, so its cost "
                "grows linearly with the page number. A keyset cursor seeks directly to the last "
                "seen `shipment_id` through the primary-key index and reads only the 50 rows it "
                "returns, so every page costs the same. Keyset pages are also stable while rows "
                "are being inserted, which matters for a live fleet. One subtlety showed up in "
                "the measurements: with the cursor only on `shipment_current_state`, the merge "
                "join still read `shipments` from its first row, because Postgres propagates "
                "equality across join keys but not range predicates. Repeating the bound on the "
                "other join input (`AND s.shipment_id > :cursor`) lets both index scans start at "
                "the cursor; the API query does exactly that."
            ),
        ),
        Case(
            key="scorecard",
            title="Carrier scorecard: raw aggregation over events vs the rollup table",
            question="`GET /carriers/{id}/scorecard?days=30`.",
            change="Read `carrier_scorecard_daily` (maintained by the jobs service) instead of aggregating raw events",
            variants=[
                Variant(
                    "before: aggregate shipment_events + stops on every request", raw_scorecard
                ),
                Variant("after: read carrier_scorecard_daily", rollup_scorecard),
            ],
            discussion=(
                "The raw query has to scan every tender event in the window (filtering on a "
                "JSONB field) and join stops to shipments. The rollup moves that work to the "
                "jobs service, which recomputes only a recent window with an idempotent "
                "`INSERT ... ON CONFLICT DO UPDATE`, so the API reads about 30 pre-aggregated "
                "rows by primary key. The trade-off is freshness: the scorecard lags by up to "
                "one rollup interval (60-120 s), which is fine for a daily metric."
            ),
        ),
        Case(
            key="partial_alerts",
            title="(Bonus) Partial index on active alerts",
            question="Active alerts for a shipment (detail page badge, scanners' resolve checks).",
            change="`CREATE INDEX ix_alerts_active_shipment ON alerts (shipment_id) WHERE resolved_at IS NULL`",
            variants=[
                Variant(
                    "before: no index on alerts.shipment_id",
                    alerts_q,
                    setup=[
                        "DROP INDEX IF EXISTS ix_alerts_active_shipment",
                        "DROP INDEX IF EXISTS ix_alerts_shipment_full",
                    ],
                ),
                Variant(
                    "full index on alerts(shipment_id), for size comparison",
                    alerts_q,
                    setup=[
                        "CREATE INDEX IF NOT EXISTS ix_alerts_shipment_full ON alerts (shipment_id)"
                    ],
                ),
                Variant(
                    "after: partial index WHERE resolved_at IS NULL",
                    alerts_q,
                    setup=[
                        "DROP INDEX IF EXISTS ix_alerts_shipment_full",
                        CANONICAL_INDEXES["ix_alerts_active_shipment"],
                    ],
                ),
            ],
            discussion=(
                "Only about 3% of alerts are active, and active alerts are what operations "
                "look at. A partial index stores only those rows: it is as fast as a full index "
                "for this query but a fraction of its size, and resolved alerts never cost index "
                "maintenance once resolved rows leave the index."
            ),
            index_sizes=["ix_alerts_active_shipment", "ix_alerts_shipment_full"],
        ),
    ]


def index_size(conn: Connection, name: str) -> int | None:
    """Total size of an index, summed over partitions for partitioned indexes."""
    row = conn.execute(
        text(
            """
            SELECT CASE WHEN c.relkind = 'I'
                        THEN (SELECT COALESCE(sum(pg_relation_size(t.relid)), 0)
                              FROM pg_partition_tree(c.oid) t)
                        ELSE pg_relation_size(c.oid) END
            FROM pg_class c
            WHERE c.relname = :n AND c.relkind IN ('i', 'I')
            """
        ),
        {"n": name},
    ).scalar()
    return int(row) if row else None


def measure(engine: Engine, v: Variant, runs: int) -> Measured:
    with engine.begin() as conn:
        for stmt in v.setup:
            conn.execute(text(stmt))
    with engine.begin() as conn:
        conn.execute(text("ANALYZE alerts"))  # cheap; others were analyzed by seed-large
    times: list[tuple[float, float, str]] = []
    with engine.connect() as conn:
        for stmt in v.session:
            conn.execute(text(stmt))
        conn.execute(text(f"EXPLAIN (ANALYZE, BUFFERS) {v.query}")).all()  # warm-up
        for _ in range(runs):
            plan = "\n".join(
                r[0] for r in conn.execute(text(f"EXPLAIN (ANALYZE, BUFFERS) {v.query}"))
            )
            exec_ms = float(EXEC_RE.search(plan).group(1))  # type: ignore[union-attr]
            plan_ms = float(PLAN_RE.search(plan).group(1))  # type: ignore[union-attr]
            times.append((exec_ms, plan_ms, plan))
        conn.rollback()
    times.sort(key=lambda x: x[0])
    med = times[len(times) // 2]
    scans: dict[str, int] = {}
    tables: set[str] = set()
    for m in NODE_RE.finditer(med[2]):
        node = (m.group(1) or "") + m.group(2)
        scans[node] = scans.get(node, 0) + 1
        if m.group(2) != "Bitmap Index Scan":
            tables.add(m.group(3))
    return Measured(
        label=v.label,
        median_ms=round(statistics.median(t[0] for t in times), 3),
        runs_ms=[round(t[0], 3) for t in times],
        planning_ms=round(med[1], 3),
        plan=med[2],
        scans=scans,
        partitions_scanned=len(tables),
    )


def restore_indexes(engine: Engine) -> None:
    with engine.begin() as conn:
        for name in NON_CANONICAL:
            conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        for stmt in CANONICAL_INDEXES.values():
            conn.execute(text(stmt))


def machine_specs() -> dict[str, str]:
    def sh(cmd: list[str]) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return "n/a"

    cpu = sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor()
    mem = sh(["sysctl", "-n", "hw.memsize"])
    docker = sh(["docker", "info", "--format", "{{.NCPU}} CPUs, {{.MemTotal}} bytes"])
    return {
        "host": f"{cpu}, {sh(['sysctl', '-n', 'hw.ncpu'])} cores, {int(mem) / 2**30:.0f} GiB RAM"
        if mem.isdigit()
        else cpu,
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "docker_vm": docker,
    }


def _fmt_bytes(n: int | None) -> str:
    if n is None:
        return "n/a"
    for unit in ("B", "kB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n = n / 1024  # type: ignore[assignment]
    return f"{n:.1f} TB"


def render(
    cases: list[Case],
    results: dict[str, list[Measured]],
    sizes: dict[str, int | None],
    dataset: dict[str, Any],
    specs: dict[str, str],
    runs: int,
) -> str:
    out: list[str] = []
    w = out.append
    w("# SQL performance case studies\n")
    w(
        "> Generated by `make sql-perf` (`src/freightpulse/perf/sql_cases.py`) on "
        f"{datetime.now(UTC):%Y-%m-%d %H:%M} UTC. Every timing and plan below is real output of "
        "`EXPLAIN (ANALYZE, BUFFERS)` from that run. Nothing is hand-edited.\n"
    )
    w("## Method\n")
    w(
        "- Dataset: `make seed-large` (COPY), in the `freightpulse_perf` database. Row counts: "
        + ", ".join(f"`{k}` {v:,}" for k, v in dataset["counts"].items())
        + ". "
        f"Table sizes: {dataset['sizes']}."
    )
    w(
        f"- Each variant: apply its DDL, warm up once, then run `EXPLAIN (ANALYZE, BUFFERS)` "
        f"{runs} times. The median **execution time** is reported, with the plan of the median run. "
        "The cache is warm, which matches the steady state of an API under load."
    )
    w(
        f"- Host: {specs['host']}; {specs['os']}. Postgres 16 runs in Docker "
        f"({specs['docker_vm']}) with `shared_buffers=512MB`, `work_mem=32MB`, "
        "`random_page_cost=1.1`."
    )
    w("- At the end the script restores exactly the index set created by the migrations.\n")

    w("## Summary\n")
    w("| Case | Before | After | Speedup |")
    w("|---|---:|---:|---:|")
    for c in cases:
        ms = results[c.key]
        before, after = ms[0], ms[-1]
        speed = before.median_ms / after.median_ms if after.median_ms else float("inf")
        w(
            f"| {c.title.split(':')[0]} | {before.median_ms:,.2f} ms | {after.median_ms:,.3f} ms "
            f"| **{speed:,.0f}x** |"
        )
    w("")

    for i, c in enumerate(cases, 1):
        w(f"## {i}. {c.title}\n")
        w(f"**Query purpose:** {c.question}\n")
        w(f"**Change:** {c.change}\n")
        w("```sql")
        w(
            c.variants[-1].query
            if c.key != "keyset"
            else c.variants[0].query + ";\n-- vs\n" + c.variants[-1].query
        )
        w("```\n")
        w("| Variant | Median exec | Planning | Scan nodes | Relations scanned |")
        w("|---|---:|---:|---|---:|")
        for m in results[c.key]:
            nodes = ", ".join(f"{k} x{v}" for k, v in sorted(m.scans.items()))
            w(
                f"| {m.label} | {m.median_ms:,.3f} ms | {m.planning_ms:,.3f} ms | {nodes} | "
                f"{m.partitions_scanned} |"
            )
        w("")
        if c.index_sizes:
            w("| Index | Size (all partitions) |")
            w("|---|---:|")
            for name in c.index_sizes:
                w(f"| `{name}` | {_fmt_bytes(sizes.get(name))} |")
            w("")
        w(c.discussion + "\n")
        for m in results[c.key]:
            w(
                f"<details><summary>Plan: {m.label} (runs: "
                f"{', '.join(f'{x:g}' for x in m.runs_ms)} ms)</summary>\n"
            )
            plan = m.plan
            lines = plan.splitlines()
            if len(lines) > 80:  # e.g. 40 partitions without pruning: keep head and tail
                plan = "\n".join(
                    [*lines[:45], f"   ... ({len(lines) - 70} lines elided) ...", *lines[-25:]]
                )
            w("```")
            w(plan)
            w("```\n")
            w("</details>\n")

    w("## Index inventory and justification\n")
    w("Every index in the migrations, and the query it exists for:\n")
    w("| Index | Definition | Serves |")
    w("|---|---|---|")
    for name, definition, why in INDEX_INVENTORY:
        w(f"| `{name}` | `{definition}` | {why} |")
    w("")
    return "\n".join(out)


INDEX_INVENTORY = [
    ("shipments_pkey", "shipments (shipment_id)", "Lookups and keyset pagination order (case 3)."),
    (
        "ix_shipments_carrier",
        "shipments (carrier_id, shipment_id)",
        "`GET /shipments?carrier_id=` in keyset order; carrier joins in rollups.",
    ),
    ("ix_shipments_lane", "shipments (lane_id)", "FK lookups; lane rollups."),
    (
        "shipment_stops_pkey",
        "shipment_stops (shipment_id, stop_sequence)",
        "Load a shipment's stops (processor batch load, detail endpoint).",
    ),
    (
        "ix_stops_open_dwell",
        "shipment_stops (arrived_at) WHERE arrived_at IS NOT NULL AND departed_at IS NULL",
        "Detention scanner: only trucks currently at a dock are indexed (partial).",
    ),
    (
        "ix_stops_facility_arrived",
        "shipment_stops (facility_id, arrived_at)",
        "Facility dwell rollup and per-facility analytics.",
    ),
    (
        "shipment_current_state_pkey",
        "shipment_current_state (shipment_id)",
        "Processor state load and version-checked bulk update; API detail.",
    ),
    (
        "ix_state_status_id",
        "shipment_current_state (status, shipment_id)",
        "`GET /shipments?status=` with keyset order; status counts.",
    ),
    (
        "ix_state_at_risk",
        "shipment_current_state (late_risk, shipment_id) WHERE late_risk <> 'LOW'",
        "`?late_risk=` and the at-risk count; ~85% LOW rows are not indexed.",
    ),
    (
        "ix_state_in_transit_ping",
        "shipment_current_state (last_ping_at) WHERE status = 'IN_TRANSIT'",
        "Stale-tracking scanner (partial).",
    ),
    (
        "shipment_events_pkey",
        "shipment_events (event_id, event_time)",
        "Idempotency: `ON CONFLICT DO NOTHING` and the processor's already-logged check.",
    ),
    ("ix_events_shipment_time", "shipment_events (shipment_id, event_time)", "Timeline (case 1)."),
    ("gps_pings_pkey", "gps_pings (event_id, event_time)", "Idempotency for pings."),
    (
        "ix_pings_shipment_time",
        "gps_pings (shipment_id, event_time)",
        "`/track?since=` breadcrumbs.",
    ),
    (
        "ix_pings_time_brin",
        "gps_pings USING brin (event_time)",
        "Fleet-wide time-range scans (case 2).",
    ),
    ("alerts_dedupe_key_key", "alerts (dedupe_key) UNIQUE", "Idempotent alert sink upserts."),
    (
        "ix_alerts_active_shipment",
        "alerts (shipment_id) WHERE resolved_at IS NULL",
        "Active alerts per shipment (case 5).",
    ),
    (
        "ix_alerts_type_triggered",
        "alerts (alert_type, triggered_at DESC)",
        "`GET /alerts?type=` newest first.",
    ),
    ("ix_alerts_triggered", "alerts (triggered_at DESC)", "`GET /alerts` newest first."),
    (
        "ix_parked_shipment / ix_parked_first_seen",
        "parked_events (shipment_id) / (first_seen_at)",
        "Unparking when `shipment.created` arrives; expiring to the DLQ.",
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    engine = make_engine(args.database_url, pool_size=2)
    with engine.connect() as conn:
        params = _sample_ids(conn)
        counts = {
            t: conn.execute(text(f"SELECT count(*) FROM {t}")).scalar_one()
            for t in ("shipments", "shipment_events", "gps_pings", "alerts")
        }
        sizes_tbl = {
            t: conn.execute(
                text(
                    "SELECT pg_size_pretty(sum(pg_total_relation_size(relid))) "
                    "FROM pg_partition_tree(:t)"
                ),
                {"t": t},
            ).scalar_one()
            for t in ("shipment_events", "gps_pings")
        }
        parts = conn.execute(
            text("SELECT count(*) FROM pg_partition_tree('gps_pings') WHERE isleaf")
        ).scalar_one()
    dataset = {
        "counts": counts,
        "sizes": f"`gps_pings` {sizes_tbl['gps_pings']} in {parts} daily partitions, "
        f"`shipment_events` {sizes_tbl['shipment_events']}",
    }
    cases = build_cases(params)
    results: dict[str, list[Measured]] = {}
    sizes: dict[str, int | None] = {}
    try:
        for c in cases:
            results[c.key] = []
            for v in c.variants:
                m = measure(engine, v, args.runs)
                results[c.key].append(m)
                with engine.connect() as conn:
                    for name in c.index_sizes:
                        size = index_size(conn, name)
                        if size:
                            sizes[name] = size
                print(f"{c.key:15s} {v.label:60s} median {m.median_ms:10.3f} ms")
    finally:
        restore_indexes(engine)
    specs = machine_specs()
    DOC.write_text(render(cases, results, sizes, dataset, specs, args.runs))
    RESULTS.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "dataset": dataset,
                "machine": specs,
                "index_sizes_bytes": sizes,
                "results": {
                    k: [
                        {"label": m.label, "median_ms": m.median_ms, "runs_ms": m.runs_ms}
                        for m in v
                    ]
                    for k, v in results.items()
                },
            },
            indent=2,
            default=str,
        )
        + "\n"
    )
    print(f"wrote {DOC.relative_to(ROOT)} and {RESULTS.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
