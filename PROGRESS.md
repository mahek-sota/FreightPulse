# FreightPulse build log

Running log of each build phase (SPEC.md section 14): what was built, how it was verified, and
any decisions or deviations. Overrides from the build request: phases run back to back with no
stop for review, and no git commands are run (the owner handles git).

## Phase checklist

- [x] 1. Scaffold
- [x] 2. Domain
- [x] 3. Storage
- [x] 4. Simulator
- [x] 5. Processor
- [x] 6. Alerts and rollups
- [x] 7. API
- [x] 8. Observability
- [x] 9. Performance
- [x] 10. Polish

## Environment

- macOS 26.5.2, Apple M5 Pro (15 cores), 24 GB RAM. Docker Desktop 24.0.5 with Compose v2.20.2;
  the Docker VM has 4 CPUs and about 4 GB RAM.
- Python 3.12.14 via `uv` 0.12.15. Resolved versions are in `uv.lock`.
- No task-list tool was available in this session, so this checklist is the phase tracker.

---

## Phase 1: Scaffold

**Built**
- `pyproject.toml` (uv, hatchling, src layout), `uv.lock`, ruff and mypy (`strict`, pydantic plugin) config.
- `src/freightpulse/config.py`: `pydantic-settings` `Settings` with prefix `FREIGHTPULSE_`. Topic names
  accept an optional `topic_prefix` so tests and benchmarks can use isolated topics on the same broker.
- `docker-compose.yml`: Redpanda v24.2.7, Redpanda Console v2.7.2, PostgreSQL 16, and a one-shot `init`
  service. App services, Prometheus and Grafana are added in later phases.
- `Dockerfile` (python:3.12-slim + uv, frozen lockfile), `.dockerignore`, `.gitignore`, `.env.example`.
- `Makefile` covering the whole lifecycle. Targets are filled in as phases land.
- `.pre-commit-config.yaml` (hygiene hooks, ruff check/format, mypy) and `.github/workflows/ci.yml`
  (lint+mypy, unit tests, integration tests with a coverage gate).
- `processing/topics.py` creates topics explicitly (6/6/3/1 partitions). `bootstrap.py` is the
  init entrypoint.
- `domain/ruff.toml` bans Kafka, SQLAlchemy, psycopg and network imports inside `domain/` so the
  layer stays pure. A unit test also checks this with an AST scan.

**Verified**
- `uv sync` installed all dependencies. `ruff check`, `ruff format --check` and `mypy src` are clean.
- `docker compose up -d redpanda console postgres` brought everything up healthy.
  `python -m freightpulse.bootstrap` created the 4 topics, and `rpk topic list` showed
  partitions 6/6/3/1.

**Decisions / deviations**
- **Postgres host port is 5433, not 5432.** A local Postgres.app already listens on 5432. Inside
  the compose network the port is still 5432.
- Redpanda rejected `--set redpanda.auto_create_topics_enabled=false` as a start flag, so I
  removed it. Topics are always created explicitly by `bootstrap` (`init` service) before
  anything produces, and producers set `allow.auto.create.topics=false`.
- Dev dependencies added beyond SPEC.md, each needed:
  - `pytest-cov`: measures the required 80% coverage on domain and processing.
  - `httpx`: required by FastAPI's `TestClient` for the API tests.
  - `uvicorn[standard]` (an extra, not a new package) pulls in `websockets`, which uvicorn needs
    to serve the `/ws/alerts` WebSocket.
- `make down` runs `docker compose down -v`, which deletes volumes so you get a truly clean
  state. `make stop` keeps the data.

---

## Phase 2: Domain (pure, no I/O)

**Built** (`src/freightpulse/domain/`)
- `events.py`: `EventEnvelope` (spec 4.1) plus a strict, typed Pydantic payload model for each of the
  9 event types. `parse_event()` validates the envelope and payload in one pass and raises
  `EventValidationError`, which goes to the DLQ as `SCHEMA_VALIDATION`. Timestamps are normalised to UTC.
  Envelope validation also rejects unsupported `schema_version` values, extra fields, and
  `event_time` more than 30 days from `produced_at`.
- `state_machine.py`: the state machine from spec 4.3, built on ranks. Forward moves are always valid,
  TENDERED and REJECTED can move sideways to each other, backward moves with a newer event time are
  `INVALID`, and EXCEPTION is an overlay that remembers the status underneath it.
- `shipment_state.py`: the pure reducer `apply_event(state, event) -> ApplyResult`. It handles
  out-of-order events, stop backfill, impossible-GPS-jump detection, ETA refresh, and late-risk
  alerts that fire only when the level changes. The processor calls only this function.
- `eta.py`: speed EWMA (pings under 5 mph are ignored) with EW variance, an HOS clock (11h driving,
  then a 10h rest), ETA with HOS breaks added, `eta_confidence`, and late-risk classification.
- `geo.py`: haversine distance, the 1.18 road circuity factor, great-circle interpolation, and bearing.
- `alerts.py`: the `AlertMessage` contract with deterministic `dedupe_key` and `alert_id = uuid5(key)`.
  It also holds the detention (2h free time; MEDIUM, HIGH and CRITICAL at 2, 3 and 4h; $75/h),
  stale-tracking (>60 min) and late-risk rules.

**Verified**
- 84 tests in `tests/unit/` pass. Hypothesis properties:
  - random event sequences never regress status, and rejected events never change state;
  - any shuffle of a valid lifecycle converges to the same order-independent state as the ordered
    sequence;
  - duplicates, deduplicated by `event_id`, are idempotent;
  - haversine is symmetric and satisfies the triangle inequality, and the HOS break count is minimal.
- Domain coverage is **99%** (`pytest --cov=freightpulse.domain`). ruff and `mypy --strict` are clean.
- An AST test confirms `domain/` imports no Kafka, DB or network modules.

**Decisions / deviations** (also recorded in ADR 0003)
- **Separate event-time guards.** Status uses `status_event_time` and position uses
  `last_ping_at`, rather than one `last_event_time` guard for everything as spec 6.2 literally says.
  The two topics are consumed independently, so a ping is often processed just after a slightly
  newer lifecycle event. A single guard would wrongly drop that ping. `last_event_time` is still
  kept (max event time seen) and drives the lateness and out-of-order metrics.
- **Forward skips are valid** (for example ACCEPTED to IN_TRANSIT after a missed geofence). This is
  needed for order independence: a newer event that arrives first must not be judged invalid.
  Backward moves with a newer timestamp, such as DELIVERED to IN_TRANSIT, are still `INVALID_TRANSITION`.
- **Exception resolution.** `shipment.exception` takes an optional `resolved: true` payload flag
  (the spec defines no resolution event). Any newer lifecycle event at the same or a later stage
  also ends the exception.
- **Stop mapping.** Arriving at stop 1 sets AT_PICKUP. Departing any non-final stop sets
  IN_TRANSIT. Arriving at the final stop sets AT_DELIVERY. Intermediate arrivals and departure from
  the final stop only update stop timestamps.
- **Order dependence of speed-derived fields.** EWMA speed, and therefore ETA, legitimately
  depends on which pings arrived in order. The convergence property covers status, stops,
  position, carrier and rate, not the EWMA.
- **Extra DLQ error type `UNKNOWN_REFERENCE`** for an unknown facility, customer or carrier.

---

## Phase 3: Storage

**Built**
- Alembic (`alembic.ini`, `alembic/env.py`) with three hand-written SQL migrations:
  - `0001`: reference tables (carriers, customers, facilities, lanes) and core tables (shipments,
    shipment_stops, shipment_current_state), with CHECK constraints and indexes.
  - `0002`: `shipment_events` and `gps_pings`, both `PARTITION BY RANGE (event_time)`, each with a
    composite `(shipment_id, event_time)` index. `gps_pings` also gets a BRIN index on `event_time`.
  - `0003`: `alerts` (UNIQUE `dedupe_key`, a partial index on active alerts), `lane_metrics_hourly`,
    `carrier_scorecard_daily`, and two processing tables: `parked_events` and `stream_watermarks`.
- `storage/tables.py`: SQLAlchemy Core `Table` objects that mirror the migrations. A test asserts
  the two stay in sync.
- `storage/partitions.py`: the partition manager, with
  - `run_maintenance` (creates yesterday through +7 days, drops partitions older than
    `FREIGHTPULSE_PARTITION_RETENTION_DAYS`, default 90);
  - `ensure_partitions` for on-demand creation;
  - a `PartitionCache` for the processor hot path.
  DDL is serialised with `pg_advisory_xact_lock`.
- `domain/reference_data.py` holds 40 real US freight hubs (DCs, ports, border crossings, intermodal)
  with real coordinates, 25 fictional carriers with SCAC/MC numbers, and 15 fictional customers.
  `storage/seed.py` upserts them idempotently.
- `bootstrap.py` is the `init` service: topics, then migrations, then partitions, then seed.
  Every step is idempotent.
- `tests/conftest.py` provides lazy testcontainers fixtures (Postgres 16, Redpanda v24.2.7), per-test
  topic prefixes and consumer groups, and table truncation between tests.

**Verified**
- `python -m freightpulse.bootstrap` against the compose DB ran migrations 0001 to 0003, created
  18 partitions, and seeded 40/25/15 rows.
- `tests/integration/test_storage.py` (6 tests) passes on a Postgres testcontainer. It covers
  metadata/schema sync, seed idempotency, partitioning with BRIN, partition routing, the
  ahead/retention window, and the cache.

**Decisions / deviations**
- Columns and tables beyond the spec, each documented in the migration:
  - `shipment_current_state` gains processor bookkeeping columns: `status_event_time`,
    `pre_exception_status`, the EWMA speed stats, the HOS clock, and the carrier/rate event times.
    These are needed to resume the reducer exactly where it stopped.
  - `alerts` gains `details JSONB` (ETA, detention cost, and so on) and `updated_at`.
  - `parked_events` provides durable parking for unknown-shipment retries (ADR 0002).
  - `stream_watermarks` provides an event-time clock for the scanners (ADR 0003).
- `shipment_events` and `gps_pings` have **no FK to `shipments`**. These are the highest-volume
  write paths, the processor already guarantees the shipment exists before appending, and FKs on
  partitioned tables add per-row lookup cost. Recorded in ADR 0004.
- Testcontainers on Docker Desktop needs `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`
  so the Ryuk reaper can mount the socket. `tests/conftest.py` sets it, and it is harmless on Linux CI.

---

## Phase 4: Simulator

**Built** (`src/freightpulse/simulator/`)
- `fleet.py`: a deterministic discrete-event simulation. Each shipment is a generator with its own RNG
  (seeded from `--seed` and the shipment index), and the generators are merged by event time with
  a heap. Each shipment runs through:
  - created, then tendered, with ~15% rejections followed by a re-tender to a different carrier,
    then accepted;
  - a deadhead to pickup timed to the appointment;
  - log-normal dwell, with a per-facility median of 60 to 100 min;
  - great-circle line-haul at 50 to 65 mph with Gaussian noise, a ping every 30 to 120 s of
    simulated time, and GPS jitter;
  - HOS: 11h of driving forces a 10h SLEEPER break;
  - a 3% chance of an exception (1 to 3h stop, then resolved);
  - delivery dwell, departure, and POD.
  Delivery windows are planned from distance, HOS and dwell, plus an offset of -3h to +1.5h, so
  roughly 1 load in 5 becomes late-risk.
- `chaos.py` implements `--duplicate-rate`, `--out-of-order-rate` (emission delayed 1 to 10 sim
  minutes), `--malformed-rate`, and `--gps-dropout-rate` (60 to 150 min silence). Malformed
  variants are truncated JSON, a missing field, a bad type, an unknown event type, an
  out-of-range value, and a "teleport" ping that is schema-valid but physically impossible.
- `cli.py` paces the stream against the wall clock (`--acceleration`, default 60x). It also takes
  `--duration`, `--max-events` and `--max-rate` (token bucket). `producer.py` is an idempotent,
  lz4, keyed producer.

**Verified**
- 14 simulator unit tests:
  - determinism for a seed;
  - time ordering;
  - every generated event validates and applies cleanly through the reducer, ending DELIVERED;
  - realistic speeds, and the implied speed between pings is always under 100 mph;
  - the tender rejection rate is about 15%;
  - chaos rates are as configured;
  - every corruption fails validation, except teleport, which is rejected as `IMPOSSIBLE_GPS_JUMP`;
  - CLI defaults match the spec.
- Live run: `python -m freightpulse.simulator --shipments 50 --acceleration 600 --duration 15`
  produced 619 messages to Redpanda (0 delivery errors). `rpk topic consume` shows
  `shipment_id`-keyed JSON envelopes.

**Decisions / deviations**
- `shipment.created` is never corrupted by chaos. It comes from the TMS, which is clean in this
  model; the ELD/carrier feeds are the messy ones. This keeps every shipment traceable.
- **Simulated time runs ahead of the wall clock** at 60x. `event_time` is simulated time and
  `produced_at` is wall-clock time, which is what end-to-end latency is measured from. That is
  why envelope validation allows up to 30 days of skew.
- Shipment ids default to a per-start-minute range (`--id-offset`), so repeated demo runs never
  collide with earlier shipments.

---

## Phase 5: Processor

**Built**
- `processing/processor.py` (`StateProcessor`): a manual-commit consumer of both input topics.
  For each batch it:
  1. validates every message;
  2. creates any missing partitions;
  3. in **one transaction**: applies creates, pulls back parked events, drops already-logged
     `event_id`s, folds the rest through the pure reducer, appends to the log
     (`ON CONFLICT DO NOTHING`), parks unknown shipments, runs the version-checked bulk state
     update, backfills stops, and advances watermarks;
  4. produces alerts and DLQ records and flushes them;
  5. commits the transaction;
  6. **then** commits offsets, synchronously.
  The range assignor co-partitions the two topics. `FREIGHTPULSE_FAULT_INJECTION` provides crash
  hooks for tests.
- `storage/repositories.py`: SQL for the processor. Bulk updates use `jsonb_to_recordset`
  (one statement per batch). The state update is version-checked and raises
  `VersionConflictError`, which triggers a batch retry.
- `processing/dlq.py`: the `DlqRecord` contract `{original_event, original_topic, original_key,
  error_type, error_detail, failed_at, ...}`. Non-UTF-8 payloads are base64-encoded.
- `processing/dlq_replay.py` and `make dlq-replay` re-publish DLQ records to their original topics,
  up to the high watermark seen at start-up. Options: `--error-type` filter and `--dry-run`.
- `observability/metrics.py`: every metric from spec 10 (Prometheus wiring comes in Phase 8).
- ADRs 0001 (Redpanda and keying) and 0002 (effectively-once).

**Verified**
- `tests/integration/test_processor.py` has 12 tests on Redpanda + Postgres testcontainers:
  - full lifecycle to DELIVERED;
  - **duplicates produce exactly one row**;
  - **shuffled order gives the same final state** as ordered;
  - **malformed, invalid-transition, impossible-jump and unknown-reference events land in the DLQ**;
  - unknown shipment is parked, then applied;
  - unknown shipment goes to the DLQ after the retry window;
  - late-risk alert is published exactly once;
  - version conflict is detected;
  - **crash before DB commit, crash after DB commit, and SIGKILL then restart all give exact counts**;
  - DLQ replay after a fix.
  The crash tests passed on two consecutive runs.
- Full suite at this point: 116 passed. Coverage: domain about 99%, processing 84%.
- Live run: the host processor on the compose stack, plus the simulator (100 shipments, 300x, 30 s).
  - 1,815 messages consumed: 546 lifecycle events and 1,230 pings committed.
  - 27 duplicates skipped.
  - 11 `SCHEMA_VALIDATION` and 1 `IMPOSSIBLE_GPS_JUMP` routed to the DLQ.
  - 21 late-risk alert messages emitted.

**Decisions / deviations**
- **Fixed a real bug found by the integration tests.** Lifecycle events from the events topic are
  often processed *after* newer pings from the telemetry topic, so they counted as out-of-order and
  never triggered an ETA recompute. Now any stop or status change recomputes ETA as of
  `max(event_time, last_ping_at)`.
- **Crash-test flakiness was in the test harness, not data loss.** A hard-killed consumer stays in
  the group until `session.timeout.ms` (45 s default). A restarted processor using `--idle-exit`
  could finish its own partitions and exit before the dead member's partitions were reassigned.
  Fix: `FREIGHTPULSE_KAFKA_SESSION_TIMEOUT_MS` is now configurable (6 s in tests), and the idle
  timer only starts after the first message.
- **`events_out_of_order_total` counts events older than the newest event already seen for that
  shipment, on either topic.** Because the two topics interleave, it is noticeably higher than the
  simulator's injected 3% delay rate. That is expected and documented in ADR 0003.
- **Within-batch order.** `shipment.created` events are applied before the rest of the batch,
  because a shipment's first pings often share its batch. Parked events are applied before the
  batch's other events.

---

## Phase 6: Alerts and rollups

**Built**
- `processing/scanners.py` covers detention and stale tracking. The detention scanner triggers when
  a truck has arrived at a stop without departing for over 2h, escalates through MEDIUM, HIGH and
  CRITICAL at 2, 3 and 4h with cost at $75/h past free time, and resolves on departure. The
  stale-tracking scanner triggers when an IN_TRANSIT shipment has had no ping for over 60 min, and
  resolves when pings resume or status changes. Both scanners run on the **fleet clock**, which is
  the max event time in `stream_watermarks`, not on wall time. They emit an alert only when it is
  new or has escalated. Both queries are served by partial indexes (`ix_stops_open_dwell`,
  `ix_state_in_transit_ping`).
- Late-risk alerts are emitted by the processor, only when the level changes (Phase 5).
- `processing/alert_sink.py` consumes `freight.alerts` and writes them idempotently to the
  `alerts` table, committing offsets manually after the DB transaction.
  - An upsert by `dedupe_key` only escalates severity.
  - A new LATE_RISK alert supersedes older active ones.
  - Each resolve closes by key, or by type for LATE_RISK.
  - Every comparison uses event time, so replays never close newer alerts.
- `processing/rollups.py` has three idempotent `INSERT ... SELECT ... ON CONFLICT DO UPDATE` rollups
  over a 48h fleet-clock window:
  - `lane_metrics_hourly`: delivered count, on-time %, average and p90 transit hours, average
    detention minutes;
  - `carrier_scorecard_daily`: tenders, acceptance rate, on-time pickup and delivery %, tracking
    compliance %;
  - facility dwell, which updates `facilities.avg_dwell_min` over the last 30 days.
- `processing/jobs.py` runs the scanners (default every 60 s, as the spec says), rollups (every 120 s)
  and partition maintenance (hourly) on independent schedules. `--once` runs each job once.

**Verified**
- `tests/integration/test_alerts_rollups.py`, 5 tests, all passing:
  - detention: free time, HIGH at 3.5h with $112.50, no re-emit without a change, CRITICAL at
    4.5h, resolve on departure with final cost $225;
  - stale tracking: trigger at 95 min, resolve when GPS resumes;
  - sink supersede logic, and a full replay leaves the table unchanged;
  - rollups give exact expected values and are idempotent;
  - end to end: processor and jobs feed Kafka, then the sink writes to Postgres.
- Live on the compose DB: `jobs --once` updated dwell for 5 facilities and 25 carrier scorecard rows.
  The alert sink then wrote the processor's alerts. The 8 HIGH and 2 MEDIUM active late-risk alerts
  match the late-risk levels in `shipment_current_state` exactly.

**Decisions / deviations**
- **Scanners use event time (the fleet clock), not wall time.** Recorded in ADR 0003.
- **Tracking compliance** is defined as the share of a carrier's delivered shipments that never
  raised a STALE_TRACKING alert. The spec does not define it.
- **Detention resolution comes from the scanner** (the departure is detected on its next run),
  not from the processor. That keeps the whole detention lifecycle in one component.

---

## Phase 7: API

**Built** (`src/freightpulse/api/`)
- `app.py`: FastAPI app factory with every endpoint from spec 8:
  - `/health` and `/ready` (Postgres `SELECT 1` plus a broker metadata call; returns 503 when not ready);
  - `/shipments` with **keyset pagination** on `shipment_id` (an opaque base64 cursor; the filters
    `status`, `carrier_id` and `late_risk` are added only when present, so indexes can be used);
  - `/shipments/{id}` (state, stops with dwell, ETA with stored and age-decayed confidence,
    active alert count);
  - `/shipments/{id}/timeline` (ordered by `event_time`);
  - `/shipments/{id}/track?since=`;
  - `/alerts?active=&type=&shipment_id=`;
  - `/lanes/{lane_id}/metrics?from=&to=`;
  - `/carriers/{carrier_id}/scorecard?days=30` (daily rows plus a tender-weighted summary);
  - `/metrics/summary` (by status, in transit, at risk, active alerts, detained now, detention
    cost today, delivered today, all on the fleet clock);
  - `WS /ws/alerts`;
  - `/metrics` (Prometheus);
  - `/docs` (OpenAPI);
  - a minimal live-alerts page at `/`.
- Typed errors: every error body is `{"error": {"code", "message", "details"}}`. Codes include
  `shipment_not_found`, `invalid_cursor`, `validation_error` and `not_found`.
- `ws.py`: one Kafka consumer thread per API instance (its own group, starting at the latest
  offset) fans out to per-client bounded asyncio queues. Slow clients drop messages instead of
  blocking.
- `schemas.py` holds the Pydantic response models. `queries.py` holds the Core and raw-SQL reads.

**Verified**
- `tests/api/test_api.py`, 13 tests, against a database seeded through the real processor path:
  40 shipments across 4 statuses and 3 carriers, plus alerts and rollups. They cover:
  - keyset pagination visits every shipment exactly once, in order;
  - the filters;
  - 400, 404 and 422 typed errors;
  - the detail fields and decayed confidence;
  - timeline ordering, and `track?since`;
  - alert filters, lane metrics with `from`/`to`, and the scorecard;
  - summary counts;
  - the OpenAPI paths, `/metrics`, `/`, and `/ready` returning 503 with a broken broker;
  - **a live WebSocket push**: an alert produced to Redpanda arrives on `/ws/alerts`.

**Decisions / deviations**
- **I found and fixed a test-fixture bug that leaked containers.**
  `with RedpandaContainer().start(...)` starts the container twice, because `__enter__` calls
  `start()` again, and only stops the second one. 14 Redpanda containers had leaked and were
  holding about 2.7 GB of the 3.8 GB Docker VM, so tests started timing out. I removed them with
  your approval, using a label filter `org.testcontainers=true`. The fixture now calls
  `start()` and `stop()` explicitly, and later runs leave no containers behind.
- "Detention cost today" is the detention ($75/h past 2h) accrued at every stop occupied during
  the current fleet-clock day, including trucks still at the dock.
- The WebSocket feed is a *live tail*. Its consumer group starts at the latest offset and
  auto-commits. The `alerts` table remains the system of record, and the page backfills from
  `GET /alerts`.

---

## Phase 8: Observability

**Built**
- Metrics (`observability/metrics.py`):
  - the spec 10 set: `events_consumed_total{topic,event_type}`, `events_dlq_total{error_type}`,
    `processing_latency_seconds` (produced_at to Postgres commit), `consumer_lag{topic,partition}`
    (from librdkafka statistics every 5 s), `db_write_batch_seconds`,
    `alerts_emitted_total{type,severity}`;
  - from spec 6.2: `events_out_of_order_total`, `event_lateness_seconds`;
  - extras: committed, duplicate, parked, version-conflict, batch size, sink writes,
    job durations, and the fleet clock.
- The processor, alert sink and jobs serve metrics on ports 9100, 9101 and 9102. The API serves
  `/metrics`.
- `prometheus/prometheus.yml`: 5 s scrape. DNS service discovery, so a scaled
  `--scale processor=3` is scraped per replica. Redpanda's own metrics are also scraped.
- Grafana 11, provisioned automatically: a Prometheus datasource and the dashboard
  `grafana/dashboards/freightpulse.json` (14 panels). It shows:
  - stat panels for p95 latency, committed events per second, lag, and DLQ events over 15 min;
  - p50/p95/p99 latency, throughput by topic, lag by topic, DB batch duration, DLQ by type,
    alerts by type and severity, out-of-order/duplicates/parked/conflicts, lateness, batch size
    per instance, and job durations.
  Anonymous viewers are allowed and it is the home dashboard.
- Full `docker-compose.yml`:
  - `init`, a one-shot service: topics, migrations, partitions, seed;
  - `processor`, `alert-sink`, `jobs`, `api`;
  - `prometheus`, `grafana`;
  - `demo` (profile `demo`), which runs `freightpulse/demo.py`: the simulator plus a status
    printout every 15 s.

**Verified**
- `make up` from a clean `down -v` built the image and started everything; `/ready` returned 200.
- `make demo` ran 500 shipments at 60x for 4 real minutes, sending 16,022 messages. Late-risk
  alerts appeared about 90 s in. Stale-tracking and detention alerts followed, detention cost
  accrued, and the status counters moved as expected.
- All 5 Prometheus targets were `up`. During the demo, `histogram_quantile` over 10 min gave
  latency **p50 0.168 s, p95 0.291 s, p99 0.382 s**. These are bucket-interpolated at demo load,
  about 67 events/s. The real benchmark is Phase 9. Lag was 0 to 1 and the DLQ counters were
  non-zero. Grafana's API returned the provisioned dashboard with 14 panels.
- `tests/unit/test_metrics.py` covers the lag parser.
- Full suite: **138 passed**. Coverage of `domain` + `processing` is **91%** (gate 80%).

**Bugs found by the live demo, then fixed and tested**
1. **Cascading GPS-jump rejections.** A corrupt *first* ping for a truck cannot be checked (there is
   no anchor), so it became the anchor. Every genuine ping after it was then dead-lettered as an
   impossible jump: 15 good pings for one shipment. The fix is **re-anchoring**. A rejected ping
   is remembered as a suspect. If the next ping agrees with the suspect (under 100 mph between
   them, within 30 min) rather than with the anchor, the anchor was the outlier and the track
   re-anchors. This added migration `0004` (`suspect_lat`, `suspect_lon`, `suspect_at`), and
   rejections may now update only these bookkeeping fields. The Hypothesis property was
   tightened to match, and there are 2 new unit tests.
2. **GPS dropout was far too aggressive.** `--gps-dropout-rate 0.01` was used as a per-ping
   chance to *start* a 60 to 150 minute outage, which dropped 18% of pings. It now means the
   expected **fraction of pings lost**: the start probability is the rate divided by the expected
   pings per episode. A unit test checks that the realised loss is close to the rate and that
   losses come in clusters.

**Other fixes in this phase**
- The API host port is configurable (`make up API_PORT=8001`). On this machine port 8000 is used
  by another project's container, so verification used 8001. The default is still 8000.
- Test topics are deleted on fixture teardown. Redpanda limits partitions per shard by memory,
  and after about 25 tests the session broker refused new topics (`INVALID_PARTITIONS`).

---

## Phase 9: Performance

**Built**
- `storage/seed_large.py` (`make seed-large`) uses `COPY` to load the `freightpulse_perf`
  database (created if missing) with:
  - 200,000 shipments and 400,000 stops;
  - **1,000,000 lifecycle events**;
  - **5,000,000 GPS pings**, written in event-time order like a real stream;
  - 300,000 alerts;
  all over the last 30 days. It then runs ANALYZE and the rollups.
- `perf/sql_cases.py` (`make sql-perf`) runs 5 case studies, each with real
  `EXPLAIN (ANALYZE, BUFFERS)`: a warm-up, then the median of 5 runs. It writes
  `docs/sql_performance.md` (plans, timings, index sizes, and an index inventory with a
  justification for every index) plus `docs/sql_performance_results.json`, then restores
  exactly the migration's index set.
- `perf/bench.py` (`make bench`) measures backlog-drain capacity and a 1k to 8k events/s latency
  sweep, for 1 and 3 processors. Latency comes from exact per-event samples (the processor's
  optional `FREIGHTPULSE_LATENCY_SAMPLE_FILE`). It writes `docs/benchmarks.md` and
  `docs/benchmarks_results.json`, plus a `--render-only` mode.

**Verified (real measured numbers)**
- `make seed-large`: 97 s in total.
- SQL case studies (median execution time, before → after):
  - `timeline`: 31.87 ms → 0.201 ms
  - `gps_range`: 37.56 ms → 1.675 ms
  - `keyset`: 48.30 ms → 0.098 ms
  - `scorecard`: 68.42 ms → 0.017 ms
  - `partial_alerts`: 10.55 ms → 0.010 ms
  - BRIN on `gps_pings.event_time` is 960 kB versus 107.9 MB for a B-tree, with nearly identical
    query time.
- Benchmark: **1 processor 9,922 events/s, 3 processors 21,360 events/s
  (2.15x)**. p99 stayed under 0.8 s at every step from 1k to 8k events/s
  (full tables are in `docs/benchmarks.md`).

**Findings fixed along the way** (each one was spotted from measurement, not assumed)
1. **Keyset pagination was 8.7 ms, not under 1 ms.** The cursor bound sat only on
   `shipment_current_state`. Postgres propagates equality across join keys but not range
   predicates, so the merge join read `shipments` from row 1. The API now repeats the bound on
   both join inputs, which gives 0.09 ms. The case study shows all three variants.
2. **The GPS case-study baseline was mislabelled.** With no time index, the planner full-scanned
   the composite `(shipment_id, event_time)` index rather than doing a seq scan. That index is now
   dropped during the case so the variants measure what their labels say.
3. **The seeder deadlocked** because it committed inside an open `COPY`. It now uses one COPY per
   day, committed between days.
4. **A real multi-instance bug: lane upserts deadlocked across processors.** The first benchmark
   showed 3 instances no faster than 1 (1.05x). A per-process CPU sample showed `0% + 0% + 68%`,
   and a reproduction with logs found 2 of the 3 processors had **crashed with `DeadlockDetected`
   on `lanes`**. Two batch transactions were upserting the same new lanes in different orders.
   - Fix: lanes are upserted in a short separate transaction, in sorted order, before the batch
     transaction.
   - Deadlocks and serialization failures are now retried like version conflicts instead of
     crashing the process.
   - Expired-parking deletes use `SKIP LOCKED`.
   - A regression test runs 3 processor subprocesses over shared lanes and asserts that all exit
     cleanly, that at least 2 did work, and that row counts are exact.
   After the fix, scaling is 2.15x and the 3-instance p99 at 1k events/s fell from 2.6 s to 0.33 s.
5. **Benchmark harness corrections.**
   - It waits until the consumer group is Stable with N members owning partitions, using the
     admin API, before measuring.
   - It uses one long-lived producer across steps. A fresh producer stamped `produced_at` before
     its broker connection was up, which added about 1 to 3 s at the start of each step.

**Decisions / deviations**
- The perf data lives in its own database (`freightpulse_perf`) and the benchmark in
  `freightpulse_bench`, on `bench.*` topics. The demo database and topics are untouched.
- The benchmark runs processors as host subprocesses against Docker Redpanda and Postgres (a 4-CPU
  VM). The doc records machine specs and calls this a laptop lower bound.

---

## Phase 10: Polish

**Built**
- `README.md` contains:
  - a problem statement in freight terms;
  - a Mermaid architecture diagram and a component table;
  - the `make up && make demo` quickstart, with URLs and copy-paste `curl` examples;
  - the API table;
  - **Design decisions & trade-offs**, linking all four ADRs;
  - a **results table generated from the measured JSON** (throughput, p95 latency, SQL speedups);
  - a testing section;
  - **What I'd do next at scale**: Avro/Protobuf with a schema registry, Flink or Kafka Streams,
    Debezium CDC, an Iceberg lakehouse, multi-region;
  - a demo GIF placeholder.
- ADRs 0001 to 0004, updated with the later findings:
  - 0001: measured scaling;
  - 0002: lanes upserted before the batch transaction, and transient-conflict retry;
  - 0003: GPS re-anchoring.

## Final review (after all 10 phases)

- **Spec walk-through.** I checked every requirement in sections 2 to 15 against the code. The
  deviations are listed below, and each is also explained in its phase above.
- **Quality gates.**
  - `make lint`: ruff check and format both clean (67 files).
  - `make typecheck`: `mypy --strict` clean on 46 source files.
  - `make cov`: **139 passed**. Coverage of `domain` + `processing` is **90%** (gate 80%); overall
    coverage is 72%, because the perf and demo scripts are not unit-tested.
  - The pre-commit config validates. Its hooks were not run because there is no git repo
    (no-git instruction).
- **Clean end-to-end run.**
  - `make down` removed all volumes.
  - `make up API_PORT=8001` was ready in about 17 s with cached images. The container code is
    current and the DB is at Alembic revision 0004.
  - `make demo` produced live late-risk and detention alerts with detention cost accruing.
  - Every README `curl` command returned the expected data, and `/docs`, Grafana, Redpanda
    Console and Prometheus all returned 200.
  - `make migrate` and `make dlq-replay` (80 events replayed) also worked.

## Deviations from SPEC.md (summary)

1. **Postgres is exposed on host port 5433**, because a local Postgres.app holds 5432. The API
   host port is configurable via `API_PORT`; I verified on 8001 because another project's
   container holds 8000 on this machine.
2. **Separate event-time guards for status and position** instead of a single `last_event_time`
   guard (ADR 0003).
3. **Lenient forward skips** in the state machine, and a `resolved` flag on `shipment.exception`
   (the spec defines no resolution event).
4. **Extra columns and tables**: processor bookkeeping columns in `shipment_current_state`
   (including the re-anchoring columns), `alerts.details` and `alerts.updated_at`,
   `parked_events`, `stream_watermarks`. Also an extra DLQ error type, `UNKNOWN_REFERENCE`.
5. **No FK from the partitioned event tables to `shipments`** (ADR 0004).
6. **Scanners run every 15 s in the demo compose stack** (the spec default of 60 s is kept in
   code), and they run on the event-time fleet clock.
7. **Lanes are upserted in a short transaction before the batch transaction**, to avoid
   cross-instance deadlocks (ADR 0002).
8. **Redpanda's `auto_create_topics_enabled=false` could not be set as a start flag.** Topics are
   always created explicitly instead.
9. **`make down` deletes volumes** (a clean slate); `make stop` keeps data.
10. **Dependencies added beyond SPEC.md**: `pytest-cov` and `httpx` (dev), and the
    `uvicorn[standard]` extra.

## Not verified here

- The GitHub Actions workflow was not executed, since there is no remote. Its YAML parses and it
  runs the same commands verified locally. The README badge has an `OWNER` placeholder.
- Pre-commit hooks were not run (they need a git repo).
- The "under 5 minutes from clone" quickstart was timed with Docker images already cached. A
  first-ever run also pulls about 6 images and builds the app image.
