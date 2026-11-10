"""The state processor: Kafka -> validate -> one Postgres transaction -> commit offsets.

Per batch (docs/adr/0002-effectively-once-semantics.md):

1. ``consume()`` up to ``batch_size`` messages from both input topics (auto-commit is off).
2. Parse and validate every message. Invalid messages are queued for the DLQ.
3. Make sure daily partitions exist for the event days in the batch (own short transaction).
4. In **one** transaction:
   a. apply ``shipment.created`` events (shipments, stops, lane, initial state);
   b. pull parked events whose shipment now exists, and expire old ones to the DLQ;
   c. drop events whose ``event_id`` is already in the log (duplicates/redeliveries);
   d. fold every remaining event into its shipment's state with the pure reducer;
   e. append accepted events to the log (``ON CONFLICT DO NOTHING``), park unknown-shipment
      events, update state with a version check, advance watermarks;
   f. publish derived alerts and DLQ records and *flush* them (at-least-once; sinks dedupe);
   g. COMMIT.
5. Only then commit the Kafka offsets, synchronously.

A crash anywhere before 4g loses nothing (the batch is redelivered); a crash between 4g and 5
redelivers events that are already in the log, which step 4c skips. Hence effectively-once.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from psycopg.errors import DeadlockDetected, SerializationFailure
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import OperationalError

from freightpulse.config import Settings, get_settings
from freightpulse.domain.alerts import AlertMessage
from freightpulse.domain.enums import DlqErrorType, EventType
from freightpulse.domain.events import (
    EventValidationError,
    ParsedEvent,
    ShipmentCreatedPayload,
    TenderedPayload,
    TenderResponsePayload,
    parse_event,
)
from freightpulse.domain.geo import road_miles
from freightpulse.domain.shipment_state import ShipmentState, apply_event, new_shipment_state
from freightpulse.observability import metrics as m
from freightpulse.processing.dlq import DlqRecord
from freightpulse.simulator.producer import EventProducer
from freightpulse.storage import repositories as repo
from freightpulse.storage.db import make_engine
from freightpulse.storage.partitions import PartitionCache

log = logging.getLogger("processor")


@dataclass(slots=True)
class Inbound:
    """One event to process: from Kafka (with offsets) or from the parking table."""

    raw: bytes
    topic: str
    key: str | None
    partition: int | None = None
    offset: int | None = None
    parsed: ParsedEvent | None = None
    from_parking: bool = False


@dataclass(slots=True)
class BatchResult:
    consumed: int = 0
    committed_lifecycle: int = 0
    committed_pings: int = 0
    duplicates: int = 0
    parked: int = 0
    unparked: int = 0
    dlq: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    alerts: int = 0
    out_of_order: int = 0
    conflicts: int = 0


class FaultInjector:
    """Test hook: ``FREIGHTPULSE_FAULT_INJECTION=crash_before_db_commit:N`` or
    ``crash_after_db_commit:N`` hard-kills the process (``os._exit``) in batch N."""

    def __init__(self, spec: str) -> None:
        self.point, _, n = spec.partition(":")
        self.at_batch = int(n) if n else 1
        self.batches = 0

    def tick(self) -> None:
        self.batches += 1

    def maybe_crash(self, point: str) -> None:
        if self.point == point and self.batches >= self.at_batch:
            log.warning("fault injection: crashing at %s (batch %d)", point, self.batches)
            os._exit(137)


class StateProcessor:
    def __init__(
        self,
        settings: Settings,
        engine: Engine | None = None,
        producer: EventProducer | None = None,
    ) -> None:
        self.settings = settings
        self.engine = engine or make_engine(settings.database_url, pool_size=2)
        self.producer = producer or EventProducer(settings, **{"linger.ms": 1})
        self.consumer: Consumer | None = None
        self.partitions = PartitionCache()
        self.faults = FaultInjector(os.environ.get("FREIGHTPULSE_FAULT_INJECTION", ""))
        # Benchmarks: exact per-event latency samples ("<commit_epoch> <latency_s>" per line).
        sample_path = os.environ.get("FREIGHTPULSE_LATENCY_SAMPLE_FILE")
        self._samples = open(sample_path, "a", buffering=1 << 16) if sample_path else None  # noqa: SIM115
        with self.engine.connect() as conn:
            self.reference = repo.load_reference(conn)
        self._lane_ids: dict[tuple[str, str], int] = {}
        self._last_sweep = 0.0
        self._running = False
        self.totals = BatchResult()

    # -- kafka -----------------------------------------------------------------------------

    def _consumer_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "group.id": self.settings.processor_group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            # Range assignment co-partitions both input topics (same partition count, same key),
            # so lifecycle events and GPS pings of a shipment land on the same instance.
            "partition.assignment.strategy": "range",
            "allow.auto.create.topics": False,
            "statistics.interval.ms": 5000,
            "stats_cb": m.update_lag_from_stats,
            "fetch.min.bytes": 1,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": self.settings.kafka_session_timeout_ms,
            "heartbeat.interval.ms": max(1000, self.settings.kafka_session_timeout_ms // 3),
        }

    def subscribe(self) -> Consumer:
        if self.consumer is None:
            self.consumer = Consumer(self._consumer_config())
            self.consumer.subscribe(
                [self.settings.shipment_events_topic, self.settings.telemetry_topic]
            )
        return self.consumer

    def close(self) -> None:
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None
        self.producer.flush()
        if self._samples is not None:
            self._samples.close()
            self._samples = None

    # -- the loop ----------------------------------------------------------------------------

    def poll_once(self) -> BatchResult | None:
        consumer = self.subscribe()
        msgs = consumer.consume(
            num_messages=self.settings.processor_batch_size,
            timeout=self.settings.processor_batch_timeout_s,
        )
        inbound: list[Inbound] = []
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", err)
                continue
            inbound.append(
                Inbound(
                    raw=msg.value() or b"",
                    topic=msg.topic() or "",
                    key=_decode_key(msg),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
            )
        sweep_due = time.monotonic() - self._last_sweep >= self.settings.parking_sweep_interval_s
        if not inbound and not sweep_due:
            return None
        result = self.process(inbound, sweep=sweep_due)
        if inbound:
            self._commit_offsets(consumer, msgs)
        return result

    def _commit_offsets(self, consumer: Consumer, msgs: list[Message]) -> None:
        latest: dict[tuple[str, int], int] = {}
        for msg in msgs:
            if msg.error() is None:
                k = (msg.topic() or "", msg.partition() or 0)
                latest[k] = max(latest.get(k, -1), msg.offset() or 0)
        if latest:
            consumer.commit(
                offsets=[TopicPartition(tp, p, off + 1) for (tp, p), off in latest.items()],
                asynchronous=False,
            )

    def run(self, max_seconds: float | None = None, idle_exit_s: float | None = None) -> None:
        """Consume until stopped. ``idle_exit_s`` counts from the last message received (the
        timer starts with the first message, so a slow group join never ends the run early)."""
        self._running = True
        started = time.monotonic()
        last_activity: float | None = None
        while self._running:
            result = self.poll_once()
            now = time.monotonic()
            if result is not None and (result.consumed or result.unparked):
                last_activity = now
            if max_seconds is not None and now - started >= max_seconds:
                break
            if (
                idle_exit_s is not None
                and last_activity is not None
                and now - last_activity >= idle_exit_s
            ):
                break

    def stop(self, *_: object) -> None:
        self._running = False

    # -- batch processing ---------------------------------------------------------------------

    def _parse(self, items: list[Inbound], dlq: list[DlqRecord]) -> list[Inbound]:
        ok: list[Inbound] = []
        for it in items:
            try:
                it.parsed = parse_event(it.raw)
            except EventValidationError as exc:
                m.EVENTS_CONSUMED.labels(topic=it.topic, event_type="invalid").inc()
                dlq.append(self._dlq(it, DlqErrorType.SCHEMA_VALIDATION, exc.detail))
                continue
            if not it.from_parking:
                m.EVENTS_CONSUMED.labels(topic=it.topic, event_type=it.parsed.event_type).inc()
            ok.append(it)
        return ok

    def _dlq(self, it: Inbound, error: DlqErrorType, detail: str) -> DlqRecord:
        return DlqRecord.build(
            it.raw,
            topic=it.topic,
            key=it.key,
            error_type=error,
            detail=detail,
            partition=it.partition,
            offset=it.offset,
        )

    def process(self, items: list[Inbound], *, sweep: bool = False) -> BatchResult:
        """Process one batch end to end (everything except the Kafka offset commit)."""
        if items:
            self.faults.tick()
        pre_dlq: list[DlqRecord] = []
        parsed = self._parse(items, pre_dlq)
        days = {_day(it.parsed.event_time) for it in parsed if it.parsed is not None}
        self.partitions.ensure(self.engine, days)
        self._ensure_lanes(parsed)

        attempts = self.settings.processor_max_conflict_retries
        for attempt in range(1, attempts + 1):
            result = BatchResult(consumed=len(items))
            dlq = list(pre_dlq)
            alerts: list[AlertMessage] = []
            committed: list[ParsedEvent] = []
            started = time.perf_counter()
            try:
                with self.engine.begin() as conn:
                    self._apply(conn, parsed, sweep, result, dlq, alerts, committed)
                    self._publish(dlq, alerts)
                    self.faults.maybe_crash("crash_before_db_commit")
                # Transaction committed.
                self.faults.maybe_crash("crash_after_db_commit")
            except (repo.VersionConflictError, OperationalError) as exc:
                # Rolled back. Optimistic-concurrency conflicts (rebalance overlap) and deadlocks
                # between instances are transient: retry the whole batch.
                if isinstance(exc, OperationalError) and not isinstance(
                    exc.orig, DeadlockDetected | SerializationFailure
                ):
                    raise
                m.VERSION_CONFLICTS.inc()
                result.conflicts += 1
                log.warning(
                    "transient conflict (attempt %d/%d): %s",
                    attempt,
                    attempts,
                    str(exc).splitlines()[0],
                )
                if attempt == attempts:
                    raise
                time.sleep(0.05 * attempt)
                continue
            m.DB_WRITE_BATCH.observe(time.perf_counter() - started)
            break
        if sweep:
            self._last_sweep = time.monotonic()

        now = datetime.now(UTC)
        for ev in committed:
            latency = max(0.0, (now - ev.envelope.produced_at).total_seconds())
            m.PROCESSING_LATENCY.observe(latency)
            if self._samples is not None:
                self._samples.write(f"{now.timestamp():.3f} {latency:.5f}\n")
        if self._samples is not None:
            self._samples.flush()
        m.BATCH_SIZE.observe(len(items))
        m.EVENTS_COMMITTED.labels(kind="lifecycle").inc(result.committed_lifecycle)
        m.EVENTS_COMMITTED.labels(kind="gps").inc(result.committed_pings)
        for rec in dlq:
            m.EVENTS_DLQ.labels(error_type=rec.error_type).inc()
            result.dlq[rec.error_type.value] += 1
        for a in alerts:
            m.ALERTS_EMITTED.labels(type=a.alert_type, severity=a.severity).inc()
        self._accumulate(result)
        return result

    def _accumulate(self, r: BatchResult) -> None:
        t = self.totals
        t.consumed += r.consumed
        t.committed_lifecycle += r.committed_lifecycle
        t.committed_pings += r.committed_pings
        t.duplicates += r.duplicates
        t.parked += r.parked
        t.unparked += r.unparked
        t.alerts += r.alerts
        t.out_of_order += r.out_of_order
        t.conflicts += r.conflicts
        for k, v in r.dlq.items():
            t.dlq[k] += v

    def _publish(self, dlq: list[DlqRecord], alerts: list[AlertMessage]) -> None:
        if not dlq and not alerts:
            return
        produced_at = datetime.now(UTC)
        errors_before = self.producer.stats["delivery_errors"]
        for rec in dlq:
            self.producer.send(
                self.settings.dlq_topic, rec.original_key or "", rec.model_dump_json().encode()
            )
        for a in alerts:
            self.producer.send(
                self.settings.alerts_topic,
                a.shipment_id,
                a.model_copy(update={"produced_at": produced_at}).to_json_bytes(),
            )
        remaining = self.producer.flush(30)
        if remaining or self.producer.stats["delivery_errors"] > errors_before:
            raise RuntimeError("could not publish DLQ/alerts; batch will be retried")

    def _ensure_lanes(self, items: list[Inbound]) -> None:
        """Upsert lanes for new shipments in their own short transaction, in sorted order.

        Lanes are shared reference rows. Upserting them inside the long batch transaction let
        two processor instances lock the same new lanes in different orders and deadlock
        (found by the 3-instance benchmark). Committing them first, sorted, removes both the
        long lock hold and the ordering problem; an unused lane row is harmless.
        """
        wanted = set()
        for it in items:
            p = it.parsed.payload if it.parsed is not None else None
            if isinstance(p, ShipmentCreatedPayload):
                key = (p.stops[0].facility_id, p.stops[-1].facility_id)
                fac = self.reference.facilities
                if key not in self._lane_ids and key[0] in fac and key[1] in fac:
                    wanted.add(key)
        if not wanted:
            return
        with self.engine.begin() as conn:
            for origin, dest in sorted(wanted):
                dist = road_miles(
                    self.reference.facilities[origin], self.reference.facilities[dest]
                )
                self._lane_ids[(origin, dest)] = repo.upsert_lane(conn, origin, dest, dist)

    def _lane_id(self, conn: Connection, origin: str, dest: str) -> int:
        key = (origin, dest)
        if key not in self._lane_ids:  # e.g. reference data reloaded mid-batch
            dist = road_miles(self.reference.facilities[origin], self.reference.facilities[dest])
            self._lane_ids[key] = repo.upsert_lane(conn, origin, dest, dist)
        return self._lane_ids[key]

    def _reference_ok(self, conn: Connection, ev: ParsedEvent) -> str | None:
        """Return an error detail if the event references unknown facilities/customers/carriers."""
        for attempt in range(2):
            p = ev.payload
            ref = self.reference
            problem = None
            if isinstance(p, ShipmentCreatedPayload):
                if p.customer_id not in ref.customers:
                    problem = f"unknown customer_id {p.customer_id}"
                for s in p.stops:
                    if s.facility_id not in ref.facilities:
                        problem = f"unknown facility_id {s.facility_id}"
            elif (
                isinstance(p, TenderedPayload | TenderResponsePayload)
                and p.carrier_id not in ref.carriers
            ):
                problem = f"unknown carrier_id {p.carrier_id}"
            if problem is None:
                return None
            if attempt == 0:  # reference data may have been seeded since start-up
                self.reference = repo.load_reference(conn)
        return problem

    def _apply(
        self,
        conn: Connection,
        parsed: list[Inbound],
        sweep: bool,
        result: BatchResult,
        dlq: list[DlqRecord],
        alerts: list[AlertMessage],
        committed: list[ParsedEvent],
    ) -> None:
        # Within-batch duplicates (same event_id twice): keep the first occurrence.
        seen: set[Any] = set()
        items: list[Inbound] = []
        for it in parsed:
            assert it.parsed is not None
            if it.parsed.event_id in seen:
                result.duplicates += 1
                m.EVENTS_DUPLICATE.inc()
                continue
            seen.add(it.parsed.event_id)
            items.append(it)

        # Drop events already in the log (redeliveries after a crash, producer duplicates).
        items = self._drop_logged(conn, items, result)

        ids = {it.parsed.shipment_id for it in items if it.parsed is not None}
        states = repo.load_states(conn, ids, self.reference.facilities)
        new_ships: dict[str, tuple[ShipmentState, dict[str, Any]]] = {}
        lifecycle_rows: list[dict[str, Any]] = []
        ping_rows: list[dict[str, Any]] = []
        park_rows: list[dict[str, Any]] = []
        changed: set[str] = set()
        stops_changed: set[str] = set()
        marks: dict[tuple[str, int], datetime] = {}

        def accept(it: Inbound) -> None:
            ev = it.parsed
            assert ev is not None
            committed.append(ev)
            if ev.event_type is EventType.GPS_PING:
                p = ev.envelope.payload
                ping_rows.append(
                    {
                        "event_id": ev.event_id,
                        "shipment_id": ev.shipment_id,
                        "event_time": ev.event_time,
                        "lat": p["lat"],
                        "lon": p["lon"],
                        "speed_mph": p["speed_mph"],
                        "driver_status": p["driver_status"],
                    }
                )
                result.committed_pings += 1
            else:
                lifecycle_rows.append(
                    {
                        "event_id": ev.event_id,
                        "shipment_id": ev.shipment_id,
                        "event_type": ev.event_type.value,
                        "event_time": ev.event_time,
                        "source": ev.envelope.source,
                        "payload": ev.envelope.payload,
                    }
                )
                result.committed_lifecycle += 1
            if it.partition is not None:
                k = (it.topic, it.partition)
                if k not in marks or ev.event_time > marks[k]:
                    marks[k] = ev.event_time

        # (a) shipment.created first: a shipment's other events may share this batch.
        rest: list[Inbound] = []
        for it in items:
            ev = it.parsed
            assert ev is not None
            if ev.event_type is not EventType.SHIPMENT_CREATED:
                rest.append(it)
                continue
            if ev.shipment_id in states:
                accept(it)  # a second created for a known shipment: log it, keep the first
                continue
            problem = self._reference_ok(conn, ev)
            if problem:
                dlq.append(self._dlq(it, DlqErrorType.UNKNOWN_REFERENCE, problem))
                continue
            payload = ev.payload
            assert isinstance(payload, ShipmentCreatedPayload)
            state = new_shipment_state(ev, self.reference.facilities)
            lane_id = self._lane_id(
                conn, payload.stops[0].facility_id, payload.stops[-1].facility_id
            )
            new_ships[ev.shipment_id] = (
                state,
                {
                    "customer_id": payload.customer_id,
                    "mode": payload.mode.value,
                    "equipment": payload.equipment.value,
                    "lane_id": lane_id,
                    "created_at": ev.event_time,
                },
            )
            states[ev.shipment_id] = state
            accept(it)
        repo.insert_new_shipments(conn, list(new_ships.values()))

        # (b) parked events whose shipment now exists go first (they arrived earlier).
        if sweep or new_ships:
            unparked = [
                Inbound(raw=raw.encode(), topic=topic, key=None, from_parking=True)
                for topic, raw in repo.take_resolvable_parked(conn)
            ]
            if unparked:
                result.unparked += len(unparked)
                unparked = self._parse(unparked, dlq)
                unparked = self._drop_logged(conn, unparked, result)
                more = {u.parsed.shipment_id for u in unparked if u.parsed} - set(states)
                states.update(repo.load_states(conn, more, self.reference.facilities))
                rest = unparked + rest
        if sweep:
            for topic, raw in repo.take_expired_parked(
                conn, self.settings.unknown_shipment_retry_s
            ):
                dlq.append(
                    DlqRecord.build(
                        raw.encode(),
                        topic=topic,
                        key=None,
                        error_type=DlqErrorType.UNKNOWN_SHIPMENT,
                        detail=(
                            "no shipment.created after "
                            f"{self.settings.unknown_shipment_retry_s:.0f}s of retries"
                        ),
                    )
                )

        # (c) fold everything else through the reducer, in arrival order.
        for it in rest:
            ev = it.parsed
            assert ev is not None
            current = states.get(ev.shipment_id)
            if current is None:
                park_rows.append(
                    {
                        "event_id": ev.event_id,
                        "shipment_id": ev.shipment_id,
                        "topic": it.topic,
                        "raw": it.raw.decode("utf-8"),
                    }
                )
                result.parked += 1
                m.EVENTS_PARKED.inc()
                continue
            if ev.event_type is EventType.SHIPMENT_TENDERED or ev.event_type in (
                EventType.TENDER_ACCEPTED,
                EventType.TENDER_REJECTED,
            ):
                problem = self._reference_ok(conn, ev)
                if problem:
                    dlq.append(self._dlq(it, DlqErrorType.UNKNOWN_REFERENCE, problem))
                    continue
            r = apply_event(current, ev)
            if r.rejection is not None:
                dlq.append(self._dlq(it, r.rejection.error_type, r.rejection.detail))
                if r.state_changed:  # e.g. remembering a suspect GPS fix for re-anchoring
                    states[ev.shipment_id] = r.state
                    changed.add(ev.shipment_id)
                continue
            accept(it)
            if r.out_of_order:
                result.out_of_order += 1
                m.EVENTS_OUT_OF_ORDER.inc()
                m.EVENT_LATENESS.observe(r.lateness_seconds)
            states[ev.shipment_id] = r.state
            changed.add(ev.shipment_id)
            if r.stops_changed:
                stops_changed.add(ev.shipment_id)
            alerts.extend(r.alerts)
        result.alerts = len(alerts)

        # (d) writes
        repo.insert_lifecycle_events(conn, lifecycle_rows)
        repo.insert_pings(conn, ping_rows)
        repo.park_events(conn, park_rows)
        # The reducer never changes ``version``: it is still the version we read (0 for shipments
        # inserted above), which is exactly what the optimistic-concurrency check expects.
        repo.update_states(conn, [states[sid] for sid in changed])
        repo.update_stops(conn, [states[sid] for sid in stops_changed])
        repo.advance_watermarks(conn, self.settings.processor_group_id, marks)
        if sweep:
            m.PARKED_EVENTS.set(repo.parked_count(conn))

    def _drop_logged(
        self, conn: Connection, items: list[Inbound], result: BatchResult
    ) -> list[Inbound]:
        by_table: dict[str, list[ParsedEvent]] = defaultdict(list)
        for it in items:
            assert it.parsed is not None
            table = "gps_pings" if it.parsed.event_type is EventType.GPS_PING else "shipment_events"
            by_table[table].append(it.parsed)
        logged: set[Any] = set()
        for table, evs in by_table.items():
            times = [e.event_time for e in evs]
            logged |= repo.existing_event_ids(
                conn, table, [e.event_id for e in evs], min(times), max(times)
            )
        if not logged:
            return items
        kept = []
        for it in items:
            assert it.parsed is not None
            if it.parsed.event_id in logged:
                result.duplicates += 1
                m.EVENTS_DUPLICATE.inc()
            else:
                kept.append(it)
        return kept


def _day(ts: datetime) -> date:
    return ts.astimezone(UTC).date()


def _decode_key(msg: Message) -> str | None:
    key = msg.key()
    if key is None:
        return None
    return key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="FreightPulse state processor")
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument(
        "--idle-exit",
        type=float,
        default=None,
        help="exit after N seconds without new messages (benchmarks/tests)",
    )
    parser.add_argument("--metrics-port", type=int, default=None)
    args = parser.parse_args()
    settings = get_settings()
    m.serve_metrics(
        args.metrics_port if args.metrics_port is not None else settings.processor_metrics_port
    )
    proc = StateProcessor(settings)
    signal.signal(signal.SIGTERM, proc.stop)
    signal.signal(signal.SIGINT, proc.stop)
    log.info(
        "processor started (group=%s, batch=%d)",
        settings.processor_group_id,
        settings.processor_batch_size,
    )
    try:
        proc.run(max_seconds=args.max_seconds, idle_exit_s=args.idle_exit)
    finally:
        proc.close()
        t = proc.totals
        log.info(
            "processor stopped: consumed=%d lifecycle=%d pings=%d dup=%d parked=%d dlq=%s",
            t.consumed,
            t.committed_lifecycle,
            t.committed_pings,
            t.duplicates,
            t.parked,
            dict(t.dlq),
        )


if __name__ == "__main__":
    main()
