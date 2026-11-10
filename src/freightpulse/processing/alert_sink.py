"""Alert sink: ``freight.alerts`` -> ``alerts`` table, idempotently (manual offset commits).

Rules:
* TRIGGER upserts by ``dedupe_key``. An existing alert only changes if the new severity is
  higher (escalation), so replays are no-ops.
* A LATE_RISK trigger supersedes (resolves) older active LATE_RISK alerts of the same shipment,
  since there is one late-risk level per shipment at a time.
* RESOLVE closes the alert with that ``dedupe_key``. For LATE_RISK it closes every active
  late-risk alert of the shipment triggered at or before the resolve time.

Time comparisons use the alert's event time, so replaying old messages never closes newer alerts.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
from collections.abc import Sequence

from confluent_kafka import Consumer, KafkaError, TopicPartition
from pydantic import ValidationError
from sqlalchemy import Connection, Engine, text

from freightpulse.config import Settings, get_settings
from freightpulse.domain.alerts import AlertMessage
from freightpulse.domain.enums import AlertAction, AlertType
from freightpulse.observability import metrics as m
from freightpulse.storage.db import make_engine

log = logging.getLogger("alert-sink")

_UPSERT = text(
    """
    INSERT INTO alerts (alert_id, shipment_id, alert_type, severity, message, triggered_at,
                        dedupe_key, details)
    VALUES (:alert_id, :shipment_id, :alert_type, :severity, :message, :triggered_at,
            :dedupe_key, CAST(:details AS jsonb))
    ON CONFLICT (dedupe_key) DO UPDATE
    SET severity = EXCLUDED.severity, message = EXCLUDED.message,
        details = alerts.details || EXCLUDED.details, updated_at = now()
    WHERE alerts.resolved_at IS NULL
      AND array_position(ARRAY['LOW','MEDIUM','HIGH','CRITICAL'], EXCLUDED.severity)
        > array_position(ARRAY['LOW','MEDIUM','HIGH','CRITICAL'], alerts.severity)
    """
)

_SUPERSEDE_LATE = text(
    """
    UPDATE alerts SET resolved_at = :at, updated_at = now()
    WHERE shipment_id = :shipment_id AND alert_type = 'LATE_RISK' AND resolved_at IS NULL
      AND dedupe_key <> :dedupe_key AND triggered_at <= :at
    """
)

_RESOLVE_KEY = text(
    """
    UPDATE alerts SET resolved_at = :at, details = details || CAST(:details AS jsonb),
                      updated_at = now()
    WHERE dedupe_key = :dedupe_key AND resolved_at IS NULL
    """
)


def apply_alerts(conn: Connection, alerts: Sequence[AlertMessage]) -> dict[str, int]:
    counts = {"trigger": 0, "resolve": 0}
    for a in alerts:
        details = json.dumps(a.details)
        if a.action is AlertAction.TRIGGER:
            if a.alert_type is AlertType.LATE_RISK:
                conn.execute(
                    _SUPERSEDE_LATE,
                    {
                        "shipment_id": a.shipment_id,
                        "dedupe_key": a.dedupe_key,
                        "at": a.triggered_at,
                    },
                )
            conn.execute(
                _UPSERT,
                {
                    "alert_id": a.alert_id,
                    "shipment_id": a.shipment_id,
                    "alert_type": a.alert_type.value,
                    "severity": a.severity.value,
                    "message": a.message,
                    "triggered_at": a.triggered_at,
                    "dedupe_key": a.dedupe_key,
                    "details": details,
                },
            )
            counts["trigger"] += 1
        else:
            if a.alert_type is AlertType.LATE_RISK:
                conn.execute(
                    _SUPERSEDE_LATE,
                    {"shipment_id": a.shipment_id, "dedupe_key": "", "at": a.triggered_at},
                )
            else:
                conn.execute(
                    _RESOLVE_KEY,
                    {"dedupe_key": a.dedupe_key, "at": a.triggered_at, "details": details},
                )
            counts["resolve"] += 1
    return counts


class AlertSink:
    def __init__(self, settings: Settings, engine: Engine | None = None) -> None:
        self.settings = settings
        self.engine = engine or make_engine(settings.database_url, pool_size=1)
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.alert_sink_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "allow.auto.create.topics": False,
                "session.timeout.ms": settings.kafka_session_timeout_ms,
                "statistics.interval.ms": 5000,
                "stats_cb": m.update_lag_from_stats,
            }
        )
        self.consumer.subscribe([settings.alerts_topic])
        self._running = False
        self.applied = 0

    def poll_once(self) -> int:
        msgs = self.consumer.consume(num_messages=500, timeout=0.5)
        alerts: list[AlertMessage] = []
        offsets: dict[tuple[str, int], int] = {}
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", err)
                continue
            offsets[(msg.topic() or "", msg.partition() or 0)] = msg.offset() or 0
            try:
                alerts.append(AlertMessage.model_validate_json(msg.value() or b""))
            except ValidationError as exc:
                log.error("skipping invalid alert message at offset %s: %s", msg.offset(), exc)
        if not offsets:
            return 0
        with self.engine.begin() as conn:
            counts = apply_alerts(conn, alerts)
        for action, n in counts.items():
            m.ALERTS_SINK_WRITTEN.labels(action=action).inc(n)
        self.consumer.commit(
            offsets=[TopicPartition(t, p, o + 1) for (t, p), o in offsets.items()],
            asynchronous=False,
        )
        self.applied += len(alerts)
        return len(alerts)

    def run(self, max_seconds: float | None = None, idle_exit_s: float | None = None) -> None:
        import time

        self._running = True
        start = time.monotonic()
        last: float | None = None
        while self._running:
            if self.poll_once():
                last = time.monotonic()
            now = time.monotonic()
            if max_seconds is not None and now - start >= max_seconds:
                break
            if idle_exit_s is not None and last is not None and now - last >= idle_exit_s:
                break

    def stop(self, *_: object) -> None:
        self._running = False

    def close(self) -> None:
        self.consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="FreightPulse alert sink")
    parser.add_argument("--max-seconds", type=float, default=None)
    args = parser.parse_args()
    settings = get_settings()
    m.serve_metrics(settings.alert_sink_metrics_port)
    sink = AlertSink(settings)
    signal.signal(signal.SIGTERM, sink.stop)
    signal.signal(signal.SIGINT, sink.stop)
    log.info("alert sink started (group=%s)", settings.alert_sink_group_id)
    try:
        sink.run(max_seconds=args.max_seconds)
    finally:
        sink.close()


if __name__ == "__main__":
    main()
