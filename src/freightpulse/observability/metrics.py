"""Prometheus metrics shared by the FreightPulse services (spec section 10)."""

from __future__ import annotations

import json
import logging
from typing import Final

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

_LATENCY_BUCKETS: Final = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0,
    7.5, 10.0, 20.0, 30.0, 60.0,
)  # fmt: skip
_LATENESS_BUCKETS: Final = (1, 5, 15, 30, 60, 120, 300, 600, 1200, 3600, 7200, 21600, 86400)

EVENTS_CONSUMED = Counter("events_consumed_total", "Messages consumed", ["topic", "event_type"])
EVENTS_DLQ = Counter("events_dlq_total", "Events routed to the DLQ", ["error_type"])
EVENTS_DUPLICATE = Counter(
    "events_duplicate_total", "Events skipped because their event_id was already processed"
)
EVENTS_PARKED = Counter(
    "events_parked_total", "Events parked because their shipment was not known yet"
)
EVENTS_COMMITTED = Counter("events_committed_total", "Events durably written to Postgres", ["kind"])
EVENTS_OUT_OF_ORDER = Counter(
    "events_out_of_order_total", "Events older than the newest event already seen for a shipment"
)
EVENT_LATENESS = Histogram(
    "event_lateness_seconds",
    "How far (event time) an out-of-order event lagged the newest event of its shipment",
    buckets=_LATENESS_BUCKETS,
)
PROCESSING_LATENCY = Histogram(
    "processing_latency_seconds",
    "End-to-end latency: producer produced_at -> committed to Postgres",
    buckets=_LATENCY_BUCKETS,
)
CONSUMER_LAG = Gauge("consumer_lag", "Messages behind the high watermark", ["topic", "partition"])
DB_WRITE_BATCH = Histogram(
    "db_write_batch_seconds",
    "Duration of one batch database transaction",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
BATCH_SIZE = Histogram(
    "processor_batch_size",
    "Messages per processed batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000, 2000),
)
VERSION_CONFLICTS = Counter(
    "state_version_conflicts_total", "Optimistic-concurrency conflicts that forced a batch retry"
)
ALERTS_EMITTED = Counter(
    "alerts_emitted_total", "Alerts published to freight.alerts", ["type", "severity"]
)
ALERTS_SINK_WRITTEN = Counter(
    "alerts_sink_written_total", "Alert messages applied by the alert sink", ["action"]
)
PARKED_EVENTS = Gauge("parked_events", "Events currently parked awaiting shipment.created")
SCANNER_RUN = Histogram(
    "scanner_run_seconds",
    "Duration of a periodic job run",
    ["job"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
FLEET_CLOCK = Gauge("fleet_clock_timestamp_seconds", "Event-time watermark used by the scanners")


def update_lag_from_stats(stats_json: str) -> None:
    """librdkafka statistics callback -> consumer_lag{topic,partition}."""
    try:
        stats = json.loads(stats_json)
    except ValueError:  # pragma: no cover - librdkafka always sends JSON
        return
    for topic, tdata in stats.get("topics", {}).items():
        for pid, pdata in tdata.get("partitions", {}).items():
            if pid == "-1":
                continue
            lag = pdata.get("consumer_lag")
            if lag is not None and lag >= 0:
                CONSUMER_LAG.labels(topic=topic, partition=pid).set(lag)


def serve_metrics(port: int) -> None:
    if port > 0:
        start_http_server(port)
        log.info("metrics on :%d/metrics", port)
