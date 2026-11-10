from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable

from confluent_kafka import Consumer, Producer
from sqlalchemy import Engine, text

from freightpulse.config import Settings
from freightpulse.domain.events import EventEnvelope
from freightpulse.processing.dlq import DlqRecord
from freightpulse.processing.processor import StateProcessor


def produce(settings: Settings, items: Iterable[EventEnvelope | tuple[str, str, bytes]]) -> int:
    """Produce envelopes (routed by type) or raw (topic, key, bytes) tuples. Returns count."""
    p = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "linger.ms": 5})
    n = 0
    for item in items:
        if isinstance(item, EventEnvelope):
            topic = (
                settings.telemetry_topic
                if item.event_type.is_telemetry
                else settings.shipment_events_topic
            )
            p.produce(topic, key=item.shipment_id, value=item.to_json_bytes())
        else:
            topic, key, raw = item
            p.produce(topic, key=key, value=raw)
        n += 1
        p.poll(0)
    assert p.flush(30) == 0
    return n


def run_processor(
    settings: Settings, engine: Engine, *, idle_s: float = 3.0, max_s: float = 60.0
) -> StateProcessor:
    proc = StateProcessor(settings, engine=engine)
    try:
        proc.run(max_seconds=max_s, idle_exit_s=idle_s)
    finally:
        proc.close()
    return proc


def read_topic(
    settings: Settings, topic: str, *, expect: int | None = None, timeout_s: float = 15.0
) -> list[bytes]:
    c = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    out: list[bytes] = []
    deadline = time.monotonic() + timeout_s
    idle_since = time.monotonic()
    while time.monotonic() < deadline:
        msg = c.poll(0.5)
        if msg is None or msg.error():
            if expect is None and out and time.monotonic() - idle_since > 2:
                break
            if expect is not None and len(out) >= expect and time.monotonic() - idle_since > 1:
                break
            continue
        out.append(msg.value() or b"")
        idle_since = time.monotonic()
    c.close()
    return out


def dlq_records(settings: Settings, **kw: object) -> list[DlqRecord]:
    return [
        DlqRecord.model_validate_json(v) for v in read_topic(settings, settings.dlq_topic, **kw)
    ]  # type: ignore[arg-type]


def count(engine: Engine, sql: str, **params: object) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar_one())


def processor_env(settings: Settings, **extra: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "FREIGHTPULSE_DATABASE_URL": settings.database_url,
            "FREIGHTPULSE_KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
            "FREIGHTPULSE_TOPIC_PREFIX": settings.topic_prefix,
            "FREIGHTPULSE_PROCESSOR_GROUP_ID": settings.processor_group_id,
            "FREIGHTPULSE_PROCESSOR_BATCH_SIZE": str(settings.processor_batch_size),
            "FREIGHTPULSE_PROCESSOR_BATCH_TIMEOUT_S": "0.2",
            # A crashed member keeps its partitions until the session times out; keep it short
            # and make --idle-exit longer so a restarted processor outlives that window.
            "FREIGHTPULSE_KAFKA_SESSION_TIMEOUT_MS": "6000",
        }
    )
    env.update(extra)
    return env


def spawn_processor(
    settings: Settings, *, idle_exit: float = 12.0, **extra: str
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "freightpulse.processing.processor",
            "--idle-exit",
            str(idle_exit),
            "--metrics-port",
            "0",
            "--max-seconds",
            "120",
        ],
        env=processor_env(settings, **extra),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
