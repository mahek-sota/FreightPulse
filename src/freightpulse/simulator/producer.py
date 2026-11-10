"""Kafka producer wrapper (keyed by shipment_id for per-shipment ordering)."""

from __future__ import annotations

import logging
from collections import Counter

from confluent_kafka import KafkaError, Message, Producer

from freightpulse.config import Settings

log = logging.getLogger(__name__)


def producer_config(settings: Settings, **overrides: str | int | float | bool) -> dict[str, object]:
    cfg: dict[str, object] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 10,
        "batch.num.messages": 10_000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
    }
    cfg.update(overrides)
    return cfg


class EventProducer:
    def __init__(self, settings: Settings, **overrides: str | int | float | bool) -> None:
        self.settings = settings
        self._producer = Producer(producer_config(settings, **overrides))
        self.stats: Counter[str] = Counter()

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.stats["delivery_errors"] += 1
            log.error("delivery failed for %s: %s", msg.topic(), err)
        else:
            self.stats["delivered"] += 1

    def send(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(topic, value=value, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.05)
        self.stats["sent"] += 1
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
