"""Kafka topic administration: explicit partition counts, never broker auto-create."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from freightpulse.config import Settings

log = logging.getLogger(__name__)


def ensure_topics(settings: Settings, timeout_s: float = 30.0) -> list[str]:
    """Create any missing topics with the partition counts from settings.

    Returns the names of topics that were created.
    """
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    wanted = settings.topic_partitions()
    to_create = [
        NewTopic(name, num_partitions=parts, replication_factor=1)
        for name, parts in wanted.items()
        if name not in existing
    ]
    if not to_create:
        return []
    created: list[str] = []
    for name, future in admin.create_topics(to_create, operation_timeout=timeout_s).items():
        try:
            future.result()
            created.append(name)
            log.info("created topic %s (%d partitions)", name, wanted[name])
        except KafkaException as exc:  # pragma: no cover - race with another creator
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return created


def delete_topics(settings: Settings, timeout_s: float = 30.0) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    names = [t for t in settings.topic_partitions() if t in existing]
    if not names:
        return
    for future in admin.delete_topics(names, operation_timeout=timeout_s).values():
        future.result()


def broker_reachable(settings: Settings, timeout_s: float = 2.0) -> bool:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    try:
        admin.list_topics(timeout=timeout_s)
    except KafkaException:
        return False
    return True
