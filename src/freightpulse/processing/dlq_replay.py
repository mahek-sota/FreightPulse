"""Replay the DLQ: re-publish each dead-lettered event's original bytes to its original topic.

Run it after fixing the cause (deploying a parser fix, seeding missing reference data, ...).
It reads ``freight.dlq`` with its own consumer group from the last replayed position up to the
high watermark observed at start-up, so a replay never chases events that fail again. Events
that still fail are dead-lettered again by the processor with a fresh ``failed_at``.
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

from freightpulse.config import Settings, get_settings
from freightpulse.processing.dlq import DlqRecord
from freightpulse.simulator.producer import EventProducer

log = logging.getLogger("dlq-replay")


def replay(
    settings: Settings,
    *,
    group_id: str = "freightpulse-dlq-replay",
    error_types: set[str] | None = None,
    dry_run: bool = False,
    poll_timeout_s: float = 2.0,
) -> Counter[str]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    topic = settings.dlq_topic
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    ends: dict[int, int] = {}
    for pid in meta.partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
        ends[pid] = high
    consumer.subscribe([topic])
    producer = EventProducer(settings)
    stats: Counter[str] = Counter()
    done: set[int] = set()
    positions: dict[int, int] = {}
    idle_polls = 0
    while len(done) < len(ends) and idle_polls < 3:
        msg = consumer.poll(poll_timeout_s)
        if msg is None:
            idle_polls += 1
            # Partitions we already sit at the end of count as done.
            for tp in consumer.assignment():
                pos = consumer.position([tp])[0].offset
                if pos >= ends.get(tp.partition, 0) or ends.get(tp.partition, 0) == 0:
                    done.add(tp.partition)
            continue
        idle_polls = 0
        if msg.error():
            log.error("dlq consume error: %s", msg.error())
            continue
        pid, off = msg.partition() or 0, msg.offset() or 0
        if off >= ends.get(pid, 0):
            done.add(pid)
            continue
        rec = DlqRecord.model_validate_json(msg.value() or b"{}")
        if error_types and rec.error_type not in error_types:
            stats["skipped"] += 1
        elif dry_run:
            stats[f"would_replay:{rec.error_type}"] += 1
        else:
            producer.send(rec.original_topic, rec.original_key or "", rec.original_bytes())
            stats[f"replayed:{rec.error_type}"] += 1
        positions[pid] = off + 1
        if off + 1 >= ends.get(pid, 0):
            done.add(pid)
    producer.flush()
    if positions and not dry_run:
        consumer.commit(
            offsets=[TopicPartition(topic, p, o) for p, o in positions.items()], asynchronous=False
        )
    consumer.close()
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--error-type",
        action="append",
        dest="error_types",
        help="only replay these error types (repeatable)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--group-id", default="freightpulse-dlq-replay")
    args = parser.parse_args()
    stats = replay(
        get_settings(),
        group_id=args.group_id,
        error_types=set(args.error_types) if args.error_types else None,
        dry_run=args.dry_run,
    )
    total = sum(v for k, v in stats.items() if k.startswith(("replayed", "would_replay")))
    log.info(
        "dlq replay finished: %d event(s) %s; %s",
        total,
        "would be replayed" if args.dry_run else "replayed",
        dict(stats),
    )


if __name__ == "__main__":
    main()
