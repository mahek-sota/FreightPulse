"""Live alert fan-out for ``/ws/alerts``.

A background thread consumes ``freight.alerts`` with a per-instance consumer group starting at
the latest offset (live tail, not system of record) and hands each message to the event loop,
which pushes it into every connected client's bounded queue. Slow clients drop messages
rather than slowing everyone down.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import uuid

from confluent_kafka import Consumer, KafkaError

from freightpulse.config import Settings

log = logging.getLogger("api.ws")

CLIENT_QUEUE_SIZE = 1000


class AlertBroadcaster:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._clients: set[asyncio.Queue[str]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.received = 0
        self.dropped = 0
        self.ready = threading.Event()

    # -- lifecycle --------------------------------------------------------------------------

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._thread = threading.Thread(target=self._consume, name="alert-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    # -- clients ----------------------------------------------------------------------------

    def subscribe(self) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=CLIENT_QUEUE_SIZE)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[str]) -> None:
        self._clients.discard(q)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    def publish(self, message: str) -> None:
        """Called on the event loop thread."""
        self.received += 1
        for q in list(self._clients):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                self.dropped += 1

    # -- kafka thread -----------------------------------------------------------------------

    def _consume(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": f"freightpulse-api-ws-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "allow.auto.create.topics": False,
            }
        )
        consumer.subscribe(
            [self.settings.alerts_topic], on_assign=lambda _c, _parts: self.ready.set()
        )
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        log.warning("alert stream error: %s", err)
                    continue
                payload = (msg.value() or b"").decode("utf-8", errors="replace")
                if self._loop is not None:
                    with contextlib.suppress(RuntimeError):  # loop closed during shutdown
                        self._loop.call_soon_threadsafe(self.publish, payload)
        finally:
            consumer.close()
