"""Chaos knobs applied between the simulation and the wire.

* duplicates: the same event (same ``event_id``) is sent again a few seconds later;
* out-of-order: an event's *emission* is delayed 1-10 simulated minutes (its event_time is not);
* malformed: the bytes are corrupted (bad JSON, missing/invalid fields, unknown type, teleport);
* GPS dropout: ``--gps-dropout-rate`` is the expected fraction of pings lost. Losses come in
  realistic episodes (a truck goes dark for 60-150 simulated minutes, e.g. a dead zone or an
  unplugged ELD), so the per-ping chance of *starting* an episode is the rate divided by the
  expected number of pings in an episode.

``shipment.created`` comes from the TMS and is never corrupted (ELD/telematics feeds are the
messy ones); this keeps every simulated shipment traceable end to end.
"""

from __future__ import annotations

import heapq
import json
import random
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from freightpulse.domain.enums import EventType
from freightpulse.domain.events import make_envelope
from freightpulse.simulator.fleet import SimEvent


@dataclass(frozen=True, slots=True)
class ChaosConfig:
    duplicate_rate: float = 0.02
    out_of_order_rate: float = 0.03
    malformed_rate: float = 0.005
    gps_dropout_rate: float = 0.01
    seed: int = 42
    mean_ping_interval_s: float = 75.0
    dropout_minutes: tuple[float, float] = (60.0, 150.0)

    @property
    def dropout_start_probability(self) -> float:
        mean_minutes = sum(self.dropout_minutes) / 2
        pings_per_episode = mean_minutes * 60 / self.mean_ping_interval_s
        return self.gps_dropout_rate / pings_per_episode


@dataclass(frozen=True, slots=True)
class Emission:
    """Something to send at simulated time ``emit_at``."""

    emit_at: datetime
    event: SimEvent
    corruption: str | None = None
    duplicate: bool = False
    delayed: bool = False

    @property
    def key(self) -> str:
        return self.event.shipment_id

    @property
    def is_telemetry(self) -> bool:
        return self.event.is_telemetry

    def to_bytes(self, produced_at: datetime) -> bytes:
        env = make_envelope(
            event_id=self.event.event_id,
            event_type=self.event.event_type,
            shipment_id=self.event.shipment_id,
            event_time=self.event.event_time,
            produced_at=produced_at,
            source=self.event.source,
            payload=self.event.payload,
        )
        raw = env.to_json_bytes()
        if self.corruption is None:
            return raw
        return corrupt(raw, self.corruption)


CORRUPTIONS = (
    "truncated",
    "missing_field",
    "bad_type",
    "unknown_event_type",
    "out_of_range",
    "teleport",
)


def corrupt(raw: bytes, kind: str) -> bytes:
    if kind == "truncated":
        return raw[: max(1, len(raw) // 2)]
    data = json.loads(raw)
    if kind == "missing_field":
        data.pop("shipment_id", None)
    elif kind == "bad_type":
        data["event_time"] = "yesterday-ish"
    elif kind == "unknown_event_type":
        data["event_type"] = "shipment.teleported"
    elif kind == "out_of_range":
        if data["event_type"] == EventType.GPS_PING:
            data["payload"]["lat"] = 999.0
        else:
            data["schema_version"] = 99
    elif kind == "teleport":
        # Schema-valid but physically impossible: the ping jumps across the country.
        data["payload"]["lat"] = round(90 - abs(data["payload"]["lat"]) * 0.5, 4)
        data["payload"]["lon"] = round(-data["payload"]["lon"] / 2 - 90, 4)
    else:  # pragma: no cover - guarded by CORRUPTIONS
        raise ValueError(kind)
    return json.dumps(data).encode()


class ChaosStream:
    """Turns an event-time-ordered stream into an emission-time-ordered stream with chaos."""

    def __init__(self, cfg: ChaosConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed + 7919)
        self.stats: Counter[str] = Counter()
        self._dropout_until: dict[str, datetime] = {}

    def _dropped(self, ev: SimEvent) -> bool:
        if not ev.is_telemetry:
            return False
        until = self._dropout_until.get(ev.shipment_id)
        if until is not None and ev.event_time < until:
            return True
        if self.rng.random() < self.cfg.dropout_start_probability:
            minutes = self.rng.uniform(*self.cfg.dropout_minutes)
            self._dropout_until[ev.shipment_id] = ev.event_time + timedelta(minutes=minutes)
            self.stats["dropout_episodes"] += 1
            return True
        return False

    def emissions(self, events: Iterator[SimEvent]) -> Iterator[Emission]:
        pending: list[tuple[datetime, int, Emission]] = []
        seq = 0

        def push(e: Emission) -> None:
            nonlocal seq
            seq += 1
            heapq.heappush(pending, (e.emit_at, seq, e))

        for ev in events:
            # Release everything that is due before this event's natural emission time.
            while pending and pending[0][0] <= ev.event_time:
                yield heapq.heappop(pending)[2]
            self.stats["generated"] += 1
            if self._dropped(ev):
                self.stats["dropped_gps"] += 1
                continue
            corruption = None
            if (
                ev.event_type is not EventType.SHIPMENT_CREATED
                and self.rng.random() < self.cfg.malformed_rate
            ):
                options = [c for c in CORRUPTIONS if c != "teleport" or ev.is_telemetry]
                corruption = self.rng.choice(options)
                self.stats[f"malformed:{corruption}"] += 1
            emit_at = ev.event_time
            delayed = False
            if self.rng.random() < self.cfg.out_of_order_rate:
                emit_at += timedelta(minutes=self.rng.uniform(1, 10))
                delayed = True
                self.stats["out_of_order"] += 1
            push(Emission(emit_at, ev, corruption, delayed=delayed))
            if self.rng.random() < self.cfg.duplicate_rate:
                self.stats["duplicates"] += 1
                push(
                    Emission(
                        emit_at + timedelta(seconds=self.rng.uniform(1, 30)),
                        ev,
                        corruption,
                        duplicate=True,
                        delayed=delayed,
                    )
                )
        while pending:
            yield heapq.heappop(pending)[2]
