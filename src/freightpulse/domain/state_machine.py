"""Shipment lifecycle state machine (spec section 4.3).

::

    CREATED -> TENDERED -> ACCEPTED -> AT_PICKUP -> IN_TRANSIT -> AT_DELIVERY -> DELIVERED
                  \\-> REJECTED -> TENDERED (re-tender)
    Any active state -> EXCEPTION -> (prior active state on resolution)

Design (see docs/adr/0003-out-of-order-handling.md):

* Every status has a *rank* along the main path. TENDERED and REJECTED share rank 1.
* Forward moves (higher rank) are always valid, including skips such as ACCEPTED -> IN_TRANSIT.
  Telematics feeds miss geofence events, and order-independence requires that a newer event
  arriving before an older one is never judged invalid.
* A move to a *lower* rank with a newer event time is ``INVALID`` (e.g. DELIVERED -> IN_TRANSIT).
* Events older than the event that set the current status are "out of order": they are
  recorded (and backfill stop timestamps) but never regress the status.
* EXCEPTION is an overlay that remembers the status underneath it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from freightpulse.domain.enums import EventType, ShipmentStatus

RANK: Final[dict[ShipmentStatus, int]] = {
    ShipmentStatus.CREATED: 0,
    ShipmentStatus.TENDERED: 1,
    ShipmentStatus.REJECTED: 1,
    ShipmentStatus.ACCEPTED: 2,
    ShipmentStatus.AT_PICKUP: 3,
    ShipmentStatus.IN_TRANSIT: 4,
    ShipmentStatus.AT_DELIVERY: 5,
    ShipmentStatus.DELIVERED: 6,
}
TENDER_STAGE: Final = frozenset({ShipmentStatus.TENDERED, ShipmentStatus.REJECTED})
TERMINAL: Final = frozenset({ShipmentStatus.DELIVERED})


class Target(StrEnum):
    """What a lifecycle event asks the state machine to do, beyond plain statuses."""

    RAISE_EXCEPTION = "RAISE_EXCEPTION"
    RESOLVE_EXCEPTION = "RESOLVE_EXCEPTION"


class Outcome(StrEnum):
    APPLIED = "APPLIED"
    NOOP = "NOOP"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class StatusView:
    status: ShipmentStatus
    pre_exception_status: ShipmentStatus | None = None

    @property
    def base(self) -> ShipmentStatus:
        """The active status underneath an EXCEPTION overlay (or the status itself)."""
        if self.status is ShipmentStatus.EXCEPTION:
            return self.pre_exception_status or ShipmentStatus.CREATED
        return self.status

    @property
    def rank(self) -> int:
        return RANK[self.base]


@dataclass(frozen=True, slots=True)
class Decision:
    outcome: Outcome
    view: StatusView
    reason: str = ""


def stop_event_target(
    event_type: EventType, stop_sequence: int, stop_count: int
) -> ShipmentStatus | None:
    """Map a stop event to a status.

    Arriving at the first stop -> AT_PICKUP; departing any non-final stop -> IN_TRANSIT;
    arriving at the final stop -> AT_DELIVERY. Arrivals at intermediate stops and departure
    from the final stop do not change status (only stop timestamps).
    """
    is_first = stop_sequence == 1
    is_last = stop_sequence == stop_count
    if event_type is EventType.STOP_ARRIVED:
        if is_first:
            return ShipmentStatus.AT_PICKUP
        if is_last:
            return ShipmentStatus.AT_DELIVERY
        return None
    if event_type is EventType.STOP_DEPARTED:
        # Departure from any non-final stop means the truck is rolling again.
        return None if is_last else ShipmentStatus.IN_TRANSIT
    raise ValueError(f"not a stop event: {event_type}")


SIMPLE_TARGETS: Final[dict[EventType, ShipmentStatus]] = {
    EventType.SHIPMENT_CREATED: ShipmentStatus.CREATED,
    EventType.SHIPMENT_TENDERED: ShipmentStatus.TENDERED,
    EventType.TENDER_ACCEPTED: ShipmentStatus.ACCEPTED,
    EventType.TENDER_REJECTED: ShipmentStatus.REJECTED,
    EventType.SHIPMENT_DELIVERED: ShipmentStatus.DELIVERED,
}


def decide(view: StatusView, target: ShipmentStatus | Target) -> Decision:
    """Decide the effect of an *in-order* lifecycle event on the current status."""
    base = view.base
    in_exception = view.status is ShipmentStatus.EXCEPTION

    if base in TERMINAL:
        if target is ShipmentStatus.DELIVERED or target is Target.RESOLVE_EXCEPTION:
            return Decision(Outcome.NOOP, view)
        return Decision(Outcome.INVALID, view, f"{base} is terminal; cannot apply {target}")

    if target is Target.RAISE_EXCEPTION:
        if in_exception:
            return Decision(Outcome.NOOP, view)
        return Decision(Outcome.APPLIED, StatusView(ShipmentStatus.EXCEPTION, view.status))

    if target is Target.RESOLVE_EXCEPTION:
        if in_exception:
            return Decision(Outcome.APPLIED, StatusView(base))
        return Decision(Outcome.NOOP, view)

    assert isinstance(target, ShipmentStatus)
    if target is ShipmentStatus.EXCEPTION:  # pragma: no cover - guarded by callers
        raise ValueError("use Target.RAISE_EXCEPTION")

    if RANK[target] > RANK[base]:
        return Decision(Outcome.APPLIED, StatusView(target))
    if target is base:
        # Same-stage lifecycle evidence while in EXCEPTION means operations resumed.
        if in_exception:
            return Decision(Outcome.APPLIED, StatusView(target))
        return Decision(Outcome.NOOP, view)
    if base in TENDER_STAGE and target in TENDER_STAGE:
        return Decision(Outcome.APPLIED, StatusView(target))
    return Decision(Outcome.INVALID, view, f"cannot move from {base} back to {target}")


def absorb_out_of_order(view: StatusView, target: ShipmentStatus | Target) -> StatusView:
    """Effect of an *older* lifecycle event: never regress status.

    The only change allowed is advancing the status *underneath* an EXCEPTION overlay, so that
    resolving the exception returns to the same state an in-order sequence would have.
    """
    if (
        view.status is ShipmentStatus.EXCEPTION
        and isinstance(target, ShipmentStatus)
        and target is not ShipmentStatus.EXCEPTION
        and view.pre_exception_status is not None
        and RANK[target] > RANK[view.pre_exception_status]
        and view.pre_exception_status not in TERMINAL
    ):
        return StatusView(ShipmentStatus.EXCEPTION, target)
    return view


def is_regression(before: StatusView, after: StatusView) -> bool:
    """True if ``after`` moved backwards along the main path (never allowed)."""
    if before.base in TERMINAL and after.base is not before.base:
        return True
    return after.rank < before.rank
