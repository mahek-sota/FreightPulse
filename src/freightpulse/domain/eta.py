"""ETA, speed smoothing, hours-of-service (HOS) and late-risk logic. Pure functions only."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from freightpulse.domain.enums import DriverStatus, LateRisk
from freightpulse.domain.geo import Point, road_miles

# --- constants ---------------------------------------------------------------------------

MIN_MOVING_SPEED_MPH: Final = 5.0
"""Pings slower than this are treated as stationary and ignored by the speed EWMA."""
EWMA_ALPHA: Final = 0.3
DEFAULT_SPEED_MPH: Final = 50.0
MIN_PLANNING_SPEED_MPH: Final = 20.0
MAX_PLANNING_SPEED_MPH: Final = 70.0

HOS_MAX_DRIVING_HOURS: Final = 11.0
HOS_REQUIRED_BREAK_HOURS: Final = 10.0
MAX_HOS_GAP_HOURS: Final = 6.0
"""Cap on time credited between two pings (protects HOS accounting from long dropouts)."""

LATE_RISK_MEDIUM_MARGIN: Final = timedelta(minutes=30)
STALE_PING_FULL_PENALTY_MIN: Final = 120.0


# --- speed EWMA --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SpeedStats:
    """Exponentially weighted mean and variance of moving speed."""

    mean: float | None = None
    var: float = 0.0
    samples: int = 0

    @property
    def cv(self) -> float:
        """Coefficient of variation (std / mean); 0 when undefined."""
        if not self.mean or self.mean <= 0:
            return 0.0
        return math.sqrt(max(self.var, 0.0)) / self.mean


def update_speed(stats: SpeedStats, speed_mph: float, alpha: float = EWMA_ALPHA) -> SpeedStats:
    """Fold one ping's speed into the EWMA. Stationary pings (< 5 mph) are ignored."""
    if speed_mph < MIN_MOVING_SPEED_MPH:
        return stats
    if stats.mean is None:
        return SpeedStats(mean=speed_mph, var=0.0, samples=1)
    diff = speed_mph - stats.mean
    mean = stats.mean + alpha * diff
    # Incremental EW variance (West 1979 / Finch 2009).
    var = (1 - alpha) * (stats.var + alpha * diff * diff)
    return SpeedStats(mean=mean, var=var, samples=stats.samples + 1)


# --- hours of service --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HosClock:
    """Driving hours used since the last qualifying 10-hour break."""

    drive_hours_used: float = 0.0
    rest_started_at: datetime | None = None
    last_status: DriverStatus | None = None

    @property
    def remaining_drive_hours(self) -> float:
        return max(0.0, HOS_MAX_DRIVING_HOURS - self.drive_hours_used)

    def remaining_rest_hours(self, now: datetime) -> float:
        """Hours of the current rest still needed before the clock resets (0 if not resting)."""
        if self.rest_started_at is None:
            return 0.0
        rested = (now - self.rest_started_at).total_seconds() / 3600
        return max(0.0, HOS_REQUIRED_BREAK_HOURS - rested)


def update_hos(
    clock: HosClock,
    status: DriverStatus,
    at: datetime,
    previous_ping_at: datetime | None,
) -> HosClock:
    """Advance the HOS clock with a new (in-order) ping.

    Time between two pings is credited as driving when the *previous* status was DRIVING.
    A continuous rest (OFF_DUTY/SLEEPER) of 10 hours resets the 11-hour driving allowance.
    """
    used = clock.drive_hours_used
    if previous_ping_at is not None and clock.last_status is DriverStatus.DRIVING:
        gap_h = (at - previous_ping_at).total_seconds() / 3600
        used += min(max(gap_h, 0.0), MAX_HOS_GAP_HOURS)

    rest_started = clock.rest_started_at
    if status.is_rest:
        if rest_started is None:
            rest_started = at
        if (at - rest_started).total_seconds() / 3600 >= HOS_REQUIRED_BREAK_HOURS:
            used = 0.0
    else:
        rest_started = None
    return HosClock(drive_hours_used=used, rest_started_at=rest_started, last_status=status)


def required_breaks(drive_hours: float, remaining_hos_hours: float) -> int:
    """Number of 10-hour breaks needed to drive ``drive_hours`` given the remaining allowance."""
    if drive_hours <= remaining_hos_hours:
        return 0
    return math.ceil((drive_hours - remaining_hos_hours) / HOS_MAX_DRIVING_HOURS)


# --- ETA ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EtaEstimate:
    eta: datetime
    miles_remaining: float
    drive_hours: float
    planning_speed_mph: float
    hos_breaks: int
    confidence: float


def planning_speed(stats: SpeedStats) -> float:
    if stats.mean is None:
        return DEFAULT_SPEED_MPH
    return min(MAX_PLANNING_SPEED_MPH, max(MIN_PLANNING_SPEED_MPH, stats.mean))


def eta_confidence(*, ping_gap_minutes: float, speed: SpeedStats) -> float:
    """0..1 confidence. Drops with stale pings, high speed variance and few samples."""
    c = 1.0
    c -= 0.5 * min(1.0, max(0.0, ping_gap_minutes) / STALE_PING_FULL_PENALTY_MIN)
    c -= min(0.3, speed.cv)
    if speed.samples < 3:
        c -= 0.2
    return round(min(1.0, max(0.05, c)), 3)


def estimate_eta(
    *,
    now: datetime,
    position: Point,
    destination: Point,
    speed: SpeedStats,
    hos: HosClock,
    ping_gap_minutes: float = 0.0,
) -> EtaEstimate:
    """ETA to ``destination`` from ``position`` with HOS breaks added when needed.

    miles_remaining = haversine x 1.18 road circuity. If the driver is mid-rest, the rest of
    that break is served first and the clock resets; otherwise each time remaining drive time
    exceeds the remaining HOS allowance a full 10-hour break is added.
    """
    miles = road_miles(position, destination)
    mph = planning_speed(speed)
    drive_h = miles / mph

    start_delay_h = 0.0
    remaining = hos.remaining_drive_hours
    rest_left = hos.remaining_rest_hours(now)
    if hos.rest_started_at is not None and hos.drive_hours_used >= HOS_MAX_DRIVING_HOURS - 1e-9:
        start_delay_h = rest_left
        remaining = HOS_MAX_DRIVING_HOURS

    breaks = required_breaks(drive_h, remaining)
    total_h = start_delay_h + drive_h + breaks * HOS_REQUIRED_BREAK_HOURS
    return EtaEstimate(
        eta=now + timedelta(hours=total_h),
        miles_remaining=round(miles, 2),
        drive_hours=drive_h,
        planning_speed_mph=mph,
        hos_breaks=breaks,
        confidence=eta_confidence(ping_gap_minutes=ping_gap_minutes, speed=speed),
    )


def degrade_confidence(confidence: float, staleness_minutes: float) -> float:
    """Confidence as seen at read time: decays as the last ping ages."""
    penalty = 0.5 * min(1.0, max(0.0, staleness_minutes) / STALE_PING_FULL_PENALTY_MIN)
    return round(max(0.05, confidence - penalty), 3)


# --- late risk ---------------------------------------------------------------------------


def classify_late_risk(eta: datetime, appt_end: datetime) -> LateRisk:
    """HIGH if the ETA is past the window end, MEDIUM if within 30 min of it, else LOW."""
    if eta > appt_end:
        return LateRisk.HIGH
    if eta > appt_end - LATE_RISK_MEDIUM_MARGIN:
        return LateRisk.MEDIUM
    return LateRisk.LOW


__all__ = [
    "EtaEstimate",
    "HosClock",
    "SpeedStats",
    "classify_late_risk",
    "degrade_confidence",
    "estimate_eta",
    "eta_confidence",
    "planning_speed",
    "required_breaks",
    "update_hos",
    "update_speed",
]
