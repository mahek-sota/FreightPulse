from __future__ import annotations

import math
from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freightpulse.domain.enums import DriverStatus, LateRisk
from freightpulse.domain.eta import (
    HOS_MAX_DRIVING_HOURS,
    HosClock,
    SpeedStats,
    classify_late_risk,
    degrade_confidence,
    estimate_eta,
    eta_confidence,
    planning_speed,
    required_breaks,
    update_hos,
    update_speed,
)
from freightpulse.domain.geo import (
    Point,
    haversine_mi,
    initial_bearing_deg,
    interpolate,
    offset,
    road_miles,
)
from tests.factories import DALLAS, MEMPHIS, T0

lat = st.floats(min_value=-80, max_value=80, allow_nan=False)
lon = st.floats(min_value=-179, max_value=179, allow_nan=False)
points = st.builds(Point, lat, lon)


# --- haversine ------------------------------------------------------------------------------


def test_haversine_known_distance_dallas_memphis() -> None:
    # Great-circle Dallas -> Memphis is ~420 mi.
    assert haversine_mi(DALLAS[1], MEMPHIS[1]) == pytest.approx(420, abs=10)


def test_haversine_one_degree_latitude_is_69_miles() -> None:
    assert haversine_mi(Point(0, 0), Point(1, 0)) == pytest.approx(69.09, abs=0.05)


def test_road_miles_applies_circuity() -> None:
    a, b = DALLAS[1], MEMPHIS[1]
    assert road_miles(a, b) == pytest.approx(haversine_mi(a, b) * 1.18)


@given(points, points)
def test_haversine_symmetric_and_non_negative(a: Point, b: Point) -> None:
    d = haversine_mi(a, b)
    assert d >= 0
    assert d == pytest.approx(haversine_mi(b, a), abs=1e-6)
    assert d <= math.pi * 3958.7613 + 1e-6


@given(points, points, points)
def test_haversine_triangle_inequality(a: Point, b: Point, c: Point) -> None:
    assert haversine_mi(a, c) <= haversine_mi(a, b) + haversine_mi(b, c) + 1e-6


@given(points, points, st.floats(min_value=0, max_value=1))
def test_interpolate_stays_on_path(a: Point, b: Point, f: float) -> None:
    total = haversine_mi(a, b)
    if total > 10_000:  # antipodal edge cases are ill-conditioned
        return
    p = interpolate(a, b, f)
    assert haversine_mi(a, p) + haversine_mi(p, b) == pytest.approx(total, abs=0.5)


def test_interpolate_endpoints_and_bearing() -> None:
    a, b = DALLAS[1], MEMPHIS[1]
    assert interpolate(a, b, 0) == a
    assert interpolate(a, b, 1) == b
    assert interpolate(a, a, 0.5) == a
    assert 45 < initial_bearing_deg(a, b) < 90  # Memphis is east-north-east of Dallas
    moved = offset(a, 1.0, 0.0)
    assert haversine_mi(a, moved) == pytest.approx(1.0, abs=0.01)


# --- speed EWMA -----------------------------------------------------------------------------


def test_ewma_ignores_slow_pings_and_converges() -> None:
    s = SpeedStats()
    s = update_speed(s, 3.0)
    assert s.mean is None and s.samples == 0
    s = update_speed(s, 60.0)
    assert s.mean == 60.0 and s.samples == 1
    for _ in range(30):
        s = update_speed(s, 50.0)
    assert s.mean == pytest.approx(50.0, abs=0.01)
    assert s.cv < 0.01


def test_speed_variance_increases_cv() -> None:
    steady = SpeedStats()
    noisy = SpeedStats()
    for i in range(20):
        steady = update_speed(steady, 60.0)
        noisy = update_speed(noisy, 30.0 if i % 2 else 70.0)
    assert noisy.cv > steady.cv
    assert eta_confidence(ping_gap_minutes=1, speed=noisy) < eta_confidence(
        ping_gap_minutes=1, speed=steady
    )


def test_planning_speed_default_and_clamps() -> None:
    assert planning_speed(SpeedStats()) == 50.0
    assert planning_speed(SpeedStats(mean=90.0, samples=5)) == 70.0
    assert planning_speed(SpeedStats(mean=8.0, samples=5)) == 20.0


# --- HOS ------------------------------------------------------------------------------------


def test_hos_accrues_only_while_driving() -> None:
    c = update_hos(HosClock(), DriverStatus.DRIVING, T0, None)
    c = update_hos(c, DriverStatus.ON_DUTY, T0 + timedelta(hours=2), T0)
    assert c.drive_hours_used == pytest.approx(2.0)
    c = update_hos(c, DriverStatus.ON_DUTY, T0 + timedelta(hours=3), T0 + timedelta(hours=2))
    assert c.drive_hours_used == pytest.approx(2.0)  # previous status was ON_DUTY
    assert c.remaining_drive_hours == pytest.approx(9.0)


def test_hos_ten_hour_rest_resets_clock() -> None:
    c = HosClock(drive_hours_used=11.0, last_status=DriverStatus.DRIVING)
    c = update_hos(c, DriverStatus.SLEEPER, T0, T0)
    assert c.rest_started_at == T0
    c = update_hos(c, DriverStatus.SLEEPER, T0 + timedelta(hours=5), T0)
    assert c.drive_hours_used == 11.0
    assert c.remaining_rest_hours(T0 + timedelta(hours=5)) == pytest.approx(5.0)
    c = update_hos(c, DriverStatus.SLEEPER, T0 + timedelta(hours=10), T0 + timedelta(hours=5))
    assert c.drive_hours_used == 0.0
    c = update_hos(c, DriverStatus.DRIVING, T0 + timedelta(hours=10, minutes=5), T0)
    assert c.rest_started_at is None


def test_hos_gap_is_capped() -> None:
    c = HosClock(last_status=DriverStatus.DRIVING)
    c = update_hos(c, DriverStatus.DRIVING, T0 + timedelta(hours=20), T0)
    assert c.drive_hours_used == 6.0


@pytest.mark.parametrize(
    ("drive", "remaining", "breaks"),
    [(5, 11, 0), (11, 11, 0), (12, 11, 1), (22, 11, 1), (23, 11, 2), (3, 1, 1), (1, 0, 1)],
)
def test_required_breaks(drive: float, remaining: float, breaks: int) -> None:
    assert required_breaks(drive, remaining) == breaks


@given(st.floats(0, 200), st.floats(0, 11))
def test_breaks_cover_drive_time(drive: float, remaining: float) -> None:
    n = required_breaks(drive, remaining)
    assert remaining + n * HOS_MAX_DRIVING_HOURS >= drive - 1e-9
    if n > 0:
        assert remaining + (n - 1) * HOS_MAX_DRIVING_HOURS < drive


# --- ETA ------------------------------------------------------------------------------------


def test_eta_without_hos_break() -> None:
    speed = SpeedStats(mean=60.0, samples=10)
    est = estimate_eta(
        now=T0, position=DALLAS[1], destination=MEMPHIS[1], speed=speed, hos=HosClock()
    )
    expected_h = road_miles(DALLAS[1], MEMPHIS[1]) / 60.0
    assert est.hos_breaks == 0
    assert est.eta == T0 + timedelta(hours=expected_h)
    assert est.miles_remaining == pytest.approx(road_miles(DALLAS[1], MEMPHIS[1]), abs=0.01)


def test_eta_adds_hos_break_when_drive_exceeds_remaining_hours() -> None:
    speed = SpeedStats(mean=60.0, samples=10)
    tired = HosClock(drive_hours_used=8.0, last_status=DriverStatus.DRIVING)  # 3h left, ~8.3h trip
    est = estimate_eta(now=T0, position=DALLAS[1], destination=MEMPHIS[1], speed=speed, hos=tired)
    fresh = estimate_eta(
        now=T0, position=DALLAS[1], destination=MEMPHIS[1], speed=speed, hos=HosClock()
    )
    assert est.hos_breaks == 1
    assert est.eta - fresh.eta == timedelta(hours=10)


def test_eta_serves_remaining_rest_first_when_out_of_hours() -> None:
    speed = SpeedStats(mean=60.0, samples=10)
    resting = HosClock(
        drive_hours_used=11.0,
        rest_started_at=T0 - timedelta(hours=4),
        last_status=DriverStatus.SLEEPER,
    )
    est = estimate_eta(now=T0, position=DALLAS[1], destination=MEMPHIS[1], speed=speed, hos=resting)
    fresh = estimate_eta(
        now=T0, position=DALLAS[1], destination=MEMPHIS[1], speed=speed, hos=HosClock()
    )
    assert est.eta - fresh.eta == timedelta(hours=6)


def test_confidence_drops_with_stale_pings_and_few_samples() -> None:
    good = SpeedStats(mean=60, samples=10)
    assert eta_confidence(ping_gap_minutes=1, speed=good) > 0.95
    assert eta_confidence(ping_gap_minutes=90, speed=good) < 0.7
    assert eta_confidence(ping_gap_minutes=0, speed=SpeedStats()) == pytest.approx(0.8)
    assert degrade_confidence(0.9, 120) == pytest.approx(0.4)
    assert degrade_confidence(0.1, 600) == 0.05


@given(st.floats(0, 10_000), st.floats(0, 100), st.integers(0, 50))
def test_confidence_bounded(gap: float, var: float, n: int) -> None:
    c = eta_confidence(ping_gap_minutes=gap, speed=SpeedStats(mean=55, var=var, samples=n))
    assert 0.05 <= c <= 1.0


# --- late risk ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("eta_offset_min", "level"),
    [
        (-120, LateRisk.LOW),
        (-31, LateRisk.LOW),
        (-30, LateRisk.LOW),
        (-29, LateRisk.MEDIUM),
        (0, LateRisk.MEDIUM),
        (1, LateRisk.HIGH),
    ],
)
def test_classify_late_risk(eta_offset_min: int, level: LateRisk) -> None:
    end = T0 + timedelta(hours=5)
    assert classify_late_risk(end + timedelta(minutes=eta_offset_min), end) is level
