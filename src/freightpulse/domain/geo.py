"""Great-circle geometry on a spherical Earth (miles)."""

from __future__ import annotations

import math
from typing import NamedTuple

EARTH_RADIUS_MI = 3958.7613
ROAD_CIRCUITY_FACTOR = 1.18
"""Road miles are ~18% longer than great-circle miles on typical US freight lanes."""


class Point(NamedTuple):
    lat: float
    lon: float


def haversine_mi(a: Point, b: Point) -> float:
    """Great-circle distance between two points in statute miles."""
    lat1, lon1, lat2, lon2 = map(math.radians, (a.lat, a.lon, b.lat, b.lon))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(min(1.0, math.sqrt(h)))


def road_miles(a: Point, b: Point) -> float:
    """Estimated road distance: great-circle distance times the circuity factor."""
    return haversine_mi(a, b) * ROAD_CIRCUITY_FACTOR


def initial_bearing_deg(a: Point, b: Point) -> float:
    """Initial compass bearing from ``a`` to ``b`` in degrees [0, 360)."""
    lat1, lat2 = math.radians(a.lat), math.radians(b.lat)
    dlon = math.radians(b.lon - a.lon)
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def interpolate(a: Point, b: Point, fraction: float) -> Point:
    """Point at ``fraction`` (0..1) of the way along the great circle from ``a`` to ``b``."""
    if fraction <= 0:
        return a
    if fraction >= 1:
        return b
    lat1, lon1, lat2, lon2 = map(math.radians, (a.lat, a.lon, b.lat, b.lon))
    d = haversine_mi(a, b) / EARTH_RADIUS_MI
    if d == 0:
        return a
    sin_d = math.sin(d)
    w1 = math.sin((1 - fraction) * d) / sin_d
    w2 = math.sin(fraction * d) / sin_d
    x = w1 * math.cos(lat1) * math.cos(lon1) + w2 * math.cos(lat2) * math.cos(lon2)
    y = w1 * math.cos(lat1) * math.sin(lon1) + w2 * math.cos(lat2) * math.sin(lon2)
    z = w1 * math.sin(lat1) + w2 * math.sin(lat2)
    return Point(math.degrees(math.atan2(z, math.hypot(x, y))), math.degrees(math.atan2(y, x)))


def offset(p: Point, north_mi: float, east_mi: float) -> Point:
    """Small planar offset (good enough for GPS noise of a few hundred metres)."""
    dlat = north_mi / 69.0
    dlon = east_mi / (69.0 * max(0.01, math.cos(math.radians(p.lat))))
    return Point(p.lat + dlat, p.lon + dlon)
