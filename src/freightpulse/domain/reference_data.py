"""Reference data: real US freight facilities (DCs, ports, border crossings) and fake carriers.

Facility coordinates are real locations of well-known freight hubs (rounded to ~100 m). Facility
names are descriptive, not operator brand names. Carriers and customers are fictional.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from freightpulse.domain.geo import Point


@dataclass(frozen=True, slots=True)
class Facility:
    facility_id: str
    name: str
    city: str
    state: str
    lat: float
    lon: float
    timezone: str
    kind: str  # DC | PORT | BORDER | INTERMODAL

    @property
    def location(self) -> Point:
        return Point(self.lat, self.lon)


@dataclass(frozen=True, slots=True)
class Carrier:
    carrier_id: str
    name: str
    scac: str
    mc_number: str


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: str
    name: str


_C = "America/Chicago"
_E = "America/New_York"
_M = "America/Denver"
_P = "America/Los_Angeles"
_AZ = "America/Phoenix"

FACILITIES: Final[tuple[Facility, ...]] = (
    Facility("FAC-DAL-01", "Dallas Southport Logistics DC", "Dallas", "TX", 32.7357, -96.8770, _C, "DC"),
    Facility("FAC-DAL-02", "Fort Worth Alliance DC", "Fort Worth", "TX", 32.9874, -97.3186, _C, "DC"),
    Facility("FAC-HOU-01", "Port Houston Barbours Cut Terminal", "La Porte", "TX", 29.6824, -95.0007, _C, "PORT"),
    Facility("FAC-LRD-01", "Laredo World Trade Bridge", "Laredo", "TX", 27.5986, -99.5357, _C, "BORDER"),
    Facility("FAC-SAT-01", "San Antonio Port DC", "San Antonio", "TX", 29.3830, -98.5770, _C, "DC"),
    Facility("FAC-ELP-01", "El Paso Ysleta Logistics Center", "El Paso", "TX", 31.7811, -106.3786, _M, "BORDER"),
    Facility("FAC-ATL-01", "Atlanta South Fulton DC", "Fairburn", "GA", 33.5670, -84.5810, _E, "DC"),
    Facility("FAC-ATL-02", "Atlanta McDonough DC", "McDonough", "GA", 33.4473, -84.1469, _E, "DC"),
    Facility("FAC-SAV-01", "Port of Savannah Garden City Terminal", "Garden City", "GA", 32.1287, -81.1428, _E, "PORT"),
    Facility("FAC-CHI-01", "Chicago Joliet Intermodal", "Joliet", "IL", 41.4553, -88.0817, _C, "INTERMODAL"),
    Facility("FAC-CHI-02", "Chicago Bedford Park DC", "Bedford Park", "IL", 41.7681, -87.8015, _C, "DC"),
    Facility("FAC-MEM-01", "Memphis Airways DC", "Memphis", "TN", 35.0456, -89.9773, _C, "DC"),
    Facility("FAC-MEM-02", "Memphis Southaven DC", "Southaven", "MS", 34.9889, -90.0126, _C, "DC"),
    Facility("FAC-NSH-01", "Nashville Lebanon DC", "Lebanon", "TN", 36.2081, -86.2911, _C, "DC"),
    Facility("FAC-CMH-01", "Columbus Rickenbacker DC", "Columbus", "OH", 39.8138, -82.9277, _E, "DC"),
    Facility("FAC-CMH-02", "Columbus West Jefferson DC", "West Jefferson", "OH", 39.9445, -83.2688, _E, "DC"),
    Facility("FAC-IND-01", "Indianapolis Plainfield DC", "Plainfield", "IN", 39.7042, -86.3994, _E, "DC"),
    Facility("FAC-LOU-01", "Louisville Shepherdsville DC", "Shepherdsville", "KY", 38.0884, -85.7158, _E, "DC"),
    Facility("FAC-ONT-01", "Ontario Inland Empire DC", "Ontario", "CA", 34.0633, -117.6509, _P, "DC"),
    Facility("FAC-ONT-02", "Moreno Valley DC", "Moreno Valley", "CA", 33.9425, -117.2297, _P, "DC"),
    Facility("FAC-LGB-01", "Port of Long Beach Pier T", "Long Beach", "CA", 33.7542, -118.2165, _P, "PORT"),
    Facility("FAC-LAX-01", "Port of Los Angeles Terminal Island", "San Pedro", "CA", 33.7361, -118.2620, _P, "PORT"),
    Facility("FAC-OAK-01", "Port of Oakland Outer Harbor", "Oakland", "CA", 37.7954, -122.2799, _P, "PORT"),
    Facility("FAC-TAC-01", "Port of Tacoma", "Tacoma", "WA", 47.2690, -122.4109, _P, "PORT"),
    Facility("FAC-PHX-01", "Phoenix Goodyear DC", "Goodyear", "AZ", 33.4353, -112.3577, _AZ, "DC"),
    Facility("FAC-DEN-01", "Denver Aurora DC", "Aurora", "CO", 39.7799, -104.7860, _M, "DC"),
    Facility("FAC-SLC-01", "Salt Lake City West DC", "Salt Lake City", "UT", 40.7608, -111.9910, _M, "DC"),
    Facility("FAC-KC-01", "Kansas City Edgerton Logistics Park", "Edgerton", "KS", 38.7645, -94.9719, _C, "INTERMODAL"),
    Facility("FAC-STL-01", "St. Louis Edwardsville DC", "Edwardsville", "IL", 38.8114, -89.9532, _C, "DC"),
    Facility("FAC-MSP-01", "Minneapolis Shakopee DC", "Shakopee", "MN", 44.7980, -93.5269, _C, "DC"),
    Facility("FAC-DET-01", "Detroit Romulus DC", "Romulus", "MI", 42.2223, -83.3966, _E, "DC"),
    Facility("FAC-EWR-01", "Port Newark-Elizabeth Marine Terminal", "Newark", "NJ", 40.6840, -74.1502, _E, "PORT"),
    Facility("FAC-NJ-02", "Cranbury Exit 8A DC", "Cranbury", "NJ", 40.3165, -74.5132, _E, "DC"),
    Facility("FAC-ABE-01", "Lehigh Valley DC", "Allentown", "PA", 40.6084, -75.4902, _E, "DC"),
    Facility("FAC-HAR-01", "Carlisle I-81 DC", "Carlisle", "PA", 40.2010, -77.1889, _E, "DC"),
    Facility("FAC-CLT-01", "Charlotte West DC", "Charlotte", "NC", 35.2271, -80.8431, _E, "DC"),
    Facility("FAC-CHS-01", "Port of Charleston Wando Welch Terminal", "Mount Pleasant", "SC", 32.8340, -79.8826, _E, "PORT"),
    Facility("FAC-JAX-01", "JAXPORT Blount Island Terminal", "Jacksonville", "FL", 30.4060, -81.5150, _E, "PORT"),
    Facility("FAC-ORL-01", "Orlando South DC", "Orlando", "FL", 28.4158, -81.2989, _E, "DC"),
    Facility("FAC-MSY-01", "Port of New Orleans Napoleon Ave", "New Orleans", "LA", 29.9230, -90.0617, _C, "PORT"),
)  # fmt: skip

CARRIERS: Final[tuple[Carrier, ...]] = tuple(
    Carrier(f"CAR-{i:03d}", name, scac, f"MC-{mc}")
    for i, (name, scac, mc) in enumerate(
        [
            ("Lone Star Freightways", "LSFW", 712004),
            ("Blue Ridge Transport", "BRTL", 455821),
            ("Prairie Line Carriers", "PRLC", 390117),
            ("Gulf Coast Haulers", "GCHL", 801552),
            ("Great Lakes Express", "GLEX", 640298),
            ("Desert Sun Logistics", "DSNL", 922140),
            ("Iron Horse Trucking", "IHTK", 518836),
            ("Magnolia Freight", "MAGF", 287345),
            ("Cascade Road Lines", "CSRL", 733618),
            ("Red River Carriers", "RRVC", 604772),
            ("Atlantic Seaboard Transport", "ASBT", 151903),
            ("Heartland Reefer Co", "HRFC", 866420),
            ("Summit Flatbed", "SMFB", 377051),
            ("Bayou Express Lines", "BYXL", 594417),
            ("Pinecrest Logistics", "PNCL", 245889),
            ("Copper State Freight", "CPSF", 980314),
            ("Northstar Haul", "NSTH", 432667),
            ("Tidewater Trucking", "TDWT", 319205),
            ("Rio Grande Cartage", "RGCT", 708861),
            ("Badger State Transport", "BDST", 559130),
            ("Palmetto Road Co", "PLMR", 145778),
            ("High Plains Carriers", "HPLC", 690042),
            ("Keystone Linehaul", "KSLH", 271559),
            ("Sierra Freight Systems", "SRFS", 884213),
            ("Cumberland Carriers", "CMBC", 503386),
        ],
        start=1,
    )
)

CUSTOMERS: Final[tuple[Customer, ...]] = tuple(
    Customer(f"CUS-{i:03d}", name)
    for i, name in enumerate(
        [
            "Acme Home Goods",
            "Northwind Grocers",
            "Contoso Electronics",
            "Fabrikam Apparel",
            "Tailspin Toys",
            "Wide World Beverages",
            "Litware Paper Co",
            "Proseware Auto Parts",
            "Adventure Works Outdoor",
            "Lucerne Building Supply",
            "Margie's Pet Supply",
            "Coho Produce",
            "Alpine Ski House",
            "Fourth Coffee",
            "Trey Research Chemicals",
        ],
        start=1,
    )
)

FACILITY_BY_ID: Final = {f.facility_id: f for f in FACILITIES}
FACILITY_LOCATIONS: Final = {f.facility_id: f.location for f in FACILITIES}
