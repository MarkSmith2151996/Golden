"""
City source registry.

Usage:
    from golden.sources import get_source, list_cities

    source = get_source("nyc")
    establishments = source.fetch_establishments(limit=100)
"""

from __future__ import annotations

from .base import CitySource

# Import all active sources
from .austin import AustinSource
from .baton_rouge import BatonRougeSource
from .boulder import BoulderSource
from .chicago import ChicagoSource
from .cincinnati import CincinnatiSource
from .delaware import DelawareSource
from .detroit import DetroitSource
from .king_county import KingCountySource
from .marin import MarinSource
from .montgomery_md import MontgomeryMDSource
from .ny_state import NYStateSource
from .ny_state_ag import NYStateAgSource
from .nyc import NYCSource
from .pg_county_md import PGCountyMDSource
from .santa_clara import SantaClaraSource
from .sf import SFSource

_REGISTRY: dict[str, type] = {
    "austin": AustinSource,
    "baton_rouge": BatonRougeSource,
    "boulder": BoulderSource,
    "chicago": ChicagoSource,
    "cincinnati": CincinnatiSource,
    "delaware": DelawareSource,
    "detroit": DetroitSource,
    "king_county": KingCountySource,
    "marin": MarinSource,
    "montgomery_md": MontgomeryMDSource,
    "ny_state": NYStateSource,
    "ny_state_ag": NYStateAgSource,
    "nyc": NYCSource,
    "pg_county_md": PGCountyMDSource,
    "santa_clara": SantaClaraSource,
    "sf": SFSource,
}

# Removed sources (datasets no longer updated):
# - dallas: Dataset stopped Jan 2024, moved to myhealthdepartment.com (no API)
# - fulton_ga: All datasets stopped Jan 2022
# - la_city: Socrata dataset stopped 2018, moved to ArcGIS (needs custom engine)
# - san_mateo: Dataset stopped Jan 2022


def get_source(city: str) -> CitySource:
    """Get a CitySource instance by city name."""
    cls = _REGISTRY.get(city.lower())
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown city: {city!r}. Available: {available}")
    return cls()


def list_cities() -> list[str]:
    """Return all registered city names."""
    return sorted(_REGISTRY.keys())
