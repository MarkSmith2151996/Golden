"""
City source registry.

Usage:
    from golden.sources import get_source, list_cities

    source = get_source("nyc")
    establishments = source.fetch_establishments(limit=100)
"""

from __future__ import annotations

from .base import CitySource
from .chicago import ChicagoSource
from .detroit import DetroitSource
from .nyc import NYCSource

_REGISTRY: dict[str, type] = {
    "detroit": DetroitSource,
    "nyc": NYCSource,
    "chicago": ChicagoSource,
}


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
