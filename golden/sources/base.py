"""
Base infrastructure for city data sources.

CitySource is a Protocol — sources implement it via composition,
not inheritance. SocrataFetcher handles the common Socrata SODA API
pagination and rate-limiting pattern.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import httpx

from ..models import Establishment

logger = logging.getLogger(__name__)


@dataclass
class CityConfig:
    city_name: str  # e.g. "detroit"
    display_name: str  # e.g. "Detroit, MI"
    state: str  # e.g. "MI"


@dataclass
class SocrataConfig(CityConfig):
    base_url: str = ""  # e.g. "https://data.cityofnewyork.us"
    dataset_id: str = ""  # e.g. "43nn-pn8j"
    app_token: str = ""  # optional Socrata app token
    page_size: int = 1000
    rate_limit_delay: float = 0.5  # seconds between requests


@runtime_checkable
class CitySource(Protocol):
    @property
    def config(self) -> CityConfig: ...

    def fetch_establishments(self, limit: int | None = None) -> list[Establishment]: ...


class SocrataFetcher:
    """Handles Socrata SODA API pagination, rate limiting, and SoQL queries."""

    def __init__(self, config: SocrataConfig, timeout: float = 30.0):
        self.config = config
        self.timeout = timeout
        self._endpoint = (
            f"{config.base_url}/resource/{config.dataset_id}.json"
        )

    def fetch(
        self,
        where: str | None = None,
        order: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Fetch rows from a Socrata dataset with automatic pagination.

        Args:
            where: SoQL $where clause (e.g. "inspection_date > '2024-01-01'")
            order: SoQL $order clause (e.g. "inspection_date DESC")
            limit: Max total rows to return (None = all available)
        """
        all_rows: list[dict] = []
        offset = 0
        page_size = self.config.page_size

        headers: dict[str, str] = {}
        if self.config.app_token:
            headers["X-App-Token"] = self.config.app_token

        with httpx.Client(timeout=self.timeout) as client:
            while True:
                # How many rows to request this page
                if limit is not None:
                    remaining = limit - len(all_rows)
                    if remaining <= 0:
                        break
                    this_page = min(page_size, remaining)
                else:
                    this_page = page_size

                params: dict[str, str] = {
                    "$limit": str(this_page),
                    "$offset": str(offset),
                }
                if where:
                    params["$where"] = where
                if order:
                    params["$order"] = order

                logger.debug(
                    f"Socrata fetch: offset={offset}, limit={this_page}"
                )
                resp = client.get(
                    self._endpoint, params=params, headers=headers
                )
                resp.raise_for_status()
                rows = resp.json()

                if not rows:
                    break

                all_rows.extend(rows)
                offset += len(rows)

                # If we got fewer rows than requested, we've reached the end
                if len(rows) < this_page:
                    break

                # Rate limiting between pages
                if self.config.rate_limit_delay > 0:
                    time.sleep(self.config.rate_limit_delay)

        logger.info(
            f"Fetched {len(all_rows)} rows from {self.config.display_name} "
            f"({self.config.dataset_id})"
        )
        return all_rows
