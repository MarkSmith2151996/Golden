"""
NYC health inspection data source.

Socrata endpoint: DOHMH New York City Restaurant Inspection Results
Dataset: 43nn-pn8j

NYC-specific: each row is a single violation. Must group by
camis (restaurant ID) + inspection_date to build the
Establishment → Inspection → Violation hierarchy.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="nyc",
    display_name="New York City, NY",
    state="NY",
    base_url="https://data.cityofnewyork.us",
    dataset_id="43nn-pn8j",
    page_size=1000,
    rate_limit_delay=0.5,
)


def _map_violation_type(critical_flag: str) -> str:
    """Map NYC critical_flag to our violation_type."""
    if critical_flag == "Critical":
        return "Priority"
    return "Core"


def _parse_date(date_str: str) -> date:
    """Parse NYC date format (ISO timestamp like '2024-01-15T00:00:00.000')."""
    return datetime.fromisoformat(date_str.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    """
    Group flat violation rows into Establishment → Inspection → Violation.

    Each row has: camis, dba, building+street+boro+zipcode, inspection_date,
    violation_code, violation_description, critical_flag, grade, etc.
    """
    # Group by camis (restaurant ID)
    by_restaurant: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        camis = row.get("camis", "")
        if camis:
            by_restaurant[camis].append(row)

    establishments = []
    for camis, restaurant_rows in by_restaurant.items():
        # Get restaurant-level info from first row
        first = restaurant_rows[0]
        address_parts = [
            first.get("building", ""),
            first.get("street", ""),
        ]
        address = " ".join(p for p in address_parts if p).strip()
        boro = first.get("boro", "")
        if boro:
            address = f"{address}, {boro}"

        # Group by inspection_date within this restaurant
        by_inspection: dict[str, list[dict]] = defaultdict(list)
        for row in restaurant_rows:
            insp_date = row.get("inspection_date", "")
            if insp_date:
                by_inspection[insp_date].append(row)

        inspections = []
        for insp_date_str, insp_rows in by_inspection.items():
            try:
                insp_date = _parse_date(insp_date_str)
            except (ValueError, TypeError):
                continue

            violations = []
            for row in insp_rows:
                desc = row.get("violation_description", "")
                if not desc:
                    continue
                violations.append(
                    Violation(
                        violation_code=row.get("violation_code", ""),
                        violation_description=desc,
                        violation_type=_map_violation_type(
                            row.get("critical_flag", "")
                        ),
                        item_description=row.get("cuisine_description", ""),
                        problem_description=desc,
                        area_description="",
                        is_corrected=False,
                    )
                )

            grade = insp_rows[0].get("grade", "")
            inspections.append(
                Inspection(
                    inspection_id=f"nyc-{camis}-{insp_date.isoformat()}",
                    inspection_date=insp_date,
                    inspection_type=insp_rows[0].get("inspection_type", ""),
                    is_in_compliance=(grade == "A"),
                    violations=violations,
                )
            )

        # Sort inspections by date descending
        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        establishments.append(
            Establishment(
                establishment_id=f"nyc-{camis}",
                name=first.get("dba", ""),
                address=address,
                city="nyc",
                zip_code=first.get("zipcode", ""),
                inspections=inspections,
            )
        )

    return establishments


class NYCSource:
    """Fetches restaurant inspection data from NYC Open Data (Socrata)."""

    def __init__(self, app_token: str = ""):
        cfg = SocrataConfig(
            city_name=CONFIG.city_name,
            display_name=CONFIG.display_name,
            state=CONFIG.state,
            base_url=CONFIG.base_url,
            dataset_id=CONFIG.dataset_id,
            app_token=app_token,
            page_size=CONFIG.page_size,
            rate_limit_delay=CONFIG.rate_limit_delay,
        )
        self._config = cfg
        self._fetcher = SocrataFetcher(cfg)

    @property
    def config(self) -> SocrataConfig:
        return self._config

    def fetch_establishments(
        self, limit: int | None = None
    ) -> list[Establishment]:
        since = date.today() - timedelta(days=365)
        where = f"inspection_date > '{since.isoformat()}'"

        logger.info(f"Fetching NYC inspection rows (since {since})...")
        rows = self._fetcher.fetch(
            where=where,
            order="inspection_date DESC",
            limit=limit,
        )

        logger.info(f"Grouping {len(rows)} NYC rows into establishments...")
        establishments = _rows_to_establishments(rows)
        logger.info(f"NYC: {len(establishments)} establishments built")
        return establishments
