"""
Chicago health inspection data source.

Socrata endpoint: Chicago Food Inspections
Dataset: 4ijn-s7e5

Chicago-specific: violations are crammed into a single pipe-delimited
text field per inspection row. Format:
  "42. TITLE - Comments: DETAILS | 18. TITLE - Comments: DETAILS | ..."
Regex parsing extracts individual violations from this string.

Default query filters to results='Fail' — only failed inspections are leads.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="chicago",
    display_name="Chicago, IL",
    state="IL",
    base_url="https://data.cityofchicago.org",
    dataset_id="4ijn-s7e5",
    page_size=1000,
    rate_limit_delay=0.5,
)

# Pattern to split pipe-delimited violations and extract code + description
# Format: "42. TITLE - Comments: DETAILS"
_VIOLATION_PATTERN = re.compile(
    r"(\d+)\.\s*(.+?)(?:\s*-\s*Comments:\s*(.*))?$"
)


def _map_risk_to_type(risk: str) -> str:
    """Map Chicago risk level to our violation_type."""
    risk_lower = risk.lower()
    if "risk 1" in risk_lower or "high" in risk_lower:
        return "Priority"
    if "risk 2" in risk_lower or "medium" in risk_lower:
        return "Foundation"
    return "Core"


def _parse_violations(violations_text: str, risk: str) -> list[Violation]:
    """Parse Chicago's pipe-delimited violations string into Violation objects."""
    if not violations_text:
        return []

    violation_type = _map_risk_to_type(risk)
    violations = []

    # Split on pipe, each segment is one violation
    segments = violations_text.split("|")
    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        match = _VIOLATION_PATTERN.match(segment)
        if match:
            code = match.group(1)
            title = match.group(2).strip()
            comments = (match.group(3) or "").strip()
            violations.append(
                Violation(
                    violation_code=code,
                    violation_description=title,
                    violation_type=violation_type,
                    item_description=title,
                    problem_description=comments,
                    area_description="",
                    is_corrected=False,
                )
            )
        else:
            # Fallback: treat whole segment as description
            violations.append(
                Violation(
                    violation_code="",
                    violation_description=segment,
                    violation_type=violation_type,
                    item_description=segment,
                    problem_description="",
                    area_description="",
                    is_corrected=False,
                )
            )

    return violations


def _parse_date(date_str: str) -> date:
    """Parse Chicago date format (ISO timestamp)."""
    return datetime.fromisoformat(date_str.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    """
    Convert Chicago rows into Establishments.

    Each row is one inspection (unlike NYC where each row is one violation).
    Multiple rows for the same license_ map to inspections on one establishment.
    """
    # Group by license_ (establishment ID)
    from collections import defaultdict

    by_license: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        license_num = row.get("license_", "")
        if license_num:
            by_license[license_num].append(row)

    establishments = []
    for license_num, est_rows in by_license.items():
        first = est_rows[0]

        address_parts = [
            first.get("address", ""),
            first.get("city", ""),
            first.get("state", ""),
            first.get("zip", ""),
        ]
        address = ", ".join(p for p in address_parts if p)

        inspections = []
        for row in est_rows:
            insp_date_str = row.get("inspection_date", "")
            if not insp_date_str:
                continue

            try:
                insp_date = _parse_date(insp_date_str)
            except (ValueError, TypeError):
                continue

            risk = row.get("risk", "")
            violations = _parse_violations(
                row.get("violations", ""), risk
            )

            results = row.get("results", "")
            inspections.append(
                Inspection(
                    inspection_id=row.get("inspection_id", ""),
                    inspection_date=insp_date,
                    inspection_type=row.get("inspection_type", ""),
                    is_in_compliance=(results == "Pass"),
                    violations=violations,
                )
            )

        # Sort inspections by date descending
        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        coords = ""
        lat = first.get("latitude", "")
        lon = first.get("longitude", "")
        if lat and lon:
            coords = f"{lat},{lon}"

        establishments.append(
            Establishment(
                establishment_id=f"chi-{license_num}",
                name=first.get("dba_name", ""),
                address=address,
                city="chicago",
                zip_code=first.get("zip", ""),
                license_number=license_num,
                establishment_type=first.get("facility_type", ""),
                coords=coords,
                inspections=inspections,
            )
        )

    return establishments


class ChicagoSource:
    """Fetches restaurant inspection data from Chicago Open Data (Socrata)."""

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
        where = (
            f"results = 'Fail' AND inspection_date > '{since.isoformat()}'"
        )

        logger.info(f"Fetching Chicago failed inspections (since {since})...")
        rows = self._fetcher.fetch(
            where=where,
            order="inspection_date DESC",
            limit=limit,
        )

        logger.info(
            f"Building establishments from {len(rows)} Chicago rows..."
        )
        establishments = _rows_to_establishments(rows)
        logger.info(f"Chicago: {len(establishments)} establishments built")
        return establishments
