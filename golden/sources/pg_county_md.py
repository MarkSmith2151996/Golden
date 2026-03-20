"""Prince George's County, MD health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="pg_county_md",
    display_name="Prince George's County, MD",
    state="MD",
    base_url="https://data.princegeorgescountymd.gov",
    dataset_id="umjn-t2iz",
)

# PG County uses boolean violation columns for specific categories
_VIOLATION_FIELDS = {
    "food_from_approved_source": "Food from approved source",
    "food_protected_from": "Food protected from contamination",
    "adequate_hand_washing": "Adequate hand washing",
    "proper_hand_washing": "Proper hand washing",
    "no_bare_hand_contact": "No bare hand contact with food",
    "ill_workers_restricted": "Ill workers restricted",
    "cooking_time_and_temperature": "Cooking time and temperature",
    "reheating_time_and_temperature": "Reheating time and temperature",
    "cooling_time_and_temperature": "Cooling time and temperature",
    "hot_holding_temperature": "Hot holding temperature",
    "cold_holding_temperature": "Cold holding temperature",
    "food_contact_surfaces_and": "Food contact surfaces clean",
    "hot_and_cold_running_water": "Hot and cold running water",
    "proper_sewage_disposal": "Proper sewage disposal",
    "rodent_and_insects": "Rodent and insects",
}


class PGCountyMDSource:
    def __init__(self, app_token: str = ""):
        cfg = SocrataConfig(**{**CONFIG.__dict__, "app_token": app_token})
        self._config = cfg
        self._fetcher = SocrataFetcher(cfg)

    @property
    def config(self) -> SocrataConfig:
        return self._config

    def fetch_establishments(self, limit: int | None = None) -> list[Establishment]:
        since = date.today() - timedelta(days=365)
        rows = self._fetcher.fetch(
            where=f"inspection_date > '{since.isoformat()}'",
            order="inspection_date DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    by_est: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        eid = r.get("establishment_id", "")
        if eid:
            by_est[eid].append(r)

    establishments = []
    for eid, est_rows in by_est.items():
        first = est_rows[0]

        inspections = []
        for r in est_rows:
            d = r.get("inspection_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            violations = []
            for field, label in _VIOLATION_FIELDS.items():
                val = r.get(field, "")
                if val and val.strip().upper() not in ("", "IN COMPLIANCE", "1"):
                    violations.append(Violation(
                        violation_code=field,
                        violation_description=f"{label}: {val}",
                        violation_type="Priority" if field in ("rodent_and_insects", "proper_sewage_disposal") else "Core",
                        item_description=label,
                        problem_description=val,
                        area_description="",
                        is_corrected=False,
                    ))

            result = r.get("inspection_results", "")
            inspections.append(Inspection(
                inspection_id=f"pgc-{eid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("inspection_type", ""),
                is_in_compliance=("compliance" in result.lower() if result else len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"pgc-{eid}",
            name=first.get("name", ""),
            address=first.get("address_line_1", ""),
            city="pg_county_md",
            zip_code=first.get("zip", ""),
            owner=first.get("owner", ""),
            establishment_type=first.get("category", ""),
            inspections=inspections,
        ))

    return establishments
