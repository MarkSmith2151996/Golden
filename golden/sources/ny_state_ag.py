"""New York State Dept of Agriculture food safety data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="ny_state_ag",
    display_name="New York State (Agriculture)",
    state="NY",
    base_url="https://data.ny.gov",
    dataset_id="d6dy-3h7r",
)


class NYStateAgSource:
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
    """Each row is one deficiency for one establishment."""
    by_name: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = f"{r.get('trade_name', '')}|{r.get('street', '')}|{r.get('city', '')}"
        by_name[key].append(r)

    establishments = []
    for key, est_rows in by_name.items():
        first = est_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in est_rows:
            d = r.get("inspection_date", "")
            if d:
                by_date[d].append(r)

        inspections = []
        for date_str, insp_rows in by_date.items():
            try:
                idate = _parse_date(date_str)
            except (ValueError, TypeError):
                continue

            grade = insp_rows[0].get("inspection_grade", "")

            violations = []
            for r in insp_rows:
                desc = r.get("deficiency_description", "")
                if not desc:
                    continue
                violations.append(Violation(
                    violation_code=r.get("deficiency_number", ""),
                    violation_description=desc,
                    violation_type="Core",
                    item_description=desc,
                    problem_description=desc,
                    area_description=r.get("county", ""),
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=f"nyag-{key}-{idate}",
                inspection_date=idate,
                inspection_type="Routine",
                is_in_compliance=(grade.upper() == "A" if grade else len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        city_name = first.get("city", "")
        street = first.get("street", "")
        addr = f"{street}, {city_name}" if city_name else street

        establishments.append(Establishment(
            establishment_id=f"nyag-{hash(key) % 10**8}",
            name=first.get("trade_name", ""),
            address=addr,
            city="ny_state_ag",
            zip_code=first.get("zipcode", ""),
            owner=first.get("owner_name", ""),
            establishment_type=first.get("establishment_type", ""),
            inspections=inspections,
        ))

    return establishments
