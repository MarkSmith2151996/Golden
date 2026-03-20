"""Cincinnati, OH health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="cincinnati",
    display_name="Cincinnati, OH",
    state="OH",
    base_url="https://data.cincinnati-oh.gov",
    dataset_id="rg6p-b3h3",
)


class CincinnatiSource:
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
            where=f"action_date > '{since.isoformat()}'",
            order="action_date DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    by_license: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        lid = r.get("license_no", "")
        if lid:
            by_license[lid].append(r)

    establishments = []
    for lid, lic_rows in by_license.items():
        first = lic_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in lic_rows:
            d = r.get("action_date", "")
            if d:
                by_date[d].append(r)

        inspections = []
        for date_str, insp_rows in by_date.items():
            try:
                idate = _parse_date(date_str)
            except (ValueError, TypeError):
                continue

            violations = []
            for r in insp_rows:
                desc = r.get("violation_description", "")
                if not desc:
                    continue
                violations.append(Violation(
                    violation_code=r.get("code", ""),
                    violation_description=desc,
                    violation_type="Core",
                    item_description=desc,
                    problem_description=r.get("violation_comments", "") or desc,
                    area_description=r.get("neighborhood", ""),
                    is_corrected=False,
                ))

            status = insp_rows[0].get("action_status", "")
            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("recordnum_insp", f"cin-{lid}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("insp_type", ""),
                is_in_compliance=(len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        lat = first.get("latitude", "")
        lon = first.get("longitude", "")
        coords = f"{lat},{lon}" if lat and lon else ""

        establishments.append(Establishment(
            establishment_id=f"cin-{lid}",
            name=first.get("business_name", ""),
            address=first.get("address", ""),
            city="cincinnati",
            zip_code=first.get("postal_code", ""),
            license_number=lid,
            coords=coords,
            inspections=inspections,
        ))

    return establishments
