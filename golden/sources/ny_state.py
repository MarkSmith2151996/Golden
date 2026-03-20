"""New York State health inspection data source (Socrata). Excludes NYC."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="ny_state",
    display_name="New York State",
    state="NY",
    base_url="https://health.data.ny.gov",
    dataset_id="cnih-y5dw",
)


class NYStateSource:
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
            where=f"date > '{since.isoformat()}'",
            order="date DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    by_facility: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        fid = r.get("nys_health_operation_id", "") or r.get("facility", "")
        if fid:
            by_facility[fid].append(r)

    establishments = []
    for fid, fac_rows in by_facility.items():
        first = fac_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in fac_rows:
            d = r.get("date", "")
            if d:
                by_date[d].append(r)

        inspections = []
        for date_str, insp_rows in by_date.items():
            try:
                idate = _parse_date(date_str)
            except (ValueError, TypeError):
                continue

            crit_str = insp_rows[0].get("total_critical_violations", "0")
            noncrit_str = insp_rows[0].get("total_noncritical_violations", "0")
            try:
                crit = int(crit_str) if crit_str else 0
                noncrit = int(noncrit_str) if noncrit_str else 0
            except ValueError:
                crit, noncrit = 0, 0

            violations = []
            for r in insp_rows:
                desc = r.get("violations", "") or r.get("description", "")
                if not desc:
                    continue
                violations.append(Violation(
                    violation_code="",
                    violation_description=desc,
                    violation_type="Priority" if crit > 0 else "Core",
                    item_description=desc[:200],
                    problem_description=r.get("inspection_comments", "") or desc[:200],
                    area_description="",
                    is_corrected=False,
                ))

            total = crit + noncrit
            inspections.append(Inspection(
                inspection_id=f"nys-{fid}-{idate}",
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspection_type", ""),
                is_in_compliance=(total == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        addr = first.get("facility_address", "") or first.get("address", "")
        city_name = first.get("city", "") or first.get("municipality", "")

        establishments.append(Establishment(
            establishment_id=f"nys-{fid}",
            name=first.get("operation_name", "") or first.get("facility", ""),
            address=f"{addr}, {city_name}" if city_name else addr,
            city="ny_state",
            zip_code=first.get("zip_code", ""),
            inspections=inspections,
        ))

    return establishments
