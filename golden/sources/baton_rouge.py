"""Baton Rouge, LA health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="baton_rouge",
    display_name="Baton Rouge, LA",
    state="LA",
    base_url="https://data.brla.gov",
    dataset_id="ux2t-b9wr",
)


class BatonRougeSource:
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
            where=f"inspectiondate > '{since.isoformat()}'",
            order="inspectiondate DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    by_permit: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        pid = r.get("permitid", "")
        if pid:
            by_permit[pid].append(r)

    establishments = []
    for pid, prows in by_permit.items():
        first = prows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in prows:
            d = r.get("inspectiondate", "")
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
                desc = r.get("shortdesc", "") or r.get("violation", "")
                if not desc:
                    continue
                is_critical = str(r.get("iscritical", "")).lower() in ("true", "1", "yes")
                violations.append(Violation(
                    violation_code=str(r.get("violation", "") or ""),
                    violation_description=desc,
                    violation_type="Priority" if is_critical else "Core",
                    item_description=desc,
                    problem_description=r.get("violation_comments", "") or desc,
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=f"br-{pid}-{idate}",
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspectionpurpose", ""),
                is_in_compliance=(len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"br-{pid}",
            name=first.get("permitname", ""),
            address=first.get("address_full_core", ""),
            city="baton_rouge",
            inspections=inspections,
        ))

    return establishments
