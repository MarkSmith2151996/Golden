"""Fulton County, GA (Atlanta area) health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="fulton_ga",
    display_name="Fulton County, GA",
    state="GA",
    base_url="https://data.fultoncountyga.gov",
    dataset_id="eyfj-j5ac",
)


class FultonGASource:
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
    """Each row is one violation for one inspection."""
    by_facility: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        fac = r.get("facility", "")
        addr = r.get("address", "")
        key = f"{fac}|{addr}"
        by_facility[key].append(r)

    establishments = []
    for key, fac_rows in by_facility.items():
        first = fac_rows[0]

        by_insp: dict[str, list[dict]] = defaultdict(list)
        for r in fac_rows:
            iid = r.get("inspection_id", "") or r.get("date", "")
            if iid:
                by_insp[iid].append(r)

        inspections = []
        for iid, insp_rows in by_insp.items():
            d = insp_rows[0].get("date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            score_str = insp_rows[0].get("score", "100")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            for r in insp_rows:
                obs = r.get("observations", "")
                if not obs:
                    continue
                risk = r.get("risk_type", "")
                vtype = "Priority" if "high" in risk.lower() else "Foundation" if "medium" in risk.lower() else "Core"
                violations.append(Violation(
                    violation_code=r.get("item_number", ""),
                    violation_description=obs,
                    violation_type=vtype,
                    item_description=obs[:200],
                    problem_description=obs,
                    area_description="",
                    is_corrected=False,
                ))

            grade = insp_rows[0].get("grade", "")
            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("inspection_id", f"ful-{key}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("purpose", ""),
                is_in_compliance=(grade.upper() == "A" if grade else score >= 90),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"ful-{hash(key) % 10**8}",
            name=first.get("facility", ""),
            address=first.get("address", ""),
            city="fulton_ga",
            zip_code=first.get("zipcode", ""),
            inspections=inspections,
        ))

    return establishments
