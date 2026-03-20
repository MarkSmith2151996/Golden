"""Los Angeles City health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="la_city",
    display_name="Los Angeles, CA",
    state="CA",
    base_url="https://data.lacity.org",
    dataset_id="29fd-3paw",
)


class LACitySource:
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
            where=f"activity_date > '{since.isoformat()}'",
            order="activity_date DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _grade_to_compliance(grade: str) -> bool:
    return grade.strip().upper() == "A"


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    by_facility: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        fid = r.get("facility_id", "") or r.get("serial_number", "")
        if fid:
            by_facility[fid].append(r)

    establishments = []
    for fid, fac_rows in by_facility.items():
        first = fac_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in fac_rows:
            d = r.get("activity_date", "")
            if d:
                by_date[d].append(r)

        inspections = []
        for date_str, insp_rows in by_date.items():
            try:
                idate = _parse_date(date_str)
            except (ValueError, TypeError):
                continue

            score_str = insp_rows[0].get("score", "")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            grade = insp_rows[0].get("grade", "")
            # LA doesn't have per-violation rows in this dataset — score-based
            violations = []
            if score < 90:
                desc = insp_rows[0].get("pe_description", "")
                violations.append(Violation(
                    violation_code=str(score),
                    violation_description=f"Health score: {score}. {desc}",
                    violation_type="Priority" if score < 70 else "Foundation" if score < 80 else "Core",
                    item_description=desc or "Health inspection",
                    problem_description=f"Score {score}/100, Grade {grade}",
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("record_id", f"la-{fid}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("service_description", ""),
                is_in_compliance=_grade_to_compliance(grade),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"la-{fid}",
            name=first.get("facility_name", ""),
            address=first.get("facility_address", ""),
            city="la_city",
            zip_code=first.get("facility_zip", ""),
            owner=first.get("owner_name", ""),
            establishment_type=first.get("pe_description", ""),
            inspections=inspections,
        ))

    return establishments
