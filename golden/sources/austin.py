"""Austin, TX health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="austin",
    display_name="Austin, TX",
    state="TX",
    base_url="https://data.austintexas.gov",
    dataset_id="ecmv-9xxi",
)


class AustinSource:
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
    by_facility: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        fid = r.get("facility_id", "")
        if fid:
            by_facility[fid].append(r)

    establishments = []
    for fid, fac_rows in by_facility.items():
        first = fac_rows[0]

        inspections = []
        for r in fac_rows:
            d = r.get("inspection_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            score_str = r.get("score", "")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            if score < 90:
                desc = r.get("process_description", "")
                violations.append(Violation(
                    violation_code=str(score),
                    violation_description=f"Inspection score: {score}. {desc}",
                    violation_type="Priority" if score < 70 else "Foundation" if score < 80 else "Core",
                    item_description=desc or "Health inspection",
                    problem_description=f"Score {score}/100",
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=f"austin-{fid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("process_description", ""),
                is_in_compliance=(score >= 90),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"austin-{fid}",
            name=first.get("restaurant_name", ""),
            address=first.get("address", ""),
            city="austin",
            zip_code=first.get("zip_code", ""),
            inspections=inspections,
        ))

    return establishments
