"""San Francisco health inspection data source (Socrata, LIVES Standard)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="sf",
    display_name="San Francisco, CA",
    state="CA",
    base_url="https://data.sfgov.org",
    dataset_id="pyih-qa8i",
)


class SFSource:
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
    by_biz: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        bid = r.get("business_id", "")
        if bid:
            by_biz[bid].append(r)

    establishments = []
    for bid, biz_rows in by_biz.items():
        first = biz_rows[0]

        by_insp: dict[str, list[dict]] = defaultdict(list)
        for r in biz_rows:
            idate = r.get("inspection_date", "")
            if idate:
                by_insp[idate].append(r)

        inspections = []
        for idate_str, insp_rows in by_insp.items():
            try:
                idate = _parse_date(idate_str)
            except (ValueError, TypeError):
                continue

            score_str = insp_rows[0].get("inspection_score", "")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            for r in insp_rows:
                desc = r.get("violation_description", "")
                if desc:
                    risk = r.get("risk_category", "")
                    vtype = "Priority" if "High" in risk else "Core" if "Low" in risk else "Foundation"
                    violations.append(Violation(
                        violation_code=r.get("violation_id", ""),
                        violation_description=desc,
                        violation_type=vtype,
                        item_description=desc,
                        problem_description=desc,
                        area_description="",
                        is_corrected=False,
                    ))

            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("inspection_id", f"sf-{bid}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspection_type", ""),
                is_in_compliance=(score >= 90),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"sf-{bid}",
            name=first.get("business_name", ""),
            address=first.get("business_address", ""),
            city="sf",
            zip_code=first.get("business_postal_code", ""),
            inspections=inspections,
        ))

    return establishments
