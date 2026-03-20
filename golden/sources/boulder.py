"""Boulder County, CO health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="boulder",
    display_name="Boulder County, CO",
    state="CO",
    base_url="https://data.colorado.gov",
    dataset_id="tuvj-xz3m",
)


class BoulderSource:
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
    """Each row is one violation for one inspection."""
    by_fac: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        fid = r.get("facilityid", "")
        if fid:
            by_fac[fid].append(r)

    establishments = []
    for fid, fac_rows in by_fac.items():
        first = fac_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in fac_rows:
            d = r.get("inspectiondate", "")
            if d:
                by_date[d].append(r)

        inspections = []
        for date_str, insp_rows in by_date.items():
            try:
                idate = _parse_date(date_str)
            except (ValueError, TypeError):
                continue

            score_str = insp_rows[0].get("inspectionscore", "")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            for r in insp_rows:
                desc = r.get("violation", "")
                if not desc:
                    continue
                points_str = r.get("violationpoints", "0")
                try:
                    points = int(float(points_str))
                except ValueError:
                    points = 0

                vtype_raw = r.get("violationtype", "")
                vtype = "Priority" if "critical" in vtype_raw.lower() else "Core"
                violations.append(Violation(
                    violation_code=r.get("violationcode", ""),
                    violation_description=desc,
                    violation_type=vtype,
                    item_description=desc,
                    problem_description=desc,
                    area_description="",
                    is_corrected=("corrected" in (r.get("violationstatus", "") or "").lower()),
                ))

            inspections.append(Inspection(
                inspection_id=f"boul-{fid}-{idate}",
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspectiontype", ""),
                is_in_compliance=(score >= 90 if score_str else len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        addr_parts = [
            first.get("streetnumber", ""),
            first.get("streetname", ""),
            first.get("streettype", ""),
        ]
        address = " ".join(p for p in addr_parts if p)

        establishments.append(Establishment(
            establishment_id=f"boul-{fid}",
            name=first.get("facilityname", ""),
            address=address,
            city="boulder",
            zip_code=first.get("zip", ""),
            establishment_type=first.get("typeoffacility", ""),
            inspections=inspections,
        ))

    return establishments
