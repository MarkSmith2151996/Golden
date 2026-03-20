"""Montgomery County, MD health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="montgomery_md",
    display_name="Montgomery County, MD",
    state="MD",
    base_url="https://data.montgomerycountymd.gov",
    dataset_id="5pue-gfbe",
)

# Montgomery uses numbered violation columns (violation1..violation22)
_VIOLATION_FIELDS = [
    "violation1", "violation2", "violation3", "violation4", "violation5",
    "violation6a", "violation6b", "violation7a", "violation7b",
    "violation8", "violation9", "violation20", "violation22",
    "violationmenu", "violationsmoking", "violationtransfat",
]


class MontgomeryMDSource:
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
            d = r.get("inspectiondate", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            violations = []
            for vfield in _VIOLATION_FIELDS:
                val = r.get(vfield, "")
                if val and val.strip().upper() not in ("", "IN COMPLIANCE", "N/A"):
                    violations.append(Violation(
                        violation_code=vfield,
                        violation_description=val,
                        violation_type="Core",
                        item_description=vfield,
                        problem_description=val,
                        area_description="",
                        is_corrected=False,
                    ))

            result = r.get("inspectionresults", "")
            inspections.append(Inspection(
                inspection_id=f"moco-{eid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("inspectiontype", ""),
                is_in_compliance=("compliance" in result.lower() if result else len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        lat = first.get("latitude", "")
        lon = first.get("longitude", "")
        coords = f"{lat},{lon}" if lat and lon else ""

        establishments.append(Establishment(
            establishment_id=f"moco-{eid}",
            name=first.get("name", ""),
            address=first.get("address1", ""),
            city="montgomery_md",
            zip_code=first.get("zip", ""),
            coords=coords,
            establishment_type=first.get("category", ""),
            inspections=inspections,
        ))

    return establishments
