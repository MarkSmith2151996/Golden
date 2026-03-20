"""Marin County, CA health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="marin",
    display_name="Marin County, CA",
    state="CA",
    base_url="https://data.marincounty.gov",
    dataset_id="73zb-z5me",
)


class MarinSource:
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
            iid = r.get("inspection_id", "") or r.get("inspection_date", "")
            if iid:
                by_insp[iid].append(r)

        inspections = []
        for iid, insp_rows in by_insp.items():
            d = insp_rows[0].get("inspection_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            violations = []
            for r in insp_rows:
                vcode = r.get("violation_code", "")
                if not vcode:
                    continue
                is_major = str(r.get("is_major_violation", "")).lower() in ("true", "1", "yes")
                violations.append(Violation(
                    violation_code=vcode,
                    violation_description=r.get("inspection_description", "") or vcode,
                    violation_type="Priority" if is_major else "Core",
                    item_description=r.get("inspection_description", "") or vcode,
                    problem_description=r.get("inspector_comments", "") or "",
                    area_description="",
                    is_corrected=str(r.get("corrected_on_site", "")).lower() in ("true", "1", "yes"),
                ))

            result = insp_rows[0].get("inspection_result", "")
            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("inspection_id", f"mar-{bid}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspection_type", ""),
                is_in_compliance=("pass" in result.lower() if result else len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"mar-{bid}",
            name=first.get("business_name", ""),
            address=str(first.get("businessaddress", "") or first.get("formatted_address", "") or ""),
            city="marin",
            zip_code=first.get("business_postal_code", ""),
            license_number=first.get("license_number", ""),
            inspections=inspections,
        ))

    return establishments
