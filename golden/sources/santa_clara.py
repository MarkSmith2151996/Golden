"""Santa Clara County, CA health inspection data source (Socrata).
Uses two datasets: inspections (2u2d-8jej) and violations (wkaa-4ccv).

Note: The inspections dataset has a typo — field is 'inpsection_id' not 'inspection_id'."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

INSPECTIONS_CONFIG = SocrataConfig(
    city_name="santa_clara",
    display_name="Santa Clara County, CA",
    state="CA",
    base_url="https://data.sccgov.org",
    dataset_id="2u2d-8jej",
)

VIOLATIONS_CONFIG = SocrataConfig(
    city_name="santa_clara",
    display_name="Santa Clara County, CA",
    state="CA",
    base_url="https://data.sccgov.org",
    dataset_id="wkaa-4ccv",
)


class SantaClaraSource:
    def __init__(self, app_token: str = ""):
        insp_cfg = SocrataConfig(**{**INSPECTIONS_CONFIG.__dict__, "app_token": app_token})
        viol_cfg = SocrataConfig(**{**VIOLATIONS_CONFIG.__dict__, "app_token": app_token})
        self._config = insp_cfg
        self._insp_fetcher = SocrataFetcher(insp_cfg)
        self._viol_fetcher = SocrataFetcher(viol_cfg)

    @property
    def config(self) -> SocrataConfig:
        return self._config

    def fetch_establishments(self, limit: int | None = None) -> list[Establishment]:
        since = date.today() - timedelta(days=365)

        # Fetch inspections
        insp_rows = self._insp_fetcher.fetch(
            where=f"date > '{since.isoformat()}'",
            order="date DESC",
            limit=limit,
        )

        # Collect all inspection IDs (field has typo: 'inpsection_id')
        insp_ids = set()
        for r in insp_rows:
            iid = r.get("inpsection_id", "")
            if iid:
                insp_ids.add(iid)

        # Fetch violations matching those inspection IDs
        viols_by_insp: dict[str, list[dict]] = defaultdict(list)
        if insp_ids:
            # Fetch violations in batches using SoQL IN clause
            id_list = list(insp_ids)
            batch_size = 50
            for i in range(0, len(id_list), batch_size):
                batch = id_list[i:i + batch_size]
                quoted = ", ".join(f"'{iid}'" for iid in batch)
                where = f"inspection_id IN ({quoted})"
                viol_rows = self._viol_fetcher.fetch(where=where, limit=limit)
                for v in viol_rows:
                    vid = v.get("inspection_id", "")
                    if vid:
                        viols_by_insp[vid].append(v)

        return _build_establishments(insp_rows, viols_by_insp)


def _parse_date(s: str) -> date:
    # Santa Clara dates can be YYYYMMDD or ISO format
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _build_establishments(
    insp_rows: list[dict], viols_by_insp: dict[str, list[dict]]
) -> list[Establishment]:
    by_biz: dict[str, list[dict]] = defaultdict(list)
    for r in insp_rows:
        bid = r.get("business_id", "")
        if bid:
            by_biz[bid].append(r)

    establishments = []
    for bid, biz_rows in by_biz.items():
        inspections = []
        for r in biz_rows:
            d = r.get("date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            iid = r.get("inpsection_id", "")  # typo in their data
            score_str = r.get("score", "100")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            for v in viols_by_insp.get(iid, []):
                desc = v.get("description", "")
                if not desc:
                    continue
                is_crit = str(v.get("critical", "")).lower() in ("true", "1", "yes")
                violations.append(Violation(
                    violation_code=v.get("code", ""),
                    violation_description=desc,
                    violation_type="Priority" if is_crit else "Core",
                    item_description=desc,
                    problem_description=v.get("violation_comment", "") or desc,
                    area_description="",
                    is_corrected=False,
                ))

            result = r.get("result", "")
            inspections.append(Inspection(
                inspection_id=iid or f"scc-{bid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("type", ""),
                is_in_compliance=("pass" in result.lower() if result else score >= 90),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"scc-{bid}",
            name="",  # Santa Clara inspections don't include business name
            city="santa_clara",
            address="",
            inspections=inspections,
        ))

    return establishments
