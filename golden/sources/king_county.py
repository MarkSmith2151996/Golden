"""King County, WA (Seattle area) health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="king_county",
    display_name="King County, WA",
    state="WA",
    base_url="https://data.kingcounty.gov",
    dataset_id="f29f-zza5",
)


class KingCountySource:
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
    """Each row is one violation for one inspection."""
    by_biz: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        bid = r.get("business_id", "") or r.get("program_identifier", "")
        if bid:
            by_biz[bid].append(r)

    establishments = []
    for bid, biz_rows in by_biz.items():
        first = biz_rows[0]

        by_insp: dict[str, list[dict]] = defaultdict(list)
        for r in biz_rows:
            key = r.get("inspection_serial_num", "") or r.get("inspection_date", "")
            if key:
                by_insp[key].append(r)

        inspections = []
        for _key, insp_rows in by_insp.items():
            d = insp_rows[0].get("inspection_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            result = insp_rows[0].get("inspection_result", "")
            violations = []
            for r in insp_rows:
                desc = r.get("description", "")
                if not desc:
                    continue
                points_str = r.get("violation_points", "0")
                try:
                    points = int(float(points_str))
                except ValueError:
                    points = 0

                vtype = "Priority" if points >= 5 else "Foundation" if points >= 3 else "Core"
                violations.append(Violation(
                    violation_code=str(points),
                    violation_description=desc,
                    violation_type=vtype,
                    item_description=desc,
                    problem_description=desc,
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=insp_rows[0].get("inspection_serial_num", f"kc-{bid}-{idate}"),
                inspection_date=idate,
                inspection_type=insp_rows[0].get("inspection_type", ""),
                is_in_compliance=("satisfactory" in result.lower() if result else True),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        lat = first.get("latitude", "")
        lon = first.get("longitude", "")
        coords = f"{lat},{lon}" if lat and lon else ""

        establishments.append(Establishment(
            establishment_id=f"kc-{bid}",
            name=first.get("name", "") or first.get("inspection_business_name", ""),
            address=first.get("address", ""),
            city="king_county",
            zip_code=first.get("zip_code", ""),
            coords=coords,
            inspections=inspections,
        ))

    return establishments
