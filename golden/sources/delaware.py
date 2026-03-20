"""Delaware statewide restaurant inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="delaware",
    display_name="Delaware (statewide)",
    state="DE",
    base_url="https://data.delaware.gov",
    dataset_id="384s-wygj",
)


class DelawareSource:
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
            where=f"insp_date > '{since.isoformat()}'",
            order="insp_date DESC",
            limit=limit,
        )
        return _rows_to_establishments(rows)


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.replace(".000", "")).date()


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    """Each row is one violation."""
    by_rest: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = f"{r.get('restname', '')}|{r.get('restaddress', '')}"
        by_rest[key].append(r)

    establishments = []
    for key, rest_rows in by_rest.items():
        first = rest_rows[0]

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in rest_rows:
            d = r.get("insp_date", "")
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
                desc = r.get("vio_desc", "") or r.get("violation", "")
                if not desc:
                    continue
                violations.append(Violation(
                    violation_code=r.get("violation", ""),
                    violation_description=desc,
                    violation_type="Core",
                    item_description=desc,
                    problem_description=desc,
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=f"de-{key}-{idate}",
                inspection_date=idate,
                inspection_type=insp_rows[0].get("insp_type", ""),
                is_in_compliance=(len(violations) == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        establishments.append(Establishment(
            establishment_id=f"de-{hash(key) % 10**8}",
            name=first.get("restname", ""),
            address=first.get("restaddress", ""),
            city="delaware",
            zip_code=first.get("restzip", ""),
            inspections=inspections,
        ))

    return establishments
