"""Dallas, TX health inspection data source (Socrata)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..models import Establishment, Inspection, Violation
from .base import SocrataConfig, SocrataFetcher

logger = logging.getLogger(__name__)

CONFIG = SocrataConfig(
    city_name="dallas",
    display_name="Dallas, TX",
    state="TX",
    base_url="https://www.dallasopendata.com",
    dataset_id="dri5-wcct",
)


class DallasSource:
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
    """Each row is one inspection with violations in numbered columns."""
    by_prog: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        pid = r.get("program_identifier", "")
        if pid:
            by_prog[pid].append(r)

    establishments = []
    for pid, prog_rows in by_prog.items():
        first = prog_rows[0]

        inspections = []
        for r in prog_rows:
            d = r.get("insp_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            score_str = r.get("score", "100")
            try:
                score = int(float(score_str)) if score_str else 100
            except ValueError:
                score = 100

            violations = []
            # Dallas stores violations in numbered columns: violation1..violation15
            for i in range(1, 16):
                desc = r.get(f"violation{i}_description", "")
                if not desc:
                    continue
                points_str = r.get(f"violation{i}_points", "0")
                try:
                    points = int(float(points_str))
                except ValueError:
                    points = 0

                if points == 0:
                    continue  # no violation points = not a real violation

                text = r.get(f"violation{i}_text", "") or ""
                memo = r.get(f"violation{i}_memo", "") or ""
                detail = f"{text} {memo}".strip() or desc

                vtype = "Priority" if points >= 4 else "Foundation" if points >= 2 else "Core"
                violations.append(Violation(
                    violation_code=str(i),
                    violation_description=desc,
                    violation_type=vtype,
                    item_description=desc,
                    problem_description=detail,
                    area_description="",
                    is_corrected=False,
                ))

            inspections.append(Inspection(
                inspection_id=f"dal-{pid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("type", ""),
                is_in_compliance=(score >= 90),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)

        addr_parts = [
            first.get("street_number", ""),
            first.get("street_name", ""),
            first.get("street_type", ""),
        ]
        if first.get("street_unit"):
            addr_parts.append(f"#{first['street_unit']}")
        address = " ".join(p for p in addr_parts if p) or first.get("site_address", "")

        establishments.append(Establishment(
            establishment_id=f"dal-{pid}",
            name=pid,  # Dallas uses program_identifier as name
            address=address,
            city="dallas",
            zip_code=first.get("zip", ""),
            inspections=inspections,
        ))

    return establishments
