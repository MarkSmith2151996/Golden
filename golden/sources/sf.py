"""San Francisco health inspection data source (Socrata, 2023-present dataset)."""

from __future__ import annotations

import logging
import re
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
    dataset_id="tvy3-wexg",
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


def _parse_violation_codes(text: str) -> list[Violation]:
    """Parse the violation_codes blob into individual Violation objects.

    Format: "114130-114130.5, ... - Description text ... | 114253 - Another desc ..."
    Each violation block is separated by ' | ' or starts with a code pattern.
    """
    if not text:
        return []

    violations = []
    # Split on ' | ' which separates distinct violation entries
    # If no pipes, treat the whole thing as one violation
    parts = [p.strip() for p in text.split(" | ")] if " | " in text else [text.strip()]

    for part in parts:
        if not part:
            continue
        # Try to extract code and description
        # Pattern: "CODE(S) - Description text"
        match = re.match(r'^([\d.,\s-]+?)\s*-\s*(.+)', part)
        if match:
            code = match.group(1).strip().rstrip(",- ")
            desc = match.group(2).strip()
        else:
            code = ""
            desc = part

        if desc:
            violations.append(Violation(
                violation_code=code,
                violation_description=desc[:500],
                violation_type="Core",
                item_description=desc[:200],
                problem_description=desc,
                area_description="",
                is_corrected=False,
            ))

    return violations


def _rows_to_establishments(rows: list[dict]) -> list[Establishment]:
    """Each row is one inspection (violations are in a text blob)."""
    by_permit: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        pid = r.get("permit_number", "")
        if pid:
            by_permit[pid].append(r)

    establishments = []
    for pid, prows in by_permit.items():
        first = prows[0]

        inspections = []
        for r in prows:
            d = r.get("inspection_date", "")
            if not d:
                continue
            try:
                idate = _parse_date(d)
            except (ValueError, TypeError):
                continue

            viol_text = r.get("violation_codes", "") or ""
            violations = _parse_violation_codes(viol_text)

            viol_count_str = r.get("violation_count", "0")
            try:
                viol_count = int(viol_count_str) if viol_count_str else 0
            except ValueError:
                viol_count = 0

            inspections.append(Inspection(
                inspection_id=f"sf-{pid}-{idate}",
                inspection_date=idate,
                inspection_type=r.get("permit_type", ""),
                is_in_compliance=(viol_count == 0),
                violations=violations,
            ))

        inspections.sort(key=lambda i: i.inspection_date, reverse=True)
        address = (first.get("street_address_clean", "") or
                   first.get("street_address", "") or "")
        establishments.append(Establishment(
            establishment_id=f"sf-{pid}",
            name=first.get("permit_type", ""),
            address=address.strip(),
            city="sf",
            coords=f"{first.get('latitude', '')},{first.get('longitude', '')}" if first.get("latitude") else "",
            inspections=inspections,
        ))

    return establishments
