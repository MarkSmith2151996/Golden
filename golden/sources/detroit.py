"""
Detroit health inspection data source.

Pulls from the Detroit Restaurant Inspections Gatsby app (static JSON).
This is NOT a Socrata source — it uses a custom Gatsby static site.

Source: https://detroitrestaurantinspections.netlify.app/
Data origin: Detroit Health Department (since 8/1/2016)
"""

from __future__ import annotations

import logging
import time
from datetime import date

import httpx

from ..models import Establishment, Inspection, Violation
from .base import CityConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://detroitrestaurantinspections.netlify.app"
INDEX_URL = f"{BASE_URL}/page-data/index/page-data.json"
DETAIL_URL = f"{BASE_URL}/page-data/establishment/{{est_id}}/page-data.json"

CONFIG = CityConfig(
    city_name="detroit",
    display_name="Detroit, MI",
    state="MI",
)


def _parse_violation(raw: dict) -> Violation:
    corrected_date = None
    if raw.get("correctedDate"):
        try:
            corrected_date = date.fromisoformat(raw["correctedDate"])
        except ValueError:
            pass

    return Violation(
        violation_code=raw.get("violationCode") or "",
        violation_description=raw.get("violationDescription") or "",
        violation_type=raw.get("violationType") or "",
        item_description=raw.get("itemDescription") or "",
        problem_description=raw.get("problemDescription") or "",
        area_description=raw.get("areaDescription") or "",
        is_corrected=raw.get("isCorrected", False),
        num_days_to_correct=raw.get("numDaysToCorrect"),
        corrected_date=corrected_date,
    )


def _parse_inspection(raw: dict) -> Inspection:
    violations = [
        _parse_violation(v)
        for v in raw.get("violationsByInspectionIdList", [])
    ]
    return Inspection(
        inspection_id=raw.get("inspectionId", ""),
        inspection_date=date.fromisoformat(raw["inspectionDate"]),
        inspection_type=raw.get("inspectionType", ""),
        is_in_compliance=raw.get("isInCompliance", True),
        violations=violations,
    )


def _parse_establishment(raw: dict) -> Establishment:
    inspections = []
    for insp_raw in raw.get("inspectionsByEstablishmentIdList", []):
        try:
            inspections.append(_parse_inspection(insp_raw))
        except (ValueError, KeyError) as e:
            logger.warning(
                f"Skipping inspection for {raw.get('establishmentId')}: {e}"
            )

    return Establishment(
        establishment_id=raw.get("establishmentId") or "",
        name=raw.get("establishmentName") or "",
        address=raw.get("address") or "",
        city="detroit",
        zip_code=raw.get("zipCode") or "",
        owner=raw.get("establishmentOwner") or "",
        license_number=raw.get("establishmentLicenseNumber") or "",
        license_type=raw.get("establishmentLicenseType") or "",
        establishment_type=raw.get("establishmentType") or "",
        status=raw.get("establishmentStatus") or "",
        coords=raw.get("coords") or "",
        inspections=inspections,
    )


class DetroitSource:
    """Fetches restaurant inspection data from Detroit's Gatsby static JSON."""

    def __init__(self, timeout: float = 30.0):
        self._timeout = timeout

    @property
    def config(self) -> CityConfig:
        return CONFIG

    def fetch_establishments(
        self, limit: int | None = None
    ) -> list[Establishment]:
        est_list = self._fetch_establishment_list()
        est_ids = [e["id"] for e in est_list]

        if limit:
            est_ids = est_ids[:limit]

        logger.info(f"Fetching details for {len(est_ids)} Detroit establishments...")
        establishments = []
        errors = 0

        with httpx.Client(timeout=self._timeout) as client:
            for i, eid in enumerate(est_ids):
                raw = self._fetch_detail_with_client(client, eid)
                if raw:
                    try:
                        establishments.append(_parse_establishment(raw))
                    except Exception as e:
                        logger.warning(f"Failed to parse establishment {eid}: {e}")
                        errors += 1
                else:
                    errors += 1

                if (i + 1) % 200 == 0 or (i + 1) == len(est_ids):
                    logger.info(
                        f"  Detroit progress: {i + 1}/{len(est_ids)} "
                        f"(errors: {errors})"
                    )

                # Throttle to avoid Netlify 403s
                time.sleep(0.1)

        logger.info(
            f"Detroit: {len(establishments)} establishments fetched "
            f"({errors} errors)"
        )
        return establishments

    def _fetch_establishment_list(self) -> list[dict]:
        """Fetch index: returns list of {id, name, address} dicts."""
        logger.info("Fetching Detroit establishment index...")
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.get(INDEX_URL)
            resp.raise_for_status()

        data = resp.json()
        raw = data["result"]["data"]["postgres"]["establishments"]
        result = [
            {
                "id": e["establishmentId"],
                "name": e["establishmentName"],
                "address": e["address"],
            }
            for e in raw
        ]
        logger.info(f"Found {len(result)} Detroit establishments")
        return result

    def _fetch_detail_with_client(
        self, client: httpx.Client, est_id: str, max_retries: int = 3
    ) -> dict | None:
        """Fetch full detail for one establishment, with retry on 403/429."""
        url = DETAIL_URL.format(est_id=est_id)
        for attempt in range(max_retries):
            try:
                resp = client.get(url)
                if resp.status_code in (403, 429):
                    wait = 2 ** attempt  # 1s, 2s, 4s
                    logger.debug(f"Rate limited on {est_id}, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                records = data["result"]["data"]["postgres"]["establishment"]
                if not records:
                    return None
                return records[0]
            except (httpx.HTTPError, KeyError) as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                logger.warning(f"Failed to fetch Detroit establishment {est_id}: {e}")
                return None
        return None
