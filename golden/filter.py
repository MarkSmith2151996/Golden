"""
Filters inspections/violations for cleaning-relevant leads.

Cleaning-relevant violations: sanitation failures, pest issues, food safety,
equipment cleanliness, physical facility cleanliness.

Not cleaning-relevant: paperwork, signage, licensing, administrative,
personnel training (hair nets, handwashing knowledge, etc.)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from .models import Establishment, Lead, Violation

logger = logging.getLogger(__name__)

# Violation codes/keywords that indicate a cleaning need.
# Based on Michigan Food Law and FDA Food Code structure.
CLEANING_RELEVANT_CODES = {
    # Physical facility cleanliness
    "6-501.12",  # Physical facilities not clean
    "6-501.11",  # Floors, walls, ceilings - maintenance
    "6-501.14",  # Ventilation cleaning
    "6-501.18",  # Cleaning of premises/refuse areas
    # Equipment and utensil cleanliness
    "4-501.11",  # Equipment in poor repair
    "4-501.14",  # Warewashing equipment maintenance
    "4-601.11",  # Equipment food-contact surfaces not clean
    "4-602.11",  # Equipment not cleaned at proper frequency
    "4-602.13",  # Non-food-contact surfaces not clean
    # Food contamination / sanitation
    "3-305.11",  # Food contamination from equipment/premises
    "3-304.11",  # Food contact with unsanitized equipment
    # Pest control
    "6-501.111",  # Presence of insects/rodents/animals
    "6-202.15",  # Outer openings protected (pest entry)
    # Toxic material storage (cleaning chemical issues)
    "7-201.11",  # Poisonous/toxic materials storage
    "7-202.11",  # Unnecessary toxic items
    # Sanitization
    "4-501.114",  # Sanitizer concentration
    "4-702.11",  # Sanitization of equipment/utensils
}

# Keyword patterns in item_description or problem_description that signal cleaning needs.
CLEANING_KEYWORDS = [
    "not clean",
    "unclean",
    "unsanitary",
    "unsanitized",
    "pest",
    "insect",
    "rodent",
    "roach",
    "mouse",
    "rat",
    "vermin",
    "mold",
    "mildew",
    "contamina",
    "debris",
    "buildup",
    "grease",
    "soiled",
    "filth",
    "refuse",
    "garbage",
    "sewage",
    "poor repair",
]

# Violation codes/keywords that are NOT cleaning relevant (skip these).
NON_CLEANING_KEYWORDS = [
    "person-in-charge",
    "hair restraint",
    "handwashing",
    "hand wash",
    "certification",
    "license",
    "signage",
    "placard",
    "posted",
    "permit",
    "beverage container",
    "eating by employees",
    "smoking",
    "bare hand",
    "glove",
    "thermometer",
    "temperature",
    "food temperature",
]


def is_cleaning_relevant(violation: Violation) -> bool:
    """Determine if a violation indicates a cleaning service need."""
    # Check by violation code first (most reliable)
    if violation.violation_code in CLEANING_RELEVANT_CODES:
        return True

    # Check by keyword matching in descriptions
    text = (
        f"{violation.item_description} {violation.problem_description} "
        f"{violation.violation_description}"
    ).lower()

    # Exclude non-cleaning violations first
    for keyword in NON_CLEANING_KEYWORDS:
        if keyword in text:
            return False

    # Include cleaning-relevant violations
    for keyword in CLEANING_KEYWORDS:
        if keyword in text:
            return True

    return False


def score_severity(violations: list[Violation]) -> int:
    """
    Score the severity/urgency of a set of violations.
    Higher = more urgent = better lead.

    Scoring:
    - Priority violations: 3 points each
    - Foundation violations: 2 points each
    - Core violations: 1 point each
    - Uncorrected violations: +1 bonus each
    """
    score = 0
    type_scores = {"Priority": 3, "Foundation": 2, "Core": 1}
    for v in violations:
        score += type_scores.get(v.violation_type, 1)
        if not v.is_corrected:
            score += 1
    return score


def filter_establishment(
    establishment: Establishment,
    since: date | None = None,
    min_severity: int = 1,
) -> Lead | None:
    """
    Filter an establishment into a Lead if it has recent, cleaning-relevant violations.

    Args:
        establishment: Parsed establishment with inspections
        since: Only consider inspections after this date (default: 90 days ago)
        min_severity: Minimum severity score to qualify as a lead
    """
    if since is None:
        since = date.today() - timedelta(days=90)

    relevant_violations: list[Violation] = []
    latest_date: date | None = None

    for inspection in establishment.inspections:
        if inspection.inspection_date < since:
            continue
        if inspection.is_in_compliance:
            continue

        for violation in inspection.violations:
            if is_cleaning_relevant(violation):
                relevant_violations.append(violation)
                if latest_date is None or inspection.inspection_date > latest_date:
                    latest_date = inspection.inspection_date

    if not relevant_violations:
        return None

    severity = score_severity(relevant_violations)
    if severity < min_severity:
        return None

    return Lead(
        establishment=establishment,
        relevant_violations=relevant_violations,
        latest_inspection_date=latest_date,
        severity_score=severity,
    )
