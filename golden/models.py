"""Data models for Golden pipeline."""

from __future__ import annotations

from datetime import date
from pydantic import BaseModel


class Violation(BaseModel):
    violation_code: str
    violation_description: str
    violation_type: str  # Priority, Foundation, Core
    item_description: str
    problem_description: str
    area_description: str
    is_corrected: bool
    num_days_to_correct: str | None = None
    corrected_date: date | None = None


class Inspection(BaseModel):
    inspection_id: str
    inspection_date: date
    inspection_type: str
    is_in_compliance: bool
    violations: list[Violation] = []


class Establishment(BaseModel):
    establishment_id: str
    name: str
    address: str
    city: str = ""
    zip_code: str = ""
    owner: str = ""
    license_number: str = ""
    license_type: str = ""
    establishment_type: str = ""
    status: str = ""
    coords: str = ""
    inspections: list[Inspection] = []


class Lead(BaseModel):
    """A filtered, actionable lead — a restaurant with cleaning-relevant violations."""
    establishment: Establishment
    relevant_violations: list[Violation]
    latest_inspection_date: date
    severity_score: int  # higher = more urgent
    city: str = ""
