"""
Outreach message drafting engine.

Uses Claude CLI to generate personalized outreach emails for restaurants
that have violations + contacts but no outreach yet.

Usage:
    python -m golden.drafter --city chicago --limit 5
    python -m golden.drafter --review
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

from sqlalchemy import and_, not_

from .database import (
    ContactRow,
    EstablishmentRow,
    OutreachRow,
    ViolationRow,
    get_session,
    init_db,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Violation filtering keywords
# ---------------------------------------------------------------------------

CLEANING_KEYWORDS = {
    "pest", "rodent", "roach", "cockroach", "mice", "mouse", "rat",
    "fly", "flies", "insect", "dirty", "unclean", "unsanitary", "filth",
    "grime", "grease", "debris", "sanitize", "sanitization", "sanitation",
    "disinfect", "mold", "mildew", "contaminated surface",
    "food contact surface", "drain", "floor", "wall", "ceiling",
    "ventilation", "hood", "exhaust", "waste", "garbage", "refuse",
    "dumpster",
}

SKIP_KEYWORDS = {
    "temperature", "thermometer", "label", "permit", "license",
    "handwashing sign", "employee", "training", "certification",
    "food storage temp",
}


def _is_cleaning_relevant(desc: str) -> bool:
    """Check if a violation description is cleaning-relevant."""
    lower = desc.lower()
    for skip in SKIP_KEYWORDS:
        if skip in lower:
            return False
    for kw in CLEANING_KEYWORDS:
        if kw in lower:
            return True
    return False


# ---------------------------------------------------------------------------
# Claude CLI integration
# ---------------------------------------------------------------------------

async def claude_draft(prompt: str) -> str:
    """Call Claude CLI and return the response text."""
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)  # Allow nested CLI calls

    proc = await asyncio.create_subprocess_exec(
        "claude", "-p", prompt, "--output-format", "json", "--max-turns", "1",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
    if proc.returncode != 0:
        raise RuntimeError(f"Claude CLI failed: {stderr.decode().strip()}")

    # Parse JSON response to extract result text
    try:
        data = json.loads(stdout.decode())
        return data.get("result", "").strip()
    except json.JSONDecodeError:
        # Fallback: treat raw output as text
        return stdout.decode().strip()


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

def _build_body_prompt(name: str, city: str, violations: list[str]) -> str:
    viol_text = "\n".join(f"- {v}" for v in violations)
    return f"""You are drafting a brief outreach email to a restaurant that recently received health inspection violations.

RULES:
- NEVER mention "failed inspection" or use accusatory language
- Position as: "we help restaurants pass re-inspections"
- Reference their specific situation without quoting violation codes
- Keep it under 150 words
- Professional but warm tone — these owners are stressed
- Include a clear call to action (reply or call)
- Sign off as "The Golden Team"

Restaurant: {name}
City: {city}
Recent issues noted during inspection:
{viol_text}

Write ONLY the email body. No subject line, no markdown, no explanation."""


def _build_subject_prompt(name: str) -> str:
    return f"""Write a short, non-threatening email subject line for an outreach email to {name}, a restaurant that recently had health inspection issues. The email offers help with re-inspection preparation.

RULES: Under 8 words. No mention of "violation" or "failed". No clickbait.

Write ONLY the subject line, nothing else."""


# ---------------------------------------------------------------------------
# Core drafting
# ---------------------------------------------------------------------------

async def draft_one(establishment_db_id: int) -> dict | None:
    """Draft an outreach email for a single establishment.

    Returns {subject, body, establishment_id, contact_id} or None if
    no eligible violations/contacts.
    """
    with get_session() as session:
        est = session.get(EstablishmentRow, establishment_db_id)
        if not est:
            logger.warning("Establishment %d not found", establishment_db_id)
            return None

        # Get violations
        violations = (
            session.query(ViolationRow)
            .filter(ViolationRow.establishment_id == establishment_db_id)
            .all()
        )

        # Filter to cleaning-relevant
        relevant = []
        for v in violations:
            desc = v.violation_description or v.problem_description or ""
            if desc and _is_cleaning_relevant(desc):
                relevant.append(desc[:200])

        if not relevant:
            logger.info("No cleaning-relevant violations for %s", est.name)
            return None

        # Get contact with email
        contact = (
            session.query(ContactRow)
            .filter(
                ContactRow.establishment_id == establishment_db_id,
                ContactRow.email != "",
            )
            .first()
        )
        if not contact:
            logger.info("No email contact for %s", est.name)
            return None

        name = est.name
        city = est.city
        contact_id = contact.id
        email = contact.email

    # Draft body
    body_prompt = _build_body_prompt(name, city, relevant[:5])
    body = await claude_draft(body_prompt)

    # Draft subject
    subject_prompt = _build_subject_prompt(name)
    subject = await claude_draft(subject_prompt)

    # Clean up — remove quotes if Claude wrapped them
    subject = subject.strip().strip('"').strip("'")

    # Save to database
    with get_session() as session:
        outreach = OutreachRow(
            establishment_id=establishment_db_id,
            contact_id=contact_id,
            channel="email",
            status="draft",
            subject=subject,
            message_body=body,
            created_at=datetime.now(timezone.utc),
        )
        session.add(outreach)
        session.flush()
        outreach_id = outreach.id

    return {
        "subject": subject,
        "body": body,
        "establishment_id": establishment_db_id,
        "contact_id": contact_id,
        "outreach_id": outreach_id,
        "email": email,
        "name": name,
    }


async def draft_batch(city: str, limit: int = 10) -> list[dict]:
    """Find eligible establishments and draft outreach for each."""
    init_db()

    with get_session() as session:
        # Subqueries: establishments that already have outreach
        outreach_sub = session.query(OutreachRow.establishment_id).scalar_subquery()

        # Find establishments with contacts (that have email) AND violations, but no outreach
        candidates = (
            session.query(EstablishmentRow)
            .join(ContactRow, ContactRow.establishment_id == EstablishmentRow.id)
            .join(ViolationRow, ViolationRow.establishment_id == EstablishmentRow.id)
            .filter(
                EstablishmentRow.city == city,
                EstablishmentRow.name != "",
                ContactRow.email != "",
                ~EstablishmentRow.id.in_(outreach_sub),
            )
            .distinct()
            .limit(limit)
            .all()
        )

        if not candidates:
            print(f"No eligible establishments found for {city}.")
            print("Requirements: has violations + has email contact + no existing outreach.")
            return []

        est_ids = [(c.id, c.name) for c in candidates]

    total = len(est_ids)
    results = []

    for i, (est_id, est_name) in enumerate(est_ids):
        print(f"\n[{i + 1}/{total}] Drafting for: {est_name} ({city})")

        # Count cleaning-relevant violations
        with get_session() as session:
            violations = (
                session.query(ViolationRow)
                .filter(ViolationRow.establishment_id == est_id)
                .all()
            )
            relevant_count = sum(
                1 for v in violations
                if _is_cleaning_relevant(v.violation_description or v.problem_description or "")
            )
            contact = (
                session.query(ContactRow)
                .filter(ContactRow.establishment_id == est_id, ContactRow.email != "")
                .first()
            )
            contact_email = contact.email if contact else "?"

        print(f"  Violations: {relevant_count} cleaning-relevant")
        print(f"  Contact: {contact_email}")

        try:
            result = await draft_one(est_id)
            if result:
                print(f"  Draft saved (outreach #{result['outreach_id']}, status=draft)")
                results.append(result)
            else:
                print("  Skipped (no eligible violations or contact)")
        except Exception as e:
            print(f"  ERROR: {e}")
            logger.exception("Failed to draft for establishment %d", est_id)

    print(f"\nDone: {len(results)} drafts created. Review with: python -m golden.drafter --review")
    return results


# ---------------------------------------------------------------------------
# Review
# ---------------------------------------------------------------------------

def review_drafts():
    """Print all drafts for review."""
    init_db()

    with get_session() as session:
        drafts = (
            session.query(OutreachRow)
            .filter(OutreachRow.status == "draft")
            .order_by(OutreachRow.id)
            .all()
        )

        if not drafts:
            print("No drafts to review.")
            return

        print(f"\n{'=' * 60}")
        print(f"  {len(drafts)} draft(s) pending review")
        print(f"{'=' * 60}")

        for draft in drafts:
            est = session.get(EstablishmentRow, draft.establishment_id)
            contact = session.get(ContactRow, draft.contact_id) if draft.contact_id else None

            est_name = est.name if est else "Unknown"
            est_city = est.city if est else "?"
            to_email = contact.email if contact else "?"

            print(f"\n--- Draft #{draft.id} ---")
            print(f"To: {to_email} ({est_name}, {est_city})")
            print(f"Subject: {draft.subject}")
            print(f"Body:")
            print(draft.message_body)
            print(f"---")
            print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Golden outreach message drafter"
    )
    parser.add_argument(
        "--city",
        help="City to draft outreach for (e.g. chicago, nyc)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max drafts to create (default: 5)",
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help="Review pending drafts",
    )
    args = parser.parse_args()

    if args.review:
        review_drafts()
    elif args.city:
        asyncio.run(draft_batch(city=args.city, limit=args.limit))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
