"""
Restaurant contact enrichment engine.

Searches DuckDuckGo for restaurant contact info, scrapes result pages
with httpx + BeautifulSoup, extracts emails/phones/websites via regex,
and saves to the contacts table in our database.

Usage:
    python -m golden.enrichment --city chicago --limit 10
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from ddgs import DDGS

from .database import ContactRow, EstablishmentRow, get_session, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Silence noisy HTTP request logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("primp").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class RestaurantContact:
    establishment_db_id: int
    name: str
    email: str = ""
    phone: str = ""
    website: str = ""
    source: str = ""
    confidence: float = 0.0


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

EMAIL_PATTERN = re.compile(r'\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b')
PHONE_PATTERN = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b')

JUNK_EMAIL_DOMAINS = {
    "example.com", "test.com", "sentry.io", "wixpress.com",
    "googleapis.com", "googleusercontent.com", "sentry-next.wixpress.com",
    "email.com", "domain.com", "yoursite.com", "website.com",
}

JUNK_PHONES = {
    "0000000000", "1234567890", "9999999999", "1111111111",
    "0123456789", "5555555555",
}

BLACKLISTED_DOMAINS = {
    "amazon.com", "ebay.com", "walmart.com",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "reddit.com", "quora.com", "linkedin.com",
    "wikipedia.org", "glassdoor.com", "indeed.com",
    "doordash.com", "ubereats.com", "grubhub.com",
    "seamless.com", "postmates.com",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_restaurant(name: str, address: str, city: str) -> list[dict]:
    """Search DuckDuckGo for restaurant contact info."""
    queries = [
        f'"{name}" {city} restaurant contact',
        f'"{name}" {address}',
    ]
    results = []
    for query in queries:
        try:
            raw = list(DDGS().text(query, max_results=5))
            for r in raw:
                results.append({
                    "url": r.get("href", ""),
                    "title": r.get("title", ""),
                    "snippet": r.get("body", ""),
                })
        except Exception as e:
            logger.warning("Search failed for '%s': %s", query, e)
    # Deduplicate by URL
    seen: set[str] = set()
    deduped = []
    for r in results:
        if r["url"] and r["url"] not in seen:
            seen.add(r["url"])
            deduped.append(r)
    return deduped


# ---------------------------------------------------------------------------
# URL filtering
# ---------------------------------------------------------------------------

def _is_blacklisted(url: str) -> bool:
    """Check if URL domain is blacklisted."""
    try:
        domain = urlparse(url).netloc.lower()
        for bl in BLACKLISTED_DOMAINS:
            if bl in domain:
                return True
    except Exception:
        return True
    return False


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _extract_text(html: str) -> str:
    """Extract visible text from HTML, stripping script/style tags."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "meta", "link"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def _filter_emails(emails: list[str]) -> list[str]:
    """Remove junk/generic emails."""
    clean = []
    for e in emails:
        e = e.lower().strip()
        domain = e.split("@")[-1] if "@" in e else ""
        if domain in JUNK_EMAIL_DOMAINS:
            continue
        # Skip image file extensions that regex can accidentally match
        if e.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
            continue
        clean.append(e)
    return clean


def _filter_phones(phones: list[str]) -> list[str]:
    """Remove junk/test phone numbers."""
    clean = []
    for p in phones:
        digits = re.sub(r'\D', '', p)
        # Normalize to 10 digits (strip leading 1)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            continue
        if digits in JUNK_PHONES:
            continue
        # Skip if all same digit
        if len(set(digits)) == 1:
            continue
        clean.append(p)
    return clean


def _score_result(url: str, name: str, emails: list[str], phones: list[str]) -> int:
    """Score a search result by relevance."""
    score = 0
    try:
        domain = urlparse(url).netloc.lower()
        path = urlparse(url).path.lower()
    except Exception:
        return 0

    # Restaurant name appears in domain
    name_words = [w.lower() for w in name.split() if len(w) > 2]
    for word in name_words:
        if word in domain:
            score += 30
            break

    # Contact/about page
    if "contact" in path or "about" in path:
        score += 10

    # Found contact info
    if phones:
        score += 10
    if emails:
        score += 15

    # Yelp
    if "yelp.com" in domain:
        score += 5

    return score


def fetch_and_extract(url: str, name: str, client: httpx.Client) -> dict:
    """Fetch a URL and extract contact info."""
    result = {"url": url, "emails": [], "phones": [], "score": 0}

    if _is_blacklisted(url):
        return result

    try:
        resp = client.get(url, timeout=10.0, follow_redirects=True)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.debug("Failed to fetch %s: %s", url, e)
        return result

    text = _extract_text(html)

    emails = _filter_emails(EMAIL_PATTERN.findall(text))
    phones = _filter_phones(PHONE_PATTERN.findall(text))

    result["emails"] = emails
    result["phones"] = phones
    result["score"] = _score_result(url, name, emails, phones)

    return result


def _detect_website(search_results: list[dict], name: str) -> str:
    """Try to detect the restaurant's own website from search results."""
    name_words = [w.lower() for w in name.split() if len(w) > 2]
    if not name_words:
        return ""

    for r in search_results:
        try:
            domain = urlparse(r["url"]).netloc.lower()
        except Exception:
            continue
        if _is_blacklisted(r["url"]):
            continue
        # Skip aggregator sites
        if any(agg in domain for agg in ("yelp.com", "tripadvisor.com",
               "yellowpages.com", "mapquest.com", "google.com", "bbb.org")):
            continue
        # If any name word appears in the domain, likely their website
        for word in name_words:
            if word in domain:
                return domain
    return ""


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

async def enrich_one(
    db_id: int, name: str, address: str, city: str
) -> RestaurantContact:
    """Enrich a single restaurant with contact info."""
    contact = RestaurantContact(establishment_db_id=db_id, name=name)

    if not name or not name.strip():
        return contact

    # Search
    search_results = await asyncio.to_thread(search_restaurant, name, address, city)
    if not search_results:
        return contact

    # Detect website
    contact.website = _detect_website(search_results, name)

    # Fetch and extract from each result
    best_score = 0
    best_email = ""
    best_phone = ""
    best_source = ""

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for r in search_results:
            extracted = await asyncio.to_thread(
                fetch_and_extract, r["url"], name, client
            )

            if extracted["score"] > best_score:
                best_score = extracted["score"]
                if extracted["emails"]:
                    best_email = extracted["emails"][0]
                if extracted["phones"]:
                    best_phone = extracted["phones"][0]
                try:
                    best_source = urlparse(r["url"]).netloc
                except Exception:
                    best_source = r["url"]

            # Also keep any email/phone even if score isn't best
            if not best_email and extracted["emails"]:
                best_email = extracted["emails"][0]
                if not best_source:
                    best_source = urlparse(r["url"]).netloc
            if not best_phone and extracted["phones"]:
                best_phone = extracted["phones"][0]
                if not best_source:
                    best_source = urlparse(r["url"]).netloc

    contact.email = best_email
    contact.phone = best_phone
    contact.source = best_source
    contact.confidence = min(best_score / 65.0, 1.0)  # 65 = max possible score

    return contact


async def enrich_batch(
    establishments: list[dict], delay: float = 3.0
) -> list[RestaurantContact]:
    """Enrich a batch of establishments with contact info."""
    results = []
    for i, est in enumerate(establishments):
        db_id = est.get("db_id", 0)
        name = est.get("name", "")
        address = est.get("address", "")
        city = est.get("city", "")

        contact = await enrich_one(db_id, name, address, city)
        results.append(contact)

        if i < len(establishments) - 1:
            await asyncio.sleep(delay)

    return results


def enrich_from_db(city: str, limit: int = 50) -> list[RestaurantContact]:
    """Pull unenriched establishments from DB, enrich them, save results."""
    init_db()

    with get_session() as session:
        # Find establishments with no contacts entry yet
        subq = session.query(ContactRow.establishment_id)
        query = (
            session.query(EstablishmentRow)
            .filter(
                EstablishmentRow.city == city,
                EstablishmentRow.name != "",
                ~EstablishmentRow.id.in_(subq),
            )
            .limit(limit)
        )
        rows = query.all()

        if not rows:
            logger.info("No unenriched establishments found for %s", city)
            return []

        establishments = [
            {
                "db_id": r.id,
                "name": r.name,
                "address": r.address,
                "city": r.city,
            }
            for r in rows
        ]

    # Run enrichment
    total = len(establishments)
    contacts_found = 0
    results: list[RestaurantContact] = []

    for i, est in enumerate(establishments):
        print(
            f"[{i + 1}/{total}] Searching: {est['name']}, "
            f"{est['city']}...",
            flush=True,
        )

        contact = asyncio.run(
            enrich_one(est["db_id"], est["name"], est["address"], est["city"])
        )
        results.append(contact)

        # Report what we found
        found_parts = []
        if contact.email:
            found_parts.append(f"email={contact.email}")
        if contact.phone:
            found_parts.append(f"phone={contact.phone}")
        if contact.website:
            found_parts.append(f"website={contact.website}")

        if found_parts:
            print(f"  Found: {', '.join(found_parts)}")
            contacts_found += 1
        else:
            print("  No contact info found.")

        # Save to database
        _save_contact(contact)

        # Rate limit
        if i < total - 1:
            time.sleep(3.0)

    hit_rate = (contacts_found / total * 100) if total > 0 else 0
    print(
        f"\nDone: {total} establishments processed, "
        f"{contacts_found} contacts found ({hit_rate:.0f}% hit rate)"
    )

    return results


def _save_contact(contact: RestaurantContact) -> None:
    """Save a RestaurantContact to the database."""
    if not contact.email and not contact.phone and not contact.website:
        # Still save an empty record so we don't re-process
        pass

    with get_session() as session:
        row = ContactRow(
            establishment_id=contact.establishment_db_id,
            email=contact.email,
            phone=contact.phone,
            website=contact.website,
            source=contact.source,
            scraped_at=datetime.now(timezone.utc),
        )
        session.add(row)
    logger.debug("Saved contact for establishment %d", contact.establishment_db_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Golden restaurant contact enrichment"
    )
    parser.add_argument(
        "--city",
        required=True,
        help="City to enrich (e.g. chicago, nyc, detroit)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max establishments to enrich (default: 10)",
    )
    args = parser.parse_args()

    enrich_from_db(city=args.city, limit=args.limit)


if __name__ == "__main__":
    main()
