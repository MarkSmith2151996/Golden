"""
Main data pipeline — multi-city orchestration.

Usage:
    python -m golden                              # all cities
    python -m golden --city detroit               # Detroit only
    python -m golden --city nyc chicago           # NYC + Chicago
    python -m golden --city nyc --limit 100       # test with 100 records
    python -m golden --days 90 -o leads.json      # all cities, 90 days, file output
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

from .filter import filter_establishment
from .sources import get_source, list_cities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    cities: list[str] | None = None,
    days: int = 90,
    limit: int | None = None,
    min_severity: int = 1,
) -> list[dict]:
    """
    Run the full data collection pipeline across one or more cities.

    1. For each city: fetch establishments via its CitySource
    2. Parse into structured models (handled inside each source)
    3. Filter for cleaning-relevant violations
    4. Return sorted leads (highest severity first) across all cities
    """
    if cities is None:
        cities = list_cities()

    since = date.today() - timedelta(days=days)
    all_leads = []

    for city_name in cities:
        try:
            logger.info(f"--- Starting {city_name} ---")
            source = get_source(city_name)
            establishments = source.fetch_establishments(limit=limit)

            city_leads = 0
            for est in establishments:
                lead = filter_establishment(
                    est, since=since, min_severity=min_severity
                )
                if lead:
                    lead.city = city_name
                    all_leads.append(lead)
                    city_leads += 1

            logger.info(
                f"--- {city_name}: {len(establishments)} establishments, "
                f"{city_leads} leads ---"
            )

        except Exception:
            logger.exception(f"Failed to process {city_name} — skipping")

    # Sort by severity across all cities (highest first)
    all_leads.sort(key=lambda l: l.severity_score, reverse=True)

    logger.info(
        f"Pipeline complete: {len(cities)} cities, "
        f"{len(all_leads)} total leads (since {since})"
    )

    return [lead.model_dump(mode="json") for lead in all_leads]


def main():
    available = list_cities()
    parser = argparse.ArgumentParser(description="Golden data pipeline")
    parser.add_argument(
        "--city",
        nargs="*",
        default=None,
        choices=available,
        help=f"Cities to process (default: all). Choices: {', '.join(available)}",
    )
    parser.add_argument(
        "--days", type=int, default=90, help="Look back N days (default: 90)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit records per city (for testing)",
    )
    parser.add_argument(
        "--min-severity",
        type=int,
        default=1,
        help="Minimum severity score (default: 1)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file (default: stdout)",
    )
    args = parser.parse_args()

    leads = run_pipeline(
        cities=args.city,
        days=args.days,
        limit=args.limit,
        min_severity=args.min_severity,
    )

    output = json.dumps(leads, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        logger.info(f"Wrote {len(leads)} leads to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
