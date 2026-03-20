"""
Main data pipeline — multi-city data collection.

Usage:
    python -m golden                              # all cities
    python -m golden --city detroit               # Detroit only
    python -m golden --city nyc chicago           # NYC + Chicago
    python -m golden --city nyc --limit 100       # test with 100 records
    python -m golden --days 365 -o data.json      # all cities, 1 year, file output
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

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
) -> list[dict]:
    """
    Run data collection across one or more cities.

    Returns a flat list of establishment dicts, each with their
    inspections and violations nested inside.
    """
    if cities is None:
        cities = list_cities()

    all_establishments = []

    for city_name in cities:
        try:
            logger.info(f"--- Starting {city_name} ---")
            source = get_source(city_name)
            establishments = source.fetch_establishments(limit=limit)

            for est in establishments:
                all_establishments.append(est.model_dump(mode="json"))

            logger.info(
                f"--- {city_name}: {len(establishments)} establishments ---"
            )

        except Exception:
            logger.exception(f"Failed to process {city_name} — skipping")

    logger.info(
        f"Pipeline complete: {len(cities)} cities, "
        f"{len(all_establishments)} total establishments"
    )

    return all_establishments


def main():
    available = list_cities()
    parser = argparse.ArgumentParser(description="Golden data collection pipeline")
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
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file (default: stdout)",
    )
    args = parser.parse_args()

    results = run_pipeline(
        cities=args.city,
        days=args.days,
        limit=args.limit,
    )

    output = json.dumps(results, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        logger.info(f"Wrote {len(results)} establishments to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
