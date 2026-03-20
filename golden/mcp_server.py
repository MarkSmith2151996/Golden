"""
Golden MCP Server — exposes the data collection pipeline to Claude Code.

Run standalone:  python golden/mcp_server.py
Register:        claude mcp add golden -- .venv/bin/python golden/mcp_server.py

All logging goes to stderr (stdout is the MCP stdio channel).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import Counter

from mcp.server.fastmcp import FastMCP

# Force logging to stderr so it doesn't pollute the MCP stdio channel
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

mcp = FastMCP("golden")

# ---------------------------------------------------------------------------
# Cached results from the last pipeline run
# ---------------------------------------------------------------------------
_last_run: list[dict] | None = None


def _cache(results: list[dict]) -> list[dict]:
    global _last_run
    _last_run = results
    return results


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def golden_run_pipeline(
    cities: list[str] | None = None,
    days: int = 90,
    limit: int | None = None,
) -> str:
    """Run the Golden data collection pipeline for specified cities.

    Args:
        cities: Cities to process (default: all). Options: austin, baton_rouge, boulder, chicago, cincinnati, dallas, delaware, detroit, fulton_ga, king_county, la_city, marin, montgomery_md, ny_state, ny_state_ag, nyc, pg_county_md, san_mateo, santa_clara, sf
        days: Look-back window in days (default: 90)
        limit: Max records per city (useful for quick tests)
    """
    from golden.pipeline import run_pipeline

    results = await asyncio.to_thread(
        run_pipeline,
        cities=cities,
        days=days,
        limit=limit,
    )
    _cache(results)

    # Build per-city counts
    city_counts: Counter[str] = Counter()
    total_inspections = 0
    total_violations = 0
    for est in results:
        city_counts[est.get("city", "unknown")] += 1
        for insp in est.get("inspections", []):
            total_inspections += 1
            total_violations += len(insp.get("violations", []))

    lines = [
        f"Pipeline complete: {len(results)} establishments collected",
        f"Total inspections: {total_inspections}",
        f"Total violations: {total_violations}",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} establishments")

    return "\n".join(lines)


@mcp.tool()
async def golden_list_cities() -> str:
    """List available cities and their data source type."""
    from golden.sources import get_source, list_cities
    from golden.sources.base import SocrataConfig

    lines = ["Available cities:", ""]
    for city_name in list_cities():
        source = get_source(city_name)
        cfg = source.config
        if isinstance(cfg, SocrataConfig):
            source_type = f"Socrata ({cfg.base_url})"
        else:
            source_type = "Gatsby (static JSON)"
        lines.append(f"  {cfg.display_name} [{city_name}] — {source_type}")

    return "\n".join(lines)


@mcp.tool()
async def golden_summarize_data(
    cities: list[str] | None = None,
    days: int = 90,
    limit: int | None = None,
) -> str:
    """Run the pipeline and return a human-readable summary with stats.

    Args:
        cities: Cities to process (default: all). Options: austin, baton_rouge, boulder, chicago, cincinnati, dallas, delaware, detroit, fulton_ga, king_county, la_city, marin, montgomery_md, ny_state, ny_state_ag, nyc, pg_county_md, san_mateo, santa_clara, sf
        days: Look-back window in days (default: 90)
        limit: Max records per city (useful for quick tests)
    """
    from golden.pipeline import run_pipeline

    results = await asyncio.to_thread(
        run_pipeline,
        cities=cities,
        days=days,
        limit=limit,
    )
    _cache(results)

    if not results:
        return "No data collected with the given parameters."

    # Per-city breakdown
    city_counts: Counter[str] = Counter()
    total_inspections = 0
    total_violations = 0
    violation_types: Counter[str] = Counter()
    compliance_yes = 0
    compliance_no = 0

    for est in results:
        city_counts[est.get("city", "unknown")] += 1
        for insp in est.get("inspections", []):
            total_inspections += 1
            if insp.get("is_in_compliance"):
                compliance_yes += 1
            else:
                compliance_no += 1
            for v in insp.get("violations", []):
                total_violations += 1
                vtype = v.get("violation_type", "Unknown")
                violation_types[vtype] += 1

    lines = [
        "=== Golden Data Summary ===",
        "",
        f"Total establishments: {len(results)}",
        f"Total inspections: {total_inspections}",
        f"Total violations: {total_violations}",
        f"In compliance: {compliance_yes} | Not in compliance: {compliance_no}",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} establishments")

    if violation_types:
        lines.append("")
        lines.append("Violation type distribution:")
        for vtype, count in violation_types.most_common():
            lines.append(f"  {vtype}: {count}")

    return "\n".join(lines)


@mcp.tool()
async def golden_check_source(city: str) -> str:
    """Test connectivity to a city's data source.

    Args:
        city: City name to check (e.g. chicago, detroit, nyc, sf, austin)
    """
    import httpx

    from golden.sources import get_source
    from golden.sources.base import SocrataConfig

    try:
        source = get_source(city)
    except ValueError as e:
        return str(e)

    cfg = source.config

    if isinstance(cfg, SocrataConfig):
        url = f"{cfg.base_url}/resource/{cfg.dataset_id}.json"
        params = {"$limit": "5"}
    else:
        # Detroit Gatsby source
        url = "https://detroitrestaurantinspections.netlify.app/page-data/index/page-data.json"
        params = {}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, dict):
            count = 1
        else:
            count = 0

        return (
            f"Source: {cfg.display_name}\n"
            f"URL: {url}\n"
            f"Status: reachable (HTTP {resp.status_code})\n"
            f"Sample records: {count}"
        )
    except httpx.HTTPStatusError as e:
        return (
            f"Source: {cfg.display_name}\n"
            f"URL: {url}\n"
            f"Status: HTTP error {e.response.status_code}\n"
            f"Error: {e}"
        )
    except Exception as e:
        return (
            f"Source: {cfg.display_name}\n"
            f"URL: {url}\n"
            f"Status: unreachable\n"
            f"Error: {type(e).__name__}: {e}"
        )


@mcp.tool()
async def golden_get_last_run() -> str:
    """Return cached results from the most recent pipeline run (avoids re-fetching)."""
    if _last_run is None:
        return "No cached results. Run golden_run_pipeline first."

    city_counts: Counter[str] = Counter()
    total_inspections = 0
    total_violations = 0
    for est in _last_run:
        city_counts[est.get("city", "unknown")] += 1
        for insp in est.get("inspections", []):
            total_inspections += 1
            total_violations += len(insp.get("violations", []))

    lines = [
        f"Cached results: {len(_last_run)} establishments",
        f"Total inspections: {total_inspections}",
        f"Total violations: {total_violations}",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} establishments")

    return "\n".join(lines)


@mcp.tool()
async def golden_violation_stats() -> str:
    """Analyze violation patterns from the last pipeline run."""
    if _last_run is None:
        return "No cached results. Run golden_run_pipeline first."

    violation_types: Counter[str] = Counter()
    violation_codes: Counter[str] = Counter()
    corrected = 0
    uncorrected = 0

    for est in _last_run:
        for insp in est.get("inspections", []):
            for v in insp.get("violations", []):
                violation_types[v.get("violation_type", "Unknown")] += 1
                code = v.get("violation_code", "")
                if code:
                    violation_codes[code] += 1
                if v.get("is_corrected"):
                    corrected += 1
                else:
                    uncorrected += 1

    total = corrected + uncorrected
    lines = [
        "=== Violation Statistics ===",
        "",
        f"Total violations: {total}",
        f"Corrected: {corrected} | Uncorrected: {uncorrected}",
    ]
    if total > 0:
        lines.append(f"Uncorrected rate: {uncorrected / total * 100:.1f}%")

    lines.append("")
    lines.append("By violation type:")
    for vtype, count in violation_types.most_common():
        lines.append(f"  {vtype}: {count}")

    lines.append("")
    lines.append("Most common violation codes:")
    for code, count in violation_codes.most_common(15):
        lines.append(f"  {code}: {count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
