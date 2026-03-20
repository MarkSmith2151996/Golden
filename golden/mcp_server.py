"""
Golden MCP Server — exposes the lead-generation pipeline to Claude Code.

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
    min_severity: int = 1,
) -> str:
    """Run the Golden lead-generation pipeline for specified cities.

    Args:
        cities: Cities to process (default: all). Options: chicago, detroit, nyc
        days: Look-back window in days (default: 90)
        limit: Max records per city (useful for quick tests)
        min_severity: Minimum severity score to qualify as a lead (default: 1)
    """
    from golden.pipeline import run_pipeline

    leads = await asyncio.to_thread(
        run_pipeline,
        cities=cities,
        days=days,
        limit=limit,
        min_severity=min_severity,
    )
    _cache(leads)

    # Build per-city counts
    city_counts: Counter[str] = Counter()
    for lead in leads:
        city_counts[lead.get("city", "unknown")] += 1

    lines = [
        f"Pipeline complete: {len(leads)} leads found",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} leads")

    # Top leads
    top = leads[:10]
    if top:
        lines.append("")
        lines.append("Top leads (by severity):")
        for i, lead in enumerate(top, 1):
            est = lead.get("establishment", {})
            name = est.get("name", "Unknown")
            addr = est.get("address", "")
            score = lead.get("severity_score", 0)
            city = lead.get("city", "")
            n_violations = len(lead.get("relevant_violations", []))
            lines.append(
                f"  {i}. {name} ({city}) — severity {score}, "
                f"{n_violations} violations, {addr}"
            )

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
async def golden_summarize_leads(
    cities: list[str] | None = None,
    days: int = 90,
    limit: int | None = None,
    min_severity: int = 1,
) -> str:
    """Run the pipeline and return a human-readable summary with stats.

    Args:
        cities: Cities to process (default: all). Options: chicago, detroit, nyc
        days: Look-back window in days (default: 90)
        limit: Max records per city (useful for quick tests)
        min_severity: Minimum severity score to qualify as a lead (default: 1)
    """
    from golden.pipeline import run_pipeline

    leads = await asyncio.to_thread(
        run_pipeline,
        cities=cities,
        days=days,
        limit=limit,
        min_severity=min_severity,
    )
    _cache(leads)

    if not leads:
        return "No leads found with the given parameters."

    # Per-city breakdown
    city_counts: Counter[str] = Counter()
    violation_types: Counter[str] = Counter()
    total_violations = 0
    corrected = 0
    uncorrected = 0

    for lead in leads:
        city_counts[lead.get("city", "unknown")] += 1
        for v in lead.get("relevant_violations", []):
            total_violations += 1
            vtype = v.get("violation_type", "Unknown")
            violation_types[vtype] += 1
            if v.get("is_corrected"):
                corrected += 1
            else:
                uncorrected += 1

    lines = [
        "=== Golden Pipeline Summary ===",
        "",
        f"Total leads: {len(leads)}",
        f"Total cleaning-relevant violations: {total_violations}",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} leads")

    lines.append("")
    lines.append("Violation type distribution:")
    for vtype, count in violation_types.most_common():
        lines.append(f"  {vtype}: {count}")

    lines.append("")
    lines.append(f"Corrected: {corrected} | Uncorrected: {uncorrected}")
    if total_violations > 0:
        pct = uncorrected / total_violations * 100
        lines.append(f"Uncorrected rate: {pct:.1f}%")

    # Top 10
    lines.append("")
    lines.append("Top 10 leads (by severity):")
    for i, lead in enumerate(leads[:10], 1):
        est = lead.get("establishment", {})
        name = est.get("name", "Unknown")
        score = lead.get("severity_score", 0)
        city = lead.get("city", "")
        n_v = len(lead.get("relevant_violations", []))
        lines.append(f"  {i}. {name} ({city}) — severity {score}, {n_v} violations")

    return "\n".join(lines)


@mcp.tool()
async def golden_check_source(city: str) -> str:
    """Test connectivity to a city's data source.

    Args:
        city: City name to check (chicago, detroit, nyc)
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
            # Gatsby wraps data in nested structure
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
        return "No cached results. Run golden_run_pipeline or golden_summarize_leads first."

    city_counts: Counter[str] = Counter()
    for lead in _last_run:
        city_counts[lead.get("city", "unknown")] += 1

    lines = [
        f"Cached results: {len(_last_run)} leads",
        "",
        "Per-city breakdown:",
    ]
    for city, count in sorted(city_counts.items()):
        lines.append(f"  {city}: {count} leads")

    lines.append("")
    lines.append("Top 5 leads:")
    for i, lead in enumerate(_last_run[:5], 1):
        est = lead.get("establishment", {})
        name = est.get("name", "Unknown")
        score = lead.get("severity_score", 0)
        city = lead.get("city", "")
        lines.append(f"  {i}. {name} ({city}) — severity {score}")

    return "\n".join(lines)


@mcp.tool()
async def golden_violation_stats() -> str:
    """Analyze violation patterns from the last pipeline run."""
    if _last_run is None:
        return "No cached results. Run golden_run_pipeline or golden_summarize_leads first."

    violation_types: Counter[str] = Counter()
    violation_codes: Counter[str] = Counter()
    corrected = 0
    uncorrected = 0
    severity_buckets: Counter[str] = Counter()

    for lead in _last_run:
        score = lead.get("severity_score", 0)
        if score >= 10:
            severity_buckets["critical (10+)"] += 1
        elif score >= 5:
            severity_buckets["high (5-9)"] += 1
        elif score >= 3:
            severity_buckets["medium (3-4)"] += 1
        else:
            severity_buckets["low (1-2)"] += 1

        for v in lead.get("relevant_violations", []):
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
        f"Total violations analyzed: {total}",
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
    for code, count in violation_codes.most_common(10):
        lines.append(f"  {code}: {count}")

    lines.append("")
    lines.append("Lead severity distribution:")
    for bucket in ["critical (10+)", "high (5-9)", "medium (3-4)", "low (1-2)"]:
        count = severity_buckets.get(bucket, 0)
        lines.append(f"  {bucket}: {count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
