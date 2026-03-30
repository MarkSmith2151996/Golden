# Independent Data Audit: Golden Dataset

**Source file**: `data/raw_data.csv` (346,735 rows) + `data/summary.csv`
**Audited**: 2026-03-30
**Audited by**: Separate Claude Code instance (independent from the development session)

---

## Dataset Overview

| Metric | Value |
|--------|-------|
| Total rows | 346,735 |
| Columns | 15 |
| Cities | 16 |
| Unique establishments | 93,862 |
| Unique violation codes | 1,079 |
| Date range | 2021-03-05 to 2026-05-17 |

---

## DATA QUALITY ISSUES

### Duplicates
- **5,304 exact duplicate rows** across all columns. Need deduplication.

### Missing Data

| Column | Empty % | Notes |
|--------|---------|-------|
| `owner` | **82.5%** (286,084 rows) | Only Detroit (98.6%) and ny_state_ag (100%) populate this |
| `establishment_type` | **65.4%** (226,810 rows) | 10 of 16 cities have zero type data |
| `zip` | **17.5%** (60,689 rows) | baton_rouge, santa_clara, sf have **zero** zip codes |
| `violation_code` | **14.1%** (48,995 rows) | ny_state has 19K rows with zero codes |
| `name` + `address` | **10.7%** (~36,970 rows) | Almost entirely santa_clara (37K rows with no name or address) |
| `inspection_date` | **0.1%** (407 rows) | |
| `in_compliance` | **0.1%** (407 rows) | |
| `city`, `establishment_id` | **0%** | Always populated |

### Logical Inconsistencies
- **49,533 rows** (14.3%) have a `violation_code` but `in_compliance=True` — contradictory
- **17,873 rows** have `in_compliance=False` but empty `violation_code`
- **3,851 rows** have `in_compliance=False` with ALL violation fields empty (code, description, problem)
- `ny_state` has ~19K rows with **zero violation codes or descriptions** — effectively empty violation data

### Encoding Issues
- **456 rows** with corrupted characters (`&#xFFFD;` replacement chars for broken apostrophes), mostly in baton_rouge
- **837 rows** with un-decoded HTML entities (`&deg;`), all in Cincinnati
- **30,282 rows** with non-ASCII in violation_description (mostly intentional en-dashes like "FOOD -- HOT HOLDING")
- These encoding issues would show up garbled in any outreach emails

### Date Issues
- **18 rows** with future dates past 2026-03-30 (delaware + pg_county_md — likely pre-scheduled inspections)
- **906 rows** from Detroit going back to **2021** — much older than other cities
- **407 rows** with empty inspection dates
- Montgomery County data is entirely from **2024** — stale

### ID Issues
- **2,027 rows** with non-standard establishment IDs:
  - Detroit (1,946 rows): bare numeric IDs (`2228`) instead of `detroit-2228`
  - SF (81 rows): non-standard format
- 1 establishment ("SHELDRAKE POINT WINERY") appears in both `ny_state` and `ny_state_ag` — legitimate overlap between two NY sources

### Violation Code Bug
- The **#1 most common "violation code"** is literally the string `True` (14,282 rows) — a boolean was stored as a code. This is a data pipeline bug.

---

## CITY-LEVEL ASSESSMENT

### Broken / Useless Sources (6 of 16)

| City | Rows | Problem |
|------|------|---------|
| **pg_county_md** | 1 | Single row. Test record or failed scrape. |
| **ny_state** | 18,978 | **Zero violation codes**. Descriptions only, most don't match cleaning keywords. |
| **santa_clara** | 36,961 | **No names, no addresses, no zips**. Violations exist but leads are unidentifiable. |
| **baton_rouge** | 15,512 | Zero zip codes. Only 1 unique violation code. Zero actionable leads. |
| **sf** | 6,788 | Zero zip codes. Zero actionable leads despite 1.5K cleaning-relevant violations. |
| **montgomery_md** | 2,759 | All data from 2024 — stale. Zero cleaning-relevant keyword matches. |

### Strong Sources

| City | Rows | Actionable Leads | Cleaning % | Notes |
|------|------|-----------------|------------|-------|
| **NYC** | 92,069 | 14,700 | 43.9% | Massive, high-quality dataset |
| **ny_state_ag** | 58,711 | 3,285 | 42.8% | 100% owner data |
| **chicago** | 20,790 | 2,184 | 34.0% | 82.9% of establishments have cleaning violations |
| **delaware** | 13,979 | 1,726 | 30.4% | Good coverage, some future dates |
| **cincinnati** | 12,184 | 1,276 | 39.4% | Good but has HTML encoding bugs |
| **boulder** | 35,722 | 520 | 12.6% | Huge violation counts per establishment (median 71) |
| **king_county** | 20,110 | 0 | 0% | Zero cleaning keyword matches — may need code mapping |

### Detroit (Target Market) — Honest Assessment

| Metric | Value |
|--------|-------|
| Total establishments | 500 |
| With any violation | 80 (16%) |
| With cleaning-relevant violation | **75** (15%) |
| With usable address | 497 (99.4%) |
| With owner data | 493 (98.6%) |
| Recent leads (90 days) | **0** |
| Cleaning-relevance rate | **50.4%** (highest of any city) |
| Always-compliant | **84%** (420 of 500) |

**Detroit has the highest quality per lead but very few leads and stale data.** 84% of Detroit establishments have zero violations — they're wasted rows. The data doesn't reach into the last 90 days.

Top Detroit violations:
1. Physical facilities not clean (6-501.12) — 92 instances
2. Equipment poor repair (4-501.11) — 67
3. Food contamination from storage (3-305.11) — 60
4. Non-food-contact surfaces not clean (4-602.13) — 59

---

## VIOLATION ANALYSIS

### Violation Types Distribution
Only 4 types exist in the dataset:

| Type | Count | Notes |
|------|-------|-------|
| Core | 205,899 | Lower severity — majority of violations |
| Priority | 107,958 | Higher severity |
| Foundation | 4,868 | |
| Other | 1 | |

### Cleaning Relevance (using filter.py logic)

| Category | Count |
|----------|-------|
| Total violation rows | 318,725 |
| Cleaning-relevant | **106,829 (33.5%)** |
| Non-cleaning (administrative, paperwork, etc.) | 211,896 (66.5%) |

### Severity Scoring (filter.py: Priority=3, Foundation=2, Core=1, +1 if uncorrected)

| Stat | Value |
|------|-------|
| Mean score per establishment | 7.7 |
| Median | 4.0 |
| 75th percentile | 10.0 |
| Max | 114 |
| Low severity (1-2) | 11,016 establishments |
| Medium (3-5) | 8,044 |
| High (6-10) | 9,846 |
| Very high (11+) | 7,851 |

### Correction Rates
- `is_corrected` is almost never `True` — only Detroit (30.9%) and Marin (3.8%) record corrections
- All other cities: 0% corrected or empty field
- This means severity scoring's "+1 if uncorrected" inflates scores for cities that simply don't track corrections

### Most-Violated Establishments
All top 20 are in **Boulder** (185, 184, 184, etc. violations each). Boulder's data appears to be cumulative historical records, inflating per-establishment counts far beyond other cities.

### Cross-City Code Sharing
- **Cincinnati** has the most unique codes (340)
- **Delaware & Detroit** share 100 codes (likely same FDA code framework)
- **ny_state_ag & NYC** share 29 codes
- Only **40 violation descriptions** appear across multiple cities (mostly boulder/chicago/santa_clara)
- **38,390 descriptions** are city-specific — very little standardization

---

## LEAD FUNNEL ANALYSIS

This is the real picture of what the dataset actually delivers:

```
346,735  total rows
  |
  | -5,304 duplicates
341,431  deduplicated rows
  |
  | -43,691 rows with no violation data
297,740  rows with violations
  |
  | -190,911 non-cleaning-relevant
106,829  cleaning-relevant violations
  |
  | (grouped by establishment)
 36,757  establishments with cleaning violations
  |
  | -8,198 missing usable address (no address or no zip)
 28,559  ACTIONABLE LEADS (with address)
  |
  | -25,246 not recent (older than 90 days)
  3,313  RECENT ACTIONABLE LEADS
  |
  | Detroit only:
     75  Detroit actionable leads
      0  Detroit recent leads
```

### Actionable Leads by City

| City | Actionable Leads | Recent Leads |
|------|-----------------|--------------|
| NYC | 14,700 | 1,494 |
| ny_state | 4,792 | 636 |
| ny_state_ag | 3,285 | 0 |
| chicago | 2,184 | 169 |
| delaware | 1,726 | 206 |
| cincinnati | 1,276 | 96 |
| boulder | 520 | 0 |
| marin | 1 | 0 |
| **detroit** | **75** | **0** |
| baton_rouge | 0 | 0 |
| santa_clara | 0 | 0 |
| sf | 0 | 0 |
| king_county | 0 | 0 |
| montgomery_md | 0 | 0 |
| pg_county_md | 0 | 0 |
| austin | 0 | 712 |

---

## SUMMARY.CSV RELIABILITY

The `summary.csv` numbers **do not match** the raw data for most cities:
- Row counts != inspection counts (raw has one row per violation, not per inspection)
- `in_compliance` is used inconsistently across sources (some cities mark every row False, some don't use it)
- Detroit violations off by 1 (1258 vs 1257)
- ny_state claims 18,978 violations but has zero violation codes
- sf shows 1,908 codes vs summary's 3,915

**The summary.csv should not be trusted without understanding the exact aggregation logic used to produce it.**

---

## ALWAYS-COMPLIANT ESTABLISHMENTS (Wasted Data)

**24,403 establishments (26% of total) have zero violations** — completely useless for lead generation:

| City | Always Compliant | Total | % Wasted |
|------|-----------------|-------|----------|
| detroit | 420 | 500 | **84.0%** |
| ny_state_ag | 11,195 | 14,628 | **76.5%** |
| austin | 2,934 | 4,725 | **62.1%** |
| ny_state | 6,947 | 18,952 | 36.7% |
| sf | 1,393 | 4,285 | 32.5% |
| baton_rouge | 884 | 3,163 | 27.9% |
| cincinnati | 558 | 2,208 | 25.3% |

---

## KEY RECOMMENDATIONS

1. **Deduplicate** the 5,304 duplicate rows
2. **Fix the `True` violation code bug** — 14,282 rows have a boolean stored as a code
3. **Drop or fix broken sources**: pg_county_md (1 row), santa_clara (no identifiers), baton_rouge/sf (no zips)
4. **Fix encoding**: decode HTML entities in Cincinnati, fix mojibake in baton_rouge
5. **Standardize Detroit IDs** to match the `city-number` convention
6. **Add zip codes** to baton_rouge, santa_clara, sf sources (or mark them as non-actionable)
7. **Remove always-compliant rows** from the lead pipeline — 26% of data is wasted
8. **Refresh Detroit data** — current data doesn't reach the last 90 days, yielding zero recent leads
9. **Investigate king_county** — 20K violations but zero cleaning-relevant matches suggests keyword/code mapping gap
10. **Don't trust summary.csv** — regenerate it with documented aggregation logic
