# Independent Audit: Golden Repository

**Repo**: https://github.com/MarkSmith2151996/Golden
**Audited**: 2026-03-30
**Audited by**: Separate Claude Code instance (independent from the development session)

---

## What Golden Does

Golden is a violation-triggered restaurant cleaning lead-generation platform:

1. **Scrapes** public health inspection data from 16 US cities/counties (Socrata SODA APIs + Detroit Gatsby)
2. **Filters** for cleaning-relevant violations (sanitation, pests, equipment cleanliness)
3. **Enriches** leads by scraping DuckDuckGo + restaurant websites for contact info (emails, phones, websites)
4. **Drafts outreach** emails via Claude CLI to pitch cleaning services to violated restaurants
5. **Business model**: Refer restaurants to cleaning companies for a 10-15% referral fee

Current dataset: 93,862 establishments, 318,726 violations across 16 cities.

---

## CRITICAL Issues (Must Fix Before Any Deployment)

### 1. No CAN-SPAM / TCPA Compliance
- `drafter.py` generates outreach emails with zero opt-out mechanism, no physical mailing address, and no unsubscribe link.
- Sending these emails as-is violates federal law (CAN-SPAM Act, TCPA).
- **Fix**: Add unsubscribe links, physical address, and opt-out handling to all generated emails before Phase 2.

### 2. No Consent Mechanism for Contacting Restaurants
- Restaurants never opt in to being contacted.
- Scraping their emails from websites and cold-emailing them about health violations is legally and ethically risky.
- DESIGN.md acknowledges this concern but nothing is implemented.
- **Fix**: Implement opt-in consent flow or get legal sign-off on cold outreach model. At minimum, implement robust opt-out tracking.

### 3. PII Stored Unencrypted
- SQLite database (`data/golden.db`) holds names, addresses, emails, phones, violation histories — all plaintext.
- No access controls, no encryption at rest, no audit logging of who accesses the data.
- **Fix**: Encrypt the database at rest (SQLCipher or encrypted volume). Add audit logging for data access. Implement role-based access control.

### 4. Zero Tests
- `tests/__init__.py` is empty. There are no unit tests, integration tests, or validation tests.
- 16 different city parsers with different date formats, field mappings, and edge cases — none tested.
- Filter logic (cleaning-relevant detection, severity scoring) is untested.
- Email/phone regex extraction is untested.
- **Fix**: Add a pytest suite covering all 16 city parsers, filter logic, regex patterns, and database loading.

---

## HIGH Priority Issues

### 5. DuckDuckGo Scraping Will Get IP-Banned
- `enrichment.py` uses the `ddgs` library with a fake Chrome User-Agent.
- No robots.txt respect. Only 3s delay between restaurants but 2 queries per restaurant with no inter-query delay.
- **Fix**: Add exponential backoff on 429/403. Add delay between all queries. Consider respecting robots.txt.

### 6. No Dependency Version Pinning
- `requirements.txt` uses `>=` not `==` for all packages.
- Supply chain risk: a compromised future version of any dependency gets auto-installed.
- Reproducibility problem: builds are not deterministic.
- **Fix**: Pin exact versions (`==`) or generate a lock file.

### 7. Broad Exception Swallowing in Pipeline
- `pipeline.py` catches bare `Exception` and silently skips entire cities:
  ```python
  except Exception:
      logger.exception(f"Failed to process {city_name} -- skipping")
  ```
- Parsing bugs, schema changes, or API failures could go unnoticed for months.
- **Fix**: Catch specific exceptions. Add alerting/metrics for city-level failures.

### 8. No Data Retention Policy
- Collects 365+ days of violation history. No deletion schedule. No data minimization.
- CCPA/GDPR exposure if any California or EU restaurant owners are in the dataset.
- **Fix**: Define and implement a retention policy. Aggregate or delete old records.

### 9. SoQL String Interpolation (Injection Risk)
- `base.py` SocrataFetcher builds queries via f-strings:
  ```python
  where=f"inspection_date > '{since.isoformat()}'"
  ```
- Currently safe because input is a `datetime.date` object, but the pattern is fragile and invites injection if the code is extended to accept user input.
- **Fix**: Use parameterized queries or at minimum add input validation/sanitization.

---

## MEDIUM Priority Issues

### 10. Detroit URL Not Validated
- `detroit.py` formats `est_id` directly into URLs without sanitization. Low risk since `est_id` comes from the index page, but no validation exists.

### 11. Email Regex Over-Matches
- `enrichment.py` EMAIL_PATTERN is permissive and could capture junk or personal addresses instead of business contacts.

### 12. No Caching / Delta Sync
- Every pipeline run re-fetches the entire dataset from all 16 sources. No way to fetch only new violations since the last run. Wasteful and slow.

### 13. Date Parsing Duplicated 16 Times
- Every source file has its own `_parse_date()` function with slightly different format handling. Should be centralized in a utility.

### 14. SQLite Concurrency Risk
- SQLite doesn't support concurrent writes. Running the pipeline and GUI simultaneously could corrupt the database.

### 15. No Configuration Management
- Dataset IDs, API endpoints, rate limits, scoring weights are all hardcoded across source files. Should be in a config file or environment variables.

---

## Data Inventory (What the Program Produces & Stores)

### SQLite Database: `data/golden.db`

| Table | Fields | Records |
|-------|--------|---------|
| EstablishmentRow | id, city, establishment_id, name, address, zip, owner, type, created_at | 93,862 |
| ViolationRow | id, establishment_id, city, inspection_date, type, compliance, code, description, problem, is_corrected, created_at | 318,726 |
| ContactRow | id, establishment_id, email, phone, website, source, scraped_at | (populated by enrichment) |
| OutreachRow | id, establishment_id, contact_id, channel, status, subject, message_body, sent_at, opened_at, replied_at, created_at | (populated by drafter) |

### CSV Files
- `data/summary.csv` — one row per city with aggregate stats (establishments, inspections, violations, compliance rates, fetch times)
- `data/raw_data.csv.gz` — compressed raw dump of all collected data

### Network Requests Made
- **Socrata SODA API** (14 cities) — GET with SoQL queries
- **Detroit Gatsby** (1 city) — GET for index + detail JSON pages
- **DuckDuckGo** (enrichment) — text search queries via `ddgs` library
- **Restaurant websites** (enrichment) — HTTP GET to scrape contact info
- **Claude API** (drafter) — model inference for email generation

---

## Architecture Summary

### Strengths
- Clean modular design: each city source is isolated, easy to add new sources
- Protocol-based `CitySource` allows Socrata and custom source types
- Pydantic models for data validation
- MCP server integration for Claude Code
- Graceful per-city failure (pipeline continues if one city errors)

### Weaknesses
- No async I/O — all HTTP requests are sequential
- No config layer — everything hardcoded
- No monitoring, alerting, or observability
- No backup strategy for the database
- Database not fully normalized

---

## Recommended Fix Priority

1. **Legal review** of the cold-outreach model before Phase 2
2. **CAN-SPAM/TCPA compliance** in the drafter module
3. **Encrypt the database** at rest
4. **Pin all dependency versions** in requirements.txt
5. **Add pytest suite** for all 16 parsers + filter + enrichment regex
6. **Implement data retention policy** with deletion schedule
7. **Add rate-limiting/backoff** for DuckDuckGo and Detroit scraping
8. **Centralize config** (dataset IDs, rate limits, scoring) into a config file
9. **Replace broad exception catches** with specific error handling + alerting
10. **Add async I/O** for parallel source fetching

---

## File-by-File Reference

| File | Purpose |
|------|---------|
| `golden/__main__.py` | CLI entry point |
| `golden/models.py` | Pydantic data models (Violation, Inspection, Establishment, Lead) |
| `golden/pipeline.py` | Main orchestrator — runs collection across all cities |
| `golden/filter.py` | Cleaning-relevant violation detection + severity scoring |
| `golden/load_data.py` | Loads raw CSV into SQLite |
| `golden/database.py` | SQLAlchemy ORM (4 tables) |
| `golden/enrichment.py` | DuckDuckGo + web scraping for contact info |
| `golden/drafter.py` | Claude CLI wrapper for outreach email generation |
| `golden/gui.py` | Tkinter GUI for manual pipeline runs |
| `golden/mcp_server.py` | MCP server exposing 6 tools for Claude Code |
| `golden/sources/base.py` | CitySource protocol + SocrataFetcher base class |
| `golden/sources/*.py` | 16 city-specific parsers (15 Socrata + 1 Detroit custom) |
