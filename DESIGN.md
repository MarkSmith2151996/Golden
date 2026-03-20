# Golden — Design Document

**Violation-Triggered Restaurant Cleaning Referral Platform**

> An automated platform that scrapes public health inspection data to identify restaurants with recent violations, then connects them with vetted commercial cleaning companies for a referral fee.

---

## Table of Contents

1. [Problem & Opportunity](#1-problem--opportunity)
2. [Value Proposition](#2-value-proposition)
3. [Market Data — Michigan](#3-market-data--michigan)
4. [Unit Economics](#4-unit-economics)
5. [Competitive Landscape](#5-competitive-landscape)
6. [Technical Architecture](#6-technical-architecture)
7. [Phase 1: Data Pipeline (Current Focus)](#7-phase-1-data-pipeline-current-focus)
8. [Phase 2: Outreach System](#8-phase-2-outreach-system)
9. [Phase 3: Cleaning Company Network](#9-phase-3-cleaning-company-network)
10. [Phase 4: Automation & Scaling](#10-phase-4-automation--scaling)
11. [Risks & Mitigations](#11-risks--mitigations)
12. [Legal Considerations](#12-legal-considerations)
13. [Action Items](#13-action-items)
14. [Reference Links](#14-reference-links)
15. [Decision Log](#15-decision-log)

---

## 1. Problem & Opportunity

Two halves of a market exist independently but nobody connects them:

- **Health inspection data aggregators** — public violation records are published by local health departments
- **Commercial restaurant cleaning companies** — businesses that specialize in deep cleans, sanitation, and compliance prep

Restaurants that just failed an inspection have an **urgent, time-sensitive need** for cleaning services. They are warm leads — not cold prospects. No company currently scrapes public health violation data to broker cleaning services to these restaurants in real time.

Golden fills that gap.

---

## 2. Value Proposition

**To Restaurants:**
"We help restaurants pass re-inspections." Positioned as a compliance partner, not as someone calling out failures. Restaurants with recent violations get connected to vetted cleaners who specialize in health code compliance.

**To Cleaning Companies:**
Pre-qualified, warm leads delivered automatically. These are restaurants that need cleaning services *right now*. Cleaning companies pay a referral fee (10-15%) only on completed jobs — zero upfront cost.

---

## 3. Market Data — Michigan

Research conducted February 2026 confirmed strong market fundamentals in metro Detroit:

| Metric | Value |
|--------|-------|
| Food service establishments (metro Detroit) | 8,000 – 9,500 |
| Establishments receiving at least one violation per inspection | ~78% |
| Annual violation events (self-renewing pipeline) | 12,500 – 15,000 |
| Local health departments in Michigan | 45 (fragmented) |

**Key facts:**
- MDARD (Michigan Dept. of Agriculture and Rural Development) does **not** cover restaurants
- Michigan health inspection data is fragmented across 45 local health departments
- Starting data sources: **Detroit Open Data API**, Wayne County, Macomb County

---

## 4. Unit Economics

| Metric | Estimate |
|--------|----------|
| Average deep clean cost | $1,500 – $3,000 |
| Referral fee (10-15%) | $150 – $450 per job |
| Conservative conversion rate | 5-10% of contacted restaurants |
| Annual violation events (metro Detroit) | 12,500 – 15,000 |
| Potential annual leads contacted (after filtering) | 5,000 – 8,000 |
| Conservative monthly revenue (at 5%) | $3,000 – $8,000 |
| Startup cost | Under $5,000 |

---

## 5. Competitive Landscape

This niche is **unoccupied**. Adjacent players exist but none combine violation data scraping with cleaning service brokerage:

| Competitor Type | Examples | Why They Don't Compete |
|----------------|----------|----------------------|
| Health inspection aggregators | Yelp (shows scores) | Doesn't broker services |
| Cleaning lead gen platforms | Thumbtack, HomeAdvisor | Don't use violation data for targeting |
| Restaurant compliance software | Various | Focus on internal checklists, not external service providers |

No startup or service was found that scrapes violation data to trigger cleaning service outreach.

---

## 6. Technical Architecture

Golden is built in four phases. Each phase depends on the one before it.

```
[Phase 1: Data Pipeline]  →  [Phase 2: Outreach]  →  [Phase 3: Cleaning Network]  →  [Phase 4: Scale]
      (scrape & filter)        (contact restaurants)     (onboard cleaners)            (automate & expand)
```

The entire business depends on reliable, fast access to violation data. **Phase 1 is the foundation. Nothing else matters until it works.**

---

## 7. Phase 1: Data Pipeline (Current Focus)

> **This is what we are building first.**

### 7.1 Overview

Build a data collection and processing pipeline that:
1. Pulls health inspection data from public APIs
2. Parses and extracts relevant violation records
3. Filters for violations that indicate a cleaning need
4. Deduplicates to avoid repeat contact for the same violation cycle
5. Outputs a clean, actionable lead list

### 7.2 Step 1 — Detroit Open Data API

Connect to the Detroit Open Data health inspection endpoint and pull recent data.

**Data points to extract per record:**
- Restaurant name
- Address
- Violation type
- Violation date
- Severity level

**Starting source:** Detroit Open Data Portal (https://data.detroitmi.gov/)

### 7.3 Step 2 — Data Cleaning & Filtering

Not all violations indicate a cleaning need. Filter for actionable violations:

| Include (cleaning-relevant) | Exclude (not cleaning-relevant) |
|----------------------------|-------------------------------|
| Sanitation failures | Paperwork violations |
| Pest issues | Signage violations |
| Food safety violations | Licensing/permit issues |
| Equipment cleanliness | Administrative violations |

### 7.4 Step 3 — Deduplication

Prevent contacting the same restaurant multiple times for the same violation cycle:
- Track restaurant + violation date combinations
- Define a cooldown period per establishment
- Maintain a contact history database

### 7.5 Output

A clean, structured lead list containing:
- Restaurant name and contact info
- Violation type(s) and date(s)
- Severity classification
- Whether the restaurant has been contacted before (and when)

---

## 8. Phase 2: Outreach System

*To be built after Phase 1 is validated.*

- Automated email/text outreach triggered by new violations
- Messaging framed as helpful compliance partner:
  > "We noticed your restaurant may be preparing for a re-inspection. We connect restaurants with vetted cleaning professionals who specialize in health code compliance."
- **Never** frame as "we saw you failed" — always position as proactive help
- Include opt-out mechanism for CAN-SPAM and TCPA compliance
- Track response rates by violation type, severity, and messaging variant
- A/B test 2-3 message variants

---

## 9. Phase 3: Cleaning Company Network

*To be built after outreach is generating responses.*

- Vet and onboard 3-5 commercial cleaning companies in metro Detroit
- Identified companies from research: Stay Clean Solutions (Livonia), Corporate Cleaning Group (Livonia)
- Establish referral fee agreements (10-15% of job value)
- Build matching logic: violation type → cleaning specialty
- Track job completion and customer satisfaction for quality control

---

## 10. Phase 4: Automation & Scaling

*To be built after the core loop is proven.*

- Full autonomous pipeline: scraper catches violation → triggers outreach → matches to cleaner → collects fee
- Expand to additional Michigan counties:
  - Oakland County
  - Washtenaw County
  - Kent County (Grand Rapids)
- Add more cleaning company partners as geographic coverage expands
- Build dashboard to monitor: pipeline health, conversion rates, revenue

---

## 11. Risks & Mitigations

### Restaurant Sensitivity
Restaurants that just received violations are often embarrassed and defensive. Expect low conversion rates (5-10%).
- **Mitigation:** Messaging must be carefully crafted and A/B tested. Never reference specific violations in outreach. Position as a general compliance service.

### Data Fragmentation
Michigan's 45 local health departments mean no single API covers everything.
- **Mitigation:** Start with Detroit's Open Data portal (most accessible). Expand county by county. Some counties may require FOIA requests or manual scraping.

### Cleaning Company Quality
Golden's reputation depends on the quality of referred cleaning companies. A bad job reflects on the platform.
- **Mitigation:** Vet partners thoroughly before onboarding. Follow up with restaurants after jobs. Remove underperforming partners quickly.

---

## 12. Legal Considerations

- Health inspection data is **public record** — scraping it is legal
- Commercial outreach based on public data is generally permissible but must comply with:
  - **CAN-SPAM** (email communications)
  - **TCPA** (text messages and phone calls)
- All communications must include opt-out mechanisms
- Consult a lawyer before scaling outreach operations

---

## 13. Action Items

| # | Action Item | Target | Status |
|---|------------|--------|--------|
| 1 | Access Detroit Open Data API — find health inspection endpoint and pull sample data | This week | **Active** |
| 2 | Parse and filter violation data — identify which violation types indicate cleaning needs | This week | Pending |
| 3 | Build scraper that runs on schedule and outputs clean lead list | Week 2 | Pending |
| 4 | Draft outreach messaging — 2-3 variants for A/B testing | Week 2 | Pending |
| 5 | Research and contact 3-5 local cleaning companies for partnerships | Week 3 | Pending |
| 6 | Build outreach automation (email triggered by new violations) | Week 3-4 | Pending |
| 7 | Launch pilot — run system on Detroit data, track conversion | Week 4-5 | Pending |
| 8 | Iterate based on conversion data — refine messaging, expand cleaning partners | Ongoing | Pending |
| 9 | Expand to Wayne County and Macomb County data sources | Month 2-3 | Pending |
| 10 | Document everything as a case study | Ongoing | Pending |

---

## 14. Reference Links

- Detroit Open Data Portal: https://data.detroitmi.gov/
- Original ideation chat: https://claude.ai/chat/49abffc3-d1d8-413b-b76f-6e264650f05d
- Follow-up research chat: https://claude.ai/chat/cb9a49fc-8422-46e8-ad28-e2a64e6ac5d4

---

## 15. Decision Log

Tracks all key decisions made during development discussions.

| # | Decision | Date |
|---|----------|------|
| 1 | Project name is **Golden** (not CleanConnect — placeholder removed) | 2026-03-19 |
| 2 | Multi-decade strategic arc (Phases 0-6) removed — out of scope for this project | 2026-03-19 |
| 3 | "Strategic Value / AI-BPO training ground" framing removed — Golden stands on its own | 2026-03-19 |
| 4 | Starting with Phase 1 (Data Pipeline) — nothing else until this works | 2026-03-19 |
