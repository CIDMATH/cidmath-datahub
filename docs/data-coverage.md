# Data coverage ledger

**Purpose.** The hub is built **pattern-first**: not every grain/vintage/extent a subject will
eventually include is loaded yet. That is deliberate — it gets the development pattern right
before scaling data volume, and lower grains will surface new issues. This ledger exists so those
gaps are **tracked, deliberate deferrals with a documented plan** — never silent omissions. It is
the single index that answers "what don't we have yet, why, and what's the plan to get it?"

**How it relates to the other layers.** ADRs hold *decisions*; `_ops.dataset_catalog.known_limitations`
holds *per-table* caveats; GitHub issues hold *concrete net-new-source* tasks. This ledger is the
*coverage* index across subjects — grain × vintage × extent, loaded vs deferred.

**Convention.** Any PR that defers coverage (a grain, a vintage, an extent, a backfill) adds or
updates a row here in the same PR. Status values: **Loaded** · **Deferred** (intentional, not yet
needed) · **Source-limited** (the upstream source doesn't provide it) · **Planned-next** (committed
for the next cycle).

_Last updated: 2026-06-16._

---

## Geography (`geography`)

| Coverage item | Status | Rationale for deferral | Trigger to load | Plan |
|---|---|---|---|---|
| State, County, Tract, ZCTA — vintages **2010, 2020** | Loaded | — | — | — |
| County/State/Tract/ZCTA — **2022 (CT planning regions)** | Deferred | Not yet needed; CT is the known driver (8 counties → 9 planning regions, effective 2022). nClimGrid weather currently codes to pre-2022 CT. | First analysis needing post-2021 CT coding, or any fact source coded to a 2022+ basis | Load TIGER 2022+ as a **new vintage** (ADR 0034 immutability — never overwrite 2020); add 2020↔2022 crosswalk; revisit ADR 0035 weather `geo_vintage`. |
| **Census block group** (all vintages) | Deferred | High-volume; not yet needed; pattern proven at tract first. | Fine-grained (sub-tract) exposure / denominator analysis | NHGIS block-group boundaries; per-`(level, vintage)` chunked write (ADR 0020 pattern); add BG crosswalks. |
| **Census block** (all vintages) | Deferred | Very high volume (~8–11M blocks/vintage); rarely needed analytically. | Explicit block-level demand | NHGIS block boundaries; careful sizing/partitioning; likely geometry-on-demand only. |
| **CBSA** (metro/micro) — any vintage | Deferred | ADR 0021 sliced delivery (slice 3); not yet delivered. | Metro-area analysis demand | NHGIS CBSA boundaries + crosswalks (ADR 0021 slice 3). |
| **Pre-2010 historic vintages** (2000, 1990, …) per level | Deferred | Not yet needed; NHGIS supports them. | Long-historic geographic analysis | NHGIS historic vintages + inter-vintage crosswalks (new vintages per ADR 0034). |
| **Full-resolution boundary geometry** | Deferred (selective) | Generalized geometry is the default (ADR 0021); full-res is large. | Precision mapping/spatial ops needing it | Load `resolution='full'` selectively into `geography.boundary` per ADR 0021. |
| International (`country` / `country_subdivision` / `subnational`) — non-current vintages | Deferred | Current vintage loaded (ISO/GADM); historic not yet needed. | Historic international analysis | Additional ISO/GADM vintages as new vintages. |

## Weather (`weather_raw` / `weather_processed` — NOAA nClimGrid-Daily)

| Coverage item | Status | Rationale for deferral | Trigger to load | Plan |
|---|---|---|---|---|
| nClimGrid-Daily — **state + county** area-averages, CONUS, 1951–present, vars `prcp/tavg/tmax/tmin` | Loaded | — | — | — |
| **Tract** grain | Deferred | Pattern-first at state/county. | Sub-county weather/exposure analysis | Area-average the nClimGrid grid to tracts (same conform pattern as county); depends on tract geography (have 2010/2020); stamp `geo_vintage` per ADR 0035. |
| **AK / HI / territories** | Source-limited | nClimGrid-Daily is **CONUS-only** (3,107 counties / 48 states observed in D1). | Need for non-CONUS weather | Requires a different source/product (not nClimGrid); record in `known_limitations`. |
| Monthly nClimGrid / additional variables | Deferred | Daily + 4 core variables cover current needs. | Demand for monthly aggregates or extra variables | Extend the nClimGrid ingest. |

## Codes (`codes`)

| Coverage item | Status | Rationale for deferral | Trigger to load | Plan |
|---|---|---|---|---|
| ICD-10-CM — **FY2026** | Loaded | — | — | — |
| ICD-10-CM — **prior fiscal-year editions** | Deferred | Pattern-first with the current edition. | Multi-year / historic diagnosis coding (e.g. claims spanning years) | Re-run `build_icd10cm` across archived FYs (each a new `edition_year` vintage). |
| ICD-9-CM — **FY2012** | Loaded | — | — | — |
| ICD-9-CM — other base editions (incl. final ~FY2014) | Deferred | Legacy/frozen; current load is FY2012 — confirm intended latest. | Historic ICD-9 coding needs | `build_icd9cm` additional editions. |
| CVX / LOINC / NDC — **current snapshot/version only** | Deferred (historic) | `vintage_snapshot` accrues new vintages going forward; historic releases rarely needed. | Historic point-in-time code lookups | Load archived releases as additional vintages (`snapshot_date` / `loinc_version`). |
| Net-new code systems (ICD-10-PCS, RxNorm, SNOMED, HCPCS, CPT, …) | Tracked elsewhere | These are net-new *sources*, not coverage gaps of existing data. | — | GitHub issues (ICD-10-PCS / RxNorm / SNOMED filed; CPT on license hold). |

## Time (`time`)

| Coverage item | Status | Rationale | Trigger | Plan |
|---|---|---|---|---|
| `calendar_date` + `epi_week`, **1900–2099** | Loaded (complete) | Full-history dimension; no deferrals. | — | — |
