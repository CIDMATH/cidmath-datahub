# D1 — Data profiling & DQ-as-outcome — findings

**Date:** 2026-06-16 · **Scope:** reference / `codes` / `geography` / `time` + `weather_processed`
(dev: `ecdh_model_dev`, `ecdh_dev`). **Method:** read-only, aggregate-only SQL
(`docs reviews / d1-profiling*.sql`); no row-level egress. Part of the SDE review plan
(`sde-review-plan.md`).

## Verdict
The data is **clean and internally consistent**. Every single-table and referential invariant
held; the only material issue is an architectural one (vintage-ambiguous conformance) that the
data made concrete. Below, graded.

## What's solid
- **PK uniqueness:** 0 duplicate keys across all `codes` tables and `geography` `(geoid, vintage)`.
- **Key nulls:** 0 nulls in declared-NOT-NULL key columns everywhere — despite the engine not
  enforcing NOT NULL (see SHOULD-FIX 1), nothing bad has reached the tables.
- **Referential integrity:** 0 orphans on geography parent FKs (county→state, tract→county/state),
  `codes` self-refs (`loinc_map_to`→`loinc`, ICD `parent`→self), and weather→geography /
  weather→`time` conformance.
- **Cardinalities match reality:** tracts 73,669 (2010) → 85,060 (2020); ~3,221 counties / 52
  states per vintage (incl. territories); `time.calendar_date` = 73,049 rows spanning exactly
  1900-01-01…2099-12-31.
- **Weather measures physically plausible:** precip ≥ 0 (0–344 mm), temps within CONUS extremes
  (tmin −48 °C … tmax +49 °C); **no sentinel leakage** (no `-9999`); balanced panel (all four
  variables present for every geoid-day).

## Findings

### MUST-FIX — weather→geography conformance is vintage-ambiguous (S6 headline)
`geography` is keyed `(geoid, vintage)` with 2010 and 2020 both loaded. `weather_processed`
conforms by `geoid` only (no vintage column). Profiling (C1b):

- **3,106 of 3,107** weather county geoids exist in **both** vintages; **48 of 48** state geoids
  likewise. (The single county that matches one vintage is a 2010↔2020 recode.)

So a naive `weather.geoid = geography.geoid` join **fans out 2×** — each of ~343M weather rows
matches both the 2010 and 2020 boundary row. Latent today (the analysis layer / `transforms/` is
empty), but it is the conformance pattern **every future fact source inherits**. The FK
`fact.geoid → geography.geoid` is under-specified: the target key is `(geoid, vintage)`, so the
FK must be too.

*Pre-mortem:* an analyst correlates case counts with county temperature, omits a vintage filter,
silently doubles every weather row, and reports an exposure-response built on duplicated
denominators. Nothing errors; the number is just wrong.

→ **Decision drafted:** ADR 0035 (fact declares the geography vintage it's coded to; canonical
join `(geoid, geo_vintage)`). See the 0035 draft.

### SHOULD-FIX
1. **No engine-level `NOT NULL` enforcement.** Every column is `is_nullable = YES`, including
   PKs, even though the build schemas declare `nullable=False`. NOT NULL is guarded only by the
   build-time DQ checks, not Delta. Currently harmless (0 nulls observed), so defense-in-depth:
   add Delta `NOT NULL` constraints on true-not-null columns so a future bad load fails at write.
2. **Weather has a revise-in-place axis.** `status` = `scaled` (347.6M) vs `prelim` (378,600).
   NOAA revises preliminary values to scaled in later releases, so reloading a date flips
   `prelim`→`scaled`. Confirm the weather reload semantics handle that (overwrite-by-date is
   fine; append would strand stale `prelim`). This is the ADR 0032 / vintage-vs-SCD2 question
   applied to weather.

### CONSIDER (low)
- **Cross-variable consistency unchecked:** marginal min/max are fine, but a row with
  `tmin > tmax` would be a silent error no current check catches. Cheap targeted check worth
  adding.
- **`weather.value` ~0.09% nulls** (78,875 per variable, identical across all four → same
  geoid-days missing everything) — almost certainly legitimate source gaps; confirm vs artifact.
- **`codes` tables are all single-vintage** (vintages = 1), so the `vintage_snapshot` retention /
  immutability paths are unexercised in situ for `codes` (geography *does* have 2 vintages).
- **`icd9cm` latest edition = 2012** (ICD-9-CM's final was ~FY2014) — confirm intended.
- **Weather is CONUS-only** (3,107 counties / 48 states vs geography's 3,221 / 52) — expected for
  nClimGrid; document the coverage limit in `known_limitations`.
- **Naming nit:** `time.calendar_date.date` uses a SQL reserved word (needs backticking) → S2.

## DQ-as-outcome verdict
The checks are solid for single-table and referential invariants. The gaps are **cross-table**
(vintage-ambiguous conformance), **cross-variable** (`tmin ≤ tavg ≤ tmax`), and
**temporal-revision** (`prelim`→`scaled`) semantics — the classes a per-table check framework
tends to miss.

## Freshness snapshot (B1)
cvx 289 (2026-06-15) · icd10cm 98,186 (FY2026) · icd9cm 13,040 (FY2012) · loinc 109,325 (v2.82) ·
loinc_map_to 4,657 (v2.82) · ndc_product 114,460 / ndc_package 215,114 (2026-06-15) ·
weather county rows 342,702,100 (1951–2026).
