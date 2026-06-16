# 0035 — Fact-to-geography vintage conformance: facts declare the boundary vintage they're coded to

## Status
Proposed. Extends ADR 0023 (conformance / FK standardization); relates to 0020 (geography
vintaging), 0028 (enriched views), 0034 (vintage model + `live` / `is_latest_vintage`). Triggered
by D1 profiling — see `docs/reviews/d1-data-profiling-findings.md`.

## Context
`geography` is multi-vintage: every entity table is keyed `(geoid, vintage)`, with 2010 and 2020
currently loaded. Fact / source tables conform to geography by `geoid` but carry **no vintage**.
D1 profiling made the consequence concrete on the one real conformed source we have:

- **3,106 of 3,107** weather county geoids exist in **both** the 2010 and 2020 geography vintages
  (and **48 of 48** states).

So a naive `weather.geoid = geography.geoid` join **fans out 2×** — each of ~343M weather rows
matches both boundary vintages. It is latent only because the analysis layer (`transforms/`) is
empty, but it is the conformance pattern **every future fact source inherits**. The root cause is
that the conformance FK `fact.geoid → geography.geoid` is under-specified: the target key is
`(geoid, vintage)`, so the FK must carry a vintage too.

Resolving the vintage from the observation date is wrong here: NOAA re-aggregates nClimGrid's full
history (1951–present) onto a **single current boundary basis**, so a 1960 observation already
sits on modern boundaries.

## Decision
1. **Every geography-conformed fact carries the geography `vintage` its geoids are coded to** — a
   `geo_vintage` column (or `vintage` where unambiguous). The canonical conformance FK is
   `(geoid, geo_vintage) → geography.<level>.(geoid, vintage)`. This makes the join unambiguous
   and the fact honest about which boundary epoch it used.
2. **The vintage is set at conform time from the source's boundary basis, not the observation
   date.** For nClimGrid the basis is **2020** (determined below): `build_nclimgrid_processed`
   stamps `geo_vintage = 2020`. If a source changes basis across releases, stamp per release.
3. **Date-derived vintage resolution is rejected as the default** — correct only for a source that
   genuinely codes era-appropriate boundaries (a per-source determination, not a global rule).
   Most modern gridded products (incl. nClimGrid) re-aggregate onto a single current basis.
4. **"I just want current geography" stays a separate convenience**, served by ADR 0034's
   `live` / `is_latest_vintage` idiom on the geography side — not by leaving the fact un-vintaged.
   A conformance view may encapsulate the `(geoid, geo_vintage)` join for ergonomics.
5. **Referential integrity becomes a DQ check** (extends 0009 / 0029): a conformed fact's
   `(geoid, geo_vintage)` must resolve in the target geography level — a geoid resolving in zero
   or in the wrong vintage fails.

## Basis determination (nClimGrid, D1 2026-06-16)
Of the eight county geoids that differ between the loaded 2010 and 2020 vintages, weather contains
**exactly one — `46102` Oglala Lakota, SD** (the post-2015 successor to `46113` Shannon) — and
**none** of the 2010-only codes (`46113`; `51515` Bedford City VA, merged 2013). Weather's
Connecticut geoids are the **eight traditional counties** (present in both vintages), not the 2022
planning regions. So NOAA's county basis is **post-2015 / pre-2022 FIPS, matching the loaded 2020
vintage**. (The Alaska recodes are absent only because nClimGrid is CONUS-only — not a basis
signal.) → `geo_vintage = 2020`. Revisit if a 2022+ geography vintage is loaded **and** NOAA
adopts the CT planning regions.

## Alternatives considered
- **Pin all joins to the latest geography vintage; drop the fact-side vintage.** Rejected as the
  *contract*: assumes every source uses the newest loaded boundaries (not guaranteed), silently
  re-labels facts, and breaks when a newer vintage lands. Retained only as a convenience
  (decision 4).
- **Derive vintage from the observation date.** Rejected as default (decision 3) — wrong for
  re-aggregated-history sources like nClimGrid.
- **Leave the join un-vintaged and dedupe downstream.** Rejected: pushes correctness onto every
  consumer, and the 343M→686M fan-out is a real performance cost. This is the
  correctness-by-convention trap D1 caught.

## Consequences
- `weather_processed.noaa_nclimgrid_daily` gains a `geo_vintage` column (schema add + backfill =
  2020); `build_nclimgrid_processed` sets it at conform time.
- The conformance FK contract becomes `(geoid, geo_vintage)` for all future fact sources —
  documented in the conformance guidance (ADR 0023) and enforced by a DQ check.
- A reusable conformance helper / view can own the `(geoid, vintage)` join (ties to the
  shared-builder work and the `live` idiom).
- Modest migration: weather is the only conformed fact today, so the blast radius is one table +
  its build — doing it now sets the pattern before fact sources multiply.
- Coverage note (from D1): nClimGrid is CONUS-only (3,107 counties / 48 states), so AK/HI/
  territories are out of scope for weather regardless of vintage — record in `known_limitations`.
