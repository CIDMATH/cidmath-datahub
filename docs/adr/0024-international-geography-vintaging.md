# 0024 — International geography vintaging (align with the US snapshot model)

## Status
Accepted — 2026-05-29

Amends the temporal-model sub-decision of [ADR 0022](0022-geography-international-scope.md). The rest of ADR 0022 (source split, FK structure, table set, access posture) stands.

## Context
ADR 0020 made US geography **vintaged**: each entity table is keyed by `(geoid, vintage)` where `vintage` is the TIGER/Line basis year, multiple vintages are loaded (2010, 2020), and NHGIS-published crosswalks translate data across vintages. The reasoning was explicit and load-bearing: census geography changes irregularly (counties merge, tracts and ZCTAs are redrawn each decade), so a single "current" snapshot silently breaks historical joins. Vintage was treated as a first-class correctness concern.

ADR 0022 took the opposite default for the international tables (`country`, `country_subdivision`, `subnational`): single-vintage snapshots — "current ISO + current GADM release" — with `full_refresh` semantics. That choice was lightly justified ("small enough to fit single-vintage"), which speaks to storage, not to historical correctness. There was no reasoned decision to forgo vintaging; it was a default that nobody pushed on.

The asymmetry is a latent trap. `full_refresh` on the international attribute tables **silently mutates history** every time GADM ships a new release or ISO recodes a subdivision: data coded to a `subdivision_code` or `gadm_gid` that later changes breaks with no translation surface and no record that the old unit ever existed — exactly the failure mode ADR 0020 went to lengths to prevent for the US. And it is worst where it hurts most: GADM ADM2/3/4 (the `subnational` table 3c introduces) churn *more* across releases than country/first-subdivision, so the deepest international table is the most exposed. We want international geography to carry the same historical guarantees as the US, and 3c — which adds the most release-volatile table — is the moment to settle it rather than pour a third table into the single-vintage mold.

## Decision

### Vintage the international tables
Add `vintage` to the primary key of the international attribute tables, aligning with ADR 0020:

- `geography.country` → PK `(country_alpha3, vintage)`
- `geography.country_subdivision` → PK `(subdivision_code, vintage)`
- `geography.subnational` → PK `(gadm_gid, vintage)`, built vintaged from the start in 3c

`geography.boundary` is already vintaged (`(geo_level, geoid, vintage, resolution)`) — no change.

### Vintage axis = GADM release year
`vintage` records the **GADM release year** (GADM 4.1 → `2022`, matching what `boundary` already stores). Boundaries are the volatile geometry and GADM releases are the natural snapshot boundary, so the release year is the meaningful, reproducible basis. The pycountry / ISO edition is captured in `source_file` (ADR 0023 P1-7), not as a second vintage axis. ISO code changes that land *between* GADM releases are not separately vintaged — an accepted imperfection: they are rare, partly captured by the `iso_3166_3_predecessor` field, and picked up at the next GADM pull. We do not invent a per-year axis that ISO and GADM don't actually provide.

### Per-vintage write semantics
A run refreshes only the vintage it targets — `DELETE WHERE vintage = <v>` then append — the same per-slice contract the `boundary` table and the ADR 0023 P1-6 fix already use. Loading a new GADM release therefore **adds a new vintage without touching existing ones**; history is preserved by default rather than by remembering to pass every vintage on every run. This is deliberately stricter than ADR 0020's US "overwrite the requested set" behavior. `update_semantics` stays `full_refresh` in the ADR 0007 vocabulary (it is a full refresh *of a vintage*).

### Cross-vintage crosswalks deferred
Unlike the US — where NHGIS publishes population-weighted interpolation crosswalks — there is **no published crosswalk** between GADM releases or ISO editions, and deriving one needs population denominators that live in the future `population` schema (the same constraint that deferred US same-level crosswalks in ADR 0021). We ship vintaged snapshots now; cross-vintage translation, if a concrete need ever arises, is a later ADR (likely an as-of view or a derived crosswalk), not a blocker for shipping vintaged snapshots. In practice international boundaries change slowly enough that most consumers use a single current vintage.

Today there is exactly one international vintage (`2022` / GADM 4.1). The value of this decision is future-proofing: the next release (4.2, …) adds a vintage instead of overwriting one.

## Alternatives considered
- **Keep single-vintage snapshots (status quo, ADR 0022).** Cheapest, but silently destroys historical joinability on every GADM/ISO change — the exact problem ADR 0020 rejected for the US — and leaves one schema with two inconsistent temporal guarantees.
- **Full US parity, crosswalks included, now.** Rejected: no published international crosswalk exists and deriving one needs population data out of scope (ADR 0021 precedent). Don't block the feasible part (vintage snapshots) on the infeasible part (crosswalks).
- **SCD2 validity ranges instead of vintage snapshots.** Same reasoning as ADR 0020 — requires inferring change effective-dates the sources don't supply; deferred to an optional derived view.
- **Vintage by arbitrary pull date rather than GADM release year.** Rejected: the release year is the reproducible basis that matches how the source versions; a pull date adds noise without aligning to the source.

## Consequences
- **Consistent historical guarantees across US and international geography.** A GADM release bump adds a vintage instead of mutating history.
- **Retrofit required.** `country` and `country_subdivision` gain a `vintage` PK column and switch to per-vintage writes; both re-run (the new column evolves in via `mergeSchema`, the same migration pattern as `geoid_system`). 3c builds `subnational` vintaged from the start, so no retrofit there.
- **Joins gain a vintage predicate.** Cross-table joins that need a specific vintage now carry `vintage` in the predicate; single-current-vintage consumers filter to the latest (document this as the default usage). FK column *names* are unchanged.
- **Boundary table unchanged** — already vintaged.
- **Multi-vintage cost is deferred.** One vintage today; storage/query cost of a second only accrues when a second release is actually loaded.
- **Cross-vintage translation remains unsolved** (no crosswalk) — an accepted limitation, flagged for a future ADR if a real need appears.
