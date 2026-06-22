# 0038 — RUCA rural-urban commuting-area codes (`geography.us_ruca_tract` / `us_ruca_zip`)

## Status
Proposed. Extends ADR 0020 (US geography reference) and ADR 0014/0015 (reference-data scope +
naming); reuses ADR 0011 (pure logic modules), 0027 (`run_build`), 0009/0029 (DQ), 0024 (vintage
reproducibility), 0006 (`ingested_at` audit column). Sits in the `geography` schema alongside
`us_state`/`us_county`/`us_tract`/`us_zcta`. Mirrors the build shape proved on `codes.icd10pcs`
(public HTTPS download, pure parser + thin entrypoint, `snapshot_replace` per vintage). Does **not**
adopt the still-proposed ADR 0034 `vintage_snapshot` semantics or ADR 0036 shared builder — both are
unmerged, so this build hand-rolls the skeleton like the current `codes.*` builds and backports when
those land. **(Update 2026-06-22: 0034/0035/0036/0037 have since merged, and 0037 was reworked — see
"Reconciliation" at the end for the realignment.)**

Per **ADR 0037**'s complexity tiers, RUCA is the **simple tier**: a single-step, flat build writing
canonical tables directly to the model catalog (no `_raw`/`_processed` layering). None of 0037's
complexity triggers apply — there is no internal hierarchy a consumer traverses (primary/secondary
are two flat coded attributes, not a rolled-up tree), and the tract and ZIP grains are independent
parallel lookups rather than constituent levels composed into one canonical table. Two flat tables
from one source is the same simple-tier shape as `codes.ndc_product`/`ndc_package` and
`codes.loinc`/`loinc_map_to`, and exactly the flat ICD-10-PCS pattern 0037 cites as validated.
**(Update: the reworked 0037 keeps RUCA "simple" = no processed stage, but places even simple
*sourced* reference on the source-path — raw in `geography_raw`, canonical promoted to model — not
straight to the model catalog. See "Reconciliation".)**

## Context
The USDA Economic Research Service (ERS) Rural-Urban Commuting Area (RUCA) codes are a sub-county
rural/urban classification keyed to census tracts (and, from 2010 on, adapted to ZIP codes). They
fill a gap our county-level geography cannot: counties — especially large western ones — mix rural
and urban territory, so a tract-grain classification is needed to distinguish remote rural
communities inside metro counties. RUCA is widely used to make programs and analyses rural-aware,
which is squarely in scope for CIDMATH's surveillance/population work.

RUCA is two-level. The **primary** code (whole number `1`–`10`, plus `99` for water/zero-population
tracts) encodes the urban core class (metro/micro/small-town/rural) and the largest commuting flow;
the **secondary** code (`1.0`, `1.1`, …, `10.3`, 21 values plus `99`) subdivides it by the
second-largest flow. The two combine flexibly to suit different rural/urban definitions, so both are
stored verbatim and we derive no single rural/urban flag. Vintages exist for 1990, 2000, 2010, and
2020 but are **not comparable across decades** — tract boundaries and the urban-core methodology
change each decade (e.g. the 2020 single-urban-area ≥50% rule replaced the 2010 rule). ZIP files
were first published in 2010; Puerto Rico was added in 2010 and the other territories in 2020. The
2020 product is published as XLSX **and** CSV; 2000/1990 only as legacy binary `.xls`.

## Decision
1. **Two tables in `geography`, one per geographic level — `us_ruca_tract` and `us_ruca_zip`** —
   mirroring how the schema already splits `us_tract` from `us_zcta` rather than fusing levels behind
   a `geography_type` discriminator. The tract table keys on **`geoid`** (the 11-digit census-tract
   GEOID) + **`vintage`**, so it is a clean attribute extension of `us_tract`
   (`JOIN us_tract USING (geoid, vintage)`); it derives `state_geoid`/`county_geoid` from the GEOID
   exactly like `us_tract` (ADR 0020), and carries the source-provided `primary_ruca`,
   `secondary_ruca`, `population`, `land_area_sqmi`, `population_density`, plus `state`/`county`
   labels. The ZIP table keys on **`zip_code`** (5-digit) + `vintage` — a ZIP is **not** a census
   GEOID, so it is named descriptively (not `geoid`) and carries `state`, `zip_code_type`,
   `po_name`, `primary_ruca`, `secondary_ruca`. **ZCTA is the Census Bureau's areal
   approximation of ZIP codes**, so `zip_code` is treated as an **approximate foreign key** to
   `geography.us_zcta.geoid` — both are 5-digit, joined on `(zip_code = geoid, vintage)`. The
   match is intentionally not 1:1 (point / PO-box ZIPs and new ZIPs have no ZCTA; ZCTA
   boundaries lag ZIP changes), so it is for approximate geographic enrichment, not identity. A
   convenience view **`geography.us_ruca_zcta`** materializes this join (inner join → the ZIP
   rows that have a matching ZCTA), so consumers get a ZCTA-keyed, geometry-enriched RUCA without
   hand-writing the bridge. We do **not** rename the key to `zcta` (that would assert a false
   identity) nor add a stored `zcta` FK column (the join is code-equality, computed on read).

2. **Versioned per RUCA vintage, `snapshot_replace` per vintage (ADR 0024).** `vintage` is the
   decennial RUCA year (`1990`/`2000`/`2010`/`2020`), aligning with the `geography` `vintage`
   convention. Each run replaces only the vintage(s) it rebuilt and leaves others intact; vintages
   are re-pullable, so the tables are vintage-reproducible. (When ADR 0034 merges, reclassify to
   `vintage_snapshot` + atomic `replaceWhere` and drop the `_current` views — registration-only.)

3. **Primary + secondary stored verbatim; codes validated against the published sets.**
   `primary_ruca` is an integer validated against `{1..10, 99}`; `secondary_ruca` is a string
   validated against the 21 published values plus `99`, normalized to the canonical dotted form
   (a bare `10` from the ZIP file becomes `10.0`). We never round secondary to a float (`1.0` vs
   `1.1` would collapse).

4. **Pure parser + thin entrypoint (ADR 0011/0027).** `reference/ruca.py` holds the code sets,
   GEOID/ZIP normalization (reusing `reference.geography.normalize_geoid`), code validation, the
   header-alias resolution, the record dataclasses, and pure DQ helpers — no Spark, no IO.
   `build_ruca.py` does the public HTTPS download, reads CSV/XLSX/`.xls` into row-dicts (pandas +
   `xlrd` for legacy `.xls`, in the job env), parses, runs DQ, and writes both tables.

5. **Header-driven, alias-tolerant parsing.** Column headers drift across the four vintages and the
   two geographies (the 2020 ZIP file is `ZIPCode,State,ZIPCodeType,POName,PrimaryRUCA,SecondaryRUCA`;
   tract headers carry the year, e.g. `Primary RUCA Code 2020`). The parser resolves each logical
   field by normalized name match rather than fixed position, so a vintage's naming variation is a
   config detail, not a re-parse.

6. **Out of scope (separate issues):** a `geography.us_ruca_code_definitions` lookup (primary/
   secondary → published description, available in `ruca.py` as constants); any derived rural/urban
   flag; ZIP↔ZCTA or tract↔county crosswalks (those belong with the geography crosswalks, ADR 0021).

## Alternatives considered
- **One table with a `geography_type` discriminator.** Rejected: it diverges from the schema's
  one-table-per-level convention (`us_tract`/`us_zcta`), forces the population/area columns nullable
  for every ZIP row, and mixes a census GEOID key with a non-GEOID ZIP key in one column.
- **Put RUCA in `codes` like the clinical code systems.** Rejected: RUCA is a geographic
  classification keyed to census tracts/ZIPs, so it belongs in `geography` next to the units it
  classifies and joins to (`us_tract`).
- **Collapse primary+secondary into one derived rural/urban flag.** Rejected: flexible combination
  of the two levels is the entire point of the scheme; derivation is a downstream choice.
- **Wait for the ADR 0036 shared builder.** Rejected: it is unmerged; hand-roll now (like the live
  `codes.*` builds) and backport with the 0034/0036 migration that already touches every build.

## Consequences
- Two new reference tables + a `reference/ruca.py` module + `build_ruca.py` + `ruca_job.yml` + unit
  tests; `xlrd` is added to the job environment for the legacy `.xls` vintages.
- `us_ruca_tract` joins cleanly to `us_tract` on `(geoid, vintage)`, giving any tract-coded dataset a
  rural/urban classification by the same key it already uses for geography.
- `us_ruca_zip` joins approximately to `us_zcta` on `(zip_code = geoid, vintage)`; the
  `geography.us_ruca_zcta` view exposes that bridge with ZCTA geometry attached. ZIPs without a
  matching ZCTA (point / PO-box / newer ZIPs) are absent from the view but remain in the base table.
  The view depends on `us_zcta` existing (geography build runs first); the build guards on its
  presence and logs a skip rather than failing if it is absent.
- Cross-decade comparisons are intentionally unsupported: `vintage` is part of the key and no
  cross-vintage referential check assumes a tract persists across decades.
- The 1990 population/land-area errata (ERS, 12/9/2025) is handled by capturing the source URL `?v=`
  version in `source_file`; the RUCA codes themselves were unaffected.
- When ADR 0034/0036 merge, this build is a backport target: reclassify to `vintage_snapshot`, swap
  `DELETE`+`append` for atomic `replaceWhere`, drop the `_current` views, and fold the parser+spec
  into the shared builder.

## Reconciliation with the now-merged ADRs 0034 / 0035 / 0037 (2026-06-22)
This ADR was written while 0034/0036/0037 were unmerged and assumed the *earlier* 0037 framing
("simple tier → built straight into the model catalog"). Those are now filed, and **0037 was
reworked** so placement no longer follows the tier. **The data model and the decisions below stand
unchanged** — this is a conventions + placement realignment, not a teardown. Targets:

1. **`vintage_snapshot` + atomic `replaceWhere`, not `snapshot_replace` + DELETE/append** (ADR 0034).
   `vintage` is the RUCA decennial year; each run atomically replaces only its vintage; vintages are
   immutable. **Drop the `_current` views** (0034 — "current" is `MAX(vintage)` / the `live` idiom).
   (This is the 0034 backport the colleague already anticipated, now actionable.)
2. **Source-path placement, not straight-to-model** (reworked ADR 0037). RUCA is *sourced*, so it
   lands **raw in the source catalog** and promotes its canonical to the model catalog like all
   reference data — even though it is *simple* (no processed stage). Because RUCA augments the
   **geography** subject (its canonicals live in `geography` and join `us_tract`/`us_zcta`), its raw
   lands in **`geography_raw`** (reworked 0037 decision 4: augmenting inputs land in the consuming
   subject's `*_raw`); the canonical `geography.us_ruca_tract` / `us_ruca_zip` are promoted to the
   model catalog. No `_processed` stage (simple). This is the one delta the original ADR could not
   anticipate.
3. **`(geoid, geo_vintage)` conformance** (ADR 0035): RUCA's `vintage` *is* the geography vintage it
   is coded to (decennial tract/ZCTA definitions), so the `(geoid, vintage)` / `(zip_code, vintage)`
   joins to `us_tract` / `us_zcta` are already 0035-conformant — confirm RUCA's `vintage` values
   match the geography `vintage` values exactly (e.g. RUCA `2020` ↔ geography vintage `2020`).
4. **Build on the ADR 0036 shared builder when it lands.** The builder is filed but not yet
   implemented (the geography block-group/block work builds it first). Until then the hand-rolled
   skeleton is acceptable; fold `ruca.py`'s parser + a `ReferenceTableSpec` into the builder in the
   same migration.

**Unchanged and sound** (preserved from the colleague's design): the two per-level tables
(`us_ruca_tract` keyed `(geoid, vintage)`, `us_ruca_zip` keyed `(zip_code, vintage)`), verbatim
primary/secondary codes (no derived flag), the ZIP↔ZCTA approximate-FK + `us_ruca_zcta` view,
alias-tolerant header parsing, code validation, cross-decade non-comparability, and `ingested_at`
(already correct).

**Migration** is placement + registration, not a rewrite: add the `geography_raw` raw layer and the
promote to the model catalog, swap the write to atomic `replaceWhere` / `vintage_snapshot`, and drop
the `_current` views; the builder fold-in follows when the builder exists. Tracked in the backlog.
