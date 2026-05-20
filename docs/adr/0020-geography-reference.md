# 0020 — Geography reference: source, grain, geometry, and access

## Status
Accepted — 2026-05-20

## Context
The `geography` schema in the integrated catalog is the canonical set of US spatial units that source-aligned data conforms to — the spatial counterpart of the `time` reference (ADR 0014). Surveillance and modeling data arrive coded to states, counties, ZCTAs, tracts, and metro/administrative areas; without a conformed geography reference, every subject re-derives names, parent relationships, centroids, and boundaries inconsistently.

Geography is materially harder than time because it changes over time and the changes are irregular: counties split, merge, and recode (Connecticut replaced counties with planning regions in 2022-23; Alaska boroughs shift); tracts and ZCTAs are redrawn each decade; metro-area definitions are revised. A single "current" snapshot silently breaks historical joins, so the reference must carry an explicit notion of *vintage*, and must offer a way to translate data coded to one vintage onto another.

We want six levels — state, county, ZCTA, census tract, CBSA (metro/micro areas), and HHS regions — with boundary geometry included (valuable for choropleth mapping and spatial-adjacency models) but without taxing the far more common attribute-only joins. Population counts are out of scope here; they live in the `population` schema. This ADR records the source, the over-time model, how geometry is stored, the table shapes, and the access posture.

## Decision

### Source: IPUMS NHGIS
We source from **IPUMS NHGIS**, which is purpose-built for historically consistent US census geography: it spans decennial vintages (state/county back to 1790, tracts since 1910 / full US since 1990, ZCTAs since 2000, CBSAs since 2009), publishes **population-weighted geographic crosswalks** between vintages, and assigns a stable `GISJOIN` key across its boundary files. Ingestion uses the IPUMS API (extracts of shapefiles) plus direct downloads for the crosswalk files (NHGIS distributes crosswalks outside the Data Finder). The API key is read at runtime from a Databricks secret scope (`ecdh-<env>-ipums`, key `nhgis_api_key`), referenced via new `ipums_secret_scope` / `ipums_secret_key` bundle variables.

HHS regions are **not** a census geography and are not in NHGIS or TIGER. They are a fixed grouping of states into 10 federal regions, so we build `geography.hhs_region` as a small static lookup plus an `hhs_region` membership column on `geography.state` — derived in code, not ingested.

### Grain: vintaged snapshots + crosswalks
Each entity table is keyed by `(geoid, vintage)` — one row per geography per census/TIGER vintage. This matches how the source publishes geography and how datasets are coded, and it is auditable (1:1 with upstream) rather than requiring us to infer change effective-dates the source doesn't provide. Cross-vintage translation (e.g., joining 2010-coded data to 2020 units) is handled by explicit `geography.crosswalk` tables carrying NHGIS interpolation weights, not by SCD2 validity ranges. If an as-of-date join pattern becomes necessary later, an SCD2-style validity **view** can be derived on top of the snapshots without re-architecting.

**What `vintage` means:** it is the **TIGER/Line basis year** of the boundary definitions — the units as the Census Bureau drew them for that year — not necessarily the survey year of any data later placed on them. NHGIS occasionally ships more than one boundary version for a single year (e.g., 2010 units exist on both a 2010-TIGER and a 2020-TIGER basis); where that occurs we record the TIGER basis as the `vintage` and load each basis as a distinct vintage if both are ever needed. Slice 1 (state + county, 2010 and 2020) has no such ambiguity; we settle the rule here so the finer levels in later slices don't surprise us.

Update semantics: `full_refresh` (ADR 0007) — each run regenerates the requested vintages.

### Tables, naming, and geometry
Reference tables are unsuffixed in the `geography` schema (ADR 0014/0015). Every table carries the two identifiers NHGIS supplies natively: a uniform **`geoid`** — the standard Census GEOID for the level (state 2-digit, county 5, tract 11, ZCTA 5, CBSA 5) — and **`gisjoin`**, NHGIS's stable, zero-padded join key that its boundary files, data tables, and crosswalks all key on (so we need it to use the crosswalks). The uniform `geoid` is the column a consumer reaches for at *any* level; because a GEOID is unique only **within** a level (county, ZCTA, and CBSA GEOIDs are all 5 digits), cross-level union/uniqueness uses `(geo_level, geoid)`. Parent links and downstream geographic foreign keys use the `<level>_geoid` form (`state_geoid`, `county_geoid`), so a fact table joins `county_geoid → geography.county.geoid`. This refines ADR 0006's looser per-level geographic-identifier wording for the integrated geography schema. Centroids are population-weighted where NHGIS provides Centers of Population (state/county/tract), geographic (TIGER interior point) otherwise. Attribute tables stay **lean** so the common case is cheap:

- `geography.state` — PK `(geoid, vintage)`; `geoid` (2-digit), `gisjoin`, `name`, `stusps` (e.g., GA), `hhs_region`, centroid, land/water area.
- `geography.county` — PK `(geoid, vintage)`; `geoid` (5-digit), `state_geoid` (FK), `gisjoin`, `name`, centroid, land/water area.
- `geography.zcta` — PK `(geoid, vintage)`; `geoid` (5-digit), `gisjoin`, centroid, area.
- `geography.tract` — PK `(geoid, vintage)`; `geoid` (11-digit), `county_geoid` + `state_geoid` (FK), `gisjoin`, centroid, area.
- `geography.cbsa` — PK `(geoid, vintage)`; `geoid` (5-digit CBSA code), `gisjoin`, `name`, type (metro/micro), centroid.
- `geography.hhs_region` — PK `hhs_region` (1-10), `name` (static lookup; `geography.state` carries the `hhs_region` membership).

**Storage guardrails:** `geoid` and `gisjoin` are stored as `STRING`, never integer — leading zeros are significant (Alabama is `01`, not `1`). `cbsa_geoid` on `geography.county` is nullable (rural counties belong to no CBSA). Centroid columns record whether they are population-weighted (NHGIS Centers of Population, available for state/county/tract) or geographic.

Boundary polygons live in a **companion table**, `geography.boundary`, keyed by `(geo_level, geoid, vintage, resolution)` with geometry stored as **WKB** (binary, more compact than WKT) plus `gisjoin`. Consumers who need polygons join to it; everyone else never scans it. The default `resolution` is **generalized** (cartographic-boundary detail — far smaller, sufficient for analysis and mapping); **full** resolution is loaded selectively where precision matters. `geography.boundary` and the larger entity tables are Liquid-Clustered by `(geo_level, vintage)` so single-level/single-vintage reads prune hard. Centroids and areas on the lean tables cover most "spatial-lite" needs without touching geometry at all.

### Access and licensing
`geography` is granted to the standard reference reader tier (engineers + `ecdh-analysts`), the same as `time` — at this stage there are no external consumers, so gating it would be ceremony. The NHGIS obligation is instead recorded in `_ops.dataset_catalog`: `license` (IPUMS NHGIS terms of use), `dua_required = true`, the official NHGIS citation, `source_url`, and `external_maintainer` (IPUMS). Redistribution permission is being requested from IPUMS; if external partners are added before it lands, revisit the grant.

### Implementation shape and slices
Pure logic (GEOID parsing/validation, HHS mapping, crosswalk normalization, weight checks) goes in `src/cidmath_datahub/reference/geography.py` (unit-tested, ADR 0011); the bundle entrypoint `bundles/_reference/src/build_geography.py` stays thin. Geometry/IO dependencies (`ipumspy`, `geopandas`, `pyogrio`, `shapely`) are added to the `_reference` job environment, not the core wheel runtime deps, to keep the shared wheel lean. DQ checks (ADR 0009): `(geoid, vintage)` uniqueness, GEOID format, parent-FK referential integrity (county→state, tract→county), and crosswalk weights summing to ≈1.0 per source unit. Delivery is sliced for review at each step:

1. **state + county** (+ HHS regions, trivially) — attributes + companion generalized geometry, vintages 2010 + 2020, plus the county 2010↔2020 crosswalk. Proves the IPUMS API → shapefile → WKB → Delta pattern end-to-end.
2. **ZCTA + tract** — the high-volume levels; add the 2000 vintage where useful.
3. **CBSA**, the remaining crosswalks across all levels, and selective full-resolution geometry.

## Alternatives considered
- **Census public-domain (TIGER/Gazetteer) instead of IPUMS.** Simpler licensing (freely shareable) and no API key, but you assemble historical consistency yourself and lack NHGIS's curated crosswalks. Rejected in favor of NHGIS's historical depth; the licensing cost is accepted and managed via the permission request + metadata.
- **SCD2 validity ranges instead of snapshots.** Elegant for as-of-date joins, but requires inferring change effective-dates the snapshot sources don't supply — fragile and hard to defend. Deferred to an optional derived view.
- **Geometry inline in the attribute tables.** Rejected: large WKB blobs would bloat every scan of the hot attribute path even with column pruning; physical separation keeps the common case cheap and lets geometry load at its own cadence/resolution.
- **Full-resolution geometry by default.** Rejected as the default for volume reasons (tracts ≈74k and ZCTAs ≈33k polygons per vintage); generalized is the default, full-res is opt-in per level/vintage.
- **Gating `geography` behind a registered-IPUMS-users group.** Considered for licensing safety; judged overkill for the current internal, pre-partner stage. Obligation tracked in metadata instead.

## Consequences
- **One conformed geography across all subjects**, vintage-aware, with explicit crosswalks for cross-vintage work — the spatial backbone subject bundles can FK against.
- **Cheap common case, optional heavy geometry.** Attribute joins never pay for polygons; mapping/adjacency users opt into `geography.boundary`.
- **New external dependency and secret.** The build needs the IPUMS API (key in a secret scope) and geospatial libraries in the `_reference` job environment; extracts are asynchronous (submit → poll → download), so the job is longer-running than `time`.
- **A licensing obligation now lives in the platform.** Until IPUMS grants redistribution permission, broadening `geography` beyond the current internal user base (e.g., to external partners) is not authorized; the metadata flags this and the grant posture should be re-checked at that point.
- **`databricks-common.yml` gains two variables** (`ipums_secret_scope`, `ipums_secret_key`) — an approval-gated change to be made when slice 1 lands.
- **HHS regions are a maintained static artifact** — if the federal definition ever changes, it's a code edit, not an ingest.
