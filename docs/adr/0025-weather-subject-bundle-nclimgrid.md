# 0025 — Weather subject bundle: NOAA nClimGrid-Daily (county + state)

## Status
Accepted — 2026-05-30

First *source-aligned subject bundle* (ADR 0002/0004), and the first consumer of the geography reference (ADR 0020). Establishes the raw → processed conformance pattern subsequent subjects follow.

## Context
Weather is the first non-reference subject and the reason the international/US geography backbone was built out: temperature and precipitation are covariates for essentially every infectious-disease model (seasonality, vector activity, transmission). It is also intrinsically spatial+temporal, so it exercises conformance to *both* the `geography` and `time` references — a good first proof of the subject pattern.

NOAA NCEI's **nClimGrid-Daily** (v1.0.0, DOI 10.25921/c4gt-r169) publishes daily Tmax/Tmin/Tavg/Prcp for the contiguous US, 1951–present, derived from GHCNd and homogenized. Crucially it ships **pre-computed area averages** keyed to common administrative regions — so the county and state series conform directly to `geography.us_county` / `us_state` with no spatial work on our side, unlike the raw gridded NetCDF (which would force us to re-derive the averaging NOAA already does). It is US-government public-domain data — no DUA, freely redistributable, a simpler access posture than GADM/IPUMS.

Two upstream realities shape the design and were verified before committing. First, **the region identifiers are NCEI codes, not FIPS** (NOAA's own example: North Carolina is state code `31`, whereas its FIPS is `37`); NOAA provides an NCEI↔FIPS cross-reference, and conformance to our FIPS-keyed geography *must* use it rather than assuming FIPS. Second, the product is **CONUS-only** — no Alaska, Hawaii, or territories.

## Decision

### Source and scope (v1)
nClimGrid-Daily **area averages**, region types `cty` (county) and `ste` (state), all four variables (Tmax, Tmin, Tavg, Prcp), **full history 1951–present**. Census-tract (`cen`) averages are deferred to a follow-on slice once county+state are in steady state (tract is ~85k regions × ~27k days × 4 vars ≈ 9B rows — a separate undertaking). The non-conforming NOAA region types (`div` climate divisions, `hc1` HUC, `wfo`, `nca`/`reg`) are out of scope: each would need its own geography reference table first (demand-driven, not now).

### Bundle, catalog, schemas
A new `bundles/weather/` bundle (one bundle per subject, ADR 0004), writing to the **source-aligned** catalog `ecdh_<env>` (not the integrated `ecdh_model_<env>`). Schemas follow ADR 0001/0002: `weather_raw`, `weather_processed`, and `weather` (analysis, bare name). The bundle creates its own schemas (`CREATE SCHEMA IF NOT EXISTS`), mirroring how `_reference` builds create theirs. v1 lands **raw + processed**; the analysis layer (`weather`) is a later slice.

### Layering
- **`weather_raw.noaa_nclimgrid_daily`** — the NCEI CSVs landed faithfully (region_type, region_code, region_name, date, variable, value), minimal transformation, NCEI codes preserved as published. Update semantics `merge_upsert` keyed on (region_type, region_code, date, variable): nClimGrid revises preliminary values for ~recent weeks, and republishes monthly files, so re-ingesting a month upserts rather than duplicates.
- **`weather_processed.noaa_nclimgrid_daily`** — typed, long-form, **conformed**: NCEI region codes translated to FIPS via NOAA's cross-reference, then carrying the geography FK (`geoid` → `geography.us_county`/`us_state`) and the time FK (`obs_date` → `time.calendar_date`). Epi-week is **not** stored on the table — it is derivable by joining `obs_date` to `time.calendar_date`, which already carries `epi_week_id`/`epi_year`/`epi_week`, so storing it here would be redundant denormalization. Pure conformance/standardization logic (code translation, validation, row assembly) lives in `src/cidmath_datahub/` and is unit-tested (ADR 0011); the bundle entrypoint stays thin.

### Conformance (the crux)
The processed layer translates NCEI state/county codes to FIPS using NOAA's published cross-reference (pulled and verified at build, not hard-coded from priors). County weather conforms to the `us_county` **2020 vintage** (nClimGrid uses a current/fixed county set; the exact vintage match is verified at build, and a DQ check asserts every weather geoid resolves to a `geography` row). This makes weather joinable to the same canonical spatial units as every other subject.

### Registration helper extension
Weather is the third metadata shape: spatial **and** temporal **and** sourced. `common.registration.DatasetCatalogEntry` (geography-shaped today) gains optional `temporal_coverage_start` / `temporal_coverage_end` / `temporal_resolution` fields, and `register_dataset`'s catalog MERGE adds those three columns. Geography/crosswalk callers pass the defaults (`None`) and are unaffected; weather populates them. This is the deliberate, minimal generalization deferred in ADR 0023 — extend for the shape that actually appeared, not speculatively.

### Access, deploy, secrets
Public-domain: `access_tier = open`, `dua_required = false`, NOAA citation recorded. No secret scope (public HTTPS bulk download — no API key, unlike IPUMS). A new `deploy-weather` GitHub Action mirrors `deploy-reference`; the bundle deploys after `_platform` and `_reference` (it FKs to `geography`/`time`).

## Alternatives considered
- **Raw gridded NetCDF instead of area averages.** Rejected: we'd re-derive the county/state averaging NOAA already publishes and couple to `geography.boundary` for no fidelity gain.
- **GHCN-Daily (station-based).** Rejected: not area-conformed (needs station→county mapping); nClimGrid is the homogenized, area-averaged form *derived from* GHCNd.
- **ERA5 / Daymet / PRISM.** Rejected for v1: global/gridded reanalysis or non-NOAA, not county-conformed, heavier. ERA5 remains the candidate if non-CONUS (AK/HI) coverage becomes a requirement.
- **Tract in v1.** Deferred on volume (~9B rows); county+state are the surveillance grains and tractable at full history.
- **Assume region codes are FIPS.** Rejected — they are NCEI codes; conformance uses NOAA's cross-reference.

## Consequences
- **First subject conforms end-to-end** to `geography` (us_county/us_state) and `time`, proving the FK/conformance model the whole reference layer exists to serve.
- **CONUS-only is a documented gap** — no AK/HI/territories from this source; recorded in `_ops` and surfaced in `known_limitations`. Non-CONUS weather is a future source (likely ERA5).
- **The NCEI↔FIPS translation is the main correctness risk** — it gets explicit DQ (every weather code resolves to a `geography` geoid; blocking) and the cross-reference is verified at build.
- **`register_dataset` gains temporal fields** — a small, real generalization; the full pipeline-pattern ADR can still wait for more subjects.
- **Volume is real but bounded** — county full-history ≈ 350M rows; chunked writes (the slice-2a pattern) apply. Tract, when added, is ~9B and gets its own treatment.
- **New deploy action + a source-aligned bundle** establish the template the next subjects (wastewater, etc.) reuse.
- **Update semantics differ from reference** — `merge_upsert` (not `full_refresh`) because of nClimGrid's rolling revisions; a monthly refresh re-ingests recent months.

**Implementation note (2026-05-31, table naming).** The raw/processed tables follow ADR 0006's `<provider>_<dataset>` pattern: `weather_raw.noaa_nclimgrid_daily` and `weather_processed.noaa_nclimgrid_daily` (provider code `noaa`, added to ADR 0006's registry in the same change; `source_provider_code = "noaa"`). An earlier draft shipped the bare `nclimgrid_daily`, which omitted the provider prefix; it was renamed to comply before the processed slice landed.

**Implementation note (2026-05-31, DC conformance quirk).** The county-suffix-equals-FIPS-county assumption holds for all 3107 NCEI counties **except** the District of Columbia: NCEI files DC under Maryland's state grouping (NCEI state `18` → FIPS `24`) with county code `511`, so the naive conformance produced the non-existent geoid `24511`. The blocking geoid-FK DQ caught it on the first dev run. The readme only cross-references *state* codes, not these county-level specials, so this is a data-derived fixup: `conform_region` carries an override `{"18511": "11001"}` (DC's real geoid), verified against the published `region_name` "DC: District of Columbia". New overrides are added only when the blocking FK surfaces an absent geoid and the data's `region_name` identifies the true entity — never guessed. The same FK check is the safety net for any retired/renamed counties a full-history backfill might surface.

**Verification (2026-05-31, 1951 probe).** Before committing to the full backfill, a 1951 raw+processed probe confirmed the fixed-modern-county-set assumption empirically: the distinct `(region_type, region_code)` set in 1951 is *identical* to 2024–2026 (zero codes on either side of the diff), and the 1951 processed run passed all blocking checks — geoid-FK (0 missing against the 2020 vintage), NCEI→FIPS coverage, and stale-geoid, plus the date→time WARN (all 1951 dates resolve in the 1900–2099 time dim). So nClimGrid applies the modern county set across the whole series, the 2020 vintage covers 1951–present, and **no earlier Census vintages are required** for the backfill.

**Implementation note (2026-05-31, stale-key remediation).** The processed write is `merge_upsert` on `(geo_level, geoid, variable, obs_date)` and never deletes — correct for the dominant routine case (re-pulling recent months for prelim→scaled revisions rewrites only changed rows). The downside surfaces only when *conformance logic changes* (e.g. adding the DC override): the previously-produced geoid's rows linger because the key set shifted. The blocking geoid-FK check catches stale *invalid* geoids; a new non-blocking `nclimgrid_processed_stale_geoids` WARN catches stale *valid* ones (which would otherwise double-count silently), recorded before the blocking checks so it survives a later raise. Remediation on a conformance change is a targeted `DELETE FROM <table> WHERE geo_level=… AND geoid='<stale>'` then re-run — far cheaper than dropping and rebuilding the full-history table. We deliberately did **not** switch to window-replace semantics, which would eliminate stale keys but rewrite the entire window on every routine monthly refresh.
