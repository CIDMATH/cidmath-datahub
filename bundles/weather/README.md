# `weather` subject bundle

First source-aligned subject bundle (ADR 0025). Writes to `ecdh_<env>`: `weather_raw` and `weather_processed`. Source: **NOAA nClimGrid-Daily** county + state area-averages (public domain, CONUS-only). Deploys after `_platform` and `_reference` (the processed layer FKs to `geography`/`time` in `ecdh_model_<env>`).

## Tables

| Schema.table | Layer | Update semantics | Source |
|---|---|---|---|
| `weather_raw.noaa_nclimgrid_daily` | raw | `merge_upsert` | NOAA nClimGrid-Daily averages (HTTP) |
| `weather_processed.noaa_nclimgrid_daily` | processed | `merge_upsert` | conformed from raw (NCEI→FIPS geoid, units mm/°C) |

Long-form: one row per `(geo_level, geoid, variable, obs_date)`; variables `prcp/tavg/tmax/tmin`; daily; full history **1951–present**; states + counties (census-tract deferred on volume, ADR 0025).

## Structure (ADR 0011 / 0027)

- `databricks.yml` — bundle definition; includes `../../databricks-common.yml`.
- `resources/nclimgrid_raw_job.yml`, `resources/nclimgrid_processed_job.yml` — deploy-only jobs (trigger from the Databricks UI).
- `src/build_nclimgrid_raw.py` / `src/build_nclimgrid_processed.py` — thin IO + Spark entrypoints.
- Parse + NCEI→FIPS conformance logic lives in `src/cidmath_datahub/weather/nclimgrid.py` (unit-tested, no Spark).

## Run

Both jobs are parameterized by `--start-year`/`--end-year` and run from the Databricks UI ("Run now with different parameters" — pass each flag and value as separate argv tokens, or `--start-year=1951`). Defaults process a recent window (2024–2026).

1. `[weather] build_nclimgrid_raw` — discover + download the monthly CSVs, parse faithfully (NCEI codes preserved), `merge_upsert`.
2. `[weather] build_nclimgrid_processed` — conform NCEI→FIPS `geoid` (FK to `geography.us_county`/`us_state` vintage 2020) + `obs_date`→`time.calendar_date`, attach units, register `_ops` metadata, grant engineer-tier.

For the full 1951–present backfill, run raw in decade chunks then a single processed run over the whole range; both are idempotent via `merge_upsert`.

### Monthly refresh (scheduled)

`[weather] build_nclimgrid_refresh (monthly)` runs the three slices `raw → processed → analysis` in dependency order over a **rolling** recent window — `--recent-years 2`, i.e. `[current_year − 2, current_year]`, computed from the run date (`nclimgrid.resolve_year_window`). It runs **07:00 ET on the 12th** of each month, after NOAA has rescaled the prior month and current-month prelim has landed. Because all three builds are `merge_upsert`, a month that flipped `prelim → scaled` is rewritten in place, new months append, and `weather.daily` reflects both. Failure alerts go to Teams + email (ADR 0010).

This job keeps the **recent window** current; it does **not** do the one-time full-history backfill. For that — or any ad-hoc slice — Run-now the per-slice jobs with explicit `--start-year`/`--end-year` (chunk the backfill by decade). `--recent-years N` and `--start-year`/`--end-year` are mutually exclusive.

## Quirks & gaps (see ADR 0025)

- **CONUS-only** — no AK/HI/territories (`known_limitations` in `_ops.dataset_catalog`).
- **NCEI codes ≠ FIPS** — state codes are NCEI's; conformed via NOAA's published cross-reference. **DC** is filed under Maryland (`18511`) and overridden to its true geoid `11001` in `conform_region`.
- **Fixed modern county set** — nClimGrid applies the 2020-vintage region set across all years (verified via the 1951 probe), so the 2020 geography vintage covers the full history.
- Blocking DQ: NCEI→FIPS coverage + geoid FK; WARN: stale-geoid guard, obs_date→time, value ranges, cell completeness.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
