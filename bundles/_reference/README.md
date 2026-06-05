# `_reference` bundle

Canonical reference data for the CIDMATH Data Hub (ADR 0014). Writes to the integrated catalog (`ecdh_model_<env>`). Deploys after `_platform`, before subject bundles.

## What it owns

Reference tables — canonical, authoritative, no analytical content. Source-aligned tables join against these to standardize geographic and temporal columns.

Currently implemented:

| Schema.table | Class | Update semantics | Source |
|---|---|---|---|
| `time.calendar_date` | computational | `full_refresh` | deterministic generation |
| `time.epi_week` | computational | `full_refresh` | deterministic generation (MMWR rules) |

Planned (not yet built):

| Schema.table | Class | Source |
|---|---|---|
| `geography.us_state`, `geography.us_county`, `geography.us_tract`, `geography.us_zcta`, `geography.us_hhs_region` | authoritative slow-changing | IPUMS NHGIS (ADR 0020) |
| `geography.us_crosswalk` | authoritative slow-changing | IPUMS NHGIS bg-sourced 2010↔2020 (ADR 0021) |
| `geography.country`, `geography.country_subdivision`, `geography.subnational` | authoritative slow-changing | ISO 3166-1/2 + GADM (ADR 0022) |
| `geography.boundary` | companion polygons (WKB), all levels | NHGIS + GADM |
| `codes.loinc`, `codes.cvx`, ... | authoritative slow-changing | code-system authorities |
| `pathogen.taxonomy` | static structural | curated/ICTV |

## Deploy

```powershell
cd bundles/_reference
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev build_time_reference
```

In CI, `deploy-reference.yml` (added when this bundle is wired into the deploy matrix) handles dev/prod deploys. Deploy order is enforced: `_platform` → `_reference` → subject bundles.

## Structure

- `databricks.yml` — bundle definition; includes `../../databricks-common.yml`
- `resources/time_job.yml` — the `build_time_reference` job
- `src/build_time.py` — thin entrypoint: calls `cidmath_datahub.reference.time`, writes tables, applies grants, registers `_ops` metadata
- The actual generation logic lives in `src/cidmath_datahub/reference/time.py` (unit-tested, no Spark dependency)

## The MMWR epi-week implementation

`time.epi_week` and the `epi_*` columns on `time.calendar_date` use CDC MMWR week rules (Sunday-start weeks; week 1 is the week with ≥4 days in the new year), computed with the `epiweeks` package (a runtime dependency in `pyproject.toml`). Behavior is pinned against known CDC values in `tests/unit/reference/test_time.py` (e.g., 2020 has 53 weeks; 2023-01-01 is 2023W01).

The default coverage is **1900 through 2099** (calendar dates and epi-weeks) — `calendar_date` runs 1900-01-01 to 2099-12-31. That's ~73k calendar rows, trivial for Delta, and wide enough for any historical or forward-looking analysis. The range is a job parameter, adjustable without code changes. Both tables are written sorted ascending (by `date` / `start_date`).

## Hierarchical-filter views (ADR 0028)

`build_geography_views.py` (job `build_geography_views_reference`) creates convenience views that denormalize stable parent display attributes onto child levels, so analysts can filter by the human-readable parent without hierarchy joins:

| View | Adds | Example |
|---|---|---|
| `geography.us_county_enriched` | `state_name`, `state_stusps`, `state_hhs_region` | `WHERE vintage=2020 AND state_stusps='GA'` |
| `geography.us_tract_enriched` | `county_name` + state name/USPS/HHS region | `WHERE vintage=2020 AND county_name='Fulton County'` |

Views (not denormalized base columns) — the canonical entity tables stay normalized; joins are vintage-keyed and INNER, with a blocking rowcount-parity DQ check. Deploy order: after `build_geography`. First entrypoint on the `run_build` seam (ADR 0027). ZCTAs are excluded (no single nesting parent). Code-based filtering on the base tables (`state_geoid`/`county_geoid`) is unchanged and needs no view.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
