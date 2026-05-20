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
| `geography.state` | authoritative slow-changing | US Census |
| `geography.county` | authoritative slow-changing (SCD2) | US Census |
| `geography.zcta`, `census_tract`, crosswalks | authoritative slow-changing | US Census TIGER/Gazetteer |
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

The default coverage is **1900–2100** (calendar dates and epi-weeks). That's ~73k calendar rows — trivial for Delta — and wide enough for any historical or forward-looking analysis. The range is a job parameter, adjustable without code changes.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
