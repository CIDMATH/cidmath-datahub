# `_reference` bundle

Canonical reference data for the CIDMATH Data Hub (ADR 0014). Writes to the integrated catalog (`ecdh_model_<env>`). Deploys after `_platform`, before subject bundles.

## What it owns

Reference tables — canonical, authoritative, no analytical content. Source-aligned tables join against these to standardize geographic and temporal columns.

Currently implemented:

| Schema.table | Class | Update semantics | Source |
|---|---|---|---|
| `time.calendar_date` | computational | `full_refresh` | deterministic generation |
| `time.epi_week` | computational | `full_refresh` | deterministic generation (MMWR rules) |
| `codes.icd10cm` | authoritative slow-changing | `full_refresh` (per edition) | CDC/NCHS ICD-10-CM order file |

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

## Hierarchical-filter views (ADR 0028 — retired)

The `_enriched` convenience views (`us_county_enriched`, `us_tract_enriched`) are **retired**: the layered builder denormalizes the parent display labels directly onto the canonical entity tables (ADR 0037 decision 7 / 0040), so `geography.us_county` and `geography.us_tract` already carry `state_name`/`state_stusps`/`state_hhs_region` (and `county_name` on tract). Filter the canonical tables directly, e.g. `WHERE vintage=2020 AND state_stusps='GA'`. `build_geography_views.py`, its job, and `us_enriched_view_definitions` have been removed. ZCTAs carry an approximate primary county (largest land-area overlap) on `geography.us_zcta` plus the full `us_zcta_county_xwalk` (see ADR 0028 for the historical view design).

## ICD-10-CM code system (ADR 0014/0015/0030)

`build_icd10cm.py` (job `build_icd10cm_reference`) builds `codes.icd10cm` — the canonical ICD-10-CM diagnosis code system from the CDC/NCHS order file plus the classification hierarchy from the tabular XML. One row per code per fiscal-year edition; PK `(icd10cm_code, edition_year)`. `is_billable` distinguishes valid leaf codes from category headers. Second adopter of the `run_build` seam (ADR 0027).

| Column | Notes |
|---|---|
| `icd10cm_code` | Canonical dotted form (`U07.1`, `J18.9`, `A00`); PK with `edition_year` |
| `edition_year` | ICD-10-CM fiscal-year edition (effective Oct 1) |
| `description` | Long description (non-null; blocking DQ) |
| `is_billable` | True for valid leaf codes, False for headers |
| `parent_icd10cm_code` | Adjacency edge: nearest ancestor code; null at a category root (ADR 0030) |
| `node_level` | Depth in the tree (`= len(ancestor_codes)`) |
| `ancestor_codes` | `ARRAY<STRING>` root→parent path, e.g. `["S72","S72.0","S72.00","S72.001"]` |
| `chapter_code`, `chapter_name` | ICD chapter, e.g. `19` / "Injury, poisoning…" (from tabular XML) |
| `block_code`, `block_name` | ICD block/section, e.g. `S70-S79` / "Injuries to the hip and thigh" |
| `source_file`, `ingested_at` | Audit columns (ADR 0006) |

Parse/normalize/validate **and hierarchy** logic is single-sourced (and unit-tested) in `src/cidmath_datahub/reference/icd10cm.py` (pure, no Spark). Editions are re-pullable, so the table is vintage-reproducible: each edition is fully replaced in place (no SCD2/snapshot). Blocking DQ: `(icd10cm_code, edition_year)` uniqueness, non-null `description`, code-format validation; WARN: cardinality (~70k+/edition), chapter/block resolution, parent resolution. The job is UI-triggered — load more editions via `--edition-year` (e.g. `2026 2021`; `U07.1` debuted FY2021).

**Hierarchy (ADR 0030).** `parent_icd10cm_code` / `ancestor_codes` / `node_level` and `chapter`/`block` all come from the **tabular XML's** `chapter → section → diag` nesting — the authoritative tree. 7th-character codes (e.g. `S72.001A`) aren't XML nodes, so they fall back to their nearest listed ancestor by prefix; a WARN DQ check (`find_adjacency_mismatches`) cross-validates the XML tree against that prefix rule. `--hierarchy build` (default) downloads the XML; `--hierarchy skip` downloads none, derives adjacency from the code set (prefix rule), and leaves chapter/block null. Subtree queries need no recursion: `WHERE array_contains(ancestor_codes, 'S72') AND is_billable`; chapter rollups are a `GROUP BY chapter_code`.

**Release scope:** each edition is the **annual Oct-1 base** (`.../ICD10CM/{year}/`) with the **mid-year Apr-1 update** (`.../ICD10CM/{year}-update/`, published since FY2025) overlaid where available — the update wins per `icd10cm_code` (`icd10cm.overlay_records`), so an edition reflects the latest within-year release. `--midyear-update`: `auto` (default) overlays when published and skips on 404, `require` fails if missing, `skip` loads base-only. The overlay's effect (codes added/revised per edition) is logged and recorded as an INFO row in `_ops.dq_results`. Deferred: as-of-Oct vs as-of-Apr snapshots (collapsed into one `edition_year`), a closure/bridge table, instructional notes (`codes.icd10_note`), `pathogen.icd10_mapping`, ICD-10-PCS.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
