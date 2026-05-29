# CLAUDE.md — CIDMATH Data Hub

This file is the persistent context for Claude (or any AI assistant) working on this repo. Read it at the start of every session before generating code or making architectural recommendations.

## Project

CIDMATH Data Hub: a Databricks-based platform for ingesting, transforming, and sharing infectious disease modeling and analytics data, deployed via Databricks Asset Bundles. Serves Emory's CIDMATH center and partners.

## Current phase

Reference data build-out. The `_platform` bundle, the shared Python package, and the `_reference` bundle are live. The geography reference is in dev for US levels (`us_state`/`us_county`/`us_tract`/`us_zcta`/`us_hhs_region` + 2010↔2020 crosswalks), countries (`country`: ISO 3166-1 + GADM ADM0), and first-level subdivisions (`country_subdivision`: ISO 3166-2 + GADM ADM1, with multi-tier code→name→fixup matching per ADR 0023). Next: GADM subnational (ADR 0022 slice 3c), then the first subject bundle.

## Conventions

### Catalogs and schemas

- **Source-aligned catalog:** `ecdh_<env>` (`ecdh_dev`, `ecdh_prod`).
- **Integrated/modeled catalog:** `ecdh_model_<env>` (`ecdh_model_dev`, `ecdh_model_prod`).
- **Source-aligned schemas:** `<subject>_raw`, `<subject>_processed`, `<subject>` (the bare subject name is the analysis layer; do not suffix with `_analysis`).
- **Integrated catalog schemas:** named for concepts — `geography`, `time`, `pathogen`, `codes`, `surveillance`, `healthcare`, `population`, `movement`, `environment`, `one_health`, `response`, `global`.
- **Operational metadata schema:** `_ops` in each catalog (leading underscore = internal).
- **Discovery schema:** `discovery` in each catalog — reader-facing curated views over `_ops` (e.g., `discovery.datasets`). No leading underscore; analysts/readers can query it without `_ops` access via UC view ownership chaining. (ADR 0019)
- Schemas in the source catalog represent **subjects**, not sources. Multiple sources for the same subject live as separate tables inside `<subject>_raw` and `<subject>_processed`. (ADR 0002)

### Table naming

- **Source-aligned tables:** singular, snake_case. Raw and processed table names = source identifier (`cdc_nwss`, `ga_dph_sample`). Analysis-layer table names = entity or concept (`sample_concentration`, `daily_aggregate`). Never repeat the subject in the table name. (ADR 0006)
- **Integrated reference tables (from `_reference`):** no Kimball suffix. Country-specific reference tables carry a `<country>_` prefix using ISO 3166-1 alpha-2 lower-cased (`geography.us_county`, `geography.us_state`); global tables stay unprefixed (`geography.country`, `geography.country_subdivision`, `time.epi_week`, `codes.loinc`). (ADR 0006, ADR 0014, ADR 0015, ADR 0022)
- **Integrated analytical content (from non-`_reference` bundles):** Kimball suffixes — `_fact`, `_dim`, `_bridge`. `surveillance.case_report_fact`, `population.cohort_dim`, `surveillance.case_to_lab_bridge`. (ADR 0015)
- **Source-specific facts:** `<provider>_<concept>_fact` (e.g., `surveillance.epic_cosmos_vaccine_coverage_fact`). Conformed facts drop the prefix: `surveillance.vaccine_coverage_fact`. (ADR 0015)
- **No version suffixes** in table names. Schema evolution via Delta; rename only on incompatible methodology break with an ADR.
- **Leading underscore** reserved for internal/operational tables.

### Column naming

- snake_case throughout.
- **Primary keys:** `<entity>_id` (`facility_id`, `sample_id`). For reference tables, use the canonical identifier (`county_fips`, `epi_week_id`, `loinc_code`).
- **Foreign keys:** same name as the referenced PK.
- **Timestamps:** `<event>_at` (UTC). **Dates:** `<event>_date`. Avoid bare `date`/`time`.
- **Booleans:** `is_<state>` or `has_<thing>`.
- **Geographic identifiers:** canonical codes — `fips_state`, `fips_county`, `zcta`, `census_tract`.
- **Measurements:** include unit where relevant — `concentration_copies_per_ml`.
- **Audit columns** on materialized tables (recommended, not enforced): `ingested_at`, `processed_at`, `source_file`, `pipeline_run_id`.

### Bundles

- `_platform` for shared infrastructure: catalogs, schemas, grants, `_ops` tables, secret scopes. Owns no data movement. (ADR 0004)
- `_reference` for canonical reference data: geography, time, code systems, pathogen taxonomy. The legitimate exception to "no data movement in platform-like bundles." (ADR 0014)
- `<subject>` for each subject area (one bundle per subject). One bundle per subject; do not fragment by source.
- Deploy order: `_platform` → `_reference` → subject bundles.

### Language and style

- Python 3.11+ for all transformation logic. Jobs use Databricks serverless environment v5 (Python 3.12.3); local dev should match within `>=3.11`.
- PySpark for distributed transforms; pandas only for small in-memory work.
- SQL via `spark.sql()` for clear set operations; complex logic in PySpark.
- R only in dedicated R-Shiny notebooks; do not mix R into pipelines.
- `ruff` for lint/format (config in `pyproject.toml`).
- Type hints required on public functions in `src/cidmath_datahub/`.
- Docstrings: Google style.
- Structured logger from `cidmath_datahub.common.logging`. No `print()` in production code.

### Update semantics (ADR 0007)

Every materialized table declares its `update_semantics` in `_ops.dataset_engineering`. Controlled vocabulary:

`append_only`, `snapshot_replace`, `merge_upsert`, `merge_scd2`, `merge_scd2_side`, `incremental_compute`, `full_refresh`.

LDP `APPLY CHANGES` is the preferred merge mechanism inside LDP pipelines. Plain MERGE is fine in Jobs.

### Data quality (ADR 0009)

Every pipeline writing to processed or analysis layer should declare at least one DQ check. LDP expectations are the default mechanism inside LDP. Severity vocabulary: `info` / `warn` / `quarantine` / `fail`. Results land in `_ops.dq_results`.

### Discovery (ADR 0005, ADR 0008)

- UC tags on tables for filterable discovery. Tag namespaces: `domain:*`, `data_type:*`, `pathogen:*`, `surveillance_category:*`, `spatial_resolution:*`, `temporal_resolution:*`, `access_tier:*`. Values come from `_ops.taxonomy_*` reference tables.
- Rich metadata in `_ops.dataset_catalog` (universal provenance) plus `_ops.dataset_engineering` (engineering state) plus per-domain extensions (`_ops.dataset_surveillance`, etc.) as needed.
- Every analysis-layer table requires a `_ops.dataset_catalog` row before merge (CI-enforced).

### CI enforcement (ADR 0016)

CI gates exactly four rules:

1. `update_semantics` values in controlled vocabulary
2. DQ severity values in controlled vocabulary
3. UC tag values in their namespace's controlled vocabulary
4. `_ops.dataset_catalog` row presence for new analysis-layer tables

Everything else is documented and review-driven. Lint-style warnings surface drift without blocking.

## Working practices

- **Plan before code.** For non-trivial changes, write a plan (files to touch, decisions implied, risks, tests) and wait for approval.
- **Capture architectural decisions in ADRs.** `docs/adr/NNNN-title.md`, three to five paragraphs.
- **Human approval required for:** changes to `databricks-common.yml` targets, anything touching secrets/IAM, prod deploys.
- **Session start:** read `CLAUDE.md` and the relevant ADRs first.

## Never do this

- Never hardcode workspace URLs, catalog names, or secret values. Use bundle variables and `${secrets.scope.key}` references.
- Never write to `ecdh_prod.*` from a dev bundle target.
- Never add a cross-bundle pipeline invocation (a `vaccine` pipeline calling a `wastewater` pipeline). Cross-bundle table reads via Unity Catalog are fine. (ADR 0004)
- Never put testable logic inside `bundles/<subject>/src/`. That directory is for thin entrypoints only; logic goes in `src/cidmath_datahub/`. (ADR 0011)
- Never deploy a domain bundle if `_platform` hasn't deployed for that target. (ADR 0004)
- Never commit `.databrickscfg`, `.env`, or any file containing tokens.
- Never use `display()` or `dbutils.notebook.exit()` in `src/` modules — they break testability.
- Never bypass Unity Catalog (no DBFS paths, no anonymous S3 reads).

## Key references

- Platform bundle: `bundles/_platform/databricks.yml`
- Shared package: `src/cidmath_datahub/`
- Shared bundle config: `databricks-common.yml`
- ADR index: `docs/adr/README.md`
- Operations: `docs/operations.md`
- Onboarding: `docs/onboarding.md`

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
- **Team chat:** Microsoft Teams — `Data Hub` channel in the CIDMATH Team Site
