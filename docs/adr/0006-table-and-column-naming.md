# 0006 — Table and column naming conventions

## Status
Accepted — 2026-05-15

## Context
The earlier ADRs settled catalog and schema naming (0001, 0002, 0003) but left table and column naming loose — "snake_case, descriptive" was the only guidance in `CLAUDE.md`. Without a concrete convention, ten contributors over two years produce ten styles, and retroactive renaming is painful: downstream dashboards, queries, ETL jobs, and partner integrations reference the names.

Table naming also serves discovery. A user browsing `ecdh_prod.wastewater.*` should be able to guess what each table holds from the name alone. Inconsistent naming makes the catalog harder to navigate even when the data is fine.

Column naming matters for the same reason and additionally affects every SQL query downstream. Foreign key columns that change name across tables make joins error-prone. Booleans named inconsistently confuse filter logic. Timestamp vs. date confusion in column names causes real bugs.

The decision here is to specify enough convention to prevent drift, leave room for judgment where rules can't anticipate everything, and avoid CI enforcement of conventions that are about taste rather than correctness.

## Decision

### Table names

**Use singular nouns.** `sample_measurement`, not `sample_measurements`. Reads better in joins (`sample_measurement.facility_id = facility.id`) and matches modern dimensional-modeling convention.

**Layer-specific patterns:**

| Layer | Pattern | Example |
|---|---|---|
| `<subject>_raw` | `<provider>_<dataset>` — table name = source identifier | `cdc_nwss`, `ga_dph_sample`, `kp_member_demographic` |
| `<subject>_processed` | Same source identifier as raw (one-to-one mapping) | `cdc_nwss`, `ga_dph_sample` |
| `<subject>` (analysis) | Entity or concept name. Include grain when not obvious. | `sample_concentration`, `daily_aggregate`, `facility_index`, `case_count_daily` |

The subject is already in the schema name. Never repeat it in the table name (`wastewater_raw.cdc_nwss`, not `wastewater_raw.wastewater_cdc_nwss`).

**Provider codes for raw/processed tables.** Use a short, conventional code for the data provider. Maintained as documentation only (no CI enforcement). Starter list — extend as needed via PR to this ADR:

| Code | Provider |
|---|---|
| `cdc` | US Centers for Disease Control and Prevention |
| `cms` | US Centers for Medicare and Medicaid Services |
| `census` | US Census Bureau |
| `hrsa` | Health Resources and Services Administration |
| `hhs` | US Department of Health and Human Services |
| `nih` | National Institutes of Health |
| `noaa` | US National Oceanic and Atmospheric Administration (incl. NCEI) |
| `who` | World Health Organization |
| `ga_dph` | Georgia Department of Public Health |
| `ga_eip` | Georgia Emerging Infections Program |
| `kp` | Kaiser Permanente |
| `marketscan` | Merative MarketScan |
| `safegraph` | SafeGraph (via DeweyData.io) |
| `advan` | Advan Research |
| `veraset` | Veraset |
| `sprinklr` | Sprinklr |
| `predicthq` | PredictHQ |
| `cidmath` | CIDMATH-collected data (e.g., social contact surveys) |

**No version suffixes in table names.** No `sample_concentration_v2`. Compatible schema changes use Delta schema evolution. Incompatible rewrites get a new table name (renaming the concept, e.g., `concentration` → `measurement`) plus an ADR documenting the migration.

**Reserved leading underscore for internal tables.** Tables that shouldn't appear in normal user discovery (intermediate staging, internal lookups, deprecated-but-retained) get a leading underscore. Consistent with the `_ops` schema convention.

**Country-prefix for country-specific reference tables.** Within the integrated catalog's reference schemas, tables that hold data for one specific country (rather than the global cross-country entity) get a `<country>_` prefix using the ISO 3166-1 alpha-2 code lower-cased: `geography.us_state`, `geography.us_county`, `geography.us_tract`, `geography.us_zcta`, `geography.us_hhs_region`, `geography.us_crosswalk`. The unprefixed names (`geography.country`, `geography.country_subdivision`, `geography.subnational`) are reserved for the global, cross-country tables that ISO 3166 and GADM key on (ADR 0022). This avoids the ambiguity that comes from a name like `geography.state` reading as "states" without saying *whose* states. Polymorphic companion tables that span both (e.g., `geography.boundary`) keep the bare name and discriminate by a `geo_level` column whose values themselves carry the country prefix (`us_state`, `country`, `subnational_adm2`, …).

**Length.** Aim for one to three words; soft cap at 30 characters. Hard cap at 50.

### Column names

**snake_case throughout.** No camelCase, no PascalCase, no hyphens.

**Primary keys: `<entity>_id`.** A table for `facility` has a `facility_id` PK. A table for `sample_measurement` has a `sample_measurement_id` PK. Use a meaningful key when one exists (e.g., FIPS codes for counties: `fips_county`).

**Foreign keys use the same name as the referenced PK.** If `facility.facility_id` is the PK, every table that references it uses `facility_id`. No `fk_facility`, no `facility_ref`.

**Timestamps vs. dates:**
- `<event>_at` for timestamps (e.g., `collected_at`, `processed_at`, `ingested_at`). Always UTC unless suffixed (`collected_at_local`).
- `<event>_date` for dates (e.g., `report_date`, `sample_date`).
- Avoid bare `date` and `time` — they're ambiguous and shadow SQL keywords.

**Booleans: `is_<state>` or `has_<thing>`.** `is_validated`, `is_active`, `has_replicate`, `has_geocode`. Never `validated`, `flag`, `status_bool`.

**Geographic identifiers use canonical code names.** `fips_state`, `fips_county`, `zcta`, `census_tract`, `census_block_group`. Avoid generic `geo_id` or `region_id`.

**Pathogen / clinical identifiers use canonical code systems.** `loinc_code`, `snomed_code`, `icd10cm_code`, `cvx_code` (vaccines). The code system is in the column name so its meaning is unambiguous.

**Measurement columns include unit when relevant.** `concentration_copies_per_ml`, `volume_ml`, `distance_meters`. Avoid raw `value` or `amount` unless the unit is implicit from the table's name.

**Audit/lineage columns (standard, present on every table where useful):**
- `ingested_at` (timestamp) — when this row landed in the raw layer
- `processed_at` (timestamp) — when this row was last updated in the processed layer
- `source_file` (string) — the file or API call this row came from, if applicable
- `pipeline_run_id` (string) — references `_ops.pipeline_runs.run_id`

### What is and isn't checkable

**Mechanically enforceable in CI (out of scope for this ADR, but easy to add later):**
- snake_case
- No reserved characters or leading digits
- Length cap
- Required audit columns on analysis-layer tables

**Documented convention, not enforced:**
- Provider codes for raw/processed table names
- Singular vs. plural
- Foreign key naming
- Timestamp/date/boolean column patterns

The line is: enforce mechanical correctness; trust contributors and code review for taste.

## Alternatives considered
- **Plural table names.** Rejected. Modern convention favors singular; joins read better; less consistency churn (no plural/singular irregular forms to remember). This is genuinely opinion-driven and either would have worked.
- **Kimball-style `fact_*` / `dim_*` prefixes in the analysis layer.** Rejected. Concept-based naming carries the meaning. Role (fact vs. dimension) is more relevant in the future integrated catalog (ADR 0003) and is captured there via UC tags rather than prefixes.
- **CI-enforced provider code registry.** Rejected per the project's preference for documentation over enforcement at this stage. Trivial to add later (a check against a `_ops.provider_codes` table) if drift becomes a problem.
- **Embedding grain or unit suffixes universally (`sample_measurement_per_ml`).** Rejected as overly prescriptive. Include grain or unit in names *when ambiguous*; trust judgment when context is clear.
- **Hungarian-style prefixes (`tbl_`, `vw_`, `mv_`).** Rejected. Unity Catalog already tracks object type; the prefix adds noise without information. Leading underscore is reserved for "internal," which is a semantic distinction worth signaling.

## Consequences
- **The catalog reads consistently from day one.** A user opening `ecdh_prod.wastewater.*` sees predictable patterns and can guess what each table holds.
- **Adding a new source is mechanical.** New provider? Pick or propose a code, name the table after it, add a row to this ADR's provider list in the same PR.
- **Foreign key joins are foot-gun-resistant.** The same name everywhere means `ON a.facility_id = b.facility_id` is the join, full stop.
- **CI burden is minimal.** This ADR doesn't add enforcement infrastructure. If the conventions drift in practice, the cost of adding CI checks later is a single workflow.
- **Some judgment calls remain.** Grain in table names, unit in column names, leading-underscore use — these are contextual. The ADR sets defaults; contributors and reviewers exercise judgment.
- **Provider codes will accumulate.** The starter list will grow. PRs that add a new provider should update this ADR's table in the same change. If the list becomes unwieldy (say, >50 entries), it migrates to `_ops.provider_codes` and this ADR points there.

## Refinement (2026-06-22, source token on source-catalog reference tables)
ADR 0037 routes **reference** data through the source catalog (`<subject>_raw` → `<subject>_processed`) before promotion to the model catalog. This refinement settles how those source-catalog reference tables are named, and confirms the canonical is unaffected:

- **Source-catalog reference tables carry the provider token** (the `<provider>_<dataset>` rule above). For **country-specific** reference, the `us_` country prefix is **retained** there too, giving `<country>_<provider>_<entity>` — e.g. `geography_raw.us_census_block_group`, `geography_processed.us_census_tract`. (This extends the country-prefix rule, previously scoped to the integrated catalog, to the source catalog.)
- **The model-catalog canonical stays source-agnostic** — no provider token: `geography.us_block_group`, `geography.us_tract`, `geography.us_state`. The integrated catalog is the conformed dimension (ADR 0003 / 0015); if a level ever has a second source, both conform to the **one** canonical, so the source identifier belongs on the source layers, not the consumer-facing name.
- **Existing canonicals are NOT renamed.** `geography.us_state` / `us_county` / `us_tract` keep their names; only the new source-catalog tables gain the provider token. (When a complex subject is migrated onto the layered builder per ADR 0037, the source-catalog tables are *created* as `us_census_*`; the re-promoted canonical keeps its source-agnostic name.)
- `census` is the provider token for geography (the originating authority, US Census Bureau), not the `ipums`/`nhgis` distributor.
