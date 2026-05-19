# 0008 — Catalog metadata schema design: universal base plus per-domain extensions

## Status
Accepted — 2026-05-15

## Context
The CIDMATH Data Hub spans a broad set of subject areas (per ADR 0005's discovery taxonomy): infectious disease surveillance, demographics, mobility, healthcare utilization, environmental data, immunization, wastewater, genomic data, policy and interventions, and others. Each of these has its own structured metadata that matters for discovery, governance, and analysis — but the metadata that matters differs by subject.

Examples:

- A surveillance dataset has pathogens, syndromes, surveillance categories (Entire Population → Vaccinated → Infected → Tested → Ascertained → Symptomatic → Outpatient/ED → Hospitalized → ICU → Deceased, per Delphi EpiPortal's taxonomy), and case definition references. None of these apply to a Census ACS demographic dataset.
- A demographics dataset has population stratifiers (age bands, race/ethnicity coding systems, sex categories), vintage/release year, and estimate-vs-census methodology. None of these apply to wastewater concentrations.
- A mobility dataset has sampling methodology, panel description, and privacy/aggregation rules. None apply to ICD-10-coded encounters.

Three competing forces shape the design:

1. **Structured, validatable, queryable metadata is the point.** A faceted discovery UI like Delphi's EpiPortal is only as good as the structure of its backing schema. Free-text fields are dead ends.
2. **A wide flat table with NULL columns for every other domain's fields is unworkable.** It bloats as new domains are added, every column add is a migration, no schema can validate domain-specific shapes.
3. **A pure JSON/MAP catch-all loses what makes structured metadata useful.** Filterability, type safety, and self-documenting schemas all degrade.

The old CIDMATH Data Catalog (Excel) and the Delphi EpiPortal both hit this problem and resolved it differently — Delphi narrowed scope (infectious disease only), the Excel sheet went wide-flat and accumulated NULL columns. Our scope is broader than Delphi's; we cannot narrow our way out of this.

## Decision

**Three categories of metadata, mapped to a small number of related tables joined by `full_table_name`.**

### The categories

| Category | Scope | Examples |
|---|---|---|
| **Provenance** | Universal. Applies to every dataset regardless of subject. | Provider, source URL, license, DUA, refresh cadence, lag, revision behavior, temporal/spatial/demographic structure, censoring, missingness, preprocessing notes, known limitations. |
| **Engineering** | Universal for tables we materialize. Applies to every table we own. | Layer, update semantics (SCD), pipeline reference, last refresh, ingestion watermark, materialization type, schema version, DQ results. |
| **Domain-specific** | Conditional. Applies only to datasets in a given subject area. | Pathogens, surveillance categories, population stratifiers, sampling methodology, encounter type, etc. |

### Table layout

```
_ops.dataset_catalog          # provenance metadata — required for every catalogued dataset
_ops.dataset_engineering      # engineering metadata — required for every table we materialize
_ops.dataset_<domain>         # domain extensions — one per major domain, added when needed
```

Concrete domain extensions, added as the corresponding domain comes online:

```
_ops.dataset_surveillance     # ID surveillance sources
_ops.dataset_demographics     # population/demographic data
_ops.dataset_mobility         # mobility/transportation
_ops.dataset_environmental    # weather, air quality, environment
_ops.dataset_healthcare       # utilization, encounters, claims
_ops.dataset_genomic          # sequencing, phylogenetics
_ops.dataset_immunization     # vaccination records, schedules
_ops.dataset_wastewater       # wastewater-specific (analyte, sampling)
... # one per real domain
```

Every catalogued dataset has exactly one row in `_ops.dataset_catalog`. Datasets we materialize additionally have one row in `_ops.dataset_engineering`. Domain extensions are optional and only present for tables within that domain — a wastewater table has a row in `dataset_surveillance` AND `dataset_wastewater` if both apply.

Long-tail / one-off domain-specific fields go in a `domain_metadata_misc` `MAP<STRING,STRING>` column on `_ops.dataset_catalog`. The misc map is the escape valve when a field doesn't yet justify its own extension table. Migration path: when a misc-map key starts appearing across many tables in one domain, it gets promoted into that domain's extension table.

### Universal table schemas

**`_ops.dataset_catalog` (provenance — universal)**

| Column | Type | Required | Purpose |
|---|---|---|---|
| `full_table_name` | string | yes | Three-level UC name; primary key |
| `subject` | string | yes | Schema subject |
| `layer` | string | yes | `raw`, `processed`, or `analysis` |
| `source_provider_code` | string | yes | Provider code (see ADR 0006) |
| `source_dataset_name` | string | no | Provider's own name for the dataset |
| `description` | string | yes | Plain-English description of what the table contains |
| `public_health_relevance` | string | no | Why this data matters for ID modeling/analytics |
| `known_limitations` | string | no | Caveats, biases, gaps, sampling issues |
| `derived_from` | array<string> | no | Source table names if this is derived/aggregated |
| `preprocessing_notes` | string | no | High-level summary of transformations applied |
| `data_suppression_notes` | string | no | Censoring rules from the source |
| `missingness_notes` | string | no | Known missingness patterns |
| `temporal_resolution` | string | no | e.g., `daily`, `weekly`, `annual` |
| `temporal_coverage_start` | date | no | Earliest date in the data |
| `temporal_coverage_end` | date | no | Latest date in the data (NULL = ongoing) |
| `spatial_resolution` | string | no | e.g., `state`, `county`, `zcta` |
| `spatial_coverage` | string | no | e.g., `Georgia`, `48 states + DC` |
| `demographic_resolution` | string | no | Stratifiers available (e.g., `age × race × sex`) |
| `demographic_coverage` | string | no | Population scope (e.g., `all ages`, `Medicare beneficiaries`) |
| `refresh_cadence` | string | no | How often this table updates |
| `reporting_lag` | string | no | Typical delay between event and availability |
| `revision_cadence` | string | no | How often the upstream provider revises past data |
| `source_url` | string | no | Where the data lives upstream |
| `source_documentation_url` | string | no | Link to upstream documentation |
| `source_data_dictionary_url` | string | no | Link to upstream data dictionary |
| `external_maintainer_name` | string | no | Contact at the data provider |
| `external_maintainer_email` | string | no | Contact email |
| `license` | string | no | Use terms |
| `dua_required` | boolean | no | Whether a Data Use Agreement is required |
| `dua_reference` | string | no | Link or identifier for the DUA |
| `access_tier` | string | no | `public`, `restricted`, `commercial` |
| `is_hosted` | boolean | yes | `true` if materialized in `ecdh_*`; `false` if catalogued only |
| `owner` | string | yes | Internal owner (person or team) |
| `last_validated` | date | no | Date metadata was last reviewed |
| `domain_metadata_misc` | map<string,string> | no | Long-tail fields not yet promoted to an extension |

**`_ops.dataset_engineering` (engineering — universal for materialized tables)**

| Column | Type | Required | Purpose |
|---|---|---|---|
| `full_table_name` | string | yes | Three-level UC name; foreign key to `dataset_catalog` |
| `update_semantics` | string | yes | Controlled vocabulary; defined in ADR 0007 |
| `materialization_type` | string | yes | `table`, `view`, `materialized_view`, `streaming_table` |
| `partition_columns` | array<string> | no | Delta partition columns |
| `cluster_columns` | array<string> | no | Liquid clustering columns |
| `history_table` | string | no | Full name of the companion history table if SCD2 |
| `pipeline_reference` | string | yes | Bundle path to the pipeline that maintains this table |
| `pipeline_run_id_last` | string | no | Most recent successful run id |
| `last_refresh_at` | timestamp | no | Last successful write |
| `ingestion_watermark` | string | no | High-water mark for incremental ingestion (source-specific) |
| `schema_version` | int | yes | Incremented on incompatible schema changes |
| `dq_status_last` | string | no | `passed`, `warned`, `failed`, `unknown` |
| `dq_results_run_id` | string | no | Reference into `_ops.dq_results` |

### How to add a new domain extension

The first time a new domain has structured metadata beyond what `domain_metadata_misc` can comfortably hold:

1. Write an ADR proposing the extension table schema (`docs/adr/NNNN-dataset-<domain>-metadata.md`). Include the column list, the rationale for each column, and which datasets it applies to.
2. Add the table definition to `bundles/_platform/resources/` so it lands in `_ops` for every environment.
3. Migrate any related fields from `domain_metadata_misc` into the new extension table.
4. Update the composite view (below) to LEFT JOIN the new extension.
5. Update CI checks to validate population of the extension where required.

The rule of thumb: a new extension is warranted when more than ~5 tables share ~3 or more domain-specific structured fields. Below that threshold, the misc map is fine.

### Composite view for cross-cutting discovery

```sql
CREATE OR REPLACE VIEW _ops.dataset_catalog_full AS
SELECT
  c.*,
  e.update_semantics,
  e.materialization_type,
  e.last_refresh_at,
  e.dq_status_last,
  s.pathogens,
  s.surveillance_categories,
  s.case_definition_reference,
  d.population_stratifiers,
  d.vintage_year,
  -- ... join each extension as it's added
  ...
FROM _ops.dataset_catalog c
LEFT JOIN _ops.dataset_engineering   e USING (full_table_name)
LEFT JOIN _ops.dataset_surveillance  s USING (full_table_name)
LEFT JOIN _ops.dataset_demographics  d USING (full_table_name)
-- ... LEFT JOIN each extension
;
```

The view is denormalized for ease of querying; underlying tables stay normalized for integrity. Discovery UIs and ad-hoc users read the view. Validation, schema evolution, and CI reads the base tables.

### Ownership and schema evolution

- `_ops.dataset_catalog` and `_ops.dataset_engineering` are owned by the platform team. Schema changes require an ADR and update the `_platform` bundle.
- Each `_ops.dataset_<domain>` extension is owned by the corresponding domain bundle's maintainers. Schema changes require an ADR for that extension.
- The composite view is owned by `_platform` and updated whenever an extension is added.
- All schema changes are versioned via Delta schema evolution where compatible; incompatible changes require migration scripts and an ADR.

### CI enforcement

Per the hybrid CI enforcement policy (ADR 0016):

- **CI-enforced:** Every analysis-layer table requires a row in `_ops.dataset_catalog` before its PR can merge. This is one of the four enforced rules in 0016.
- **Documented-only (review-driven):** Every materialized table populating `_ops.dataset_engineering`; domain extension row presence; correctness of the `is_hosted` flag relative to actual materialization status. Surfaced as lint-style CI warnings (visible in PR comments) when missing, but not blocking.

## Alternatives considered
- **(A) Wide flat single table with NULLable columns for every field.** Rejected. Schema bloats without bound, every domain addition is a migration touching the universal table, NULL forests obscure what's actually populated.
- **(C) Pure EAV (entity-attribute-value).** Rejected. Loses schema validation, kills query elegance, classic anti-pattern. Reasonable people have tried this; reasonable people have regretted it.
- **(D) Universal base + a single `MAP<STRING,STRING>` for all domain fields.** Rejected as the *primary* mechanism. Workable as the escape valve (which is what `domain_metadata_misc` is) but as the sole mechanism it sacrifices the structure that makes the catalog useful.
- **(E) Universal base + materialized JSON view per domain.** Rejected as more complex than (B) with no real advantage at our scale.
- **One catalog table per bundle, owned by that bundle.** Rejected. Cross-bundle discovery becomes a multi-bundle query rather than a single view. The `_platform` bundle owning `_ops.dataset_catalog` keeps cross-cutting discovery simple.

## Consequences

- **Discovery scales cleanly with new domains.** Adding `genomic` data later means creating `dataset_genomic` as a new extension, not refactoring an unwieldy universal schema.
- **Each extension has a clear owner.** Surveillance metadata schema evolution lives with the surveillance ADR; demographic metadata evolution lives with the demographics ADR. No cross-domain coordination overhead.
- **Generic discovery uses one table; domain-specific discovery uses two.** The query reflects the question's scope, which is the right shape.
- **The misc map is critical for momentum.** It lets us add metadata as we learn what matters without an ADR-and-migration ceremony for every new field. We promote to a proper extension when the field has earned it.
- **`is_hosted` lets the catalog answer two questions in one place.** "What data do we have?" (where `is_hosted = true`) and "What data could we have?" (where `is_hosted = false`). Without this, we'd need a separate external-sources registry that would drift.
- **The composite view is the one thing that breaks the locality of ownership.** Adding an extension means the platform team updates the view. This is acceptable — a single 5-line edit per extension — but worth noting as a coordination point.
- **Schema changes need real discipline.** Three tables (universal × 2 + domain) plus a view means each change has more touch points than a single-table schema. The discipline pays for itself in long-term clarity; the cost is non-zero on every change.
- **CI rules are tractable.** Catalog presence is a simple JOIN; engineering presence applies only to `is_hosted = true`; domain extension presence applies when subject maps to a known extension. All checkable in a few SQL queries in the CI workflow.
