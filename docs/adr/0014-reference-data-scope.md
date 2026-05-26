# 0014 — Reference data: scope, bundle structure, and standardization via informational foreign keys

## Status
Accepted — 2026-05-15

## Context
Many tables in the data hub will share the same conceptual "reference" data — geographic identifiers, calendar/epi-week definitions, code systems (LOINC, SNOMED, ICD-10, CVX), pathogen taxonomies, and similar canonical lookups. Without an explicit pattern, each subject's pipeline would either embed its own copy of these mappings (drift, duplication) or join to whichever copy a contributor happened to find first (inconsistency, ambiguity about authority).

ADR 0003 originally deferred building the integrated catalog (`ecdh_model_<env>`) until a cross-source analytical use case emerged. On reflection, reference data standardization *is* a foundational cross-source use case — and arguably the most leveraged one, because every subject benefits from it immediately. The integrated catalog should be built now, populated initially with reference content, with subject-specific analytical content (the original deferral target) still gated on real use cases.

A second question follows: what exactly counts as "reference data"? The category isn't monolithic. A calendar table, the FIPS code list, and pathogen R0 estimates all look reference-like at a glance but behave very differently — one is computational, one is slow-changing authoritative truth, one is an empirical estimate that updates regularly. Conflating them in a single home would mix data whose change profiles, ownership, and analytical roles are different.

## Decision

### Reference data typology

Four classes, distinguished by where their values come from and how often they change:

| Class | Definition | Examples | Update profile |
|---|---|---|---|
| **Computational reference** | Values are deterministic; can be generated from rules without any external data. | `time.calendar_date`, `time.epi_week` (MMWR), `time.iso_week`, `time.fiscal_year_federal` | Once generated for a range, extend forward. No revisions to past values. |
| **Authoritative slow-changing reference** | Values come from an external authoritative source on a periodic release schedule. Most rows stable for years; occasional definitional changes (e.g., Connecticut counties → planning regions in 2022). | `geography.state`, `geography.county`, `geography.zcta`, `geography.census_tract`, code-system tables (LOINC, SNOMED, ICD-10, CVX) | Refresh on source release cadence (Census annually, code systems quarterly/semi-annually). |
| **Static structural reference** | The taxonomy/structure of a concept. Names, hierarchies, synonyms, mappings. Stable shape; occasional additions. | `pathogen.taxonomy`, `pathogen.icd10_mapping`, `pathogen.cvx_mapping`, `geography.crosswalk_zcta_to_county` | Rare updates. Append-mostly. |
| **Empirical / time-varying parameters** | Values are *outputs* of analysis — estimates, fits, projections. They look reference-like but are not authoritative; they're the result of computation over data. | Flu seasonal R0 estimates, vaccine effectiveness for current strain, NPI effectiveness coefficients, current-season serotype distributions | Recomputed regularly. Each version is an analytical artifact, not a canonical fact. |

### The bright-line rule

**A table belongs in `_reference` if and only if its values are canonical/authoritative (defined by an external standard, an authoritative external source, or deterministic computation), and it contains no analytical or estimated content.**

This is the *canonical-vs-derived* axis. It is independent of the *update behavior* axis: a reference table can be static (like `time.epi_week`) or time-varying with SCD2 history (like `geography.county`, which Census revises). Time-varying behavior does not make a table "analytical" — `geography.county`'s SCD2 mechanics simply record when the authoritative source revised the canonical truth. The naming convention (ADR 0015) reflects this: reference tables carry no Kimball suffix regardless of SCD behavior; only derived analytical content gets `_fact`/`_dim`/`_bridge` suffixes.

The first three classes belong in `_reference`. The fourth — empirical/time-varying parameters — does **not** belong in `_reference` regardless of how reference-like it looks. Those tables live in the subject bundle that produces them (e.g., a `forecasting` bundle) or in the integrated catalog under the relevant concept schema as analytical outputs (e.g., `ecdh_model.pathogen.parameter_estimates_v1` with SCD2 semantics).

Test for the boundary: *if someone changed the value in this table, would they need to defend the change empirically (i.e., "we re-ran the estimation and got 1.4 instead of 1.2") or definitionally (i.e., "Census moved this county into a different state")?* Empirical defense → not reference. Definitional defense → reference.

### Bundle structure

A dedicated `_reference` bundle alongside `_platform`:

```
bundles/
├─ _platform/        # infrastructure only: catalogs, schemas, grants, _ops tables, secret scopes
├─ _reference/       # canonical reference data: time, geography, code systems, pathogen taxonomy
├─ wastewater/       # subject bundle
├─ vaccine/          # subject bundle
└─ ...
```

`_reference` writes to schemas in `ecdh_model_<env>` (the integrated catalog). It reads from external sources (Census TIGER files, CDC NWSS source documentation for code mappings, etc.) and produces canonical tables.

`_reference` deploys *after* `_platform` (which creates the catalogs and schemas) and *before* any subject bundle that consumes its tables (which is, in practice, every subject bundle). This ordering is enforced by CI workflow dependencies.

### Initial schemas owned by `_reference`

```
ecdh_model_<env>.time.calendar_date
ecdh_model_<env>.time.epi_week
ecdh_model_<env>.time.iso_week
ecdh_model_<env>.time.fiscal_year_federal

ecdh_model_<env>.geography.state
ecdh_model_<env>.geography.county
ecdh_model_<env>.geography.zcta
ecdh_model_<env>.geography.census_tract
ecdh_model_<env>.geography.hhs_region
ecdh_model_<env>.geography.census_region
ecdh_model_<env>.geography.census_division
ecdh_model_<env>.geography.crosswalk_zcta_to_county
ecdh_model_<env>.geography.crosswalk_county_to_hhs_region
ecdh_model_<env>.geography.urban_rural_nchs
```

Future schemas as needs emerge:
- `ecdh_model_<env>.codes.loinc`
- `ecdh_model_<env>.codes.snomed`
- `ecdh_model_<env>.codes.icd10`
- `ecdh_model_<env>.codes.cvx`
- `ecdh_model_<env>.pathogen.taxonomy`
- `ecdh_model_<env>.pathogen.icd10_mapping`

### Update semantics by reference class

Per ADR 0007's controlled vocabulary:

| Reference class | Update semantics | Rationale |
|---|---|---|
| Computational (time) | `incremental_compute` or `full_refresh` | Deterministic generation; either extend forward or recompute the range as needed |
| Authoritative slow-changing (geography, code systems) | `merge_scd2` on tables where definitional history matters; `merge_upsert` otherwise | Historical truth (e.g., what was the county definition on 2021-12-31) is analytically valuable for time-spanning queries |
| Static structural (taxonomies, mappings) | `merge_upsert` | Latest definition is canonical; history rarely queried |

The `geography.county` table uses `merge_scd2` so the Connecticut 2022 reorganization (counties → planning regions) is queryable historically. Most code system tables are `merge_upsert` — what matters is the current canonical mapping.

### Schema ownership in the integrated catalog

A subtlety worth being explicit about: schemas in `ecdh_model_<env>` are organized by concept, not by bundle. Multiple bundles can write tables into the same schema. For example, `ecdh_model_<env>.pathogen` will eventually hold both:

- Static structural reference tables written by `_reference` (`pathogen.taxonomy`, `pathogen.icd10_mapping`)
- Analytical output tables written by subject bundles (e.g., `pathogen.parameter_estimates` written by a future `forecasting` bundle)

Ownership is per *table*, not per *schema*. The table's `pipeline_reference` field in `_ops.dataset_engineering` records the owning bundle. CI verifies no two bundles write to the same table.

The schema itself is created by `_reference` (since reference data lands first) and remains under `_reference`'s declaration responsibility. When a subject bundle wants to write into an existing concept schema, it does not redeclare the schema — it just declares its tables within it.

### Standardization via informational foreign keys

Reference tables become useful only when consuming tables actually link to them. To make standardization auditable rather than aspirational:

**Rule:** Any column in any table that follows a canonical reference pattern must declare an informational `FOREIGN KEY` to the corresponding reference table.

Canonical patterns (initial list — extend as new reference tables land):

| Column pattern | References |
|---|---|
| `state_fips` | `ecdh_model_<env>.geography.state.state_fips` |
| `county_fips` | `ecdh_model_<env>.geography.county.county_fips` |
| `zcta` | `ecdh_model_<env>.geography.zcta.zcta` |
| `census_tract_geoid` | `ecdh_model_<env>.geography.census_tract.census_tract_geoid` |
| `hhs_region` | `ecdh_model_<env>.geography.hhs_region.hhs_region` |
| `epi_week_id` | `ecdh_model_<env>.time.epi_week.epi_week_id` |
| `loinc_code` | `ecdh_model_<env>.codes.loinc.loinc_code` (when LOINC reference exists) |
| `cvx_code` | `ecdh_model_<env>.codes.cvx.cvx_code` |

Foreign key constraints in Delta/UC are **informational, not enforced at write time**. They:

- Document the relationship in catalog metadata (visible in UC explorer and `information_schema`)
- Get picked up by Unity Catalog's lineage tracking
- Are used by query optimizers in some cases
- Are auditable: a SQL query against `information_schema.column_relationships` reports which tables declare which FKs.

Declaration syntax inside a bundle resource:

```yaml
columns:
  - name: county_fips
    type: string
    comment: "FIPS code identifying the county. References geography.county."
constraints:
  - name: fk_county_fips
    type: foreign_key
    columns: [county_fips]
    referenced_table: "${var.model_catalog}.geography.county"
    referenced_columns: [county_fips]
```

### CI behavior

- **Warn (not block)** on PRs that introduce a column matching a canonical pattern without an FK declaration. Lint-style: surface the gap, don't gate the merge.
- **Block** on PRs that declare an FK to a non-existent reference table or column. This catches typos in references.
- The canonical-pattern list lives in `cidmath_datahub.common.reference_patterns` and is updated when a new reference table lands.

This stops short of hard enforcement consistent with the project's preference for documentation over CI gates where the cost of friction outweighs the cost of drift.

### What does NOT belong in `_reference`

To prevent `_reference` from becoming a junk drawer, examples of things that look reference-like but belong elsewhere:

- **Source data dictionaries.** A MarketScan code-to-description mapping that's specific to MarketScan's coding system belongs in `marketscan_processed`, not `_reference`. It's source metadata, not canonical reference.
- **Provider code list** (per ADR 0006). Lives in `_ops.provider_codes` because it's operational metadata for the platform, not analytical reference.
- **Domain-specific lookup tables.** A "list of wastewater treatment facilities operating in Georgia" belongs in `wastewater` (it's subject content), not `_reference`.
- **Empirical parameter estimates.** As covered above, these go in their producing subject bundle or in `ecdh_model_<env>` as analytical content.
- **User-curated mappings that lack an authoritative source.** If we maintain our own pathogen-to-broad-category grouping for internal use, that's curated subject metadata, not canonical reference. Put it in `_ops` or in the consuming subject bundle.

**Implementation refinement (2026-05-26, naming).** The illustrative table names above (`geography.state`, `geography.county`, `geography.zcta`, etc.) were written assuming a US-only `geography` schema. Once ADR 0022 brought international scope (ISO 3166-1/2 + GADM) into the same schema, the US-specific tables were renamed with a `us_` prefix to keep the country scope visible (ADR 0006): `geography.us_state`, `geography.us_county`, `geography.us_zcta`, `geography.us_tract`, `geography.us_hhs_region`, `geography.us_crosswalk`. The new global tables (`geography.country`, `geography.country_subdivision`, `geography.subnational`) keep the unprefixed names. The FK declaration example below uses `geography.county` as it was originally written — the same pattern works for `geography.us_county` post-rename.

## Alternatives considered
- **Put geography and time in `_platform`.** Rejected. Mixes data movement into the infrastructure bundle and creates a slippery-slope risk for future "where does this go?" questions. `_reference` as a dedicated bundle costs essentially nothing and creates a clean home.
- **Put reference data in the source-aligned catalog** (e.g., `ecdh_<env>.geography.*`). Rejected. Conflicts with ADR 0002 (schemas are subject areas with raw/processed/analysis layers) — geography isn't a "subject" in that sense. Also makes cross-environment cross-references awkward when the integrated catalog comes online.
- **One reference bundle per concept** (`_reference_geography`, `_reference_time`, `_reference_codes`). Rejected. Over-fragmented. A single `_reference` bundle holds tens of small reference tables comfortably.
- **No formal reference-data category — let each subject ingest its own copy of geography, time, codes.** Rejected. Drift inevitable; standardization impossible; cross-source joins become a translation exercise.
- **Hard-enforce FK declarations in CI.** Rejected at this stage. Friction during pipeline development is real; lint-style warnings give visibility without blocking work. Revisit if drift accumulates.

## Consequences
- **`_reference` is built early and serves as a foundation.** Subject bundles can assume canonical geography and time tables exist from day one.
- **The integrated catalog is no longer purely deferred.** It comes online with `_reference`'s content. ADR 0003 is revised accordingly. Subject-specific analytical content (the original deferral target) is still gated on real use cases.
- **Cross-catalog FK references are normal.** Source-aligned tables in `ecdh_<env>` declare FKs into `ecdh_model_<env>` reference tables. UC supports this; SQL syntax is straightforward.
- **Standardization becomes auditable.** A SQL query against `information_schema.column_relationships` identifies which tables conform to the canonical patterns and which don't.
- **Update semantics differ across reference classes.** Computational tables use deterministic recomputation; authoritative slow-changing tables use SCD2 to preserve definitional history; static structural tables use upserts. The `update_semantics` field on `_ops.dataset_engineering` records the choice per table.
- **Schema ownership in the integrated catalog is per-table, not per-bundle.** Multiple bundles can write into `ecdh_model_<env>.pathogen` (reference tables from `_reference`, analytical outputs from a future forecasting bundle). The pipeline reference on each table records who owns it.
- **A new contributor question — "is this reference?" — has a clear test.** The empirical-vs-definitional defense framing in the bright-line rule resolves most ambiguity.
- **Deploy ordering gains a step.** `_platform` → `_reference` → subject bundles. Codified in CI; manual deploys must follow the same order.
- **The `_reference` bundle will accumulate.** Over time it grows to hold many small canonical tables. That's the intent; the misc-catchall risk is contained because the bright-line rule rejects empirical content.
