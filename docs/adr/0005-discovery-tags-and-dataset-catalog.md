# 0005 — Data discovery via Unity Catalog tags and a curated dataset catalog

## Status
Accepted — 2026-05-15
**Revised 2026-05-15:** Schema details for the dataset catalog moved to ADR 0008 (catalog metadata schema design). Tag namespace list expanded after review of Delphi EpiPortal and the CIDMATH legacy data catalog. ADR 0008 supersedes the original single-table schema; this ADR remains the source of truth for the discovery *mechanism*.

## Context
The CIDMATH Data Hub serves researchers and public health practitioners who need to find data relevant to a question they're investigating. "What data do we have on wastewater surveillance?" "Which datasets cover lab and testing at the county level?" "What's available with case-counts for influenza at the state level?" These questions need to be answerable without asking the data engineering team.

The Insight Net poster taxonomy already enumerates nine domain categories and roughly thirty sub-domains. This taxonomy is good as metadata but is the wrong shape for catalog/schema structure: a single dataset often maps to multiple domains, the taxonomy is coarser than schemas, and forcing it into folder structure would require contorting the schema design.

The legacy CIDMATH Data Catalog (Excel) captured useful metadata — source, license, refresh cadence, public health relevance — but it lives outside the data, isn't version-controlled, drifts from reality, and can't be queried from Databricks. The Delphi EpiPortal demonstrates what a mature discovery surface looks like when backed by structured, queryable metadata.

## Decision

**Two complementary discovery mechanisms.**

### 1. Unity Catalog tags on tables

Every analysis-layer table (and key tables in raw/processed where useful) carries one or more tags from the namespaces below. Tags are applied via `ALTER TABLE ... SET TAGS (...)` in pipeline definitions or as a step in the platform bundle. UC's catalog explorer UI supports filtering by tag, and `information_schema.tags` supports programmatic queries.

**Tag namespaces:**

| Namespace | Purpose | Example values |
|---|---|---|
| `domain:*` | Insight Net taxonomy domains (multiple allowed per table) | `wastewater_surveillance`, `lab_and_testing`, `immunization`, `demographics` |
| `data_type:*` | What kind of measurement or record this is | `case_counts`, `mobility`, `wastewater_measurement`, `vaccination_event`, `lab_result` |
| `pathogen:*` | Pathogens/syndromes covered (multiple allowed) | `sars_cov_2`, `influenza_a`, `rsv`, `mpox`, `pathogen_independent` |
| `surveillance_category:*` | Disease-surveillance lifecycle stage (per Delphi EpiPortal taxonomy) | `entire_population`, `vaccinated`, `infected`, `tested`, `ascertained_case`, `symptomatic`, `outpatient_ed`, `hospitalized`, `icu`, `deceased` |
| `spatial_resolution:*` | Geographic granularity | `national`, `hhs_region`, `state`, `county`, `zcta`, `census_tract`, `facility`, `sewershed` |
| `temporal_resolution:*` | Temporal granularity | `daily`, `weekly`, `monthly`, `annual`, `hourly` |
| `access_tier:*` | Access classification | `public`, `restricted`, `commercial` |

*(The `kimball_role:*` tag namespace from earlier drafts was dropped — see ADR 0015. The integrated catalog encodes role in the table name suffix (`_fact`, `_dim`, `_bridge`), making the tag redundant.)*

Controlled vocabularies for each namespace live in `_ops` reference tables (e.g., `_ops.taxonomy_domain`, `_ops.taxonomy_pathogen`, `_ops.taxonomy_surveillance_category`). Adding a new value to a controlled vocabulary requires updating the reference table via a PR; documented but not CI-enforced (per the project's preference for documentation over enforcement at this stage).

A single table can — and typically will — carry multiple tags across multiple namespaces. A wastewater table covering SARS-CoV-2 and influenza at sewershed level would carry: `domain:wastewater_surveillance`, `data_type:wastewater_measurement`, `pathogen:sars_cov_2`, `pathogen:influenza_a`, `spatial_resolution:sewershed`, `temporal_resolution:weekly`, `access_tier:public`.

### 2. Curated dataset catalog table system

Rich metadata that doesn't fit UC tags lives in a system of tables in `_ops` per environment, designed as a universal base + per-domain extensions + a misc escape valve. The pattern and the full table schemas are defined in **ADR 0008 — Catalog metadata schema design**.

In summary:

- `_ops.dataset_catalog` holds universal provenance metadata for every catalogued dataset (description, source, license, temporal/spatial/demographic structure, refresh/lag/revision behavior, DUA, censoring, missingness notes, etc.).
- `_ops.dataset_engineering` holds engineering metadata for every materialized table (layer, update semantics, pipeline reference, last refresh, schema version).
- `_ops.dataset_<domain>` extensions hold structured metadata that only applies to datasets within a given subject area (e.g., `dataset_surveillance` for pathogen/case-definition fields; `dataset_demographics` for population stratifiers).
- A composite view `_ops.dataset_catalog_full` LEFT JOINs everything for cross-cutting discovery.

The catalog tables are themselves Delta tables, owned by the `_platform` bundle (universal tables) and the corresponding domain bundle (extensions). They become the backing store for a future discovery UI — a Tableau view, a Shiny app, an internal portal patterned after Delphi's EpiPortal.

### 3. CI enforcement

Every analysis-layer table that lands in the hub requires:

- A row in `_ops.dataset_catalog` (universal — always required).
- A row in `_ops.dataset_engineering` if `is_hosted = true`.
- A row in the appropriate domain extension when the table's subject maps to a known extension.

The CI check parses the bundle resources for new analysis-layer tables and verifies entries exist. Catalogued-only datasets (`is_hosted = false`) get a `dataset_catalog` row but no engineering row.

## Alternatives considered
- **Taxonomy as folder/schema structure.** Rejected. A single table maps to multiple domains; folders can't represent this. Forces contortions in schema design.
- **UC tags alone.** Rejected as sole mechanism. Tags are key/value pairs with no rich text support, no narrative metadata, no array fields, and limited cardinality. They can't carry "public health relevance" or "known limitations" usefully.
- **Dataset catalog table alone.** Rejected as sole mechanism. Without UC tags, users browsing the catalog UI can't filter by domain — they'd have to query the metadata table separately, which raises friction.
- **A single wide catalog table with all metadata.** Rejected. See ADR 0008's full analysis. Schema bloats unboundedly as new domains are added, NULL forests obscure structure, no clean ownership.
- **Keep the Excel data catalog and link to it.** Rejected. Drifts from reality, isn't queryable, lives outside the data lifecycle.

## Consequences
- **Two-tier discovery model.** Users browsing UC find tables by tag filter (fast, low-friction, native UI). Users wanting depth query `_ops.dataset_catalog_full` for full context.
- **Replaces the Excel data catalog.** The dataset catalog tables are the new canonical source of dataset metadata. Migration happens incrementally as tables land in the hub.
- **Metadata is version-controlled and queryable.** Updates to metadata are PRs (the catalog rows are themselves Delta-versioned). Discovery queries work in SQL. Future tooling reads from `dataset_catalog_full`.
- **Tag taxonomy needs governance.** The list of valid values per namespace is controlled via the `_ops.taxonomy_*` reference tables, not freeform. Adding a value is a deliberate PR.
- **Ongoing maintenance burden on contributors.** Every new analysis-layer table requires catalog entries and appropriate UC tags. Enforced via CI. Real cost, but the cost of having a usable catalog.
- **Schema evolution for the catalog is itself a real concern.** Per ADR 0008, the catalog tables have an explicit ownership and ADR process for changes. They aren't casually mutable.
- **The catalogued-only `is_hosted = false` capability** lets the same surface answer "what could we use?" alongside "what do we have?" — patterned after Delphi's "Hosted by Delphi?" flag.
- **Future-proofs the discovery UI.** When we eventually build a public-facing or internal data portal, it reads from `dataset_catalog_full` rather than scraping schemas or maintaining a parallel metadata store.
