# 0002 — Schema represents a subject area, not a source

## Status
Accepted — 2026-05-15

## Context
Many of our subject areas are fed by more than one data source. Wastewater is fed by Georgia DPH samples, CDC NWSS, and local collection programs. Immunization is fed by GRITS, IIS data from multiple jurisdictions, and Kaiser Permanente EHR linkages. Disease surveillance can pull from EIP, NREVSS, and DPH case files.

When laying out Unity Catalog schemas, we have to choose between two organizational principles:

1. **Schema per subject** — one schema per subject (`wastewater_raw`), with multiple source tables inside (`wastewater_raw.ga_dph_samples`, `wastewater_raw.cdc_nwss`).
2. **Schema per source** — one schema per source (`wastewater_ga_dph_raw`, `wastewater_cdc_nwss_raw`), with each source isolated in its own namespace.

The choice matters because it shapes how users find data, how pipelines are organized, where unification logic lives, and how the analysis layer is structured.

## Decision
**Schema = subject area, not source.** A schema like `wastewater_raw` represents the *subject of wastewater data*. Multiple sources contributing to that subject become separate tables within the schema. The processed layer cleans each source-table individually. The analysis layer (`wastewater`, no suffix per ADR 0001) holds the unified, user-facing tables that combine sources.

Example layout:

```
ecdh_dev.wastewater_raw.ga_dph_samples            # one source
ecdh_dev.wastewater_raw.cdc_nwss                   # another source
ecdh_dev.wastewater_raw.local_collection_logs      # a third source

ecdh_dev.wastewater_processed.ga_dph_samples       # cleaned, typed
ecdh_dev.wastewater_processed.cdc_nwss
ecdh_dev.wastewater_processed.local_collection_logs

ecdh_dev.wastewater.sample_concentrations          # unified across sources
ecdh_dev.wastewater.daily_aggregates
ecdh_dev.wastewater.facility_index
```

The unification logic — joining, deduplicating, harmonizing units, resolving conflicting measurements, applying source-of-record rules — lives in the transition from the processed layer to the analysis layer. It is the explicit job of that step.

## Alternatives considered
- **Schema per source.** Rejected. Splits a single subject across many schemas, which fragments the user-facing namespace (a researcher looking for "wastewater data" sees a long list of source-specific schemas, not one place to start). It also pushes unification across schema boundaries, which complicates grants and obscures the data lineage story.
- **Single flat schema with prefixed tables (`wastewater_ga_dph_samples`).** Rejected. Loses Unity Catalog's natural three-level namespace structure and leaves the schema list uselessly short.
- **Subject-per-schema for analysis, source-per-schema for raw/processed.** Rejected. Inconsistent organization across layers is harder to teach, harder to script against, and forces engineers to translate between schemes constantly.

## Consequences
- **Users discover data by subject.** "Where is wastewater data?" has a one-word answer: `wastewater`. They don't need to know which sources contribute.
- **Sources are tables, not schemas.** Table names within `<subject>_raw` and `<subject>_processed` use the source identifier (`ga_dph_samples`, `cdc_nwss`). Source-specific naming lives at the table level, where it doesn't pollute the schema namespace.
- **Unification has a named home.** The processed→analysis transition is where multi-source data becomes unified. This is an explicit, designable step — not an accidental side effect of layout.
- **Adding a new source to an existing subject is additive.** New table in `<subject>_raw`, new table in `<subject>_processed`, updated unification logic in the analysis layer. No new schemas, no new grants.
- **A source contributing to multiple subjects appears in multiple raw schemas.** This is acceptable: the data lives in tables organized by their subject use, not by their origin. The dataset catalog (`_ops.dataset_catalog`, see ADR 0005) tracks which source feeds which subjects.
- **Source-specific pipelines still exist.** Pipeline jobs are typically per-source within a subject (e.g., `wastewater_ingest_cdc_nwss`, `wastewater_ingest_ga_dph`). The bundle, however, is per-subject (see ADR 0004).
