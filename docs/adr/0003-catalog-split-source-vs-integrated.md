# 0003 — Source-aligned and integrated/modeled data live in separate catalogs

## Status
Accepted — 2026-05-15
**Revised 2026-05-15:** Integrated catalog is no longer fully deferred. Per ADR 0014, it comes online with reference data (`geography`, `time`, and code systems) owned by the `_reference` bundle. Subject-specific analytical content within the integrated catalog (the original deferral target — e.g., conformed dimensional facts derived from multiple sources) remains gated on real cross-source use cases.

## Context
The data hub will eventually hold two kinds of data with fundamentally different organizational principles:

1. **Source-aligned data.** Organized by where it came from. Schemas follow `<subject>_<layer>` (or bare-subject for analysis, per ADR 0001). The shape of each table mirrors the source. Pipelines own data by source/subject. This is what we are building first.
2. **Integrated/modeled data.** Organized by public-health concept, not by source. Schemas are concepts: `geography`, `pathogen`, `time`, `population`, `surveillance`. Tables compose data from multiple processed sources into entities and facts that reflect the domain. This is what supports cross-source analysis and the "relational data model" goal in the 2026-27 priorities.

A single dimension like `geography` already implies many tables: states, counties, ZCTAs, census tracts, hospital service areas, crosswalks. So `geography` is naturally a *schema* in the integrated catalog, holding many entity tables — not a single dimensional table.

These two organizational principles cannot coexist cleanly in a single catalog without making schema naming inconsistent. If `wastewater_raw` (subject + layer) and `geography` (pure concept) share a catalog, the schema list mixes two semantic patterns and users can't tell at a glance what kind of thing they're looking at. Grant patterns also collide — different access tiers apply to source-aligned layers versus the integrated model.

## Decision
Two catalogs per environment:

```
ecdh_<env>            # source-aligned (raw, processed, analysis layers)
ecdh_model_<env>      # integrated / relational model (concept-organized schemas)
```

Both catalogs are built now. `ecdh_dev` / `ecdh_prod` (source-aligned) and `ecdh_model_dev` / `ecdh_model_prod` (integrated) are created by the `_platform` bundle on Day 2. The integrated catalog's initial content is reference data — `geography`, `time`, and code systems — owned by the `_reference` bundle (see ADR 0014). Subject-specific analytical content within the integrated catalog (e.g., conformed dimensional tables that compose multiple sources for a use case like the fall respiratory virus response) is still gated on real demand and is not built ahead of need.

Schemas in the integrated catalog follow the concept-naming pattern (`geography`, `time`, `pathogen`, `codes`, `surveillance`, `healthcare`, `population`, `movement`, `environment`, `one_health`, `response`, `global`). Table names follow ADR 0015: reference tables (from `_reference` bundle) carry no suffix; derived analytical content uses Kimball-style `_fact`, `_dim`, and `_bridge` suffixes.

Schema ownership in the integrated catalog is per *table*, not per *schema*. Multiple bundles may write into the same concept schema — `_reference` writes the canonical reference tables; future subject bundles or a `forecasting` bundle may write analytical outputs into the same schema. Each table's owning bundle is recorded in `_ops.dataset_engineering.pipeline_reference` (per ADR 0008).

Cross-catalog reads are supported by Unity Catalog and used directly in SQL (`ecdh_dev.wastewater.sample_concentration` joined to `ecdh_model_dev.geography.county`). DAB targets define two catalog variables so pipelines can reference `${var.source_catalog}` and `${var.model_catalog}` without hardcoding names. Source-aligned tables declare informational `FOREIGN KEY` constraints into `ecdh_model_<env>` reference tables to make the standardization auditable (per ADR 0014).

## Alternatives considered
- **Single catalog, mixed schema patterns.** Rejected. Schema list becomes semantically inconsistent (`wastewater_raw`, `geography`, `vaccine_processed`, `pathogen`). Users have to learn two patterns to parse the catalog. Grant patterns collide.
- **Single catalog, integrated model as schemas prefixed with `model_*`.** Rejected. Better than the prior alternative but still couples source-aligned and integrated grant patterns in one catalog. Splitting at the catalog level gives cleaner ownership and access boundaries.
- **Separate workspaces.** Rejected. Too heavy for the actual concern (which is schema organization, not workspace-level isolation). Adds operational overhead with no proportional benefit.
- **Build the integrated catalog now.** Rejected. We don't have a real use case yet, and pristine integrated models built ahead of demand typically don't survive contact with the first real use case. Lock in the pattern, defer the work.

## Consequences
- **Cleaner schema semantics within each catalog.** Every schema in `ecdh_<env>` follows `<subject>_<layer>`. Every schema in `ecdh_model_<env>` will be a concept.
- **Independent grant models.** Source-aligned catalog uses the suffix-based access tier (engineers on `*_raw`/`*_processed`, users on bare-subject). Integrated catalog will likely have a flatter access model (read access for all analysts, write restricted to the modeling team).
- **DAB requires two catalog variables.** `source_catalog` and `model_catalog` in `databricks-common.yml`, with different values per target. Pipelines reference `${var.source_catalog}` or `${var.model_catalog}` as appropriate. The `_platform` bundle (ADR 0004) owns both catalogs.
- **Cross-catalog joins are slightly heavier syntactically.** Three-level table references become normal. Unity Catalog handles this without performance penalty.
- **Integrated layer is gated by use case, not by architecture readiness.** When someone needs to compose data across sources for a real analysis, that analysis becomes the seed for the first integrated tables — not an abstract design exercise.
- **Ad-hoc cross-source analysis has a home in the meantime.** Until the integrated catalog exists, cross-source work that doesn't yet warrant the conformed model lives in `ecdh_<env>.projects.<topic>` (e.g., `ecdh_dev.projects.respiratory_response_2026`). Promotion path: prove value in `projects.*` → conform definitions → graduate to `ecdh_model_*`.
