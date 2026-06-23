# Architecture Decision Records

This directory holds Architecture Decision Records (ADRs) for the CIDMATH Data Hub. Each ADR captures a single architectural or design decision: the context that motivated it, the decision itself, the alternatives considered, and the consequences accepted.

## Why ADRs exist

Every architectural decision feels obvious in retrospect, but six months later nobody remembers the reasoning. Without a written record, decisions get relitigated when new contributors arrive, sometimes reversed in ignorance of why they existed. An ADR is the answer to "we already thought about that — here's what we decided, and why."

ADRs are also the diff history of the architecture. When a decision no longer fits, you don't delete the old ADR; you write a new one that references it and marks the old one as **Superseded**.

## When to write one

Write an ADR for any decision that:

- Will be hard to reverse (schema names, catalog structure, bundle layout)
- Future contributors might second-guess without context
- Reflects a real trade-off between alternatives rather than an obvious best path
- Establishes a convention others will be expected to follow

You do **not** need an ADR for routine implementation choices (which lint rule, what to name a local variable, how to structure a single function).

## Format

Each ADR is a short markdown file — three to five paragraphs is the target. Filename pattern: `NNNN-short-kebab-case-title.md`. Numbers are sequential and never reused.

### Template

```markdown
# NNNN — Short title in sentence case

## Status
Proposed | Accepted | Superseded by NNNN — YYYY-MM-DD

## Context
What problem you were facing. What constraints applied. Why this decision had
to be made. Two or three paragraphs of background.

## Decision
What you chose. Be specific. Include code snippets, schema fragments, or
diagrams if they clarify.

## Alternatives considered
What else was on the table and why each was rejected. One bullet or short
paragraph per alternative. The point is to show the decision was deliberate.

## Consequences
What becomes easier, what becomes harder, what risks you've accepted. Include
both positive and negative consequences honestly.
```

### Status values

- **Proposed** — drafted, not yet accepted. Used during PR review.
- **Accepted** — current decision, in effect.
- **Superseded by NNNN** — replaced by a later ADR. Both files stay in the repo; the new ADR explains what changed.

## Workflow

1. A new architectural decision comes up in discussion.
2. The person proposing the decision writes an ADR in **Proposed** status and opens a PR.
3. Reviewers discuss the ADR (not the implementation) until consensus.
4. Merge moves status to **Accepted**. Implementation work proceeds.
5. If the decision later changes, a new ADR is written that explicitly supersedes the old one. The old ADR's status is updated to **Superseded by NNNN**.

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-layering-vocabulary.md) | Layering vocabulary: raw / processed / analysis | Accepted |
| [0002](0002-schema-is-subject-not-source.md) | Schema represents a subject area, not a source | Accepted |
| [0003](0003-catalog-split-source-vs-integrated.md) | Source-aligned and integrated/modeled data live in separate catalogs | Accepted (revised) |
| [0004](0004-monorepo-bundle-per-domain.md) | Monorepo with multiple Databricks Asset Bundles, one per subject area | Accepted (revised) |
| [0005](0005-discovery-tags-and-dataset-catalog.md) | Data discovery via Unity Catalog tags and a curated dataset catalog | Accepted (revised) |
| [0006](0006-table-and-column-naming.md) | Table and column naming conventions | Accepted |
| [0007](0007-update-semantics-and-history.md) | Table update semantics and history handling | Accepted |
| [0008](0008-catalog-metadata-schema-design.md) | Catalog metadata schema design: universal base plus per-domain extensions | Accepted (revised) |
| [0009](0009-data-quality-framework.md) | Data quality framework | Accepted (revised) |
| [0010](0010-observability-and-alerting.md) | Observability and alerting | Accepted |
| [0011](0011-testing-strategy.md) | Testing strategy | Accepted |
| [0012](0012-identity-and-deploy-authorization.md) | Identity, service principals, and deploy authorization | Accepted (revised) |
| [0013](0013-documentation-standards.md) | Documentation standards | Accepted (revised) |
| [0014](0014-reference-data-scope.md) | Reference data: scope, bundle structure, and standardization via informational foreign keys | Accepted (revised) |
| [0015](0015-integrated-table-naming.md) | Integrated catalog table naming (Kimball suffixes for derived analytical content; reference tables unsuffixed) | Accepted |
| [0016](0016-ci-enforcement-policy.md) | CI enforcement policy (hybrid — controlled-vocabulary membership and dataset_catalog row presence enforced; everything else documented-only) | Accepted |
| [0017](0017-bootstrap-lessons.md) | Bootstrap lessons: friction we hit getting the platform live | Accepted |
| [0018](0018-access-groups-and-grant-model.md) | Access groups and the grant model (engineer / reader tiers; which bundle grants what) | Accepted |
| [0019](0019-analyst-discovery-view.md) | Analyst-facing discovery view (curated `discovery.datasets` over `_ops` via view ownership chain) | Accepted |
| [0020](0020-geography-reference.md) | Geography reference: IPUMS NHGIS source, vintaged snapshots + crosswalks, lean attributes + companion WKB geometry | Accepted |
| [0021](0021-geography-crosswalks.md) | Geography crosswalks: ship NHGIS bg-sourced 2010↔2020 files as published, normalized into one long-form table | Accepted |
| [0022](0022-geography-international-scope.md) | Geography reference: international scope (ISO 3166-1/2 codes + GADM boundaries; new country, country_subdivision, subnational tables) | Accepted |
| [0023](0023-shared-pipeline-helpers-and-gadm-matching.md) | Shared pipeline helpers (GADM IO module) and multi-tier ISO↔GADM subdivision matching (code → name → fixup) | Accepted |
| [0024](0024-international-geography-vintaging.md) | International geography vintaging — align country/country_subdivision/subnational with the US vintage model (amends ADR 0022 temporal sub-decision) | Accepted |
| [0025](0025-weather-subject-bundle-nclimgrid.md) | Weather subject bundle: NOAA nClimGrid-Daily county+state area-averages (first source-aligned subject; NCEI→FIPS conformance to geography + time) | Accepted |
| [0026](0026-job-vs-ldp-pipeline-selection.md) | Job vs Lakeflow Declarative Pipeline selection — consolidated criterion (when to use a Job vs LDP), DQ implications, analysis-layer LDP pilot | Accepted |
| [0027](0027-bundle-authoring-and-pipeline-standardization.md) | Bundle authoring + pipeline standardization — `common/pipeline.run_build` orchestration seam, thin-entrypoint contract, DAB template + authoring-guide plan (closes the pipeline-standardization backlog item) | Accepted |
| [0028](0028-geography-hierarchical-filter-views.md) | Geography hierarchical-filter views — `us_county_enriched`/`us_tract_enriched` denormalize parent labels via vintage-keyed joins for filtering child geographies by readable parent; views over denormalized columns (first `run_build` adopter) | Accepted |
| [0029](0029-dq-check-helper-library.md) | Reusable DQ-check helper library — pure SQL builders + bound `TableDQ` (`unique`/`not_null`/`fk`/`cardinality`/`rowcount_equals`) single-source the recurring check+record+raise; bespoke checks stay inline (extends 0009 within the 0027 seam) | Accepted |
| [0030](0030-icd10-hierarchy.md) | ICD-10-CM hierarchy — adjacency list + materialized path + denormalized chapter/block on `codes.icd10cm`, sourced from the tabular XML's chapter→section→diag nesting (prefix-rule fallback for 7th-char codes); additive, edition-scoped | Accepted |
| [0031](0031-icd9-hierarchy.md) | ICD-9-CM hierarchy + shared code-system hierarchy contract — `codes.icd9cm` mirrors `codes.icd10cm`'s columns/semantics; adjacency from the prefix rule (primary, inverting 0030), chapter/block from NCHS Appendix E (`DC_3D` RTF); standalone module, frozen base editions | Accepted |
| [0032](0032-source-history-preservation.md) | Source-history preservation for revise-in-place sources — raw immutable Volume snapshots + in-table revision tracking via `snapshot_replace` (keyed by `snapshot_date`, geography-style); when to use vs SCD2. First applied to `codes.cvx` | Accepted |
| [0033](0033-catalog-grant-governance-and-drift-check.md) | Catalog-grant governance + drift check — keep schema-and-below grants in the deploy pipeline but catalog-level grants governance-owned (never the deploy SP); make `grant_catalog_permissions.sql` a drift-checked source of truth (`scripts/verify/audit_catalog_grants.py`). Amends 0012/0018 | Accepted |
| [0034](0034-vintage-retained-snapshot-semantics.md) | Vintage-retained snapshot semantics — `update_semantics="vintage_snapshot"` (atomic per-vintage `replaceWhere`); vintages immutable (revisions = new vintage key, never in-place overwrite); SCD2 escalation rule with a named trigger; currency vocabulary (`is_current` / `is_latest_vintage` / `live`) and the deferred `in_force_from` applicability axis. Amends 0007 | Proposed |
| [0035](0035-fact-geography-vintage-conformance.md) | Fact-to-geography vintage conformance — geography-conformed facts carry the geography `vintage` they're coded to; canonical join `(geoid, geo_vintage)`; basis set at conform time, not from obs date; nClimGrid basis determined = 2020. Extends 0023; relates 0020/0028/0034 | Proposed |
| [0036](0036-shared-reference-table-builder.md) | Shared reference-table builder — `ReferenceTableSpec` + `build_reference_table` (config/composition) over `run_build`; owns the recurring skeleton (atomic vintage write, register/grant scaffolding, IO, argparse, row-derivation) so conventions (0034 / `ingested_at` / `TableDQ`) hold by construction; parse + DQ + schema injected; prove greenfield on ICD-10-PCS then a hard case. Extends 0027 | Proposed |
| [0037](0037-reference-data-complexity-tiers-and-layering.md) | Reference-data ingestion path — **one uniform path for ALL reference** (`raw → [processed] → canonical`): raw always lands in the source/landing catalog, canonical always promoted to model. The **processed stage is optional, gated by complexity** (hierarchy / multi-grain / multiple shapes), not by catalog. **Generated** reference (time) follows the same path (its raw = the generator output) — no carve-out. Augmenting inputs (RUCA/SVI) land raw in the consuming subject. Extends 0003's framing (source = origin-agnostic raw/landing layer); amends 0014/0030; simplifies/extends 0036. Proving grounds: geography BG+block + ICD-10-CM relayered | Proposed |
| [0038](0038-ruca-rural-urban-commuting-areas.md) | RUCA rural-urban commuting-area codes — USDA ERS RUCA in `geography` as two per-level tables (`us_ruca_tract` keyed `(geoid, vintage)` as a `us_tract` attribute extension; `us_ruca_zip` keyed `(zip_code, vintage)`, ≥2010); primary+secondary codes verbatim; `snapshot_replace` per decennial vintage (non-comparable across decades); alias-tolerant header parsing; mirrors the ICD-10-PCS build. Extends 0020/0014; simple tier per 0037 | Proposed |
| [0039](0039-raw-payload-landing-zone.md) | Raw payload landing zone — every **extracted** source payload (file OR API/JSON/query, format-faithful) lands verbatim in an engineer-only UC **Volume** in the source catalog before parsing; the 1:1 raw Delta is built from it (zero-API rebuilds/reprocessing + fidelity/replay). **Retention mode mirrors `update_semantics`**: per-vintage-immutable (fetch-once/skip-if-present) / snapshot-per-run / per-batch / none (generated). Builder (0036) gains a **Phase 0** + `RawLanding.fetch_to_volume`/`read_from_volume`. Extends + partially supersedes 0032 (re-pullable sources now keep a local copy); refines 0037 | Proposed |

## Future ADRs (backlog)

The following are known gaps to be addressed when the need is concrete. Captured here so they're not forgotten and so contributors know we deliberately deferred them rather than missed them.

| Anticipated # | Topic | Likely trigger |
|---|---|---|
| ~~TBD~~ | ~~**Pipeline standardization and modular composition**~~ | **Resolved by ADR 0027** (`run_build` seam + thin-entrypoint contract + DAB template/authoring-guide plan), building on ADR 0023 (shared GADM IO) and ADR 0026 (job-vs-LDP). Remaining follow-ons (template, authoring guide, CI guardrails) are tracked in ADR 0027's consequences, not as a separate backlog item. |
| TBD | Schema evolution and breaking-change policy | First time a source adds a column or we need to rename one |
| TBD | Data retention and VACUUM policy | When storage cost or commercial-data contract expiry forces the question |
| TBD | Notebook-to-pipeline graduation policy | First exploratory notebook that becomes a candidate for productionization |
| TBD | Per-domain metadata extension ADRs | One per domain extension as it lands (e.g., `dataset_surveillance` schema) |
| TBD | Type system conventions | Emerges from first pipeline (decimal vs. double, identity columns, etc.) |
| TBD | Performance: partitioning, clustering, optimize cadence | When a real workload demands tuning |
| TBD | Lakehouse Monitoring rollout | After 2-3 stable analysis-layer tables exist |
| TBD | Delta Sharing for external consumers | First external partner request |
| TBD | Cost management and budget alerts | Once meaningful spend accrues |
| TBD | Logging conventions | Stub now; formalize when patterns diverge |
| ~~TBD~~ | ~~**Geography parent-attribute ergonomics (hierarchical filtering)**~~ | **Resolved by ADR 0028** — convenience views (`geography.us_county_enriched`, `us_tract_enriched`) denormalize parent labels via vintage-keyed joins; the chosen views-over-denormalized-columns approach keeps base tables normalized. |

## Further reading

- Michael Nygard's original ADR post: https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions
- ADR GitHub organization (templates and tooling): https://adr.github.io
