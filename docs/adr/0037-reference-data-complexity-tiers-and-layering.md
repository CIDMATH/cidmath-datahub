# 0037 — Reference-data complexity tiers and raw→processed layering

## Status
Proposed. **Amends ADR 0014** (reference-data scope/structure) and **ADR 0030** (ICD hierarchy:
flat → layered, additive); **extends ADR 0036** (the shared builder gains a multi-stage mode).
Relates to 0001 (raw/processed/analysis layering), 0003 (source vs integrated catalogs), 0021/0028
(geography grains + enriched views), 0032 (raw Volume snapshots), 0034 (vintage model). Triggered by
the observation that the one-shot reference pattern oversimplifies hierarchical / multi-grain
reference data.

## Context
ADR 0014 framed reference data as **one-shot builds** that download → parse → normalize → derive
structure → enrich → write the canonical table directly in the model catalog. That fits *simple
lookups*, but it oversimplifies genuinely **hierarchical or multi-grain** reference data in two ways:

- The build script does too much in one step (e.g. `build_icd10cm.py` parses, derives the
  adjacency/ancestry, denormalizes chapter/block, and writes — all at once), which is hard to test,
  reprocess, or audit stage-by-stage.
- The resulting **flat** table can't express the source's internal structure: ICD-10-CM has a real
  chapter → block → category → code hierarchy; geography has state → county → tract → block-group →
  block grains. Flattening forces these into a schema they don't fit (the concern that motivated
  this ADR).

Source-aligned data already has the right machinery — **raw (as-is) → processed (conformed/enriched)**
layering (ADR 0001), used by weather. The realization: **complex reference data is just
source-aligned *dimension* data**, so it should use the same workflow. The single-step path
(validated by the flat ICD-10-PCS build) stays correct for simple lookups; it just isn't the only
path.

## Decision
1. **Tier reference data by complexity; the tier picks the workflow.**
   - **Simple → single-step, flat** (ADR 0014 pattern via the ADR 0036 builder): one canonical table,
     no raw/processed split. E.g. CVX, MVX, a state/FIPS list, race/ethnicity, units, ICD-10-PCS.
   - **Complex → raw → processed layering**: ingest constituents as-is into raw, derive structure +
     enrich in processed, promote a canonical/enriched consumer table. E.g. ICD-9/10-CM (hierarchy),
     geography (multi-grain), LOINC Parts.

2. **Criterion for "complex"** — any one triggers layering:
   (a) **internal hierarchy/levels** the consumer will traverse or roll up (ICD chapters/blocks; LOINC
   Parts); (b) **composes from multiple constituent grains or source files** that are individually
   meaningful (geography levels; multi-file releases); (c) **one source feeds multiple downstream
   shapes** (a flat lookup *and* level tables *and* an enriched table). None apply → simple tier.

3. **Placement: source catalog for raw + intermediate; model catalog for the canonical.** Complex
   reference raw + processed-intermediate tables live in the **source catalog** (`ecdh_dev` /
   `ecdh_prod`) in `<subject>_raw` / `<subject>_processed` schemas — treating complex-reference
   ingestion as the source-aligned activity it is (ADR 0003) — and the **canonical/enriched** table is
   promoted to the **model catalog** (`ecdh_model_*`) under its subject schema (`codes` / `geography`),
   where consumers and conformance expect it. (Same shape as weather; the difference is reference
   promotes a canonical dimension upward.)

   **Placement follows role, not only the dataset's own tier.** The tier (decision 1) picks the
   *workflow* (single-step vs layered); it does not by itself fix the *catalog*. A **simple** reference
   dataset that is an **input to a complex subject's processed layer** — an augmenting classification
   such as **RUCA** (Rural-Urban Commuting Area: a flat 1–10 code per census-tract / ZCTA geoid), SVI,
   ADI, urbanicity — is still ingested **single-step** (its own tier is simple), but its **raw lands in
   the consuming subject's `*_raw` (source catalog)** so that subject's processed step joins it as a
   same-catalog dependency and the source→model flow is preserved (no model→source back-reference). If
   the classification is also wanted as a direct standalone lookup, promote a canonical to the model
   catalog (e.g. `geography.us_tract_ruca`); otherwise denormalize it onto the enriched table. Such
   augmenting inputs are vintage-stamped *and* coded to a geography vintage, so the ADR 0035
   `(geoid, geo_vintage)` conformance contract applies to their keys.

4. **Layering *adds*, doesn't replace, the flat consumer table.** The canonical flat/enriched table
   (e.g. `codes.icd10cm` with denormalized chapter/block) stays the **conformance workhorse** — facts
   join one code, not five level tables. The per-level/hierarchy tables are *additive* analytical
   structure derived in processed. So a complex source yields: raw constituents + processed
   level/derived tables + the promoted canonical table.

5. **Raw = the Volume snapshot *and* a raw table.** Keep the immutable Volume snapshot of the source
   artifact (ADR 0032) for fidelity/reproducibility, **and** land a raw *table* (parsed but
   unstructured, 1:1 with source rows) so processed steps build from a queryable raw layer without
   re-parsing. (Simple tier: Volume snapshot stays optional, as today.)

6. **The vintage model carries through unchanged (ADR 0034).** Every layer is vintage-stamped with
   `vintage_snapshot` semantics; the vintage key flows raw → processed → canonical; immutability and
   the currency vocabulary apply per layer.

7. **The shared builder (ADR 0036) gains a multi-stage mode.** Extend `ReferenceTableSpec` /
   `build_reference_table` from single-table to a small **pipeline** — a raw-ingest stage plus one or
   more processed-transform stages plus the promote+register of the canonical table (e.g.
   `build_reference_pipeline([...])`). Single-step remains the simple-tier path. Each stage still runs
   through `run_build` and inherits the converged conventions (atomic write, `ingested_at`, `TableDQ`,
   registration).

8. **Proving grounds — both shapes.**
   - **Geography block-group + block** proves the **multi-grain composition** shape: raw per-level
     tables → processed enrichment/joins. Greenfield (no live-table rework) and clears ledger-deferred
     grains.
   - **ICD-10-CM relayered** proves the **hierarchy** shape: raw (order file / tabular XML) → processed
     chapter/block/category level tables → enriched canonical `codes.icd10cm`. This is a rework of a
     live table — treat as a careful migration (data is reproducible from CDC; drop+rebuild per the
     established runbook), and it amends ADR 0030's flat-hierarchy decision.

## Alternatives considered
- **Layer all reference data.** Rejected: ceremony for simple lookups; the single-step path is
  correct and proven for flat code systems (YAGNI).
- **Keep all reference one-shot (status quo).** Rejected: forces hierarchical/multi-grain data into
  flat schemas it doesn't fit, and the build scripts already strain doing everything in one step.
- **Hierarchy via denormalized columns / views only (0028, 0030).** Partial: denormalization and the
  enriched views serve the *flat-consumer* need and stay — but they don't give queryable **per-level
  tables** for rollups/joins. The complex tier adds those; it doesn't discard the views.
- **Model-catalog-only placement.** Rejected (per decision 3): mixes raw source ingestion into the
  integrated catalog, against ADR 0003.

## Consequences
- ADR 0014 is amended (reference is **tiered**, not uniformly one-shot); ADR 0030 is amended for ICD
  (flat → layered, additive); ADR 0036 is extended (multi-stage builder).
- New `<subject>_raw` / `<subject>_processed` schemas in the **source** catalog for complex subjects;
  the canonical table is promoted to the model catalog. Per-layer `_ops` registration + grants apply
  (raw/processed engineer-only; canonical reader-tier).
- More tables per complex source (raw + level + canonical) — justified by the analytical capability
  (hierarchy rollups, multi-grain joins) and the auditability/reprocessability of split stages.
- **Migration:** ICD-10-CM relayer is a rework (reproducible drop+rebuild); geography BG/block is
  additive/greenfield. Both are the proving grounds; the simple tier is unchanged.
- A **decision tree** ("does this source need layering?") belongs in the authoring guide / ADR 0036 so
  contributors pick the tier consistently. The coverage ledger moves BG/block from *deferred* to
  *in-progress* when the geography test lands.
- Risk — **over-tiering**: contributors layering simple sources for symmetry. Mitigation: the
  criterion (decision 2) is the gate, and the default is simple/single-step unless a complexity
  trigger is met.

## Implementation notes (non-normative)
Decision tree for a new reference source:
```
Hierarchy/levels consumers traverse OR multiple meaningful grains OR multiple downstream shapes?
   ├─ no  → SIMPLE: single-step build → flat canonical table (model catalog)
   └─ yes → COMPLEX: raw tables (source catalog) → processed level/enriched tables
            → promote canonical/enriched table (model catalog)
```
Sketch of the complex pipeline (one `run_build` per stage; ADR 0036 multi-stage):
- **raw**: ingest each constituent as-is into `<subject>_raw.<thing>` (+ Volume snapshot), vintage-stamped.
- **processed**: derive level/hierarchy tables and joins into `<subject>_processed.*`.
- **canonical**: build/enrich the consumer table into `ecdh_model_*.<subject>.<table>`, register + grant.

First implementations = the two proving grounds above; sequence each as its own PR.
