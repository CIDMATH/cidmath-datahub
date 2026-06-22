---
name: Complex / layered reference data
about: A hierarchical or multi-grain reference dataset using raw→processed layering (ADR 0037) on the multi-stage builder (ADR 0036)
title: "<subject>: <short imperative summary> (layered reference)"
labels: ["reference-data", "data-hub"]
---

<!--
For the assignee (and their Claude Code): read CLAUDE.md and the relevant ADRs first —
especially 0037 (reference-data complexity tiers + raw→processed layering), 0036 (shared
builder + its multi-stage mode), 0014 (reference scope), 0034 (vintage model), 0003
(source vs model catalog), 0011 (thin entrypoints / pure logic). This issue gives the
task-specific spec; the standing docs hold the conventions — point your agent at them,
don't restate them. Delete placeholder text as you fill each section.

USE THIS TEMPLATE only for COMPLEX reference data (ADR 0037). A SIMPLE lookup (flat
code/description, no hierarchy or multi-grain) belongs on the "Data Hub task" template as a
single-step build. A source-aligned FACT (not a dimension) belongs on "New subject bundle".
-->

## Tier check (ADR 0037 — confirm this is the complex tier)
At least one must be true (else this is a simple lookup → use "Data Hub task"):
- [ ] Has an internal **hierarchy / levels** consumers will traverse or roll up.
- [ ] **Composes from multiple constituent grains or source files** that are individually meaningful.
- [ ] **One source feeds multiple downstream shapes** (a flat lookup *and* level tables *and* enriched).

## Goal
<!-- One or two sentences: the canonical reference output, and the levels/grains it exposes. -->

## Source spec (docs-first)
- **Provider code (ADR 0006):** <!-- e.g. cdc / cms / nlm — if new, add to ADR 0006's registry in the same PR -->
- **Dataset / artifact(s):** <!-- the source files/levels ingested as-is -->
- **Source URL + documentation URL:** <!-- docs-first: link the source's own docs -->
- **Format & access:** <!-- file/API; auth/secret needed? (use the shared secret helper) -->
- **Cadence & history:** <!-- versioned per release? revise-in-place? → raw Volume snapshot (ADR 0032) -->
- **License / DUA:** <!-- public domain? DUA? citation? access_tier -->

## Layering (ADR 0037)
- **raw** (`ecdh_<env>.<subject>_raw`, source catalog): each constituent ingested **as-is**, 1:1
  with source, vintage-stamped (+ Volume snapshot per ADR 0032).
- **processed** (`ecdh_<env>.<subject>_processed`, source catalog): derive the level/hierarchy tables
  and the cross-level joins/enrichment.
- **canonical** (`ecdh_model_<env>.<subject>.<table>`, model catalog): the promoted enriched table —
  the **conformance workhorse** consumers join. Layering *adds* the level tables; it does not drop
  the flat canonical table (ADR 0037 decision 4).

## Builder + conventions (ADR 0036 multi-stage)
Build on the **multi-stage** `build_reference_table` (raw → processed → promote canonical) — not a
hand-rolled per-stage script. Inherited by construction: `vintage_snapshot` + atomic `replaceWhere`,
`ingested_at`, **no `_current` views** (use `MAX(vintage)` / the `live` idiom), `TableDQ` for common
checks, schema-declared-once, pure parser in `src/cidmath_datahub/<subject>/` (ADR 0011).

## Tables & semantics
- **raw:** <!-- <subject>_raw.<thing> per constituent; PK incl. vintage -->
- **processed (levels):** <!-- e.g. <subject>_processed.<level> tables + PKs; the hierarchy/grain joins -->
- **canonical:** <!-- ecdh_model.<subject>.<table>; PK (key, vintage); shape (incl. denormalized parents) -->
- **vintage key:** <!-- snapshot_date | edition_year | vintage | <version> — flows through all layers -->

## DQ (ADR 0009/0029 — TableDQ + injected)
- **Blocking (FAIL + raise):** <!-- per-table PK uniqueness; non-null keys; hierarchy/parent referential integrity within vintage; domain-specific shape checks -->
- **WARN:** <!-- per-level cardinality bands; canonical↔level parity; freshness -->

## Scope
**In:** <!-- raw + processed levels + canonical, which vintages/grains -->
**Out (separate issues):** <!-- crosswalks, extra grains/levels, deeper decomposition, etc. -->

## Acceptance criteria
- [ ] Built on the **ADR 0036 multi-stage builder** (raw → processed → canonical); parse/derive logic in `src/cidmath_datahub/<subject>/`, **unit-tested**.
- [ ] Raw + processed-intermediate in the **source** catalog (`<subject>_raw`/`_processed`); canonical promoted to the **model** catalog.
- [ ] `vintage_snapshot` + atomic write; `ingested_at`; **no `_current` views**; vintage key flows through all layers (ADR 0034).
- [ ] Level/hierarchy tables present; canonical stays the flat conformance table (shape stable if reworking an existing one).
- [ ] DQ via `TableDQ` (common) + injected domain checks; hierarchy/parent referential integrity recorded.
- [ ] `_ops` registration per layer (raw/processed engineer-only; canonical reader-tier); grants verified (ADR 0008/0018).
- [ ] ADR added/updated if a decision was made (and the README index updated); coverage ledger updated if grains/vintages changed.
- [ ] Note any place the multi-stage builder had to bend (feeds ADR 0036/0037).
- [ ] `ruff format src tests && ruff check src tests` clean (+ changed bundle files); `pytest -q` green; `databricks bundle validate --target dev` passes.

## Verification (after dev deploy + run)
<!-- Per-layer + per-level: PK uniqueness, hierarchy referential integrity, cardinality bands,
canonical↔level parity, _ops.dq_results blocking checks passed, discovery.datasets row present. -->

## Notes
<!-- Source quirks, the data dictionary link, sequencing/dependencies (e.g. depends on the multi-stage builder landing first). -->
