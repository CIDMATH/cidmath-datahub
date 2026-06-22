---
name: Reference subject (raw → processed → canonical)
about: Add or change reference data — sourced or generated — via the uniform ADR 0037 path on the shared builder (ADR 0036)
title: "<subject>: <short imperative summary>"
labels: ["reference-data", "data-hub"]
---

<!--
For the assignee (and their Claude Code): read CLAUDE.md and the relevant ADRs first —
especially 0037 (the uniform reference path: raw → [processed] → canonical), 0036 (shared
builder), 0014 (reference scope), 0034 (vintage model), 0003 (catalog split / raw-landing
layer), 0011 (pure logic), and — if it conforms to geography — 0035. This issue is the
task-specific spec; the standing docs hold the conventions — point your agent at them, don't
restate them. Delete placeholder text as you fill each section.

USE THIS TEMPLATE for any REFERENCE dataset — sourced (fetched) or generated (computed).
A source-aligned FACT (not a dimension) uses "New subject bundle" instead.
-->

## Goal
<!-- One or two sentences: the canonical reference output, and any levels/grains it exposes. -->

## Source / generation spec (docs-first)
- **Origin:** `fetched` (external source) or `generated` (computed by our logic).
- **Provider code (ADR 0006):** <!-- fetched: e.g. cms / cdc / nlm — add to ADR 0006's registry if new. generated: n/a. -->
- **Artifact / logic:** <!-- fetched: the file(s)/API + URL + documentation URL (docs-first). generated: the rule/algorithm and its inputs. -->
- **Access:** <!-- fetched: format; auth/secret? (use the shared secret helper). generated: n/a. -->
- **Cadence & history:** <!-- versioned per release? revise-in-place? → raw Volume snapshot (ADR 0032, fetched only). -->
- **License / DUA / access_tier:** <!-- public domain? DUA? citation? -->

## Path (ADR 0037 — uniform: raw → [processed] → canonical)
- **raw** (`ecdh_<env>.<subject>_raw`, source/landing catalog): the fetched-as-is **or** generated
  output, 1:1, vintage-stamped (+ Volume snapshot for revise-in-place fetched sources).
- **processed** (`ecdh_<env>.<subject>_processed`, source catalog) — **OPTIONAL, only if complex**
  (below): derive level/grain tables and enrich.
- **canonical** (`ecdh_model_<env>.<subject>.<table>`, model catalog): the promoted enriched/flat
  table — the conformance workhorse. Layering *adds* level tables; it doesn't drop the flat canonical.

**Processed stage? (ADR 0037 criterion — any one ⇒ include it; else `raw → promote`):**
- [ ] internal **hierarchy / levels** consumers traverse or roll up
- [ ] composes from **multiple constituent grains or files**
- [ ] **one source feeds multiple downstream shapes**

**Augmenting a complex subject?** (e.g. RUCA/SVI on geography) — land raw in *that subject's*
`<subject>_raw`, promote a standalone canonical only if wanted directly; conform to its vintage
(`(geoid, geo_vintage)`, ADR 0035).

## Builder + conventions (ADR 0036)
Build via `build_reference_table` (`raw → [processed] → promote canonical → register → grant`); skip
the processed stage when simple; run the generator in place of a fetch for the raw step when
generated. Inherited by construction: `vintage_snapshot` + atomic `replaceWhere`, `ingested_at`,
**no `_current` views**, `TableDQ`, schema-declared-once, pure logic in `src/cidmath_datahub/<subject>/`.

## Tables & semantics
- **raw:** <!-- <subject>_raw.<thing>(s); PK incl. vintage key -->
- **processed (if complex):** <!-- <subject>_processed.<level> tables + PKs; the hierarchy/grain joins -->
- **canonical:** <!-- ecdh_model.<subject>.<table>; PK (key, vintage); shape -->
- **vintage key:** <!-- snapshot_date | edition_year | vintage | <version> — flows through all layers (ADR 0034) -->

## DQ (ADR 0009/0029 — TableDQ + injected)
- **Blocking (FAIL + raise):** <!-- PK uniqueness; non-null keys; (if layered) hierarchy/parent referential integrity within vintage; domain shape checks -->
- **WARN:** <!-- cardinality bands; canonical↔level parity (if layered); freshness -->

## Scope
**In:** <!-- raw (+ processed if complex) + canonical; which vintages/grains -->
**Out (separate issues):** <!-- crosswalks, extra grains/levels, deeper decomposition -->

## Acceptance criteria
- [ ] Built via `build_reference_table` (ADR 0036): `raw → [processed] → promote canonical`; logic in `src/cidmath_datahub/<subject>/`, **unit-tested**.
- [ ] Raw in the **source/landing catalog** (`<subject>_raw`); canonical promoted to the **model catalog** (ADR 0037).
- [ ] `vintage_snapshot` + atomic write; `ingested_at`; **no `_current` views**; vintage key flows through all layers (ADR 0034).
- [ ] Processed stage present **iff** a complexity trigger applies; if layered, the flat canonical stays the conformance table.
- [ ] DQ via `TableDQ` (common) + injected domain checks; referential integrity recorded where layered.
- [ ] `_ops` registration per layer (raw/processed engineer-only; canonical reader-tier); grants verified (ADR 0008/0018).
- [ ] ADR added/updated if a decision was made (+ README index); coverage ledger updated if grains/vintages changed.
- [ ] `ruff format src tests && ruff check src tests` clean (+ changed bundle files); `pytest -q` green; `databricks bundle validate --target dev` passes.

## Verification (after dev deploy + run)
<!-- Per layer: PK uniqueness, (hierarchy) referential integrity, cardinality bands, canonical↔level
parity, _ops.dq_results blocking checks passed, discovery.datasets row present. -->

## Notes
<!-- Source/generation quirks, data dictionary link, sequencing/dependencies. -->
