# I2 — `_ops` metadata model (+ the registration writer, U2) — findings

**Date:** 2026-06-17 · **Scope:** `bundles/_platform/src/setup_ops_tables.py` (the `_ops` schema +
`dataset_catalog_full` / `discovery.datasets` views), `common/registration.py` (the writer + entry
dataclasses), checked against the live `_ops` table list (A1) and the build call sites. Part of the
SDE review plan.

## Verdict
The model is well-architected — but **the writer populates only a subset of the schema it writes
into**, so several defined columns are structurally always-null (some surfaced to analysts), run
history isn't captured at all, and two controlled-vocab fields have drifted from their documented
sets and aren't validated. No must-fix, but real "promises more governance than it delivers" gaps.

## What's solid
- Clean three-part split — `dataset_catalog` (universal provenance) / `dataset_engineering`
  (engineering state) / `dq_results` (DQ log) — plus taxonomy + provider-code reference tables, all
  ADR-backed (0008/0009/0005/0006).
- `register_dataset` is idempotent (MERGE on `full_table_name`) and the dataclasses validate
  `update_semantics` + `materialization_type` at construction.
- The analyst discovery surface is well-designed: `discovery.datasets` is a curated view over `_ops`
  exposed to readers via the UC view-ownership chain (ADR 0019), and the setup job **verifies** the
  tiers — including the negative `verify_schema_no_access(analysts, _ops)` (the security-critical
  assertion).
- `dq_status_last` is pragmatically *derived* in the views (latest result per check, rolled up)
  rather than read from the perpetually-null engineering column — with a comment explaining why.

## Findings

### SHOULD-FIX
1. **Writer ⊂ schema: ~11 `dataset_catalog` columns are never populated**, several of them surfaced
   to analysts as perpetually-null. `DatasetCatalogEntry` (and the MERGE) omit `source_dataset_name`,
   `preprocessing_notes`, `data_suppression_notes`, `missingness_notes`, `demographic_resolution`,
   `demographic_coverage`, `refresh_cadence`, `reporting_lag`, `revision_cadence`,
   `external_maintainer_email`, `domain_metadata_misc`. The `discovery.datasets` view *selects*
   `missingness_notes`, `data_suppression_notes`, `demographic_*`, `refresh_cadence`, `reporting_lag`,
   `revision_cadence` — so the analyst-facing surface advertises columns that are **always null**.
   Notably, ADR 0007 names `revision_cadence` / `reporting_lag` as *the* mechanism for recording
   source revision behavior — currently unreachable. → Add the consequential fields (at least
   cadence / lag / revision) to `DatasetCatalogEntry`, and drop or defer the rest from the DDL + view
   so discovery doesn't show empties.

2. **`pipeline_runs` is defined but has no writer — run history isn't captured.** Nothing writes it,
   and the `common.pipeline_runs` helper the DDL comment references **doesn't exist**. So there's no
   record of when a build ran, succeeded/failed, what it wrote, or who triggered it. `run_build` is
   the natural writer (it already has `run_id` / `pipeline_reference` / start / end / success-or-raise).
   → Wire `run_build` to write a `pipeline_runs` row, or drop the table and its claim. (This is also
   the run-observability gap O1 will care about.)

3. **`dataset_engineering` has consequential unwired columns.** `freshness_sla_hours` (freshness
   alerting), `history_table` (the `merge_scd2_side` contract ADR 0007 *requires* populated — needed
   the moment ADR 0034's SCD2 escalation lands), plus `partition_columns`, `pipeline_run_id_last`,
   `ingestion_watermark`. None are settable via `DatasetEngineeringEntry`. At least
   `freshness_sla_hours` + `history_table` should be wireable.

4. **Controlled-vocab drift + missing validation.** `layer`: the DDL comments
   "raw / processed / analysis / model", but the code uses **`reference`** (14×) and `processed`
   (1×) — `reference` isn't in the documented set and `model` is documented but unused. `access_tier`:
   DDL comments "public / restricted / commercial", but code uses **`open`** / `restricted` —
   `open` ≠ `public`, `commercial` unused. And only `update_semantics` + `materialization_type` are
   validated at construction; `layer` / `access_tier` / `subject` / `source_provider_code` /
   `spatial_resolution` are not. → Reconcile each vocab (doc ↔ usage), promote them to validated
   controlled vocabularies (`vocabularies.py` + dataclass `__post_init__`), and confirm CI (ADR 0016)
   covers them.

### CONSIDER (low)
5. The `dq_status_last` roll-up subquery is duplicated **verbatim** in both `dataset_catalog_full`
   and `discovery.datasets`. Factor it (e.g. a small `_ops.dataset_dq_status` view both select from).
6. `last_validated` is set to `CURRENT_DATE()` on every `register` even when no validation ran —
   conflates "registered/refreshed" with "validated." Minor semantic.

## Pre-mortem
The failure mode is **silent metadata rot**: `discovery.datasets` is the analyst's source of truth
for "what exists, how fresh, how often the source revises," but `refresh_cadence` / `reporting_lag` /
`revision_cadence` / freshness are perpetually null and `pipeline_runs` is empty — so "when did this
last run, how stale is it, how often does upstream revise" can't be answered, despite columns
existing to answer them. The governance surface looks complete and isn't, because the writer is a
strict subset of the schema.

## Ties to ADR 0036 (the shared builder)
All four SHOULD-FIX items are *registration* concerns, and ADR 0036's builder owns the registration
scaffolding — so it's the single place to close them: wire the missing catalog/engineering fields,
write `pipeline_runs` from `run_build`, and validate the controlled-vocab fields centrally. These
fixes should ride along with the builder rather than being applied N times per build.
