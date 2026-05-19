# 0007 — Table update semantics and history handling

## Status
Accepted — 2026-05-15

## Context
Every materialized table in the hub has to answer: what happens when new data arrives? Some answers are obvious — a daily wastewater feed appends new measurements. Others are not — when CDC NWSS restates a measurement from three weeks ago, do we overwrite, keep history, or treat the old value as superseded? When the Census releases the 2030 ACS, does our `population` table replace 2020 ACS or keep both?

These questions span three concerns that are easy to conflate:

1. **Source behavior.** Does the upstream source append, revise, or fully restate? At what cadence? This is metadata (captured in `_ops.dataset_catalog.revision_cadence`, `reporting_lag`).
2. **Materialization semantics.** What writing operation does our pipeline perform — append, upsert (MERGE), full refresh, snapshot? This is what this ADR governs.
3. **History retention.** Do we keep historical versions of revised rows? In the same table (SCD2)? In a side table? In Delta time-travel only?

The old CIDMATH catalog and the Delphi EpiPortal both track upstream revision cadence as metadata but neither documents the *pipeline-side* semantics. Without a uniform policy, contributors will pick patterns inconsistently — one wastewater pipeline does MERGE, another does full refresh, a third quietly drops late-arriving data — and downstream consumers can't tell which.

Delta Lake and Lakeflow Declarative Pipelines (LDP) both offer well-defined primitives for these patterns; this ADR picks defaults and a controlled vocabulary so every pipeline declares its semantics explicitly.

## Decision

### Per-layer defaults

| Layer | Default update semantics | Rationale |
|---|---|---|
| **raw** | `snapshot_replace` (full overwrite each ingestion) OR `append_only` (each ingestion adds a partition) | Raw mirrors the source. Replace when the source is a snapshot file/API; append when the source is a stream of events. Never revise raw — preserve fidelity. |
| **processed** | `merge_upsert` keyed on source primary key | Processed cleans per-source data and handles revisions. MERGE on the source's natural key absorbs revisions and late-arriving rows. |
| **analysis** | Varies — explicit declaration required | Analysis tables compose multiple processed sources; the right semantics depend on the analytical use case. No default — every analysis table declares. |

### Controlled vocabulary for `update_semantics`

This is the field on `_ops.dataset_engineering` (per ADR 0008) that every materialized table populates. Values:

| Value | Meaning | Typical layer |
|---|---|---|
| `append_only` | New rows only. Existing rows immutable. | raw (event-stream sources), analysis (fact-style event tables) |
| `snapshot_replace` | Each refresh fully replaces table contents with the latest snapshot. | raw (snapshot-API sources), analysis (rollup tables) |
| `merge_upsert` | MERGE on a key; new rows inserted, matching rows updated. Late-arriving revisions overwrite. | processed (most), analysis (Type 1 dimensions) |
| `merge_scd2` | MERGE with effective-dated history rows; revised rows close out the previous version and insert a new one. | analysis (entities where "value at point in time" matters) |
| `merge_scd2_side` | Current state in the main table (`merge_upsert` behavior); full history in a companion `_history` table. | analysis (high-traffic current-state tables that still need audit history) |
| `incremental_compute` | Derived table recomputed incrementally from upstream (LDP streaming table or materialized view). | analysis (derivations from one or more processed sources) |
| `full_refresh` | Table fully recomputed from upstream on every run. | analysis (small reference tables, complex aggregations where incremental isn't worth it) |

`is_hosted = false` rows in `dataset_catalog` (catalogued-only) have no `dataset_engineering` row and no update semantics. They're external; we don't manage their refresh.

### SCD policy

**SCD Type 1 is the default** for everything that isn't explicitly time-aware. `merge_upsert` implements it. Old values are not retained in the table; Delta time travel retains them implicitly for the retention window (default 30 days).

**SCD Type 2 (effective-dated history rows) applies when "value at point in time" matters analytically.** Examples: vaccine schedules (the recommended schedule at a point in time, not just today's), policy interventions (the masking rule in effect on a given date), payer mix (the insurance distribution at admission, not now), facility ownership (who operated this facility during the encounter). `merge_scd2` implements it.

**SCD Type 4 (history in a companion `_history` table)** applies when SCD2 would bloat the main table impractically or when the current-state table is hit by high-read workloads that don't need history. `merge_scd2_side` implements it; the companion table is named `<table>_history` (e.g., `wastewater.facility` and `wastewater.facility_history`).

**SCD Type 3 (limited history as additional columns) is not used.** It's a niche pattern with confusing semantics; SCD2 or SCD4 are clearer.

**Reliance on Delta time travel alone is not sufficient for SCD purposes.** Time travel's retention is operational (default 30 days, capped by `VACUUM`) and not designed for analytical history. Use it for debugging and recovery, not for answering "what was the value on 2024-08-15?"

### Revision handling

When the upstream source revises past data:

- **raw `snapshot_replace`:** the new snapshot overwrites; old values exist only via Delta time travel.
- **raw `append_only`:** revisions arrive as new event rows. The source provides a version or timestamp; we trust the source to indicate currency. We do not deduplicate at raw.
- **processed `merge_upsert`:** the MERGE matches on the natural key and overwrites. The previous value is lost from the table but retained via time travel.
- **processed `merge_scd2`:** the MERGE detects a value change, closes the previous row's effective range, and inserts a new row.
- **analysis tables built via `incremental_compute`:** rely on upstream change data feed (CDF) where available, or full recomputation of affected partitions.

### Implementation patterns

**Plain Jobs writing Delta tables (Python/PySpark):**

```python
# merge_upsert
(target.alias("t")
   .merge(source.alias("s"), "t.measurement_id = s.measurement_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
```

**Lakeflow Declarative Pipelines:**

- `append_only` → `@dlt.table` with no apply_changes; or a streaming table from a source that emits inserts only.
- `snapshot_replace` → `@dlt.table` with full recompute, OR `APPLY CHANGES FROM SNAPSHOT` with `STORED AS SCD TYPE 1`.
- `merge_upsert` → `APPLY CHANGES INTO ... STORED AS SCD TYPE 1`
- `merge_scd2` → `APPLY CHANGES INTO ... STORED AS SCD TYPE 2 TRACK HISTORY`
- `merge_scd2_side` → `APPLY CHANGES INTO ... STORED AS SCD TYPE 1` for the main table, plus a separate streaming table that captures change data via CDF for the history side.
- `incremental_compute` → streaming table or materialized view derived from upstream.
- `full_refresh` → `@dlt.table` with `pipelines.reset.allowed = false` only if reset is dangerous; otherwise let it recompute.

LDP's `APPLY CHANGES` is the preferred primitive for merge semantics inside LDP pipelines. Outside LDP (e.g., a one-off Job), use Delta's `MERGE` directly.

### Per-table declaration

Every materialized table declares its update semantics by populating `_ops.dataset_engineering.update_semantics` with one of the controlled-vocabulary values. The pipeline's bundle resource sets this when the table is registered. A CI check verifies the value is in the controlled vocabulary.

When `merge_scd2_side`, the `history_table` field on `dataset_engineering` is also required and must point at a real, registered table.

### History table conventions

- Naming: `<base_table>_history` (e.g., `wastewater.facility_history`).
- Columns: full schema of the base table plus `_effective_from` (timestamp), `_effective_to` (timestamp, nullable for current rows), `_is_current` (boolean), `_change_type` (`insert`, `update`, `delete`).
- Cluster/partition: typically by `_effective_from` and the base table's primary key.
- Querying: history tables are queried with `_effective_from <= as_of_ts AND (_effective_to > as_of_ts OR _effective_to IS NULL)` for point-in-time reads. A view per history table can expose this idiom cleanly.

### What CI checks

- `update_semantics` is in the controlled vocabulary.
- If `update_semantics = merge_scd2_side`, `history_table` is populated and references a registered table.
- If `update_semantics = merge_upsert`, the pipeline definition references a MERGE key (parsed from the bundle resource or asserted in code).

These checks are mechanical and easy. Anything deeper (does the pipeline actually implement the declared semantics correctly?) is a code review concern, not a CI concern.

## Alternatives considered
- **No project-wide policy; let each pipeline decide.** Rejected. Without a policy, every reviewer relitigates the choice and consumers can't predict behavior across tables.
- **Single default for everything (e.g., always `merge_upsert`).** Rejected. Raw layer must preserve source fidelity (no MERGE); analysis tables vary too much by use case to default sensibly.
- **Rely on Delta time travel for all history needs.** Rejected. Time travel is operational (retention bounded), not analytical (point-in-time queries are clunky and become impossible after VACUUM).
- **SCD Type 3 (history-as-columns).** Rejected. Niche, confusing, doesn't scale beyond "previous value" and "previous-previous value."
- **Use LDP exclusively; ignore plain-Jobs MERGE.** Rejected. Some ingestion patterns (one-off file pulls, complex API pagination) fit Jobs better than LDP. The policy needs to cover both.

## Consequences
- **Every materialized table makes its semantics explicit.** Consumers reading `_ops.dataset_engineering` can predict behavior before querying.
- **SCD2 is available without being default.** The cost of SCD2 (write amplification, larger tables, more complex queries) is real; opt-in for the cases that need it.
- **History tables are first-class.** When SCD4 is the right pattern, the companion `_history` table is registered and discoverable, not a side artifact.
- **LDP's APPLY CHANGES becomes the default mechanism inside pipelines.** Engineers don't write hand-rolled MERGE inside LDP; the controlled vocabulary maps to LDP primitives.
- **CI catches the mechanical mistakes.** Wrong vocabulary value, missing history table reference, undeclared MERGE key. Doesn't catch semantic correctness — code review still matters.
- **Revision behavior at upstream and revision handling downstream are decoupled.** Source revision cadence is provenance metadata (`_ops.dataset_catalog.revision_cadence`); how we materialize revisions is engineering metadata (`update_semantics`). They're related but live in different tables and can be reasoned about separately.
- **Migration cost when changing a table's semantics.** Switching from `merge_upsert` to `merge_scd2` is a real migration (the SCD2 columns need backfilling). Treat semantics declarations as durable; changes go through an ADR.
