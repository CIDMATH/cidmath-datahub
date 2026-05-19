# 0009 — Data quality framework

## Status
Accepted — 2026-05-15

## Context
ADR 0008 introduced `_ops.dataset_engineering.dq_status_last` and `dq_results_run_id` as fields that every materialized table populates. ADR 0007 introduced controlled update semantics. Neither defined *how* quality is actually checked, what severity levels mean, what happens when a check fails, or where results land. Without that policy, every pipeline contributor will hand-roll DQ checks differently — some will print warnings to logs and call it done, others will halt pipelines on minor issues, and the resulting hub will have inconsistent reliability characteristics that consumers can't reason about.

Two adjacent realities shape this:

1. **Lakeflow Declarative Pipelines have built-in expectations** (`@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`). They're the path of least resistance for DQ inside LDP and integrate with LDP's event log and pipeline metrics natively.
2. **Plain Jobs writing Delta tables need a different mechanism.** They can't use `@dlt.expect`. Either we hand-roll checks or adopt a library (Great Expectations, Soda, Deequ). Each has trade-offs in dependency weight and learning curve.

The framework needs to be opinionated enough to enforce consistency, flexible enough to support both LDP and Jobs, and lightweight enough that engineers actually use it rather than work around it.

## Decision

### Severity vocabulary

Every DQ check declares one of four severities:

| Severity | What it does | When to use |
|---|---|---|
| `info` | Records the check result; no side effects on the pipeline or data | Tracking expected-but-noisy conditions (e.g., "row count is below typical range") |
| `warn` | Records the result, emits a warning to the run log and observability surface | Soft anomalies that should be investigated but don't compromise the data |
| `quarantine` | Failing rows are diverted to a `<table>_quarantine` companion table; the main table loads the passing rows; the run is flagged | Bad rows that shouldn't reach the main table but shouldn't halt the pipeline |
| `fail` | The pipeline run is aborted; the target table is not updated | Critical violations: schema mismatch, primary key collision, total-row-count zero, source unreachable |

### Per-layer default severities

| Layer | Default severity for typical checks |
|---|---|
| `raw` | `info` for most; `fail` for schema-incompatible sources or zero-row source pulls (those mean ingestion is broken, not data is dirty) |
| `processed` | `warn` for soft issues; `quarantine` for row-level data violations (bad keys, out-of-range values); `fail` for schema or referential failures that would corrupt the table |
| `analysis` | `warn` minimum; `fail` for any violation that would make the table actively misleading (e.g., negative case counts, future dates) |

These are defaults; per-check overrides are fine when justified in code comments.

### Standard check types

Every pipeline includes checks from the following categories where applicable. Not all apply to every table — relevance is judgment, but absence of any check on a category should be explicit.

| Category | Examples |
|---|---|
| **Schema** | Required columns present; column types match expectations; no unexpected columns |
| **Nullability** | Primary keys non-null; required business fields non-null |
| **Uniqueness** | Primary key uniqueness; natural key uniqueness within a partition |
| **Range** | Numeric measurements within plausible bounds; dates not in the future (or past a sensible horizon) |
| **Cardinality** | Row count within expected range vs. historical (e.g., today's load is ±50% of trailing 30-day average) |
| **Referential** | Foreign keys resolve to a row in the referenced table |
| **Freshness** | Max date in the table is within the expected lag from current time |
| **Business rules** | Pipeline-specific invariants (e.g., wastewater concentration ≥ 0; vaccination dose number in {1,2,3,4,5}) |

### Implementation mechanism by orchestration primitive

**Inside LDP pipelines (the default per ADR 0004):**

Use the native expectation decorators directly:

```python
import dlt

@dlt.table(name="wastewater_processed.cdc_nwss")
@dlt.expect("valid_concentration", "concentration_copies_per_ml >= 0")
@dlt.expect_or_drop("non_null_sample_id", "sample_id IS NOT NULL")
@dlt.expect_or_fail("not_empty", "count(*) > 0")
def cdc_nwss_processed():
    ...
```

Map of LDP decorator → severity:

| LDP decorator | Severity in our vocabulary |
|---|---|
| `@dlt.expect(name, condition)` | `warn` |
| `@dlt.expect_or_drop(name, condition)` | `quarantine` (but LDP drops; see note below) |
| `@dlt.expect_or_fail(name, condition)` | `fail` |
| `@dlt.expect_all([...])` | Multiple `warn` |

**Important nuance on quarantine in LDP.** LDP's `expect_or_drop` silently drops failing rows; it does not write them to a quarantine table by default. To get our quarantine semantics, write a paired streaming table that captures the dropped rows from the source via CDF or an explicit filter on the inverse condition:

```python
@dlt.table(name="wastewater_processed.cdc_nwss_quarantine")
def cdc_nwss_quarantine():
    return (
        dlt.read_stream("cdc_nwss_raw")
        .filter("sample_id IS NULL OR concentration_copies_per_ml < 0")
        .withColumn("_quarantined_at", current_timestamp())
        .withColumn("_quarantine_reason", lit("non_null_sample_id OR valid_concentration"))
    )
```

A helper in `cidmath_datahub.common.dq` will encapsulate this pattern so contributors don't reinvent it.

**Inside plain Jobs:**

Use a lightweight in-house DQ helper rather than a heavyweight library at this stage. The helper lives in `cidmath_datahub.common.dq` and provides:

- `check(name, condition_expr, severity)` — evaluates against the working DataFrame
- `quarantine_failing_rows(df, condition_expr, target_table_name)` — writes failing rows to the quarantine companion table
- `record_results(checks, table_name, run_id)` — writes a row per check to `_ops.dq_results`

This keeps dependencies minimal. If/when a Job's DQ needs exceed what the helper provides comfortably (statistical profiling, distribution drift detection, schema inference checks), we'll evaluate Great Expectations or Soda at that point — but only with a real driving use case.

### Where results land: `_ops.dq_results`

Every DQ check execution writes one row to `_ops.dq_results`:

| Column | Type | Purpose |
|---|---|---|
| `run_id` | string | Pipeline run identifier; links to `_ops.pipeline_runs` |
| `pipeline_reference` | string | Bundle path of the pipeline |
| `table_name` | string | Three-level UC name of the table the check ran against |
| `check_name` | string | Short identifier (e.g., `valid_concentration`) |
| `category` | string | One of: schema, nullability, uniqueness, range, cardinality, referential, freshness, business_rule |
| `severity` | string | `info` / `warn` / `quarantine` / `fail` |
| `passed` | boolean | Whether the check passed |
| `failing_row_count` | long | Number of rows that violated the check (NULL for table-level checks) |
| `total_row_count` | long | Total rows examined |
| `failure_rate` | double | `failing_row_count / total_row_count` when applicable |
| `details` | string | Free-text or JSON with check-specific info (failing values sample, ranges, etc.) |
| `checked_at` | timestamp | When this check ran |

The table is partitioned by `checked_at` date and clustered by `table_name` and `check_name` for trend queries.

A view `_ops.dq_results_latest` exposes the most recent check execution per (table, check) pair for dashboards.

### Quarantine table naming and lifecycle

- Naming: `<schema>.<table>_quarantine` (e.g., `wastewater_processed.cdc_nwss_quarantine`).
- Schema: same as the source table plus `_quarantined_at` (timestamp) and `_quarantine_reason` (string).
- Retention: quarantine tables retain rows for 90 days by default; older rows are vacuumed. Override per table if business need requires.
- Resolution path: quarantine is for human review. A runbook describes how to investigate, fix upstream or in processed, and either reprocess or accept.

### CI checks

Per the hybrid CI enforcement policy (ADR 0016):

- **CI-enforced:** DQ severity values used in pipelines and written to `_ops.dq_results` must be in the controlled vocabulary (`info`/`warn`/`quarantine`/`fail`). One of the four enforced rules in 0016. A typo (`failed` instead of `fail`) breaks alert routing silently, so this is mechanically critical.
- **Documented-only (review-driven):** Presence of at least one `@dlt.expect_or_fail` per pipeline writing to processed or analysis layer. Reviewer judgment; some pipelines may legitimately not need one.
- **Lint-style CI output (non-blocking):** A DQ check coverage report — which check categories are exercised per table — published as part of CI output. Surfaces gaps without blocking; useful for spotting tables with no checks at all.

## Alternatives considered
- **Adopt Great Expectations as the framework.** Rejected as the initial standard. Heavy dependency, learning curve, separate execution model from Spark. May revisit when we hit checks GE handles uniquely well (statistical profiling, suite-of-checks management). Doesn't preclude future adoption.
- **Adopt Soda.** Rejected for similar reasons; smaller than GE but still imposes a separate tooling layer.
- **Adopt Databricks Lakehouse Monitoring.** Considered. Excellent for *profiling and drift detection* across time, but it's complementary to (not a replacement for) row-level DQ checks. Plan to layer it on later for analysis-layer tables once they're stable; out of scope for initial framework.
- **No standard framework — let each pipeline pick.** Rejected. Guarantees divergence and frustrates downstream consumers trying to interpret reliability.
- **Severity = binary (pass/fail).** Rejected. Most real DQ findings sit between "ignore" and "halt the pipeline"; collapsing the severity axis loses information.

## Consequences
- **Every materialized table has structured DQ metadata.** `_ops.dq_results` is queryable for trend analysis; dashboards and alerts can drive off it (ADR 0010).
- **LDP's native expectations are the default.** Engineers write DQ where the data is being written, in the same file as the transform. Minimal context-switching.
- **Quarantine is opt-in but well-supported.** The helper makes the LDP-quarantine pattern easy. Where quarantine doesn't fit (some analysis tables can't have "partial" data), explicit fail is the right call.
- **Initial framework is intentionally lightweight.** No external DQ library to learn or maintain. Trade-off: less sophisticated statistical/distributional checks. Mitigation: Lakehouse Monitoring layered on top for that capability when it earns its place.
- **DQ practice diverges only on judgment, not on mechanics.** The severity vocabulary and check categories are uniform; engineers' judgment is on *which* checks to write for *which* tables.
- **`dq_status_last` field on `dataset_engineering` becomes meaningful.** Populated from the most recent run's worst severity (`fail > quarantine > warn > info > passed`).
- **Quarantine tables add storage and ops overhead.** 90-day default retention plus VACUUM. Worth it for the ability to investigate without blocking pipelines.
