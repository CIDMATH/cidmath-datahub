# 0026 — Job vs Lakeflow Declarative Pipeline: when to use which

## Status
Accepted — 2026-05-31

## Context
The repo runs two execution models for building tables: plain **Databricks Jobs** (a `spark_python_task` that reads, transforms, and writes Delta via explicit `MERGE`/`write`) and **Lakeflow Declarative Pipelines** (LDP, formerly Delta Live Tables — `@dlt.table` definitions with declared dependencies, expectations, and an automatic lineage DAG).

Guidance for choosing between them is real but **scattered across three ADRs**, and no single document states the decision criterion:

- **ADR 0004** lists both "job and LDP pipeline definitions" as first-class bundle resources.
- **ADR 0007** makes LDP `APPLY CHANGES` the preferred merge primitive *inside* pipelines, plain Delta `MERGE` the primitive *outside* them, and explicitly **rejected** "use LDP exclusively" because "some ingestion patterns (one-off file pulls, complex API pagination) fit Jobs better than LDP."
- **ADR 0009** designed the DQ framework to bridge both (`@dlt.expect*` ↔ the `DQSeverity` vocabulary for LDP; `DQRecorder` for Jobs) and calls LDP "the default per ADR 0004."

The first two subject/reference bundles (`_reference` geography/time, `weather`) all shipped as **Jobs**. Before more subjects land and re-litigate the choice — or copy the Jobs precedent without understanding when it does and doesn't apply — this ADR consolidates the criterion and records the weather decision as a worked example. It changes no existing code; it makes the implicit rule explicit.

## Decision

### The criterion

Choose per *table* (not per bundle — a bundle may contain both), by the nature of the work:

**Use a Job when any of these hold:**
- **Ingestion is network/IO-shaped** — HTTP pulls, API pagination, autoindex/file discovery, throttled downloads, archive extraction. These don't fit an LDP source function (which is meant to return a DataFrame deterministically, and whose sweet spots are Auto Loader / Kafka / cloud-storage sources). This is the ADR 0007 carve-out. *Every raw-layer landing in this repo is here.*
- **The table's primary data-quality guarantees are referential, aggregate, or cross-table** — multi-table foreign-key integrity, coverage against an external reference set, uniqueness over a window, cross-catalog checks. LDP `@dlt.expect*` is **row-level boolean only**; these assertions stay query-based regardless, so a Job keeps all of a table's DQ uniform in `_ops.dq_results`.
- **It's a one-off or windowed batch** parameterized by run-time arguments (e.g. `--start-year`/`--end-year` backfills).

**Use an LDP pipeline when:**
- The work is a **declarative table→table transform/derivation** whose DQ is **mostly row-level** (value ranges, null checks, enum membership) — a clean fit for `@dlt.expect*`.
- **Multiple interdependent tables within a subject** benefit from LDP's dependency DAG and orchestration (the lineage graph and ordering come for free).
- The natural update model is **streaming/incremental** (Auto Loader, `APPLY CHANGES` CDC) or **materialized-view recompute**.

**Default by layer** (a starting point, not a mandate — apply the criterion above):

| Layer | Default | Why |
|---|---|---|
| `raw` | **Job** | Ingestion is network/IO-shaped (the ADR 0007 carve-out). |
| `processed` | **Job** if its DQ is referential/aggregate-heavy or its source is awkward to express as an LDP input; otherwise **LDP**. | Source-conformance transforms vary; pick by the DQ shape. |
| `analysis` | **LDP preferred** | Unification/derivation across processed sources, mostly row-level DQ, multiple interdependent tables, MV/streaming-friendly — LDP's strengths. |

### Invariants that hold across both models
- **Registration is identical.** Both register into `_ops.dataset_catalog` / `dataset_engineering` via `common.registration.register_dataset` (ADR 0008). `pipeline_runs.pipeline_type` already distinguishes `job` / `dlt_pipeline`.
- **DQ severity vocabulary is identical.** Jobs use `DQRecorder`; LDP uses `@dlt.expect*` mapped to the same `DQSeverity` values (ADR 0009).
- **`update_semantics` vocabulary is identical** (ADR 0007); only the *primitive* differs (`MERGE` in a Job, `APPLY CHANGES` in LDP).

### One integration cost to budget for LDP adoption
LDP expectations write results to the **LDP event log**, not to `_ops.dq_results`. The unified `discovery.datasets.dq_status_last` (ADR 0019) reads `_ops.dq_results`. So the first LDP pipeline that relies on expectations for blocking DQ must also **ETL its event-log expectation results into `_ops.dq_results`** (a small shared helper), or its DQ status won't surface in discovery. Until that helper exists, LDP tables whose DQ matters for discovery should record via `DQRecorder` in an explicit step, exactly as Jobs do.

### Worked example — the weather bundle (Jobs)
- **`weather_raw.noaa_nclimgrid_daily` → Job.** Ingestion is throttled HTTP autoindex discovery + per-file pulls — the ADR 0007 carve-out.
- **`weather_processed.noaa_nclimgrid_daily` → Job.** Its three *blocking* checks — NCEI→FIPS coverage (against a Python set), the cross-catalog `geoid` FK to `geography.us_*` filtered by vintage, and natural-key uniqueness — are all referential/aggregate/cross-table and do not map to row-level `@dlt.expect*`. Moving it to LDP would cover only the row-level value-range check via expectations and split the rest into query-based checks plus an event-log→`_ops` bridge, for a lineage benefit that Unity Catalog already provides automatically. A Job keeps all of its DQ uniform in `_ops.dq_results`.

## Alternatives considered
- **LDP exclusively.** Rejected (re-affirming ADR 0007): network/IO ingestion and referential-DQ-heavy transforms fit Jobs better; forcing them into LDP adds friction for no gain.
- **Jobs exclusively.** Rejected: forgoes LDP's genuine strengths for the analysis layer (declarative DAG, expectations, MV/streaming, first-class lineage). The point of this ADR is *not* to entrench Jobs.
- **Per-engineer choice, case by case.** Rejected: inconsistency is the failure mode (two execution models, two log surfaces, two DQ idioms with no rule). A documented criterion lets reviewers hold the line.
- **Adopt LDP for `weather_processed` now to get lineage.** Rejected: Unity Catalog already captures table/column lineage for Jobs at runtime; the missing piece was *declarative catalog lineage*, addressed far more cheaply by populating `dataset_catalog.derived_from` (done alongside this ADR). LDP's referential-DQ gap outweighs the incremental lineage benefit here.

## Consequences
- **The criterion is written down once.** New subjects apply the table above instead of copying whichever precedent they saw first or re-arguing the choice.
- **Weather and the geography reference stay Jobs** — consistent with the criterion (ingestion + referential DQ), not an accident to be "fixed."
- **The analysis layer is the LDP pilot.** When the first analysis-layer table lands (unification across processed sources), build it as an LDP pipeline, build the event-log→`_ops.dq_results` bridge at the same time, and revisit this ADR with what the pilot taught us.
- **Catalog lineage no longer depends on the execution model.** `derived_from` records upstream tables for any table in either model; UC auto-lineage remains the runtime complement.
- **A future ADR may narrow the `processed` default** once enough processed tables exist to say whether referential DQ is the norm (keep Job) or the exception (flip to LDP). This ADR deliberately leaves `processed` as "it depends," judged by DQ shape.
