# Authoring a subject bundle

How to add a new source-aligned subject bundle (raw → processed) to the CIDMATH Data Hub. This is the step-by-step companion to ADR 0027 (pipeline standardization), CLAUDE.md (conventions), and the worked example in `bundles/weather/`.

> **Read first:** CLAUDE.md (conventions), ADR 0001/0002/0003 (layers, schema=subject, catalog split), ADR 0006 (naming), ADR 0008/0009/0018 (registration, DQ, grants), ADR 0011 (thin entrypoints), ADR 0026 (Job vs LDP), ADR 0027 (this pattern).

## 1. Scaffold from the template

From the repo root:

```bash
databricks bundle init templates/subject-bundle
```

Answer the prompts:

| Prompt | Meaning | Example |
|---|---|---|
| `subject_name` | Subject area, snake_case (schema + bundle name) | `wastewater` |
| `provider_code` | Primary source provider (ADR 0006). **If new, add it to ADR 0006's registry in the same PR.** | `cdc` |
| `primary_dataset` | Dataset id; table becomes `<provider>_<dataset>` | `nwss` → `cdc_nwss` |
| `update_semantics` | Processed-table semantics (ADR 0007) | `merge_upsert` |

This generates `bundles/<subject>/`, the logic package `src/cidmath_datahub/<subject>/`, and `tests/unit/<subject>/`. The generated code is ruff-clean and the placeholder test passes immediately; the build hooks raise `NotImplementedError` until you fill them.

## 2. Implement the logic module (no Spark)

Put parsing/transform/conformance logic in `src/cidmath_datahub/<subject>/<dataset>.py` — pure functions, unit-tested (ADR 0011). **Docs-first:** read the source's own documentation before inferring its format; record `source_documentation_url`/`source_data_dictionary_url` in registration. Add real tests in `tests/unit/<subject>/`.

## 3. Fill the entrypoint hooks

Each `bundles/<subject>/src/build_<dataset>_*.py` calls `cidmath_datahub.common.pipeline.run_build` with four hooks. `run_build` owns the canonical lifecycle — `ensure → [DQ context: work] → register → grant` — and guarantees the DQ buffer flushes on success **and** failure, while `register`/`grant` run **only** if `work` succeeded.

- **`ensure(spark)`** — `CREATE SCHEMA/TABLE IF NOT EXISTS` (idempotent DDL).
- **`work(ctx)`** — extract → parse (via your logic module) → write → record DQ via `ctx.recorder.record(...)`. Raise on a blocking failure. Choose the DQ shape that fits:
  - *validate-then-write* (in-memory checks before writing) — typical for small/reference-cardinality builds.
  - *write-then-query-validate* (write, then query-based checks) — typical for large source-conformance builds (this is what weather does; the blocking FK/coverage/uniqueness checks read the written table).
- **`register(spark)`** — `registration.register_dataset(...)` (ADR 0008). Set `derived_from=[<upstream table>]` for lineage. **Pass `register=None`** for raw (engineer-tier staging, not catalogued — the opt-out is explicit and logged).
- **`grant(spark)`** — `grants.grant_schema_engineer(...)` for raw/processed; add `grant_schema_reader(...)` for analyst-facing analysis schemas (ADR 0018).

See `bundles/weather/src/build_nclimgrid_*.py` for a complete, working pair.

## 4. Data quality (ADR 0009)

Record at least one check per processed/analysis table via `ctx.recorder`. Use the `DQSeverity` / `DQCategory` vocabularies (CI-enforced). Make referential integrity (FK to geography/time) and natural-key uniqueness **blocking** (`FAIL`, then `raise`); use `WARN` for sanity guards (value ranges, completeness, stale keys). Results land in `_ops.dq_results` and surface in `discovery.datasets`.

## 5. Wire the deploy workflow

The template does **not** generate the GitHub workflow (its `${{ }}` syntax collides with the template engine). Copy it by hand:

```bash
cp .github/workflows/deploy-weather.yml .github/workflows/deploy-<subject>.yml
```

Then edit: the workflow `name`, the `paths:` filter (`bundles/<subject>/**`), the `working-directory`, and the deploy step's run line. Keep it **deploy-only** (no auto-run) for backfill-style subjects.

## 6. Verify and ship

```bash
ruff format src tests && ruff check src tests   # plus: ruff on the bundle files you added (bundles/ is outside CI's scope)
pytest -q
databricks bundle validate --target dev   # from bundles/<subject>/
```

Open a PR. Merge to main auto-deploys to dev. Then run the jobs from the Databricks UI in order (raw → processed). Confirm the run log shows the expected parameters, and check `_ops.dq_results` / `discovery.datasets` for the DQ verdict.

## What the template intentionally leaves to you

- **Analysis layer** — add the bare `<subject>` schema + entrypoint when a real cross-source use case warrants it (ADR 0003).
- **LDP** — the template scaffolds the Job pattern; use an LDP pipeline where ADR 0026's criteria favor it (typically the analysis layer).
- **History preservation** — if your source doesn't preserve its own revision history (e.g. CDC surveillance), you need Volume snapshots and/or SCD2 rather than plain `merge_upsert` (see the project memory / forthcoming ADR).
