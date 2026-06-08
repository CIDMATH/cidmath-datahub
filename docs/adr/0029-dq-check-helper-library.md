# 0029 — Reusable DQ-check helper library

## Status
Accepted — 2026-06-08

## Context
ADR 0009 established the data-quality framework: every build records check
outcomes to `_ops.dq_results` via `DQRecorder.record(...)`, using the controlled
`DQCategory` / `DQSeverity` vocabularies, and a blocking check (FAIL or
QUARANTINE) raises to abort the run. ADR 0027 then standardized *where* DQ runs —
inside the `run_build` seam, between `ensure` and `register`, so results flush on
both success and failure.

What ADR 0009 did **not** standardize is *how* an individual check is written.
In practice the same handful of checks recur in nearly every entrypoint —
natural-key uniqueness, not-null, foreign-key integrity, cardinality bounds, and
rowcount parity — and each was hand-coded as the same four-step ritual: build a
`COUNT(*)` SQL string, run it, call `recorder.record(...)` with the right
category/severity/counts, then `raise ValueError(...)` if it failed and was
blocking. The weather raw and processed builds alone carried two near-identical
copies of the uniqueness check; the six geography entrypoints carry more. This
hand-coding is verbose, easy to get subtly wrong (wrong category, a `record`
call that doesn't match its own `raise`, recording a catalog-qualified
`table_name` and breaking the ADR 0019 discovery join), and untested because the
SQL is embedded in Spark-only functions.

## Decision
Add a small reusable check library to `cidmath_datahub/common/dq.py`, alongside
`DQRecorder`:

- **Pure SQL builders** — `count_sql`, `duplicate_count_sql`, `null_count_sql`,
  `orphan_count_sql` — module-level functions that take a table, columns/keys,
  and an optional `where`, and return a `SELECT COUNT(*) AS n ...` string. They
  touch no Spark, so they are unit-tested directly (see
  `tests/unit/common/test_dq.py`).
- **A bound `TableDQ` dataclass** that captures the per-table context once: the
  recorder, the Spark session, the `query_table` (catalog-qualified, used in the
  SQL), the `record_table` (schema-qualified, recorded in `_ops.dq_results` — the
  ADR 0019 discovery view joins on `schema.table`, never catalog-qualified), and
  an optional row `where` applied to every check. Its methods — `unique`,
  `not_null`, `fk`, `cardinality`, `rowcount_equals` — each run the relevant
  builder, record via the recorder with the correct fixed category, and raise on
  failure for a blocking severity unless `raise_on_fail=False`. Defaults encode
  the house style: `unique`/`not_null`/`fk`/`rowcount_equals` default to
  `FAIL` + raise; `cardinality` defaults to `WARN` + no raise.

An entrypoint now *declares* its common checks:

```python
dq = TableDQ(recorder=ctx.recorder, spark=spark,
             query_table=full, record_table=FULL_TABLE_REL, where=where)
dq.unique(keys=["geo_level", "geoid", "variable", "obs_date"],
          check_name="nclimgrid_processed_key_uniqueness")
```

The helpers cover the common ~80%. Genuinely bespoke checks stay inline and keep
calling `recorder.record(...)` directly — coverage against an in-memory Python
set (NCEI→FIPS), a multi-level FK aggregate recorded as one row, density /
cell-completeness, a stale-geoid set-diff, value-range business rules, and a
gracefully-skipped time-dimension FK. The library lowers the cost of the routine
checks without forcing the irregular ones through an ill-fitting abstraction.

As the proof, both weather entrypoints' natural-key uniqueness checks were
refactored onto `dq.unique(...)` (behavior-preserving: same category, severity,
counts, and blocking-raise). The six geography entrypoints adopt the helpers in
the same pass that retrofits them onto `run_build`.

## Alternatives considered
- **Leave checks hand-coded (status quo).** Rejected: the duplication is real
  and the failure modes (mismatched record/raise, catalog-qualified
  `record_table`) are exactly the kind of thing a shared, tested helper prevents.
- **A declarative check spec (list of dataclasses) executed by the seam.** More
  machinery than warranted today; it also fights the bespoke checks, which don't
  fit a fixed schema. The bound-object API keeps checks as plain method calls in
  the build, readable next to the bespoke ones. Can revisit if a spec proves
  valuable once more subjects exist.
- **Generic SQL expression checks (pass an arbitrary predicate).** Maximally
  flexible but gives up the typed category mapping and self-documenting method
  names that make the common cases safe; the bespoke escape hatch already covers
  the long tail.

## Consequences
Routine checks become one-liners with the correct category/severity and the
correct (schema-qualified) record name by construction, and the SQL is unit-
tested without a cluster — directly serving the ADR 0011 thin-entrypoint goal.
New subject bundles authored via the template (ADR 0027) get a clear, low-effort
default for their DQ. The seam contract (ADR 0027) and the recorder/vocabulary
contract (ADR 0009) are unchanged — `TableDQ` is a thin layer over the same
`recorder.record(...)`. The cost: a check expressed via a helper produces a
generic raise message (`DQ check '<name>' failed on <table>: <details>`) rather
than a bespoke one, and runs one extra `COUNT(*)` for its denominator; both are
negligible against the clarity gained. Bespoke checks remain first-class, so the
library must not grow to force-fit irregular cases — the inline escape hatch is
deliberate.
