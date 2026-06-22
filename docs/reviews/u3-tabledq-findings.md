# U3 — `TableDQ` / DQ helper (ADR 0029) — findings

**Date:** 2026-06-22 · **Scope:** `src/cidmath_datahub/common/dq.py` (`DQRecorder`, the pure SQL
builders, `TableDQ`), against adoption across all `build_*.py`. Part of the SDE review plan.

## Verdict
`DQRecorder` and `TableDQ` are well-built — but **`TableDQ` is query-based (it runs SQL against the
*written* table), while every reference/codes build validates *in memory* before writing.** So
`TableDQ` is structurally inapplicable to the reference builds, and the recurring DQ boilerplate ADR
0029 set out to remove persists there. The fix isn't "adopt `TableDQ`" — it's an **in-memory DQ
helper**, which the 0036 builder should own.

## Adoption (the data)
`TableDQ` is used in **2 builds** (weather: `nclimgrid_raw`/`_processed`) plus `build_time`. **Every
codes + geography build uses zero `TableDQ` and hand-rolls `record()`** — `build_ndc` 14×, `loinc`
11×, `snomed` 11×, `icd9cm` 9×, `icd10cm`/`icd10pcs`/`cvx` 8× each, etc. (Also note: the team has
shipped more code systems since — `rxnorm`, `snomed`, `icd9_procedures`, `ruca` — all hand-rolling.)

## Why (the root cause — refines the I1 reading)
The reference builds **validate-then-write**: parse → run pure in-memory checks on the records
(`ndc.find_duplicate_product_keys(products)`, `find_missing_*`, …) → `record()` → `raise` on FAIL →
*then* write, so a bad table never lands. `TableDQ` is **write-then-query**: it `SELECT`s against
`query_table` after the write. Source-conformance builds (weather) write first then query — so they
use `TableDQ`; reference builds can't, because the table doesn't exist yet when they validate. So I1's
"NDC hand-rolls ~210 lines instead of the helper" is a **pattern mismatch**, not laziness — and the
in-memory pattern is arguably the *safer* one for reference data (fail before the bad table lands).

## What's solid
- `DQRecorder`: buffers, flushes on `__exit__` even on exception (failed checks persist), validates
  category/severity against the vocab, derives `failure_rate`, JSON-encodes `details`. Clean
  separation — persistence only; callers own raise/no-raise.
- The pure SQL builders (`count`/`duplicate`/`null`/`orphan`) are unit-testable; `TableDQ` binds the
  per-table context and gets `record_table` (schema.table) vs `query_table` (catalog.schema.table)
  right for the ADR 0019 discovery join. Sensible severity defaults (unique/not_null/fk → FAIL+raise;
  cardinality → WARN). Genuinely good — *for the query-based flow*.

## Findings

### SHOULD-FIX
1. **No in-memory DQ helper → ADR 0029 doesn't actually serve reference builds.** The recurring
   reference DQ logic (duplicate-key, missing-field, format/charset, membership) is reimplemented as
   per-source `find_*` functions in each `reference/*.py` *and* recorded via per-build hand-rolled
   `record()`. Provide a generic **in-memory** helper in `common/` — `find_duplicate_keys(records,
   keys)`, `find_null_fields(records, cols)`, a membership/range checker, and a "record these results
   with standard severity/category/raise" sink — the in-memory analog of `TableDQ`. **This is the DQ
   piece the 0036 builder must own** (the builder can't just "use `TableDQ`" for reference builds).
   **Revised 2026-06-22 (supersedes the "build an in-memory helper" recommendation above):** the gap
   is resolved by **unifying the validation pattern**, not by a parallel helper — validate the
   raw/processed **staging** with `TableDQ` and **gate the promote** to the canonical (ADR 0037
   decision 8 + the ADR 0027 amendment). One DQ family; in-memory validation stays an optional
   fast-path for tiny data. This also covers large reference (e.g. census block ~8M rows) that can't
   validate in-memory at all. So `TableDQ` becomes the single DQ helper — just extend it (#2) and fix
   the `fk` grain (#3).

2. **Coverage gap even in `TableDQ`:** only `unique`/`not_null`/`fk`/`cardinality`/`rowcount_equals`.
   No **range** (value bounds — D1 wanted weather min/max sanity), **freshness**, or **controlled-vocab
   membership** — exactly the checks builds hand-roll (NDC's marketing-date-order + DEA-schedule
   vocab, etc.). Add them (to both the query and in-memory helpers).
3. **`fk` metric grain mismatch:** `failing_row_count` = distinct orphan *key values* but
   `total_row_count` = *rows*, so `failure_rate` (distinct-keys ÷ rows) mixes grains and misleads.
   Count orphan rows, or record distinct-keys ÷ distinct-keys, and document.

### CONSIDER (low)
4. **Passing checks drop `details`** (`details if not passed else None`) — you lose the actual value
   on pass, so there's no cardinality/row-count *trend* signal in `dq_results`. Keep a compact actual
   even on pass for `cardinality`/`freshness`.
5. **`rowcount_equals` applies the bound `where` to `other_table` too** — which may not have those
   columns. Take a separate `other_where` (or none).
6. **`flush()` failure on the *success* path is swallowed** (logged). On success that silently loses
   the DQ audit while the build reports success and proceeds to register/grant. Swallowing is right on
   the *exception* path (don't mask the original); on success a flush failure should fail the build.

## Ties
- **I1:** corrects the interpretation — the hand-rolled `record()` is a query-vs-in-memory pattern
  mismatch, not non-adoption by neglect.
- **I2 / ADR 0036:** the recorder writes `dq_results` but not `dataset_engineering.dq_status_last`
  (hence the view derives it). The builder owning DQ is the place to provide the in-memory helper
  (#1), the missing check types (#2), and optionally wire `dq_status_last`. Recommend folding the
  **in-memory DQ helper** into ADR 0036's scope alongside the registration gaps.
