# 0011 — Testing strategy

## Status
Accepted — 2026-05-15

## Context
Data pipelines are notoriously hard to test well. The temptation is to skip tests entirely ("we'll see if it works in dev") or to over-engineer them ("every transformation needs a fixture and three assertion patterns"). Both lead to brittle, low-confidence systems.

ADR 0004 placed testable logic in `src/cidmath_datahub/` (the shared Python package) and reserved `bundles/<subject>/src/` for thin entrypoints. This separates concerns nicely but only pays off if the testable logic is actually tested. ADR 0009 introduced runtime DQ via LDP expectations and `_ops.dq_results`, but those validate data at execution time — they don't prevent broken code from shipping.

The framework needs to cover three test types with different scopes, runners, and CI behavior, and to make the most-common case (a small change to a transformation function) trivially testable.

## Decision

### Three test types

| Type | Scope | Runner | Speed | When it runs |
|---|---|---|---|---|
| **Unit** | A single function or module in `src/cidmath_datahub/`. No Spark required, or local SparkSession only. | `pytest` | Seconds | Every commit, every PR, locally during dev |
| **Data** | DataFrame transforms with realistic synthetic input; verifies output shape and content. Local SparkSession. | `pytest` + `chispa` | Seconds to a minute | Every commit, every PR |
| **Integration** | A pipeline deployed to a dev workspace target, executed against fixtures or a small slice of real data, verifies end-to-end behavior including DQ. | `pytest` + Databricks CLI | Minutes | Every merge to `main`; on demand for PRs touching pipelines |

LDP's runtime expectations (ADR 0009) are not "tests" in this sense — they validate live data. They complement, not replace, the test suite.

### Directory layout

```
tests/
├─ unit/                          # pure logic, no Spark
│  ├─ common/
│  │  ├─ test_naming.py
│  │  └─ test_dq_helpers.py
│  ├─ ingest/
│  └─ transforms/
├─ data/                          # DataFrame transforms with local Spark
│  ├─ ingest/
│  │  └─ test_cdc_nwss_parse.py
│  └─ transforms/
│     └─ test_wastewater_unify.py
├─ integration/                   # end-to-end against dev workspace
│  └─ test_wastewater_pipeline.py
├─ fixtures/                      # synthetic test data
│  ├─ cdc_nwss/
│  │  ├─ valid_sample.json
│  │  └─ malformed_sample.json
│  └─ shared/
│     └─ small_geography.csv
└─ conftest.py                    # shared pytest fixtures (Spark session, etc.)
```

Test files mirror the structure of `src/cidmath_datahub/`. Every public function in `src/cidmath_datahub/` should have a corresponding test file in `tests/unit/` or `tests/data/`.

### Tooling and conventions

- **`pytest`** — test runner. Configured in `pyproject.toml`. Standard `tests/` layout discovered automatically.
- **`chispa`** — DataFrame assertion library. `assert_df_equality`, `assert_column_equality` for clean error messages on transform tests.
- **Local SparkSession** for data tests. A `pytest` fixture in `conftest.py` creates a small SparkSession scoped to the test module; tests share it.
- **Synthetic fixtures over recorded real data.** Fixtures in `tests/fixtures/` are hand-crafted small examples covering normal cases, edge cases, and malformed inputs. Never commit real PHI, restricted data, or large datasets to fixtures.
- **`responses` or `requests-mock`** for HTTP ingestion testing. Stub external APIs at the request level; don't hit them in CI.
- **No mocking of Spark itself.** Use a real local SparkSession. Mocking Spark APIs is fragile and produces tests that pass while real Spark fails.
- **Property-based tests via `hypothesis`** are encouraged for transforms with non-obvious invariants (e.g., "sum of daily aggregates equals sum of weekly aggregates"). Optional, not required.

### Unit tests

Cover pure Python logic with no Spark dependency. Targets:

- `cidmath_datahub.common.naming` — schema/table name builders
- `cidmath_datahub.common.dq` — helper functions for DQ result construction
- Parsers (e.g., source-specific JSON-to-dict converters)
- Validation functions
- Domain logic (e.g., epi-week computation, demographic age-band assignment)

Goal: 90%+ coverage of `src/cidmath_datahub/common/`. Lower elsewhere is acceptable when logic is thin.

### Data tests

Cover DataFrame transforms with realistic synthetic input. Local SparkSession from `conftest.py`. Pattern:

```python
def test_wastewater_unify_dedupes_by_sample_id(spark):
    input_df = spark.read.json("tests/fixtures/cdc_nwss/duplicate_samples.json")
    result = unify_wastewater_sources(input_df)
    assert result.count() == 3  # 5 input rows, 2 are duplicates
    assert_column_equality(result, expected_unified_df, "sample_id")
```

Every transform module in `src/cidmath_datahub/transforms/` should have data tests covering:

- Happy path (typical input → expected output)
- Edge cases (empty input, single-row input, all-duplicate input)
- Malformed input handling (where the transform is expected to filter or fail explicitly)

### Integration tests

Cover end-to-end pipeline behavior in a real Databricks dev workspace. Run via `databricks bundle deploy --target dev` followed by `databricks bundle run`, then assertions against the resulting tables.

Pattern:

1. Deploy the pipeline to a CI-only dev target (`mode: development` with a unique CI run identifier prefix).
2. Trigger a single run against a small fixture dataset (a subset of source data committed to the repo or stored in a known location).
3. Assert against the output table: row counts, schema, key DQ results in `_ops.dq_results`.
4. Tear down the CI dev artifacts.

Integration tests are slower (minutes) and run on every merge to `main`, not on every PR commit. Run on PR commits when the PR touches files in `bundles/<subject>/` or `src/cidmath_datahub/`.

### Test data fixtures

- **Small** — fixtures should be hundreds to thousands of rows, not millions. Tests run in seconds, not minutes.
- **Synthetic** — generated to look like real data, not extracted from real data. Avoids any privacy risk and lets fixtures cover edge cases that real data may not have.
- **Committed to the repo** for source fixtures. Stored as JSON, CSV, or Parquet under `tests/fixtures/`.
- **Generated via a script** when synthesizing structured data is non-trivial. The script lives in `tests/fixtures/_generators/` and produces deterministic output (fixed seed). Commit the generated fixtures, not just the script — tests should be reproducible without running the generator.
- **Domain-shared fixtures** (e.g., small `geography.county` reference data) live in `tests/fixtures/shared/`.

### CI behavior

| Stage | What runs | Trigger | Failure behavior |
|---|---|---|---|
| Pre-commit (local) | `ruff format`, `ruff check`, `pytest tests/unit -q` | git commit | Block commit |
| PR validation | All unit + data tests; `databricks bundle validate` for affected bundles | PR open / push | Block merge if failing |
| Merge to main | All unit + data tests; integration tests for affected bundles | Push to main | Block deploy if failing |
| Nightly | Full integration test suite across all bundles; coverage report | Cron | Notify in `#cidmath-data-alerts` |
| Tag (release) | Full unit + data + integration suite; deploy to prod gated on success | Tag push | Block deploy |

### Coverage

- Coverage is tracked via `pytest-cov` and reported in CI.
- Target: 80%+ on `src/cidmath_datahub/common/`; 60%+ on `src/cidmath_datahub/ingest/` and `transforms/`; coverage on `bundles/<subject>/src/` (thin entrypoints) is not tracked.
- No hard CI gate on coverage initially — track the metric, address drops in code review. Add a gate if regression becomes a pattern.

### What we don't test (explicitly)

- **The Databricks runtime itself.** Don't write tests that verify Spark or Delta behavior — they're already tested upstream.
- **External API behavior.** Stub external services; tests should never depend on a third-party API being up.
- **Trivial getters/setters or pure Pydantic models.** Unit tests on data classes with no behavior add noise without confidence.
- **Generated code (e.g., LDP-generated DDL).** It's regenerated from the source code we do test.

## Alternatives considered
- **Adopt `dbt`-style data tests as the primary mechanism.** Rejected. We're not using dbt and don't intend to. LDP expectations cover the analogous runtime DQ; Python tests cover the pre-runtime correctness.
- **Use Databricks Connect for all data tests instead of a local SparkSession.** Rejected. Databricks Connect requires a workspace, adds latency, and introduces a dependency for what should be a fast local feedback loop. Use Connect for integration tests, local Spark for data tests.
- **Test only via integration tests against the dev workspace.** Rejected. Integration tests are too slow for the inner dev loop. Unit + data tests need to run in seconds.
- **No integration tests; rely on LDP expectations at runtime.** Rejected. LDP expectations catch data issues; they don't catch the case where the pipeline silently writes to the wrong table or skips an entire source. Integration tests verify the structural correctness of the pipeline itself.
- **Property-based testing as the default.** Rejected as a requirement, encouraged as an option. Property tests are excellent for invariant-heavy transforms; making them required would slow down simple cases.

## Consequences
- **Inner dev loop is fast.** Unit + data tests run in seconds. Engineers actually run them locally.
- **Integration tests catch the failures unit tests can't.** Wrong target table, mis-configured LDP, broken bundle resource — caught before main, not in prod.
- **CI runtime is bounded.** PR validation runs in 2-5 minutes; merge runs in 10-15. Manageable without expensive parallelization.
- **Synthetic fixtures decouple test reliability from source availability.** CI doesn't break when CDC NWSS is rate-limiting.
- **Coverage is tracked, not enforced.** Tracking surfaces regression; hard gates produce gaming.
- **Maintenance burden of fixtures.** Synthetic fixtures need updates when source schemas change. Trade-off worth it — testing against live sources is more brittle.
- **No mocking of Spark — small price for a real local SparkSession.** Startup cost on test runs (a few seconds for the first test) is acceptable.
- **The line between "data test" and "integration test" needs judgment.** A transform that writes a Delta table can be tested either way. Default to data test when possible (faster); use integration when the value-add is in the deploy/orchestration layer.
