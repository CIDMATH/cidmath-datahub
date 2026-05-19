# 0016 — CI enforcement policy

## Status
Accepted — 2026-05-15

## Context
Earlier ADRs (0005, 0008, 0009, 0013, 0014, etc.) each proposed CI enforcement of particular conventions: catalog row presence, controlled vocabulary membership, UC comments, DQ check presence, foreign key declarations, audit columns, naming length caps, and more. Drafted independently, the cumulative effect would be ~7 mechanical CI gates on every PR. This conflicts with the project's earlier preference (stated when locking in provider codes in ADR 0006) for documentation over enforcement, and creates real friction for contributors.

A coherent project-wide policy is needed to decide which rules deserve CI gates and which don't, rather than relitigating per-ADR.

The trade-off:

- **CI enforcement** prevents drift mechanically. New contributors can't accidentally violate the convention; reviewers don't need to remember to check.
- **Documented-only conventions** trust reviewers and team culture. No CI friction; conventions can be relaxed contextually when sensible.
- **Friction is real.** Every CI gate that blocks a PR is one more thing a developer hits when trying to ship work. Too many gates produce gaming, workarounds, and frustration.
- **Drift is also real.** Without enforcement, conventions decay slowly as new contributors arrive and reviewers' attention varies.

The right policy enforces the rules where drift produces *real downstream bugs* and trusts review elsewhere.

## Decision

**Hybrid enforcement.** CI gates only the things that, if violated, produce bugs that are hard to detect downstream and easy to prevent upstream. Everything else is documented in ADRs and CLAUDE.md, and relies on code review to catch drift.

### What CI gates (the enforced rules)

1. **`update_semantics` value is in the controlled vocabulary (ADR 0007).** Wrong value here means downstream tools that route on update semantics (alerting, monitoring, dashboards) will silently mishandle the table. Mechanically enforceable. CI fails the PR.

2. **DQ severity value is in the controlled vocabulary (ADR 0009).** Same logic — `_ops.dq_results.severity` is used by alert routing in ADR 0010. A typo (`failed` instead of `fail`) breaks alert routing silently. CI fails the PR.

3. **Unity Catalog tag values are in their namespace's controlled vocabulary (ADR 0005).** `_ops.taxonomy_*` reference tables hold the valid values for each tag namespace (`domain:*`, `pathogen:*`, `surveillance_category:*`, etc.). Tagging with off-vocabulary values fragments discovery. CI checks tag applications against the taxonomy tables. Fails the PR if a value isn't recognized.

4. **`_ops.dataset_catalog` row exists for every new analysis-layer table (ADRs 0005, 0008).** Discovery falls apart if tables exist in the analysis layer but not in the catalog. CI parses bundle resources for new analysis-layer tables and verifies catalog rows are added in the same PR. Fails the PR if missing.

That's the complete CI-enforced set. Four rules.

### What is documented-only (not CI gated)

The following are convention but not enforced:

- **Provider code list** (ADR 0006). PR review covers it.
- **Snake_case and name length caps** (ADR 0006). Ruff/lint catches most of it locally; review handles the rest.
- **Audit column conventions** (`ingested_at`, `processed_at`, `source_file`, `pipeline_run_id` — ADR 0006). PR review.
- **Unity Catalog table and column comments** (ADR 0013). Reviewers check; lack of comments is visible in the catalog.
- **Presence of at least one `expect_or_fail` per pipeline** (ADR 0009). Reviewer judgment; some pipelines may legitimately not need one.
- **`_ops.dataset_engineering` row presence** for materialized tables (ADR 0008). Reviewer checks; missing rows surface in operations dashboards.
- **Per-domain extension row presence** (ADR 0008). Reviewer judgment based on table's subject.
- **Foreign key declarations on canonical-pattern columns** (ADR 0014). Lint-style warning in CI output (not blocking); reviewer's call.
- **Docstring presence and style** (ADR 0013). Review.
- **Per-bundle README sections** (ADR 0013). Review.
- **Coverage thresholds** (ADR 0011). Tracked, not gated.

### Lint-style warnings (CI output, non-blocking)

The CI workflow can surface advisory warnings without failing the build. Used for things where signal is useful but blocking is too aggressive:

- FK declarations missing on canonical-pattern columns (e.g., `*_fips` columns without an FK to `geography`).
- Coverage drops compared to the previous commit.
- Catalog completeness gaps (analysis-layer tables that lack `dataset_engineering` rows).
- Audit column omissions on analysis-layer tables.

Warnings are visible in the PR comment thread but don't block the merge. They become a soft nudge that surfaces drift without manufacturing friction.

### How CI gates are implemented

Each enforced rule is a small Python script in `.github/workflows/ci/` that runs against the bundle resources and the dev Databricks workspace (read-only). They are:

- `check_update_semantics.py` — parses bundle resources for `update_semantics` declarations; verifies each is in the vocabulary.
- `check_dq_severity.py` — scans pipeline source for DQ helper calls and LDP expectation decorators; verifies severity arguments are in vocabulary.
- `check_tag_vocab.py` — parses bundle resources for tag applications; verifies values exist in the `_ops.taxonomy_*` tables (querying dev workspace).
- `check_dataset_catalog_presence.py` — diffs analysis-layer tables in the bundle against `_ops.dataset_catalog` rows; verifies new tables have new rows.

Lint-style warnings are produced by similar scripts with non-zero exit codes mapped to warnings instead of failures.

### When to add or relax a rule

- **Adding a CI gate** requires an ADR revision (this one) that explains why the rule earns enforcement. The bar: violation produces a downstream bug hard to detect from inspection, AND enforcement is mechanically checkable, AND the friction is justified.
- **Relaxing a CI gate** (downgrading to documented-only) requires the same ADR revision. The bar: the rule is producing more friction than drift it prevents.
- **A lint-style warning becoming an error** is a softer change but still benefits from being captured here.

### Local enforcement (pre-commit)

Lightweight checks run locally via pre-commit hooks (already part of the toolchain per ADR 0011):

- `ruff format` and `ruff check` (snake_case, length, lint rules)
- `pytest tests/unit -q` (fast feedback)

These are local, not CI gates per se, but they catch most lint-class issues before they reach CI.

## Alternatives considered
- **Full enforcement of every drafted rule** (~7 CI gates). Rejected. Too much friction; encourages working around the rules; team is small enough that review catches most drift.
- **Documented-only across the board** (zero CI gates). Rejected. Drift in controlled vocabularies (`update_semantics`, DQ severity, tags) produces silent downstream bugs that are hard to detect; mechanically enforcing them costs little.
- **Per-ADR enforcement decisions made independently.** Rejected — what we drafted originally. The cumulative effect was incoherent.

## Consequences
- **Mechanically critical rules are CI-gated; everything else trusts review.** Clear, defensible policy.
- **PR friction is bounded.** Four CI gates means most PRs pass without CI complaint; the gates that exist catch real bugs.
- **Drift in non-enforced rules will happen.** Provider codes will accumulate; audit columns will occasionally be missed; UC comments will sometimes be terse. Acceptable cost in exchange for low friction. Review catches the worst of it.
- **Adding or relaxing a rule is a deliberate ADR move, not a quiet PR edit.** Prevents both rule creep ("let's add one more check") and rule erosion ("let's just disable this gate today").
- **Lint-style warnings give us a middle ground.** When something is worth surfacing but not blocking, warnings handle it. Used sparingly to avoid noise.
- **The CI infrastructure is small.** Four enforced rules plus a handful of advisory warnings; each is a 50-100 line Python script. Maintainable without a dedicated devex investment.
