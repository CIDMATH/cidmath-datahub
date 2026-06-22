---
name: Data Hub task
about: Any change that isn't a reference subject or a source bundle — a view, shared helper, fix, refactor, or doc
title: "<area>: <short imperative summary>"
labels: ["data-hub"]
---

<!--
For the assignee (and their Claude Code): read CLAUDE.md and the relevant ADRs
(docs/adr/) first — they hold the conventions (naming, layers, DQ, grants,
registration, the run_build seam). This issue gives the task-specific spec;
point your agent at the standing docs rather than restating them. Delete the
placeholder text as you fill each section.

Routing: building a REFERENCE dataset (sourced OR generated)? Use the "Reference subject" template
(ADR 0037 — raw → [processed] → canonical). A source-aligned FACT uses "New subject bundle". This
"Data Hub task" template is for everything else — views, shared helpers, fixes, refactors, docs.
-->

## Goal
<!-- One or two sentences: what outcome, and why. -->

## Context / pointers
<!-- Link the relevant ADR(s), the file(s)/module(s) involved, and any prior work to mirror.
e.g. "Follow ADR 0028 / build_geography_views.py for the views pattern." -->

## Approach
<!-- The intended change. Keep logic in src/cidmath_datahub/ (unit-tested) and entrypoints thin
via run_build where a table is built (ADR 0011/0027). Note any new ADR if a real decision is implied. -->

## Scope
**In:** <!-- what this issue covers -->
**Out (separate issues):** <!-- explicit boundaries so the change stays focused -->

## Acceptance criteria
- [ ] Testable logic in `src/cidmath_datahub/`, **unit-tested**; entrypoints thin (ADR 0011).
- [ ] Controlled-vocabulary values (DQ severity/category, update_semantics, materialization_type) used correctly (CI-enforced, ADR 0016).
- [ ] If it builds/changes a table: DQ recorded; `_ops` registration updated (`derived_from` where derived); grants correct (ADR 0008/0009/0018). (A *reference* table belongs on the "Reference subject" template, not here.)
- [ ] If a decision was made: an ADR added/updated (`docs/adr/NNNN-*.md`) and the index updated.
- [ ] `ruff format src tests && ruff check src tests` clean (also `ruff` any changed bundle files); `pytest -q` green; bundle changes pass `databricks bundle validate --target dev`.

## Verification
<!-- How to confirm it works (queries, expected DQ results, test coverage, etc.). -->
