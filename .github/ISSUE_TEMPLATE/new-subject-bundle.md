---
name: New subject bundle
about: Add a new source-aligned subject bundle (raw → processed) via the DAB template + run_build seam
title: "[<subject>] new subject bundle: <provider> <dataset>"
labels: ["subject-bundle", "data-hub"]
---

<!--
For the assignee (and their Claude Code): before starting, read CLAUDE.md and the
ADRs it references — especially 0001/0002/0003 (layers, schema=subject, catalog
split), 0006 (naming), 0007/0009/0018 (update semantics, DQ, grants), 0011 (thin
entrypoints), 0025 (worked example: weather), 0027 (run_build seam + authoring).
Then follow docs/authoring-a-bundle.md. This issue gives the task-specific spec;
the standing docs give the conventions — don't duplicate them, point your agent
at them. Delete the placeholder text as you fill each section.
-->

## Goal
<!-- One sentence: the outcome. e.g. "Land CDC NWSS wastewater into weather-style raw + processed, conformed to geography/time." -->

## Source spec
- **Provider code (ADR 0006):** <!-- e.g. cdc — if new, add it to ADR 0006's registry in the same PR -->
- **Dataset / table id:** <!-- e.g. nwss → table cdc_nwss -->
- **Source URL + documentation URL:** <!-- docs-first: link the source's own docs; we read them before inferring format -->
- **Format & access:** <!-- CSV/JSON/API; auth/secret needed?; pagination/throttling? -->
- **Cadence & history:** <!-- update frequency; **does the source preserve its own revision history?** If not (e.g. CDC weekly revises in place), flag it — needs Volume snapshots / SCD2, not plain merge_upsert (see project memory / forthcoming ADR). -->
- **License / DUA:** <!-- public domain? DUA required? citation? -->

## Scaffold
Run from the repo root and answer the prompts:
```bash
databricks bundle init templates/subject-bundle
# subject_name=<subject>  provider_code=<provider>  primary_dataset=<dataset>  update_semantics=<...>
```

## Tables & semantics
- **Raw:** `<subject>_raw.<provider>_<dataset>` — faithful landing; update semantics `merge_upsert` (or per source).
- **Processed:** `<subject>_processed.<provider>_<dataset>` — conformed. Update semantics: `<...>` (ADR 0007).
- **Conformance:** <!-- geoid → geography.us_* (which vintage?); date/event → time.calendar_date; units/typing -->
- **Blocking DQ (FAIL + raise):** <!-- e.g. geoid FK to geography, source-code coverage, natural-key uniqueness -->
- **WARN DQ:** <!-- value ranges, completeness/density, date→time guard, stale-key guard -->

## Scope
**In:** <!-- e.g. raw + processed, recent window first then full backfill -->
**Out (separate issues):** <!-- e.g. analysis layer, LDP, extra grains -->

## Acceptance criteria
- [ ] Scaffolded from `templates/subject-bundle`; entrypoints stay thin and call `cidmath_datahub.common.pipeline.run_build` (ADR 0011/0027).
- [ ] Parse/transform/conformance logic lives in `src/cidmath_datahub/<subject>/` and is **unit-tested** against real sample records.
- [ ] At least one **blocking** DQ check (referential integrity + natural-key uniqueness) recorded via `ctx.recorder`; WARN guards as appropriate (ADR 0009).
- [ ] Processed table **registered** in `_ops.dataset_catalog` with `derived_from` set; raw uses `register=None` (ADR 0008/0018).
- [ ] Naming follows ADR 0006 (`<provider>_<dataset>`, `<subject>_raw`/`_processed`); new provider added to ADR 0006's registry.
- [ ] Deploy workflow added (`cp .github/workflows/deploy-weather.yml deploy-<subject>.yml`, edit subject + paths).
- [ ] An ADR added/updated if a non-obvious decision was made.
- [ ] `ruff format . && ruff check .` clean; `pytest -q` green; `databricks bundle validate --target dev` passes.

## Verification (after dev deploy + run)
<!-- The queries/checks to confirm correctness — e.g. raw cell-completeness, raw↔processed row parity, _ops.dq_results blocking checks passed, discovery.datasets row present. Mirror the weather verification pattern. -->

## Notes
<!-- Anything source-specific, known quirks, links to the data dictionary, etc. -->
