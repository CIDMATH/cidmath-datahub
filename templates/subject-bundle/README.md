# Subject-bundle template

A Databricks Asset Bundle custom template that scaffolds a new **source-aligned subject bundle** for the CIDMATH Data Hub, wired to the shared `run_build` orchestration seam (ADR 0027) and following the conventions in CLAUDE.md and ADRs 0001–0026.

## What it generates

From four prompts (`subject_name`, `provider_code`, `primary_dataset`, `update_semantics`) it emits, into the repo:

```
bundles/<subject>/
  databricks.yml
  README.md
  resources/<dataset>_raw_job.yml
  resources/<dataset>_processed_job.yml
  src/build_<dataset>_raw.py          # thin entrypoint -> run_build (hooks stubbed)
  src/build_<dataset>_processed.py    # thin entrypoint -> run_build (hooks stubbed)
src/cidmath_datahub/<subject>/
  __init__.py
  <dataset>.py                        # pure parse/transform logic (unit-tested home)
tests/unit/<subject>/
  __init__.py
  test_<dataset>.py
```

It scaffolds **structure**, not behavior: the entrypoints already call `run_build` with the canonical `ensure -> work -> register -> grant` lifecycle and raise `NotImplementedError` from the `TODO` hooks. The generated code is ruff-clean as emitted; you fill the hooks and the logic module.

## Use

From the repo root:

```bash
databricks bundle init templates/subject-bundle
```

(Or `--output-dir` a scratch location first to preview.) Then follow `docs/authoring-a-bundle.md`.

## Scope (v1)

- **Job-based raw + processed** — the dominant source-bundle pattern. The analysis layer and any LDP pipeline (ADR 0026) are added by hand when you reach them.
- The **GitHub deploy workflow is not templated** (GitHub Actions `${{ }}` collides with Go-template `{{ }}`); copy `.github/workflows/deploy-weather.yml` to `deploy-<subject>.yml` and swap the subject name — the authoring guide covers this.
