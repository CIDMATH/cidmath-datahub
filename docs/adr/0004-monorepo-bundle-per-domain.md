# 0004 — Monorepo with multiple Databricks Asset Bundles, one per subject area

## Status
Accepted — 2026-05-15

## Context
We expect the CIDMATH Data Hub to grow to many pipelines across many subject areas (wastewater, vaccine/immunization, mobility, demographics, surveillance, etc.). Three structural options were on the table for organizing Databricks Asset Bundles:

1. **Single bundle in one repo.** One `databricks.yml` at the repo root, all pipelines in `resources/`, all shared code in `src/`. Simplest to set up.
2. **One bundle per subject in separate repos.** Each subject area gets its own repository. Maximum isolation; minimum code sharing.
3. **One bundle per subject in a single repo (monorepo).** Multiple bundles inside one repository, sharing a Python package and common bundle configuration.

The decision is shaped by three factors: expected scale (many pipelines), the value of independent deploy boundaries per subject (a bad deploy in one subject should not affect others), and the operational cost of refactoring later if we start with the wrong structure.

A separate concern: the project needs shared infrastructure (catalogs, the `_ops` schema, base grants, secret scopes) that doesn't belong to any single subject and must exist before any subject bundle can deploy.

## Decision
**Monorepo with multiple bundles, plus a dedicated `_platform` bundle for shared infrastructure.**

Repository structure:

```
cidmath-datahub/
├─ pyproject.toml                  # builds cidmath_datahub wheel
├─ databricks-common.yml           # shared workspace, targets, variables, tags
├─ bundles/
│  ├─ _platform/
│  │  └─ databricks.yml            # catalogs, schemas, _ops tables, grants, secret scopes
│  ├─ _reference/                  # canonical reference data (per ADR 0014)
│  │  ├─ databricks.yml            # geography, time, code systems pipelines
│  │  ├─ resources/
│  │  └─ src/
│  ├─ wastewater/
│  │  ├─ databricks.yml            # includes ../../databricks-common.yml
│  │  ├─ resources/                # job and LDP pipeline definitions
│  │  └─ src/                      # thin LDP/job entrypoints for this subject
│  └─ <subject>/                   # one bundle per subject, same layout
├─ src/
│  └─ cidmath_datahub/             # shared Python package, single source of truth
├─ tests/
├─ docs/adr/
└─ .github/workflows/
   ├─ ci.yml
   ├─ deploy-platform.yml
   ├─ deploy-reference.yml         # deploys _reference; depends on _platform
   └─ deploy-domain.yml            # matrix over discovered domains, path-filtered; depends on _reference
```

**Underscore-prefixed bundles are special bundles** — they own cross-cutting concerns rather than a subject area. Current special bundles:

- `_platform` — infrastructure only (ADR 0004). Catalogs, schemas, grants, `_ops` tables, secret scopes. Never owns data movement.
- `_reference` — canonical reference data (ADR 0014). Geography, time, code systems, pathogen taxonomy. Writes to `ecdh_model_<env>`. The exception to "data movement lives in subject bundles" because reference data has no natural subject home.
- `_orchestration` (future, if needed) — cross-bundle workflow coordination if multi-domain pipelines ever require it.

Deploy ordering: `_platform` → `_reference` → subject bundles. Codified in CI workflow dependencies.

**Key rules:**

- **Bundle granularity = subject area.** One bundle per subject. Multiple sources for one subject (e.g., GA-DPH and CDC NWSS for wastewater) live in the same bundle. Do not fragment a subject across bundles.
- **`_platform` deploys first and owns only infrastructure.** Catalogs, schemas (the `_ops` schema), base grants, the dataset catalog table, secret scope creation, service principal grants. It never owns a pipeline that moves data.
- **Shared Python is a wheel.** `src/cidmath_datahub/` is built into a single wheel via `pyproject.toml`. Each domain bundle installs the wheel as a library in its job and LDP definitions. Refactor shared code in one PR; all consumers pick up the change on next deploy.
- **Shared bundle config via `include`.** Every `bundles/<subject>/databricks.yml` includes `../../databricks-common.yml` to inherit workspace URL, target definitions, catalog variables, and base tags.
- **Cross-bundle table reads are fine; cross-bundle pipeline invocations are not.** A `vaccine` pipeline reading from `ecdh_prod.wastewater.*` is just a Unity Catalog read — perfectly acceptable. But `vaccine`'s pipeline must never directly invoke `wastewater`'s pipeline. If multi-domain orchestration becomes necessary, a dedicated `bundles/_orchestration/` bundle will own those workflows.
- **Bundles are created lazily.** Add a bundle when there is real, in-flight work for a subject. Do not pre-create empty bundle directories for every entry in the data taxonomy.

## Alternatives considered
- **Single bundle in one repo.** Rejected. Couples all pipelines into one deploy unit; blast radius of any deploy is the entire hub. `bundle validate` slows as the resource list grows. At the scale we expect (10+ pipelines), the migration cost to split later becomes real work. Discussed in the chat record: this would have been the right call for 1-3 pipelines and a small team that didn't know its domain boundaries yet. Neither applies here.
- **Separate repos per subject.** Rejected. Fragments shared utilities — each repo would need to install the common wheel from a private package index, version-bump consumers manually, and the friction discourages refactoring. For a single-team data hub, monorepo wins on both code sharing and operational simplicity.
- **No `_platform` bundle; manage shared infra manually or per-bundle.** Rejected. Either path produces drift between environments (manual setup) or duplicated and inconsistent infrastructure declarations (per-bundle). `_platform` is the natural home for cross-cutting concerns and the deploy-first ordering is enforceable.
- **Vendoring common code into each bundle (copy or submodule).** Rejected. Pure duplication.

## Consequences
- **Per-domain deploy isolation.** A bad deploy in wastewater can't break vaccine. CI uses path filters so only the affected bundles validate and deploy on a typical change.
- **Shared code remains in one place.** Refactoring `cidmath_datahub.common.naming` is one PR. Consumers pick up changes on next deploy of each bundle. Changes to `src/cidmath_datahub/**` trigger validate + deploy across all bundles in CI because they all consume the wheel.
- **Deploy ordering is real.** `_platform` must deploy before any subject bundle in a fresh environment. CI handles this via workflow dependencies; manual deploys must follow the same ordering. New environment setup runs `_platform` first.
- **The `_platform` interface must stay stable.** Every domain bundle depends on the catalogs, schemas, and grants that `_platform` creates. Backwards-incompatible changes to platform infrastructure require coordinated updates across consumers. Mitigation: keep the platform interface minimal and well-documented; treat catalog and schema names as a stable contract.
- **More upfront setup than single-bundle.** Roughly one extra day of Day 1-2 scaffolding (wheel build, common.yml, platform bundle, matrix CI). Worthwhile at the expected scale.
- **Resists premature fragmentation.** The "bundle = subject" rule prevents the temptation to create a bundle for every dataset. A subject bundle holds all of its source pipelines internally and unifies them at the analysis layer.
- **Wrong domain boundaries are the main failure mode.** If we create two bundles that should have been one (or vice versa), the cost of re-partitioning is non-trivial. Mitigation: only create a bundle when there is concrete work for that subject. Don't speculatively partition based on the taxonomy alone.
