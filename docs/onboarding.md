# Onboarding

Welcome to the CIDMATH Data Hub. This is your first-week guide. Read [`CLAUDE.md`](../CLAUDE.md) and skim the [ADR index](adr/README.md) alongside this.

## 1. Access

Before you can do anything, an admin needs to grant you:

- **Databricks workspace access** — membership in the `ecdh-data-engineers` workspace group (gives you the dev-tier grants).
- **GitHub** — write access to `CIDMATH/cidmath-datahub`.

Ask the data hub owner (see the repo README contact section) to set these up.

## 2. Local setup

You need Python 3.11+ and the Databricks CLI.

```powershell
# Clone the repo to a non-protected location (NOT under Documents — see ADR 0017)
cd C:\dev
git clone https://github.com/CIDMATH/cidmath-datahub.git
cd cidmath-datahub

# Install the package and dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks (runs ruff + unit tests on commit)
pre-commit install

# Install the Databricks CLI if you don't have it
winget install Databricks.DatabricksCLI

# Install the just task runner (encodes the lint/test/validate commands)
winget install Casey.Just
```

On macOS/Linux the commands are the same except `pip install -e '.[dev]'` (quote the brackets) and `brew install databricks` / curl-based CLI install.

## 3. Authenticate to Databricks

You authenticate as yourself (Emory NetID) for local work. CI authenticates as a service principal via OIDC — you never handle SP credentials.

```powershell
# Workspace auth (for bundle deploys, secrets, queries)
databricks auth login --host https://dbc-926acb48-1c75.cloud.databricks.com --profile cidmath-workspace

# Set this profile as your session default to avoid "multiple profiles matched"
$env:DATABRICKS_CONFIG_PROFILE = "cidmath-workspace"
```

To make the profile default permanently across sessions:

```powershell
[Environment]::SetEnvironmentVariable("DATABRICKS_CONFIG_PROFILE", "cidmath-workspace", "User")
```

Verify:

```powershell
databricks current-user me
```

## 4. Run the tests

```powershell
# Fast unit tests (no Spark)
pytest tests/unit -q

# Data tests (local SparkSession — slower first run)
pytest tests/data -q
```

If unit tests pass, your environment is set up correctly.

## 5. Understand the layout

```
bundles/_platform/    Infrastructure: catalogs, _ops schema/tables, grants. Deploys first.
bundles/_reference/   Canonical reference data: geography, time, code systems.
bundles/<subject>/    One bundle per subject area (wastewater, vaccine, ...).
src/cidmath_datahub/  Shared Python package. All testable logic lives here.
tests/                Unit, data, integration tests + fixtures.
docs/adr/             Architecture decision records — read these to understand "why".
scripts/setup/        One-time bootstrap scripts (SP creation, grants, etc.).
```

The single most important structural rule: **production logic lives in `src/cidmath_datahub/`, not in `bundles/<subject>/src/`.** Bundle `src/` directories hold only thin pipeline entrypoints. This keeps logic unit-testable without a cluster. (ADR 0011)

## 6. Reading list (in order)

1. [`CLAUDE.md`](../CLAUDE.md) — conventions cheat-sheet
2. [ADR 0001](adr/0001-layering-vocabulary.md) — raw / processed / analysis layers
3. [ADR 0002](adr/0002-schema-is-subject-not-source.md) — schema = subject
4. [ADR 0004](adr/0004-monorepo-bundle-per-domain.md) — bundle structure
5. [ADR 0006](adr/0006-table-and-column-naming.md) — naming conventions
6. [ADR 0008](adr/0008-catalog-metadata-schema-design.md) — `_ops` metadata schema
7. [ADR 0017](adr/0017-bootstrap-lessons.md) — gotchas you'll otherwise rediscover the hard way

## 7. Your first contribution

A good low-stakes starter task: add a value to one of the `_ops.taxonomy_*` controlled vocabularies (e.g., a new `domain:*` tag value). This touches the workflow end-to-end — branch, edit, PR, CI, review, merge — without high stakes.

The contribution flow:

```powershell
# Branch off main
git checkout -b add-taxonomy-value

# Make your change, then commit (pre-commit hooks run automatically)
git add -A
git commit -m "Add <domain> to taxonomy_domain"

# Push and open a PR
git push -u origin add-taxonomy-value
# Open the PR in GitHub; CI runs lint + tests + validate.
# Get one approving review, then merge. Merge to main auto-deploys to dev.
```

### Working an issue with Claude Code

Work is tracked as GitHub issues. Open one from a template (Issues → New issue → **New subject bundle** or **Data Hub task**); blank issues are disabled so every task starts structured. Each template embeds the goal/spec/scope/acceptance-criteria shape and points at the conventions.

If you use Claude Code (or another AI assistant), the most reliable workflow is:

1. **Branch off the issue**, then point your assistant at the **issue body** as the spec.
2. It will read `CLAUDE.md` and the ADRs the issue references automatically — that's where the conventions live, so the issue stays task-specific rather than re-explaining them.
3. For a new subject bundle: have it **scaffold from `templates/subject-bundle`** (`databricks bundle init`) and fill the `run_build` hooks, rather than copy-pasting an existing bundle — this is what keeps new work consistent with what's already there. Follow `docs/authoring-a-bundle.md`.
4. Work the **acceptance-criteria checklist** in the issue; don't expand scope beyond it (open a follow-up issue instead).
5. Before the PR: `ruff format src tests && ruff check src tests` (CI's lint scope — also run `ruff` on any bundle files you changed, since `bundles/` is outside the automated scope), `pytest -q`, and `databricks bundle validate --target dev` for bundle changes. The shortcut for all of this is `just check` (then `just validate-all` for bundle config). The pre-commit hooks and CI enforce the same.

The issue + the standing docs (CLAUDE.md, ADRs, authoring guide) together give the assistant enough context to produce code that matches the existing patterns. When in doubt, mirror the worked example: the `weather` bundle (`bundles/weather/`, ADR 0025) and `build_geography_views.py` (ADR 0028, the `run_build` exemplar).

## 8. How deploys work

- **Personal dev iteration:** `databricks bundle deploy --target dev` from a bundle directory deploys to your personal namespace (resources prefixed `[dev <you>]`). Use sparingly against the shared dev catalog — prefer letting CI deploy (ADR 0017 explains the ownership reason).
- **Shared dev:** merge to `main` → `deploy-platform.yml` / `deploy-domain.yml` auto-deploy to dev via the `ecdh-deploy-dev` SP.
- **Prod:** push a `v*` tag → workflow waits for a required reviewer's approval → deploys to prod via `ecdh-deploy-prod`.

## 9. Where to get help

- **Teams:** the `Data Hub` channel in the CIDMATH Team Site.
- **Operations questions:** [`docs/operations.md`](operations.md).
- **"Why is it like this?":** the relevant ADR in [`docs/adr/`](adr/README.md).
