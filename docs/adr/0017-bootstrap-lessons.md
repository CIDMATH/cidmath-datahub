# 0017 — Bootstrap lessons: friction we hit getting the platform live

## Status
Accepted — 2026-05-19

## Context
Initial bootstrap of the `_platform` bundle ran into several issues that aren't visible in the final repo state. Things now look clean because we iterated past the friction — which is fine for the future-state but loses the *why* for choices that look arbitrary in retrospect. This ADR captures the lessons so contributors don't have to rediscover them.

It also functions as a heads-up for the next bundle's first deploy: most of these gotchas will recur in slightly different forms when we stand up `_reference`, the first subject bundle, or a fresh prod environment.

## Decision

This is a retrospective ADR — no new architectural decisions, just documented observations and the rationales for choices already made elsewhere. Each section names the issue, what we did, and the lesson.

### 1. DAB resource type portability across CLI versions

**What we hit.** Databricks Asset Bundles documents `resources.schemas` and `resources.grants` as supported resource types. In practice, support varies by CLI version:

- `resources.grants` produced an `unknown field: grants` warning and didn't apply anything.
- `resources.schemas` was tracked in DAB's deployment state but didn't reliably create the schemas in UC (or created them in a way the SP couldn't access).

**What we did.** Moved schema creation and grants out of DAB resource files and into `bundles/_platform/src/setup_ops_tables.py`. The setup job runs SQL DDL (`CREATE SCHEMA IF NOT EXISTS`, `GRANT ...`) as part of the post-deploy step.

**Lesson.** For Unity Catalog objects above the table level (catalogs, schemas, grants), prefer SQL DDL inside a setup job over DAB resource types. DDL is portable across CLI versions and gives you clearer error messages. Use DAB resource types for jobs and pipelines where they're proven.

### 2. Ownership in shared catalogs when local user and CI both deploy

**What we hit.** During iteration we ran `databricks bundle deploy --target dev` locally as a user. That created the `_ops` schemas owned by the user. When CI ran the same deploy via the SP, the SP got `INSUFFICIENT_PRIVILEGES` because it didn't own the schema and didn't have explicit grants on it.

**What we did.** Dropped the dev `_ops` schemas (no real data yet) and let CI's SP recreate them as the owner.

**Lesson.** Local deploys to the *shared dev catalog* create ownership mismatches with CI's SP. Two long-term practices to prevent this:

1. **Treat CI as the canonical deploy path for shared catalogs.** Local deploys should target personal namespaces (`mode: development` already prefixes resource names; the missing piece is that catalog-level UC objects don't get user-namespaced). When iterating on bundle config, validate locally but deploy via push-to-branch.
2. **If a human must deploy locally to a shared catalog**, transfer ownership of the created objects to the deploy SP afterward via `ALTER SCHEMA ... OWNER TO`. Or drop + redeploy via CI to clean up.

The general principle: in any catalog that CI will later modify, only CI's identity should own things.

### 3. Delta DDL gotchas on serverless environment v5

Three specific things tripped the setup job:

**Column defaults require an explicit Delta feature.** `CREATE TABLE ... DEFAULT 1` errors with `WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED` unless the table also declares `TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')`. This was added to every `_ops` table that uses DEFAULT clauses.

**CLUSTER BY and PARTITIONED BY are mutually exclusive.** `dq_results` originally specified both (partition for time pruning, cluster for filter pruning). v5's parser rejects the combination with `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`. Liquid Clustering (`CLUSTER BY`) supersedes partitioning in modern Delta — drop the partition clause and add the time column to the cluster keys.

**Lesson.** Use Liquid Clustering (`CLUSTER BY`) by default; reach for `PARTITIONED BY` only when there's a specific reason it can't be replaced. Always include `TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')` when using `DEFAULT` clauses, or accept that callers must always supply values explicitly.

### 4. Python version must match serverless environment version

**What we hit.** The shared `cidmath_datahub` wheel had `requires-python = ">=3.11"`. Databricks serverless environment version 1 (the default in some places) ships Python 3.10.12, so the wheel refused to install: `Package 'cidmath-datahub' requires a different Python: 3.10.12 not in '>=3.11'`.

**What we did.** Set the job's environment client to v5 (Python 3.12.3), kept `requires-python = ">=3.11"`, and parameterized the client version via the `serverless_client_version` variable in `databricks-common.yml` so all jobs and pipelines pick it up consistently. Bumping to v6 when it ships is a one-line change.

**Lesson.** Pin the serverless environment version explicitly in your bundle, and keep `requires-python` aligned with what that environment ships. Don't rely on defaults — they change.

### 5. Auth profile management: account vs. workspace

**What we hit.** Account-level commands (`databricks account service-principal-federation-policy create ...`) require an account-level profile in `.databrickscfg`. Workspace-level commands (`databricks bundle deploy ...`, `databricks secrets ...`) require a workspace-level profile. The CLI maintains both kinds in the same config file.

Two related friction points:

- **"Profile does not contain account profiles"** when running account commands without first authenticating to the account: `databricks auth login --host https://accounts.cloud.databricks.com --account-id <id>`.
- **"Multiple profiles matched"** when multiple profiles (including the implicit `DEFAULT`) point at the same workspace host. Setting `DATABRICKS_CONFIG_PROFILE` in the environment disambiguates.

**Lesson.** Maintain explicit named profiles, set `DATABRICKS_CONFIG_PROFILE` as a user environment variable to your usual workspace profile, and remember that account commands need a separate auth step. Document the profile names in your operations runbook.

### 6. OIDC federation: numeric id vs. application_id

**What we hit.** A service principal has *two* identifiers and they're used in different places:

- The numeric `id` (e.g., `75962650827339`) — used as the argument to `databricks account service-principal-federation-policy create <sp-id>`.
- The `application_id` UUID (e.g., `a55b6164-c0eb-42cf-a438-7de33c150f4a`) — used as `DATABRICKS_CLIENT_ID` in GitHub Actions workflows and as the principal in UC GRANT statements.

Mixing them up produces confusing errors. The `create_service_principals.py` setup script prints both with explicit labels for which is for what.

**Lesson.** Numeric `id` for managing the SP record (admin APIs). `application_id` UUID for the SP authenticating itself (OAuth, OIDC, GRANT). Different contexts; both needed.

### 7. GitHub Actions OIDC requires three pieces working together

For a workflow job to authenticate to Databricks via OIDC:

1. The workflow (or job) needs `permissions: id-token: write` so GitHub mints an OIDC token.
2. The job needs `environment: <env>` so environment-scoped variables (`DATABRICKS_CLIENT_ID`) are available.
3. The step needs `DATABRICKS_AUTH_TYPE: github-oidc`, `DATABRICKS_HOST`, and `DATABRICKS_CLIENT_ID` env vars.

Forgetting any of these produces auth errors that look unrelated to OIDC. The `ci.yml` `bundle-validate` job initially failed because it had only env var #3.

**Lesson.** When wiring up a new OIDC-authenticated job, copy a known-working job's full pattern, including `permissions`, `environment`, and all three env vars. Validate each is set before debugging anything else.

### 8. Windows / PowerShell specifics

- **PowerShell drops inner double quotes** when passing JSON literals to native commands. Use `--json @file.json` syntax rather than inline JSON; create the file first via here-strings or Notepad.
- **Bash scripts (`.sh`) don't run natively** in PowerShell. Where possible, prefer Python scripts using the Databricks SDK over `.sh` for cross-platform compatibility (see `create_federation_policies.py`).
- **Default Git on Windows has `core.autocrlf=true`**, which converts LF to CRLF on checkout. A repo with LF in version control will show every line as "changed" in `git status` after a fresh clone. Mitigation: add `.gitattributes` with `* text=auto eol=lf` and run `git rm --cached -r . && git reset --hard HEAD` once to renormalize.
- **`Documents` folder is a protected location** for Windows packaged apps (Microsoft Store / MSIX installations like Claude). Repos cloned there can be invisible to sandboxed file mounts. Use `C:\dev\` or similar for repos that need to be accessible by Cowork or other packaged tools.

**Lesson.** Where instructions involve shell quoting, JSON-on-CLI, or file paths, default to assuming Windows quirks until proven otherwise. Build setup tooling in Python or with `--json @file` patterns instead of inline shell strings.

## Alternatives considered
- **Not writing this ADR.** Tempting because the issues are now in the past. But the choices we made (script-owned schemas, parameterized client version, etc.) look arbitrary without the context. Future-me will second-guess them and reinvent worse versions.
- **Filing each as a separate ADR.** Rejected. None of these are decisions with serious alternatives; they're observations and consequences. Bundling them as one retrospective keeps the ADR series focused on architectural decisions.
- **Writing as a wiki page instead of an ADR.** Rejected. ADRs live with the code and survive contributor turnover. Wiki pages drift.

## Consequences
- **Future contributors have context for choices that look arbitrary.** When someone reads `setup_ops_tables.py` and wonders why grants are in Python instead of DAB resources, this ADR explains.
- **The next bundle's first deploy will be faster.** When we stand up `_reference` or the first subject bundle, we already know to expect (and have solved) most of these gotchas.
- **The prod deploy story is partially de-risked.** Catalog ownership, OIDC binding, and DDL gotchas behave identically in prod. The local-vs-CI ownership lesson is the main thing to remember during prod first-deploy: don't deploy `_platform` locally to `ecdh_prod` first; let CI do it.
- **This ADR will age.** Some of these issues will be fixed in newer Databricks CLI versions (DAB grants resource maturity, schema management) or new serverless versions (column defaults default-on?). The ADR isn't superseded when that happens — it's just historical. Add follow-up notes if/when the upstream fixes land.
