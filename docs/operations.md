# Operations

This document covers the prerequisites that must be completed once during initial workspace setup (by an account admin), the routine deploy procedures, and emergency procedures.

If you're a contributor looking to make your first commit, see `onboarding.md` instead.

## One-time prerequisites (account admin)

These operations require Databricks account admin or metastore admin privileges and are done **once per environment** before the `_platform` bundle can deploy.

### 1. Catalogs

Create four Unity Catalog catalogs, each with a managed storage location in S3:

| Catalog | Purpose | Storage root example |
|---|---|---|
| `ecdh_dev` | Source-aligned dev | `s3://emory-cidmath-databricks/unity-catalog/<metastore-id>/ecdh_dev` |
| `ecdh_prod` | Source-aligned prod | `s3://emory-cidmath-databricks/unity-catalog/<metastore-id>/ecdh_prod` |
| `ecdh_model_dev` | Integrated dev | `s3://emory-cidmath-databricks/unity-catalog/<metastore-id>/ecdh_model_dev` |
| `ecdh_model_prod` | Integrated prod | `s3://emory-cidmath-databricks/unity-catalog/<metastore-id>/ecdh_model_prod` |

Use Databricks UI or `databricks catalogs create`. Tag each catalog with `project=cidmath-datahub`, `env=<env>`.

### 2. Workspace groups

Create the two access groups that back the grant model (ADR 0018) and add members.

- **`ecdh-data-engineers`** — engineer tier: full access to raw/processed/analysis schemas and `_ops`; read-only on reference schemas. For people who build and operate pipelines.
- **`ecdh-analysts`** — reader tier: read-only (`USE SCHEMA`, `SELECT`) on analysis-layer and reference schemas; never raw/processed/`_ops`. The template for end-user/consumer groups.

```bash
databricks groups create --display-name ecdh-data-engineers
databricks groups create --display-name ecdh-analysts
databricks groups add-member <group-id> --user-name <user@emory.edu>
```

Grants come from two places (ADR 0018):

- **Catalog-level `USE CATALOG`** for both groups is granted by an admin in `scripts/setup/grant_catalog_permissions.sql` (step below). The deploy SP can't grant catalog-level privileges — it lacks `MANAGE`/ownership on the catalog — so this is a one-time admin step, alongside the SP catalog grants.
- **Schema-level grants** (engineer-tier on `_ops`, reader-tier on `discovery` and reference schemas like `time`) are applied automatically by the bundle deploy jobs, which can do so because the SP owns the schemas it creates.

The groups must exist **before** either runs, otherwise the GRANT statements error on a non-existent principal (same gotcha as service principals — ADR 0017). If you add the `ecdh-analysts` group after the platform has deployed, run the new `USE CATALOG` lines in `grant_catalog_permissions.sql` and re-run the `_platform`/`_reference` jobs to apply the schema-level analyst grants.

Those same jobs also **verify** the grant model immediately after applying it: they read the grants back and assert each group holds exactly the intended tier (and that analysts hold nothing on `_ops`). A mismatch fails the job and the deploy, so a broken access model can't ship silently — you don't need to verify by hand. For an out-of-band audit or a true end-to-end test from an analyst identity, see `scripts/verify/`.

### 3. Service principals

Create two service principals at the account level:

```
ecdh-deploy-dev
ecdh-deploy-prod
```

Grant each its environment-scoped permissions:

| SP | Permissions |
|---|---|
| `ecdh-deploy-dev` | `WORKSPACE_ACCESS` to the workspace, `USE_CATALOG` + `CREATE_SCHEMA` + `CREATE_TABLE` on `ecdh_dev` and `ecdh_model_dev`, `MODIFY` on `ecdh_dev._ops.*` and `ecdh_model_dev._ops.*` (once schemas exist). Member of `ecdh-data-engineers` group. |
| `ecdh-deploy-prod` | Same shape, scoped to `ecdh_prod` and `ecdh_model_prod`. |

### 4. GitHub OIDC federation

Configure the Databricks account to trust GitHub OIDC tokens for these SPs. Use the helper script at `scripts/setup/create_federation_policies.sh` which wraps the Databricks CLI:

```bash
bash scripts/setup/create_federation_policies.sh dev  <DEV-SP-NUMERIC-ID>
bash scripts/setup/create_federation_policies.sh prod <PROD-SP-NUMERIC-ID>
```

The numeric IDs come from the output of `create_service_principals.py` in step 3. The script creates one federation policy per SP with:

- **Issuer:** `https://token.actions.githubusercontent.com`
- **Audiences:** `[020f2275-adfe-44a9-99fa-e65e9369cea9]` (the Databricks account ID)
- **Subject:** `repo:CIDMATH/cidmath-datahub:environment:dev` (or `:prod` for the prod SP)

This means: GitHub Actions can only obtain tokens for these SPs when the workflow runs in the matching repository AND the matching environment. The `prod` environment has `required_reviewers` set, which gates prod deploys behind human approval.

Reference: https://docs.databricks.com/aws/en/dev-tools/auth/provider-github

### 5. Secret scopes

Create Databricks-backed secret scopes and **grant the deploy service principals READ on each scope they read at runtime.** This last part is easy to miss: a CI-deployed job (and every prod job) runs as the deploy SP, not as you, so a scope you can read yourself is *not* automatically readable by the job. Creating a scope makes you (the creator) its `MANAGE` owner; the SP starts with no ACL and must be granted `READ` separately.

**Teams incoming webhook** (alerting):

```bash
databricks secrets create-scope ecdh-dev-teams-webhook
databricks secrets create-scope ecdh-prod-teams-webhook

# Add the Teams webhook URL as the `data_hub` key in each scope
databricks secrets put-secret ecdh-dev-teams-webhook data_hub
databricks secrets put-secret ecdh-prod-teams-webhook data_hub
```

The webhook URL itself comes from the Teams channel's "Workflows" or "Incoming Webhook" connector. See `docs/runbooks/configure-teams-webhook.md` (TBD).

**IPUMS NHGIS API key** (geography reference build, ADR 0020):

```bash
databricks secrets create-scope ecdh-dev-ipums
databricks secrets create-scope ecdh-prod-ipums

# Add the NHGIS API key as the `nhgis_api_key` key in each scope
databricks secrets put-secret ecdh-dev-ipums nhgis_api_key
databricks secrets put-secret ecdh-prod-ipums nhgis_api_key
```

The key comes from your IPUMS account: https://account.ipums.org/api_keys

**Grant the deploy SPs READ on the IPUMS scopes.** `build_geography.py` calls `dbutils.secrets.get` as the run identity, so the deploy SP must hold `READ` (the `secret-scopes.secrets/get` permission). Use the SP's **application ID (UUID)**, not its display name (same gotcha as `run_as` — ADR 0017):

```bash
# dev SP (ecdh-deploy-dev) on the dev IPUMS scope
databricks secrets put-acl ecdh-dev-ipums  a55b6164-c0eb-42cf-a438-7de33c150f4a READ
# prod SP (ecdh-deploy-prod) on the prod IPUMS scope
databricks secrets put-acl ecdh-prod-ipums caff7ad3-d82f-4692-98cc-678dc6807cbd READ

# verify
databricks secrets list-acls ecdh-dev-ipums
```

Without this, the geography job fails at the key fetch with `PERMISSION_DENIED: User <sp-uuid> does not have secret-scopes.secrets/get permission on scope ecdh-<env>-ipums`. The same pattern applies to any future job that reads a secret at runtime via `dbutils.secrets.get`: grant the deploy SP `READ` on that scope.

### 6. GitHub repository configuration

In the `CIDMATH/cidmath-datahub` GitHub repository settings:

1. Create environments `dev` and `prod`.
2. On `prod`, add `required_reviewers` (Connor at minimum; expand as the team grows).
3. Add repository variable `DATABRICKS_HOST` with the workspace URL.
4. Configure branch protection on `main`:
   - Require 1 approving review
   - Require status checks to pass (the `lint-and-test`, `bundle-validate`, and `convention-checks` jobs)
   - Require branches to be up to date before merge
   - Require linear history
   - Disable force-push and branch deletion

## Routine operations

### Deploying

| Target | Trigger | Approval | SP used |
|---|---|---|---|
| Personal dev | `databricks bundle deploy --target dev` from your laptop | None | Your NetID |
| Shared dev | Merge to `main` | PR review | `ecdh-deploy-dev` |
| Prod | Push tag matching `v*` | GitHub Environment reviewer | `ecdh-deploy-prod` |

Deploy order matters: `_platform` deploys first; `_reference` next; subject bundles last. CI workflow dependencies enforce this.

### Deploying a domain bundle

```bash
cd bundles/<subject>
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

In CI, deploys happen automatically via `.github/workflows/deploy-domain.yml` on path-filtered changes to `bundles/<subject>/**` or `src/cidmath_datahub/**`.

### Triggering a pipeline manually

```bash
cd bundles/<subject>
databricks bundle run --target dev <pipeline_name>
```

### Pausing a pipeline

If a pipeline needs to be temporarily silenced (upstream outage, planned maintenance):

```bash
# Pause schedule
databricks pipelines update --pipeline-id <id> --paused

# Resume
databricks pipelines update --pipeline-id <id> --no-paused
```

Set `freshness_check_paused = true` on the corresponding `_ops.dataset_engineering` row to silence freshness alerts during the pause.

### Reading the alerting dashboards

Three Databricks SQL dashboards under the `_platform` SQL workspace:

- **Pipeline health** — per-bundle run status, last 7 days
- **Data quality trends** — per-table DQ severity counts, last 30 days
- **Cost and capacity** — DBU consumption per bundle, last 30 days

## Emergency procedures

### Rolling back a prod deploy

Tag a previous known-good commit with a higher version tag and let the deploy workflow run:

```bash
git tag v1.2.4-rollback <previous-good-sha>
git push origin v1.2.4-rollback
```

The deploy workflow will redeploy the previous code. Data tables themselves are unaffected by this — Delta time travel handles data rollback separately (see `docs/runbooks/rollback-table.md`).

### Halting all deploys

If a systemic issue makes any deploy unsafe (e.g., a broken `databricks-common.yml`), disable both deploy workflows in the GitHub Actions UI. Re-enable after fix is merged.

### Contacts

- **Owner / primary on-call:** Connor Van Meter (connor.vanmeter@emory.edu)
- **Teams channel:** `Data Hub` in the CIDMATH Team Site
- **Databricks account admin:** [TBD — fill in]
- **AWS account admin:** [TBD — fill in]
