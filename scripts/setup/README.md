# `scripts/setup/` — one-time bootstrap operations

Scripts in this directory are **prerequisites** for the `_platform` bundle. They run once during initial workspace bootstrap to create identities and apply baseline grants that the bundle assumes exist.

The bundle itself does not run these scripts. They're meant to be executed by an admin from a laptop, against the Databricks account.

## Order of operations

```
1. Account admin creates catalogs (ecdh_dev, ecdh_prod, ecdh_model_dev, ecdh_model_prod)
   |                              with managed S3 storage locations
   v
2. python create_service_principals.py    <-- this directory
   |  creates ecdh-deploy-dev, ecdh-deploy-prod
   |  assigns them to the workspace
   |  prints both `id` (numeric) and `application_id` (UUID) — capture both
   v
3. bash create_federation_policies.sh dev <dev-sp-numeric-id>
   bash create_federation_policies.sh prod <prod-sp-numeric-id>     <-- this directory
   |  creates OIDC federation policy on each SP via Databricks CLI
   |  policy binds SP to repo:CIDMATH/cidmath-datahub:environment:<env>
   v
4. databricks sql exec grant_catalog_permissions.sql   <-- this directory
   |  grants USE CATALOG + CREATE SCHEMA to each SP on its env catalogs
   v
5. In GitHub: create environments `dev` and `prod`
   |  Add Variable DATABRICKS_CLIENT_ID to each environment with the
   |  corresponding SP's application_id (UUID) as the value
   v
6. _platform bundle deploys via GitHub Actions on push to main / tag push
```

Steps 1, 3, 4 require account-admin or metastore-admin privileges. Step 2 is the Python script in this directory and needs account-level authentication. Step 5 is GitHub repo settings. Step 6 happens automatically once the repo is pushed.

## Step 2: Create service principals

### Prerequisites

- `databricks` CLI installed (`pip install databricks-cli` or via the official installer).
- `databricks-sdk` Python package (`pip install databricks-sdk>=0.30`).
- Account-admin or workspace-admin privileges on the Databricks account.
- The numeric workspace ID (find with `databricks workspaces list` against the account).

### Authenticate to the account

```bash
databricks auth login --host https://accounts.cloud.databricks.com \
    --account-id 020f2275-adfe-44a9-99fa-e65e9369cea9
```

This opens a browser, prompts you to sign in with your Emory NetID, and stores the resulting OAuth token in your local Databricks profile.

### Find your workspace ID

```bash
databricks workspaces list
```

The output lists all workspaces in the account. Note the numeric `WORKSPACE_ID` column for `emory-cidmath-databricks-workspace`.

### Run the script

```bash
python scripts/setup/create_service_principals.py \
    --account-id 020f2275-adfe-44a9-99fa-e65e9369cea9 \
    --workspace-id <your-numeric-workspace-id>
```

The script:

1. Checks if `ecdh-deploy-dev` already exists; creates it if not.
2. Checks if `ecdh-deploy-prod` already exists; creates it if not.
3. Assigns both SPs to the workspace at USER permission level.
4. Prints the application IDs for both SPs.

Re-running the script with existing SPs is safe — it skips creation and just prints the IDs.

### Capture the output

The script prints something like:

```
======================================================================
CAPTURE BOTH IDs FOR EACH SP — they get used in different places:
======================================================================
  ecdh-deploy-dev
    id (numeric)        : 1234567890123456
      ^ Use as the argument to scripts/setup/create_federation_policies.sh
        and to `databricks account service-principal-federation-policy create`.
        This is the SP's internal record id — the 'primary key'.

    application_id (UUID): a1b2c3d4-e5f6-7890-abcd-ef1234567890
      ^ Use as DATABRICKS_CLIENT_ID in GitHub Actions workflows,
        and stored as a GitHub Environment variable per env.
        This is the SP's OAuth client identifier.
```

**Save both values** for both SPs. They get used in different steps below.

## Step 3: Configure OIDC federation policies

Each SP needs a federation policy that binds it to a specific GitHub repo + environment combination. Run the script once per SP:

```bash
bash scripts/setup/create_federation_policies.sh dev  <DEV-SP-NUMERIC-ID>
bash scripts/setup/create_federation_policies.sh prod <PROD-SP-NUMERIC-ID>
```

Use the **numeric `id`** values from Step 2 (not the `application_id` UUIDs). The script wraps this CLI call:

```bash
databricks account service-principal-federation-policy create <sp-id> --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["020f2275-adfe-44a9-99fa-e65e9369cea9"],
    "subject": "repo:CIDMATH/cidmath-datahub:environment:<env>"
  }
}'
```

What the subject claim does: the dev SP only accepts OIDC tokens claiming to be from the `dev` environment in the `CIDMATH/cidmath-datahub` repository. Without this binding, federation would be too broad and any workflow could claim to be the deploy SP.

What the audiences claim does: it's the intended recipient of the token. Setting it to the Databricks account ID (recommended by Databricks docs) ensures GitHub-issued tokens are scoped to your specific account rather than usable against any Databricks customer.

**Verification:** After running, list the policies on each SP:

```bash
databricks account service-principal-federation-policy list <sp-numeric-id>
```

You should see exactly one policy per SP with the correct subject claim.

Reference: https://docs.databricks.com/aws/en/dev-tools/auth/provider-github

## Step 4: Apply catalog grants

After the four catalogs exist (Step 1) and the SPs exist (Step 2):

```bash
databricks sql exec --file scripts/setup/grant_catalog_permissions.sql
```

Or, if you prefer the Databricks SQL editor in the workspace UI: open `grant_catalog_permissions.sql` and run it as a SQL query. Use a SQL warehouse that has access to the catalogs.

The script:

1. Grants `USE CATALOG` + `CREATE SCHEMA` on `ecdh_dev` and `ecdh_model_dev` to `ecdh-deploy-dev`.
2. Grants the same on `ecdh_prod` and `ecdh_model_prod` to `ecdh-deploy-prod`.
3. Runs verification queries showing the resulting grants. Confirm:
   - Dev SP has grants on dev catalogs only (not prod).
   - Prod SP has grants on prod catalogs only (not dev).

If verification shows unexpected grants, investigate before proceeding — environment isolation depends on this being correct.

## Step 5: Configure GitHub repository

In the GitHub repository settings for `CIDMATH/cidmath-datahub`:

1. **Create environments.** Settings → Environments → New environment. Create both `dev` and `prod`.
2. **Add required reviewers to `prod`.** On the `prod` environment, set Required reviewers to include yourself (Connor) at minimum. This is what gates the prod deploy behind manual approval.
3. **Add a Variable per environment.** On each environment, add a Variable named `DATABRICKS_CLIENT_ID` with the corresponding SP's `application_id` (UUID) as the value:
   - `dev` environment → `DATABRICKS_CLIENT_ID` = the dev SP's application_id UUID
   - `prod` environment → `DATABRICKS_CLIENT_ID` = the prod SP's application_id UUID
4. **Add a repository-level Variable.** Settings → Variables → New repository variable named `DATABRICKS_HOST` with value `https://dbc-926acb48-1c75.cloud.databricks.com`.
5. **Configure branch protection on `main`** (can be deferred until after first successful CI run): 1 approving review required, status checks must pass, linear history, no force-push.

The `application_id` is not a secret (it's a public OAuth client identifier), so it's stored as a Variable rather than a Secret. Treating it as a Variable also makes it visible in the workflow logs for debugging.

## Step 6: Push and verify

Push the repo to GitHub. The first push to a feature branch should trigger `ci.yml` to run (lint, tests, bundle validate). Merging to `main` should trigger `deploy-platform.yml` which authenticates via OIDC and deploys `_platform` to dev.

Failure modes you might see on first deploy:

- **"Permission denied" on catalog access** — Step 4 grants are wrong or weren't applied.
- **"Could not authenticate" / "invalid client"** — OIDC federation policy in Step 3 has the wrong subject claim, or `DATABRICKS_CLIENT_ID` in GitHub doesn't match the SP's application_id.
- **"Catalog not found"** — Step 1 (catalog creation) hasn't happened.
- **"workspace not authorized"** — The SP wasn't assigned to the workspace; re-run Step 2.

## Idempotency

All scripts here are safe to re-run:

- `create_service_principals.py` checks for existing SPs by display name and skips creation.
- `create_federation_policies.sh` will error if a policy already exists for that SP; list and delete first if you need to recreate (the script's output explains how).
- `grant_catalog_permissions.sql` uses `GRANT` (not `REVOKE` + `GRANT`), so re-running is a no-op on already-granted permissions.

This means you can re-run during troubleshooting without risk of breaking state — with the federation-policy caveat noted.
