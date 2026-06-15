# Access verification

Tools for confirming the analyst access boundary holds as designed (ADR 0018 — access groups and grant model; ADR 0019 — analyst discovery view).

The intended boundary for the `ecdh-analysts` reader tier:

- **Can** read `discovery.datasets` (the curated dataset catalog) and the `time` reference tables.
- **Cannot** read anything in `_ops` (it is engineer-only).

## Automatic verification (runs on every deploy)

You do **not** normally need to run anything here by hand. The grant model is verified *as part of every deploy*: after the `_platform` and `_reference` jobs apply their grants, they immediately read them back with `SHOW GRANTS` and assert each group holds exactly the intended tier — including the negative assertion that analysts hold **nothing** on `_ops`. A mismatch raises, which fails the job and therefore the deploy. This logic lives in `cidmath_datahub.common.grants` (the `verify_*` functions) and is unit-tested in `tests/unit/common/test_grants.py`.

That deploy-time check runs as the deploy service principal (it owns the schemas it created, so it can `SHOW GRANTS` on them). It confirms the *configuration*. The manual tools below remain useful for two things the deploy-time check can't do: an out-of-band audit at any time (not just at deploy), and a true end-to-end test from an actual analyst identity.

## 1. Grant audit (quick, no extra identity)

`audit_analyst_grants.sql` runs `SHOW GRANTS` against the analyst group and confirms it was granted exactly the intended privileges — and nothing on `_ops`. Run it as yourself in a Databricks SQL editor (you need to be a metastore admin, catalog owner, or the object owner to `SHOW GRANTS`). Each statement has an `EXPECT:` comment describing the correct result.

This verifies the *configuration*. It's sufficient for most purposes because Unity Catalog grants are purely additive: if the analyst group has no grant on `_ops`, an analyst cannot reach `_ops`.

## 2. Live boundary test (thorough, needs an analyst-only principal)

`verify_analyst_access.py` actually issues queries and classifies each as allowed or blocked. It must run **as a principal that is a member of `ecdh-analysts` only** — not as you, because if you're also in `ecdh-data-engineers`, the engineer grants mask the analyst restriction and the test tells you nothing.

The easiest such identity is a dedicated test service principal. The steps below are ones **you run under your own admin credentials** — creating identities and handling their secrets isn't something the assistant can do for you.

### Setup (one time)

1. **Create the test service principal** (account-level), and add it to `ecdh-analysts` *only*:

   ```bash
   # Create the SP (note its application_id from the output)
   databricks service-principals create --display-name ecdh-analyst-test

   # Add it to the analyst group (use the group id and the SP application_id)
   databricks groups patch <ecdh-analysts-group-id> \
     --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
              "Operations":[{"op":"add","path":"members",
              "value":[{"value":"<sp-internal-id>"}]}]}'
   ```

   (The Databricks UI — Settings → Identity and access → Service principals / Groups — is often simpler than the SCIM patch for adding a member.)

2. **Generate an OAuth secret** for the SP (Settings → the SP → *Generate secret*, or `databricks service-principal-secrets create <sp-id>`). You hold this secret; never paste it into a shared location.

3. **Make sure the grants exist.** The `_platform` and `_reference` deploy jobs must have run *after* `ecdh-analysts` was created, so the grants have been applied. If you created the group afterward, re-run those bundles.

4. **Have a SQL warehouse** the SP can use (grant the SP `CAN_USE` on it). Note its warehouse id.

### Run

Point your environment at the analyst SP (do **not** use your own profile). Two values trip people up (ADR 0017):

- `DATABRICKS_CLIENT_ID` is the SP's **application_id** — a **UUID**. It is *not* the SP's numeric id (that numeric id is only for admin APIs / federation policies).
- `DATABRICKS_CLIENT_SECRET` is the **OAuth secret value** from *Generate secret* — a long opaque token, not a UUID.

`DATABRICKS_AUTH_TYPE=oauth-m2m` is important: without it, if the OAuth login fails the SDK silently falls back to your *own* profile, so you'd test as an engineer (who can read `_ops`) and every result would be wrong. The script now refuses to run unless auth resolves to OAuth M2M.

PowerShell (Windows):

```powershell
$env:DATABRICKS_HOST = "https://dbc-926acb48-1c75.cloud.databricks.com"
$env:DATABRICKS_AUTH_TYPE = "oauth-m2m"
$env:DATABRICKS_CLIENT_ID = "<analyst-sp-application-id-UUID>"
$env:DATABRICKS_CLIENT_SECRET = "<the-oauth-secret-you-generated>"
$env:DATABRICKS_WAREHOUSE_ID = "<warehouse-id>"

python scripts/verify/verify_analyst_access.py --catalog ecdh_model_dev
```

bash / zsh (macOS / Linux):

```bash
export DATABRICKS_HOST=https://dbc-926acb48-1c75.cloud.databricks.com
export DATABRICKS_AUTH_TYPE=oauth-m2m
export DATABRICKS_CLIENT_ID=<analyst-sp-application-id-UUID>
export DATABRICKS_CLIENT_SECRET=<the-oauth-secret-you-generated>
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>

python scripts/verify/verify_analyst_access.py --catalog ecdh_model_dev
```

Expected output: every check `PASS` — the three reads allowed, the two `_ops` reads blocked. A non-zero exit code means the boundary is not as intended.

### Cleanup

When you're done, you can leave the test SP in place for future re-checks, or delete it (UI, or `databricks service-principals delete <sp-id>`). It only ever had reader-tier access, so it's low risk to keep.

## 3. Catalog-grant drift check (ADR 0033)

`audit_catalog_grants.py` makes `scripts/setup/grant_catalog_permissions.sql` a *checkable* source of truth. Catalog-level grants are **not** applied by the bundle deploy jobs — the deploy SP lacks `MANAGE` on the catalog (ADR 0012/0018), so they're applied by an account admin from that SQL file. This script parses the declared `GRANT ... ON CATALOG ...` statements and compares them against what `SHOW GRANTS` actually reports, failing on drift (a declared grant missing, or — for a declared principal — a catalog privilege it holds that the file doesn't declare). Commented-out grants in the file are treated as *not declared*.

The parse/diff logic is pure and unit-tested (`tests/unit/verify/test_audit_catalog_grants.py`); only the `SHOW GRANTS` read touches a workspace.

**Identity:** run it as a principal that can `SHOW GRANTS` on the catalog — a catalog owner, metastore admin, or the same governance identity that applies the SQL file. The OIDC deploy SP generally **cannot** (no `MANAGE`), which is exactly why catalog grants stay governance-owned. This is therefore a governance-run / scheduled check, not a per-PR deploy-SP check (`.github/workflows/audit-catalog-grants.yml`, `workflow_dispatch`).

```powershell
$env:DATABRICKS_HOST = "https://dbc-926acb48-1c75.cloud.databricks.com"
$env:DATABRICKS_WAREHOUSE_ID = "<warehouse-id>"
# authenticate as a catalog owner / metastore admin (your own admin profile is fine)

python scripts/verify/audit_catalog_grants.py --catalogs ecdh_dev ecdh_model_dev
```

Exit code 0 = catalog grants match the declared file; non-zero = drift (or the file needs updating to match an intended change).
