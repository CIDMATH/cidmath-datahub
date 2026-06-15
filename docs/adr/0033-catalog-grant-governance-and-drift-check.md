# 0033 — Catalog-grant governance and drift checking

## Status
Accepted — 2026-06-15

## Context
ADR 0018 split grant application by securable level, and ADR 0012 set the identity
model that makes the split necessary. **Schema-level** grants are applied (and
verified) by the bundle deploy jobs: the deploy service principal owns the schemas
it creates, so it can `GRANT` and `SHOW GRANTS` on them. **Catalog-level** grants
(`USE CATALOG`, `CREATE SCHEMA`, and the access groups' catalog traversal) are
*not* applied by the deploy jobs — the deploy SP holds only `USE_CATALOG` +
`CREATE_*` and is deliberately **not** a catalog owner or metastore admin (ADR
0012), and in Unity Catalog you can only `GRANT` on a securable you own or hold
`MANAGE` on. So catalog grants are declared in
`scripts/setup/grant_catalog_permissions.sql` and applied by an account admin.

That file is version-controlled, but nothing checks that the live catalog still
matches it. The `codes.cvx` work surfaced the gap from the other direction: a
missing `READ VOLUME` grant went unnoticed until a read failed. The question
raised was whether to migrate catalog grants into code like schema grants. Doing
that the obvious way — granting the deploy SP catalog `MANAGE` so the jobs can
apply catalog grants — would collapse the blast-radius boundary ADR 0012 is built
on: a leaked dev deploy token could then re-grant anything at catalog scope.

## Decision
Keep the **schema-and-below → deploy pipeline; catalog-level → governance**
split. Do **not** grant the deploy SP catalog `MANAGE`. Instead make the existing
governance-owned SQL file a *checkable* source of truth:

- `scripts/setup/grant_catalog_permissions.sql` remains the declarative source of
  truth for catalog-level grants, applied by an account admin / catalog owner.
- `scripts/verify/audit_catalog_grants.py` parses the declared
  `GRANT ... ON CATALOG ...` statements and diffs them against live `SHOW GRANTS`
  output, failing on drift — a declared grant that is missing, or (for a declared
  principal) a catalog privilege held but not declared. Commented-out grants are
  treated as not declared. The parse + diff logic is pure and unit-tested
  (`tests/unit/verify/test_audit_catalog_grants.py`, per ADR 0011); only the
  `SHOW GRANTS` read touches a workspace.
- The audit runs under a **governance identity** that can `SHOW GRANTS` on the
  catalog (catalog owner / metastore admin), on a manual or scheduled cadence
  (`.github/workflows/audit-catalog-grants.yml`, `workflow_dispatch`; the schedule
  is enabled once such an identity is configured). It is intentionally **not** a
  per-PR gate run as the deploy SP — that SP cannot `SHOW GRANTS` at catalog scope
  anyway, and catalog grants change rarely.

The rule of thumb, now explicit: **schema-and-below grants are applied and verified
by the deploy pipeline; catalog-level grants are declared in the governance SQL
file, applied by an admin, and drift-checked — never applied by the deploy SP.**

## Alternatives considered
- **Grant the deploy SP catalog `MANAGE` and apply catalog grants from the
  bundles.** Rejected: it widens the deploy SP's blast radius to re-granting
  anything at catalog scope, undoing the environment-bounded isolation of ADR 0012
  for little benefit, since catalog grants change rarely.
- **Full IaC (Terraform `databricks_grant`) for catalog grants under a separate
  governance principal.** A legitimate heavier option; deferred. It still requires
  a privileged non-deploy identity, so it buys reproducibility but not a change to
  the trust boundary. The SQL-file-plus-drift-check gets most of the benefit now;
  this can supersede it later if catalog grants start churning.
- **Leave the SQL file unverified (status quo).** Rejected: it's the only grant
  surface with no automated check, so drift (manual edits, forgotten grants like
  the CVX `READ VOLUME` case) goes unnoticed until something breaks.
- **Run the drift check per-PR as the deploy SP.** Rejected: the deploy SP can't
  `SHOW GRANTS` on the catalog, so it would error rather than detect drift; and a
  per-PR cadence is overkill for a rarely-changing surface.

## Consequences
- **Catalog grants become declarative and drift-detected** without expanding the
  deploy SP's privileges — the security posture of ADR 0012/0018 is preserved.
- **One source of truth** (`grant_catalog_permissions.sql`) is now enforced, not
  just documented; an unintended drift fails the audit.
- **A governance identity is required** to run the audit (and to apply the file).
  Until one is configured, the workflow stays manual (`workflow_dispatch`) and the
  schedule is left commented — an honest stub rather than a check that would fail
  as the wrong identity.
- **The deploy-time verification for schema grants is unchanged** (ADR 0018) and
  remains the model for everything the SP owns, including the new `READ VOLUME`
  grant on raw-snapshot Volumes (ADR 0032).
- **A future move to Terraform-based catalog IaC remains open** and would supersede
  the SQL-file half of this decision; the governance-not-deploy-SP boundary would
  still hold.
