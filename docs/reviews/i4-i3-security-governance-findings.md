# I4 + I3 — Security & governance review — findings

**Date:** 2026-06-16 · **Scope:** grants, deploy authorization, secret handling
(`common/grants.py`, `databricks-common.yml`, `scripts/setup/grant_catalog_permissions.sql`,
`scripts/verify/audit_catalog_grants.py`, the `_get_secret` builds). **Method:** repo-level
review. Part of the SDE review plan.

## Verdict
**No must-fix issues — strong posture for this stage.** Findings are refinements, concentrated in
the *manually-applied, not-drift-checked* corners. (Caveat: a repo review can't see live
workspace ACLs/grants — see the confirmation checklist; findings assume code reflects reality.)

## What's solid
- **Deploy identity:** OIDC federation, no GitHub-stored client secret (ADR 0012). Only SP
  *application IDs* + scope *names* are committed (identifiers, not secrets). `.databricks/`
  (tfstate + terraform binary) is gitignored — no state/secret leak.
- **Least privilege:** deploy SPs hold only `USE CATALOG` + `CREATE SCHEMA`, own what they create,
  no catalog `MANAGE`. Dev SP scoped to dev catalogs / prod to prod, with explicit *negative*
  `SHOW GRANTS` checks. Prod deploys reviewer-gated (GitHub Environments).
- **Grant model:** reader/engineer tiers; reference schemas reader-only for both groups; analyst
  prod access opt-in (commented out). `grants.py` verifies with `exact=True` and has a
  `verify_no_privileges` negative test — a real privilege-creep gate at deploy.
- **Catalog grants** governance-owned and drift-checked (ADR 0033 auditor flags missing *and*
  over-granted privileges; pure/unit-tested).
- **Secrets:** read via `dbutils.secrets` (never in code); no secret values printed/logged;
  `logging.py` bans `print`. Low current stakes — these are *download* credentials, not PHI access,
  over public reference data.

## Findings

### SHOULD-FIX
1. **Secret-scope ACLs aren't codified or drift-checked (I3).** Catalog grants got the full ADR-0033
   treatment (declared source-of-truth + auditor); secret-scope `READ` ACLs are applied by hand
   with no in-repo source of truth and no drift check. A scope accidentally granted `READ` to a
   broad group / `users` wouldn't be caught. → Extend the 0033 pattern to secret scopes (drafted as
   a follow-up issue). Runtime check meanwhile: confirm each scope grants `READ` only to its deploy
   SP + admins.
2. **`_get_secret` is copy-pasted** across `build_crosswalk` / `build_geography` / `build_loinc`
   (soon RxNorm/SNOMED). Move to `common/` (e.g. `common/secrets.py`) — one audited helper beats
   five copies; ties to the #1 shared-builder work.

### CONSIDER (low)
3. **Catalog-grant auditor only checks *declared* pairs** — catches over-granting to a declared
   principal, but not an **entirely undeclared** principal granted catalog access (a mistaken/rogue
   admin grant). Consider auditing all catalog grantees against an allowlist, or documenting the
   limitation.
4. **Confirm auth headers are never logged** — `build_loinc` builds a Basic-auth header (base64
   `user:pass`); verify the HTTP fetch layer doesn't log request headers.
5. **No documented secret-rotation cadence** for the long-lived download credentials — add a
   rotation/expiry note as sources grow.

## Pre-mortem
The failure six months out isn't in the automated model — it's in the manual corners it doesn't
cover: a secret scope gets `READ` granted to a broad group during a hurried setup and nobody
notices (no ACL drift check, #1), or an admin grants a contractor `USE CATALOG`+`SELECT` on
`ecdh_model_prod` for a one-off and never revokes it (auditor only checks declared pairs, #3). The
strong, drift-checked automation stops at deploy-SP-applied catalog grants; the human-applied
grants (secret ACLs, ad-hoc catalog grants) are the soft spot. Closing #1 and #3 hardens that seam.
