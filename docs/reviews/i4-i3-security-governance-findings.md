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
   SP + admins. **Confirmed live 2026-06-16:** `ecdh-prod-ipums` is **missing entirely** (every
   other dev/prod scope exists) — a missing declared scope is the same class of undetected drift, so
   the auditor must check scope **existence**, not just ACLs.
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

## Runtime confirmation (2026-06-16)
Ran the secret-scope portion of the confirmation checklist. **All dev + prod scopes present and as
expected except `ecdh-prod-ipums`, which is absent** (other prod scopes — loinc/umls/teams-webhook —
exist). Impact: the **prod** geography + crosswalk builds read the NHGIS key from this scope
(`ipums_secret_scope` for the prod target), so they would fail at `_get_secret(...)` until it's
created — i.e. prod geography is **not yet runnable**. Not a vulnerability; a **prod-provisioning
gap**. Remediation (when prod geography is stood up): create `ecdh-prod-ipums`, add the
`nhgis_api_key` secret, grant `READ` to the prod deploy SP (`caff7ad3-…`); track in
`docs/operations.md` (prod setup). This is a concrete instance of SHOULD-FIX #1 — nothing checks
scope existence, so the gap was invisible.

**Resolved + signed off (2026-06-16):** `ecdh-prod-ipums` was created and granted `READ` to the prod
SP (`caff7ad3-…`). Full ACL contents then captured for all `ecdh-*` scopes — **clean**: each grants
`READ` only to its correct per-env deploy SP and `MANAGE` only to the admin; no groups, no `users`,
no cross-env leakage.

**Nit (vestigial config):** the `ecdh-{dev,prod}-teams-webhook` scopes have **no SP `READ`** — which
is correct, because all job alerting uses the managed notification-destination ID
(`teams_destination_id`), not the raw URL. No code reads `teams_webhook_scope`/`_key`, so those two
variables (and the stored webhook secret) are unused → consider removing them from
`databricks-common.yml` to cut confusion and a small unused attack surface (a stored webhook URL
nobody reads). Non-`ecdh` scopes (`project-secrets`, `towerscout`) belong to other projects in the
shared workspace — out of scope here.
