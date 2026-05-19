# 0012 — Identity, service principals, and deploy authorization

## Status
Accepted — 2026-05-15

## Context
ADR 0004 specified that prod deploys go through GitHub Environments with manual approval and that the deploys themselves use a service principal rather than human credentials. ADR 0010 has alerting per-bundle owners. ADR 0011 has integration tests deploying to a CI-only dev target. None of these specified *which* SPs exist, *which grants they hold*, or *how* devs deploy locally without using a shared credential.

The team's existing state (per the setup checklist): a `github-actions` service principal exists from prior CI/CD exploration. SSO via Emory NetID works. No formal grant model has been documented.

The decision matters for three reasons:

1. **Blast radius.** A single SP with workspace admin rights can do anything; if its token leaks, the worst case is severe. Right-sized SPs with minimum necessary grants reduce that.
2. **Auditability.** Per-environment SPs make it possible to attribute every action to the correct context (CI dev deploy vs. CI prod deploy vs. human action).
3. **Day-to-day ergonomics.** Devs deploying to their personal dev target shouldn't need access to prod credentials. The setup should make the right thing easy and the wrong thing impossible.

The decision is constrained by Databricks' identity model (account-level identities, workspace-level group membership, OAuth M2M tokens) and GitHub's identity model (organization secrets, GitHub OIDC for keyless authentication).

## Decision

### Three identity types

| Identity | Used by | Auth method |
|---|---|---|
| **Human (Emory NetID)** | All team members for interactive work and personal dev deploys | SSO via SAML (existing) |
| **Service Principal (per env)** | CI workflows for automated deploys | OAuth M2M (Databricks-managed client secrets) |
| **GitHub OIDC token → Databricks** | CI workflows authenticating to Databricks without long-lived secrets | Federated identity (preferred) |

### Service principals: one per environment, broad scope within env

```
ecdh-deploy-dev    — deploys all bundles to the dev workspace
ecdh-deploy-prod   — deploys all bundles to the prod workspace
```

Each SP is used by GitHub Actions to deploy ALL bundles (platform + every domain bundle) to its environment. Per-bundle SPs were considered (see alternatives) and rejected for our scale.

**Rationale for env-scoped, not bundle-scoped:** With bundle-per-domain (ADR 0004), per-bundle SPs would mean ~10+ SPs per environment, each with overlapping but slightly different grants. The management overhead exceeds the audit value at our scale. Environment is the right boundary because it's the boundary that *actually matters for blast radius*: a prod deploy can break prod regardless of which bundle it's in.

### Service principal grants

**`ecdh-deploy-dev`** at workspace level:
- `WORKSPACE_ACCESS` to `emory-cidmath-databricks-workspace` (dev workspace)
- Member of the `ecdh-data-engineers` workspace group
- `CAN_USE` on dev SQL warehouses
- `CAN_USE` on dev cluster policies

**`ecdh-deploy-dev`** at Unity Catalog level:
- `USE_CATALOG` on `ecdh_dev`
- `CREATE_SCHEMA`, `CREATE_TABLE`, `CREATE_VOLUME` on `ecdh_dev`
- `MODIFY` on `ecdh_dev._ops.*` (so the platform bundle can manage the ops tables)
- `USE_CATALOG` on `ecdh_model_dev` (when that catalog is created)

**`ecdh-deploy-prod`** mirrors the above but scoped to `ecdh_prod` and the prod workspace. Critically, `ecdh-deploy-prod` has *no* access to dev catalogs and vice versa — environment isolation is enforced at the grant level, not just by trust.

Neither SP is a metastore admin or account admin. Catalog creation itself (a one-time, account-admin-only operation) is done by a human account admin during initial setup, not by the SP.

### How SPs authenticate

**Use GitHub OIDC federation.** OIDC is confirmed available on the Databricks account. GitHub Actions presents an OIDC token to Databricks; Databricks (configured to trust github.com's OIDC provider) exchanges it for an access token scoped to the SP. No long-lived secrets stored in GitHub.

Setup:

1. Create a Service Principal Federation Policy on each SP (CLI or account console). For each policy:
   - **Issuer:** `https://token.actions.githubusercontent.com`
   - **Subject:** `repo:CIDMATH/cidmath-datahub:environment:<env>` — binds the SP to a specific GitHub repo + environment combination. `ecdh-deploy-prod`'s policy uses `environment:prod`; `ecdh-deploy-dev`'s uses `environment:dev`.
   - **Audiences:** the Databricks account ID (`020f2275-adfe-44a9-99fa-e65e9369cea9`). The Databricks docs recommend setting audiences to the account ID; if omitted, the account ID is used by default.
2. GitHub workflows authenticate by setting three environment variables on each job: `DATABRICKS_AUTH_TYPE: github-oidc`, `DATABRICKS_HOST`, and `DATABRICKS_CLIENT_ID` (the SP's `application_id` UUID). The job also declares `permissions: id-token: write` and `environment: dev|prod`. The CLI and SDK then handle the token exchange transparently.

OAuth M2M client credentials are not used. If OIDC ever becomes unavailable (Databricks deprecates the integration, account policy changes), we'd fall back to M2M client secrets in GitHub Environments with a 90-day rotation cadence — but that's an emergency path, not the steady state.

### Dev personal deploys

Each developer can deploy to a *personal* dev target derived from their identity. DAB supports this via `mode: development`, which prefixes job/pipeline names with `[dev <username>]` and isolates them from each other.

**Mechanism:**

- Dev runs `databricks auth login` with their Emory NetID — authenticates as a *human user*, not an SP.
- Their dev deploys target `mode: development`, which auto-prefixes resources with their username and writes to a personal schema/path scoped within the dev catalog.
- Dev users have `USE_CATALOG`, `CREATE_SCHEMA` on `ecdh_dev` — they can create their own dev schemas inside it (e.g., `ecdh_dev.wastewater_raw_connor`).

**Constraint:** dev users do *not* have access to `ecdh_prod`. Their NetID is in the `ecdh-data-engineers` group; that group has dev-only grants. Production access is granted only to specific identities (initially: the account admins).

### CI in domain bundle PRs

PR validation workflows run with read-only auth — they call `databricks bundle validate` (no deploy) using a read-only token scoped to the dev workspace. This catches syntactic and resource-reference errors without consuming dev environment slots.

Integration tests (ADR 0011) deploy to the dev environment using `ecdh-deploy-dev`. They use `mode: development` with a CI-specific username prefix (e.g., `[ci-<pr-number>]`) so test artifacts don't collide with developers' personal dev work, and so cleanup is unambiguous.

### Production deploy gating

Per ADR 0004:

- Prod deploys are tag-triggered (e.g., `v1.2.0`).
- Deploy job runs in a GitHub Environment named `prod` with `required_reviewers` set to the data team's GitHub team.
- The job uses `ecdh-deploy-prod` (via OIDC federation) to authenticate to Databricks.
- A human reviewer must approve before the deploy step executes.

This means: SP credentials with prod write access are *only* usable when a human has explicitly approved the workflow run. With OIDC, there are no long-lived credentials to leak in the first place — every token is ephemeral and bound to the approved workflow context.

### Token rotation and audit

- OIDC tokens: ephemeral, no rotation needed. This is the steady-state mechanism.
- OAuth M2M client secrets: not used. If they ever are (emergency fallback), rotation cadence is every 90 days with `docs/runbooks/rotate-sp-credentials.md` capturing the process.
- Human credentials: managed by Emory IT; no project-level rotation.
- Audit: `system.access.audit` captures every SP action with the SP's identity. Quarterly review of SP audit logs by the data team owner.

### Adding a new identity

Process for adding a new human dev:

1. Their NetID gets added to the `ecdh-data-engineers` workspace group by the workspace admin.
2. They run `databricks auth login` locally.
3. They can deploy to dev via `mode: development`. No prod access by default.

Process for adding a new SP (rare):

1. Write an ADR proposing why a new SP is needed (the default is the env-scoped pair is sufficient).
2. Account admin creates the SP and applies grants.
3. Update `_platform` bundle's grant declarations to maintain consistency in code.

## Alternatives considered
- **One SP per bundle per environment.** Rejected. Adds ~20+ SPs (10 bundles × 2 envs) to manage. Audit per-SP is theoretically nicer but practically not used at our scale — we'll attribute by environment + run, not by bundle SP.
- **One global SP for both environments.** Rejected. Single point of failure; a leak compromises everything. The blast radius reduction from env-scoping is significant.
- **Use a single shared "deploy user" account that humans share.** Rejected. No real audit (which human used the shared account?); credentials shared among humans always leak; bad security hygiene.
- **PATs (Personal Access Tokens) for CI instead of SPs.** Rejected. PATs are tied to a human account and inherit human credentials' permissions; bad practice for automation.
- **OAuth M2M client secrets as the default.** Rejected. OIDC is confirmed available on the account; using long-lived secrets would add rotation overhead with no security benefit. M2M is documented only as an emergency fallback.
- **Allow devs to deploy to prod with manual approval but no SP.** Rejected. Even with approval, human-credential prod deploys make audit murkier ("Was that you, or was it a script you ran?"). SP-only prod deploys give clear attribution.

## Consequences
- **Blast radius is environment-bounded.** A leaked dev SP can't touch prod and vice versa.
- **Audit by environment is clean.** Every prod action attributable to `ecdh-deploy-prod`; every dev CI action to `ecdh-deploy-dev`; every human action to the developer's NetID.
- **OIDC eliminates a class of secrets to manage.** No GitHub secrets to rotate; tokens are ephemeral per workflow run.
- **Dev personal deploys are frictionless.** `databricks auth login` with NetID + `mode: development` and you're productive.
- **Catalog creation is a human-only operation.** Required first-time setup is done by an account admin, not the SP. Trade-off: small operational moment for new environments; benefit: no SP needs metastore admin.
- **Operational overhead is small.** Two SPs to manage; OIDC handles auth ephemerally so no rotation. Quarterly audit review remains as the routine practice.
- **Per-bundle audit is less granular.** Tooling can attribute a deploy to a specific bundle and run but not to a separate SP identity. Acceptable.
- **First-day setup is more involved than a single global SP would have been.** Trade-off accepted: ~30 extra minutes of setup vs. years of better security posture.
