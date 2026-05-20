# 0018 — Access groups and the grant model

## Status
Accepted — 2026-05-20

## Context
Up to this point the only identities with access to the data hub were the deploy service principals (`ecdh-deploy-dev` / `ecdh-deploy-prod`), which own everything they create, and the `ecdh-data-engineers` group, which holds engineer-tier grants on the `_ops` schema. That was enough to bootstrap the platform and build the first reference tables, but it does not cover the first real consumer: an analyst who needs to explore the `time` reference data without being able to modify anything or see internal/operational tables.

The access-tier model itself was already implied — the `cidmath_datahub.common.grants` helper distinguishes an *engineer* tier from a *reader* tier, and several ADRs (0004 on bundle responsibilities, 0014 on reference data) gesture at who reads what. But the group-to-tier mapping, the privileges each tier carries, and — most importantly — *which bundle applies which grant on which securable* were never written down in one place. This ADR records that model so it does not have to be reverse-engineered from grant statements scattered across setup jobs.

A constraint inherited from ADR 0017: Unity Catalog objects above the table level (catalogs, schemas, grants) are managed with SQL DDL inside setup/build jobs rather than DAB `resources.grants`, because grants-as-resource support varies across Databricks CLI versions. The grant model here is therefore expressed as helper-driven `spark.sql` calls, run idempotently as part of each bundle's deploy job.

## Decision

### Two groups, two privilege tiers
- **`ecdh-data-engineers` — engineer tier.** Schema privileges `USE SCHEMA, SELECT, MODIFY, CREATE TABLE`. This is the group for people who build and operate pipelines.
- **`ecdh-analysts` — reader tier.** Schema privileges `USE SCHEMA, SELECT` only. This is the template for end-user/consumer groups; future reader groups (e.g., a partner-specific group) follow the same pattern with their own bundle variable.

`USE CATALOG` is required to traverse into any schema and is granted once per catalog, at the catalog level, to every group that needs to reach content inside it. The privilege sets live in `cidmath_datahub.common.grants` (`ENGINEER_SCHEMA_PRIVILEGES`, `READER_SCHEMA_PRIVILEGES`).

### What each tier may read
- **Engineers** read everything: raw / processed / analysis schemas, reference schemas, and `_ops`.
- **Analysts (readers)** read analysis-layer schemas and reference schemas only — **never** raw, processed, or `_ops`. `_ops` holds operational/engineering metadata and is engineer-only by design.

### Where each grant is applied (placement principle)
Whoever owns a securable applies the grants on it. Catalogs are owned by an admin; schemas are owned by the deploy SP that creates them. Concretely:

- **Catalog-level `USE CATALOG`** for both groups: granted by an admin in `scripts/setup/grant_catalog_permissions.sql`. This is *not* done by the bundles, because granting on a catalog requires `MANAGE`/ownership and the deploy SP has only `USE CATALOG` + `CREATE SCHEMA`. (We learned this the hard way — an early version had `setup_ops_tables.py` try to grant catalog `USE CATALOG` and the job failed with `PERMISSION_DENIED: does not have MANAGE on Catalog`.)
- **`_platform`** (in `setup_ops_tables.py`, run for both the source and model catalogs): grants engineer-tier on the `_ops` schema to engineers, and reader-tier on the `discovery` schema to both groups. Analysts receive nothing on `_ops`.
- **`_reference`** (in `build_time.py`, and future reference builders): grants reader-tier on each reference schema (`time`, later `geography`, `pathogen`, `codes`, ...) to *both* the engineers and analysts groups.
- **Subject bundles** (future): grant engineer-tier on their `<subject>_raw` / `<subject>_processed` schemas to engineers, and reader-tier on their analysis-layer `<subject>` schema to analysts (and any other consumer groups).

The schema-level grants all work because the deploy SP owns the schemas it creates. The split — admin owns catalog grants, SP owns schema grants — mirrors the catalog-vs-schema ownership boundary itself.

### Reference schemas are read-only for human groups
Reference schemas are canonical, computationally generated, and pipeline-owned. Both the engineer and analyst groups therefore receive only the **reader** tier on them — even engineers do not get `MODIFY` / `CREATE TABLE` on `time`. The owning bundle's deploy service principal remains the schema owner and retains full control, so reference data changes only by re-running its pipeline, never by hand. This is the one place the engineer group is intentionally narrower than "full access to everything."

### Mechanism and group lifecycle
Schema-level grants are SQL DDL issued through the `grants` helper inside deploy jobs; they are idempotent, so re-running a deploy re-asserts the intended state. Catalog-level `USE CATALOG` is SQL DDL in the admin bootstrap script. Group names are bundle variables (`data_engineers_group`, `analysts_group`) in `databricks-common.yml`, so the model is environment-agnostic. **Creating the workspace groups and adding members is a manual account/workspace-admin step**, performed out of band (it is not something a bundle deploy should do) and recorded in the operations runbook.

## Alternatives considered
- **Table-level grants instead of schema-level.** Rejected: too granular to maintain, and the schema is the natural unit of access here (a subject's analysis layer is one schema). Table-level control can be layered in later for specific restricted datasets without changing this model.
- **DAB `resources.grants`.** Rejected per ADR 0017 — inconsistent support across CLI versions. SQL DDL in setup jobs is portable and gives clearer errors.
- **A single combined group.** Rejected: the whole point is to separate a read-only consumer tier from the engineer tier.
- **Granting analysts limited access to `_ops`** (e.g., the `dataset_catalog` discovery view). Tempting for discoverability, but rejected for now to keep `_ops` cleanly internal; a curated, analyst-facing discovery surface can be exposed later as an explicit view in a non-`_ops` schema if demand appears.
- **Per-user grants.** Rejected: grants go to groups, never individuals, so access is managed by group membership.

## Consequences
- **An analyst can explore the `time` data** once three things are true: the `ecdh-analysts` group exists, an admin has run the `USE CATALOG` grants in `grant_catalog_permissions.sql`, and the `_platform`/`_reference` jobs have applied the schema-level reader grants. The analyst then has `USE CATALOG` on `ecdh_model_<env>` plus reader-tier on `time` and `discovery`, and nothing else.
- **The grant model is self-verifying at deploy time.** After applying grants, the setup/build jobs read them back (`SHOW GRANTS`) and assert each group holds exactly its intended tier — including the negative assertion that analysts hold nothing on `_ops`. A mismatch raises and fails the deploy, so drift or an accidental over-grant can't ship silently and no manual verification step is required. The assertion logic lives in `cidmath_datahub.common.grants` (`verify_*`) and is unit-tested. Catalog-level `USE CATALOG` is the one grant not read back this way (the deploy SP may not own the catalog); it is self-checking at apply time instead.
- **Adding a new reader/consumer group is a small, well-trodden change**: add a bundle variable, pass it to the relevant jobs, grant reader-tier where that group should read. The pattern is now explicit.
- **A manual prerequisite exists**: the `ecdh-analysts` workspace group must be created and populated by an admin before the grants resolve to anything meaningful (granting to a non-existent principal will error, mirroring the SP gotcha in ADR 0017). This is documented as an operations step.
- **Engineers cannot hand-edit reference data**, by design. An engineer who needs to correct `time` must change and re-run the pipeline, or act as the owning service principal. This protects canonical reference data from drift but is a friction point to be aware of.
- **Finer-grained controls are deferred**: row-/column-level security, dynamic views for masking, and restricted (DUA-gated) dataset access are not addressed here and will get their own ADR when a concrete dataset requires them.
