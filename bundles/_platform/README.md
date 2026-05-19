# `_platform` bundle

Shared infrastructure for the CIDMATH Data Hub. Deploys first, before any other bundle.

## What it owns

- The `_ops` schema in each catalog (`ecdh_<env>._ops` and `ecdh_model_<env>._ops`).
- The `_ops` tables: `dataset_catalog`, `dataset_engineering`, `dq_results`, `pipeline_runs`, the `taxonomy_*` reference tables, and `provider_codes`. Created by `setup_ops_tables` job which runs SQL DDL against the catalogs.
- Base grants on catalogs and schemas for the `ecdh-data-engineers` workspace group and the deploy service principals.

## What it does **not** own

- **Catalogs.** `ecdh_<env>` and `ecdh_model_<env>` are created out-of-band by an account admin during initial workspace setup. The platform bundle assumes they exist.
- **Service principals and OIDC federation.** Created and configured out-of-band per `docs/operations.md` and ADR 0012.
- **Secret scopes.** Created out-of-band via the Databricks CLI per `docs/operations.md`.
- **Data movement.** No pipelines that move data. Reference data lives in `_reference`; subject data lives in `bundles/<subject>/`.

## Deploy

```bash
# Personal dev (deploys to your namespace within ecdh_dev)
cd bundles/_platform
databricks bundle validate --target dev
databricks bundle deploy --target dev

# Run the ops setup job once to create _ops tables
databricks bundle run --target dev setup_ops_tables
```

In CI, this happens automatically via `.github/workflows/deploy-platform.yml` on changes to `bundles/_platform/**` or `src/cidmath_datahub/**`.

## Prerequisite checklist

Before this bundle can deploy, an account admin must have done:

1. ✅ Created `ecdh_dev`, `ecdh_prod`, `ecdh_model_dev`, `ecdh_model_prod` catalogs in Unity Catalog with managed storage locations.
2. ✅ Created the `ecdh-data-engineers` workspace group and added engineers to it.
3. ✅ Created service principals `ecdh-deploy-dev` and `ecdh-deploy-prod` and granted them catalog-level permissions.
4. ✅ Configured GitHub OIDC federation from `github.com/CIDMATH/cidmath-datahub` to the deploy SPs.
5. ✅ Created Databricks secret scopes `ecdh-dev-teams-webhook` and `ecdh-prod-teams-webhook`, each containing the `data_hub` key with the Teams incoming webhook URL.

See `docs/operations.md` for the full procedure.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
