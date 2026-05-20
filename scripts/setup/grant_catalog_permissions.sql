-- Catalog-level grants for the deploy service principals.
--
-- Run this AFTER:
--   1. Catalogs ecdh_dev, ecdh_prod, ecdh_model_dev, ecdh_model_prod exist
--      (created by account admin per docs/operations.md step 1).
--   2. Service principals ecdh-deploy-dev and ecdh-deploy-prod exist
--      (created by scripts/setup/create_service_principals.py).
--
-- Run as a user with catalog-owner or metastore-admin privileges. Execute
-- in a Databricks SQL editor or via `databricks sql query --file ...`.
--
-- These grants let each SP:
--   - USE CATALOG: see and reference objects in the catalog
--   - CREATE SCHEMA: create schemas inside the catalog (the platform bundle
--     does this when deploying)
--
-- The SP becomes the owner of any schema/table it creates, so it inherits
-- MODIFY/SELECT/etc. on everything it owns without additional grants.
--
-- IMPORTANT: Databricks UC GRANT statements identify service principals by
-- their application_id (UUID), not display name. The application_ids below
-- correspond to:
--   `a55b6164-c0eb-42cf-a438-7de33c150f4a`  = ecdh-deploy-dev
--   `caff7ad3-d82f-4692-98cc-678dc6807cbd`  = ecdh-deploy-prod
-- Service principal application_ids are quoted with backticks, same as
-- users and groups. If SPs are recreated, application_ids will change and
-- this file must be updated.

-- ====================================================================
-- DEV environment: ecdh-deploy-dev gets access to ecdh_dev and ecdh_model_dev
-- ====================================================================

GRANT USE CATALOG ON CATALOG ecdh_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;
GRANT CREATE SCHEMA ON CATALOG ecdh_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;

GRANT USE CATALOG ON CATALOG ecdh_model_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;
GRANT CREATE SCHEMA ON CATALOG ecdh_model_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;

-- Access-group catalog traversal (ADR 0018). These must be granted by a
-- catalog owner / metastore admin (this script), NOT by the bundle deploy
-- jobs: the deploy SP lacks MANAGE on the catalog, so it cannot grant
-- catalog-level privileges. Schema-level reader/engineer grants are still
-- applied automatically by the bundles (the SP owns the schemas it creates).
-- USE CATALOG alone exposes no data — readers still need per-schema USE
-- SCHEMA + SELECT, which the bundles grant only where each group should read.
GRANT USE CATALOG ON CATALOG ecdh_dev TO `ecdh-data-engineers`;
GRANT USE CATALOG ON CATALOG ecdh_model_dev TO `ecdh-data-engineers`;
GRANT USE CATALOG ON CATALOG ecdh_dev TO `ecdh-analysts`;
GRANT USE CATALOG ON CATALOG ecdh_model_dev TO `ecdh-analysts`;

-- Explicit deny on prod catalogs is unnecessary (default is no access),
-- but worth verifying: the dev SP must NOT have any grant on ecdh_prod
-- or ecdh_model_prod. Run the verification queries at the bottom of this
-- file to confirm.

-- ====================================================================
-- PROD environment: ecdh-deploy-prod gets access to ecdh_prod and ecdh_model_prod
-- ====================================================================

GRANT USE CATALOG ON CATALOG ecdh_prod TO `caff7ad3-d82f-4692-98cc-678dc6807cbd`;
GRANT CREATE SCHEMA ON CATALOG ecdh_prod TO `caff7ad3-d82f-4692-98cc-678dc6807cbd`;

GRANT USE CATALOG ON CATALOG ecdh_model_prod TO `caff7ad3-d82f-4692-98cc-678dc6807cbd`;
GRANT CREATE SCHEMA ON CATALOG ecdh_model_prod TO `caff7ad3-d82f-4692-98cc-678dc6807cbd`;

-- Engineers operate prod, so they get catalog traversal in prod too.
GRANT USE CATALOG ON CATALOG ecdh_prod TO `ecdh-data-engineers`;
GRANT USE CATALOG ON CATALOG ecdh_model_prod TO `ecdh-data-engineers`;

-- Analyst access to PROD is a deliberate policy decision, not a default.
-- Uncomment when analysts should be able to explore prod (they still only see
-- schemas where a bundle has granted them reader-tier USE SCHEMA + SELECT):
-- GRANT USE CATALOG ON CATALOG ecdh_prod TO `ecdh-analysts`;
-- GRANT USE CATALOG ON CATALOG ecdh_model_prod TO `ecdh-analysts`;

-- ====================================================================
-- Verification — run these after the GRANTs above and confirm the output
-- ====================================================================

-- Dev SP should have grants on dev catalogs only:
SHOW GRANTS `a55b6164-c0eb-42cf-a438-7de33c150f4a` ON CATALOG ecdh_dev;
SHOW GRANTS `a55b6164-c0eb-42cf-a438-7de33c150f4a` ON CATALOG ecdh_model_dev;

-- Dev SP should have NO grants on prod catalogs (these should return empty):
SHOW GRANTS `a55b6164-c0eb-42cf-a438-7de33c150f4a` ON CATALOG ecdh_prod;
SHOW GRANTS `a55b6164-c0eb-42cf-a438-7de33c150f4a` ON CATALOG ecdh_model_prod;

-- Prod SP should have grants on prod catalogs only:
SHOW GRANTS `caff7ad3-d82f-4692-98cc-678dc6807cbd` ON CATALOG ecdh_prod;
SHOW GRANTS `caff7ad3-d82f-4692-98cc-678dc6807cbd` ON CATALOG ecdh_model_prod;

-- Prod SP should have NO grants on dev catalogs (these should return empty):
SHOW GRANTS `caff7ad3-d82f-4692-98cc-678dc6807cbd` ON CATALOG ecdh_dev;
SHOW GRANTS `caff7ad3-d82f-4692-98cc-678dc6807cbd` ON CATALOG ecdh_model_dev;
