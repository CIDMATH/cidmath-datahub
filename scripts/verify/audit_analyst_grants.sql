-- Audit the grants held by the analyst reader group (ecdh-analysts).
--
-- This is the quick verification of the access model (ADR 0018, ADR 0019). It
-- does NOT need a separate identity: run it as yourself in a Databricks SQL
-- editor, as long as you can SHOW GRANTS (metastore admin, catalog owner, or
-- the object owner). It confirms the *configuration* — that the analyst group
-- was granted exactly what the model intends and nothing on _ops.
--
-- For a true end-to-end check (an analyst-only principal actually querying),
-- use scripts/verify/verify_analyst_access.py instead. See the README here.
--
-- Prerequisite: the _platform and _reference deploy jobs must have run with
-- the ecdh-analysts group already existing, so the GRANTs have been applied.
--
-- Swap ecdh_model_dev / ecdh_dev for the prod catalogs to audit prod.

-- ====================================================================
-- Integrated catalog (ecdh_model_dev): where `time` and `discovery` live
-- ====================================================================

-- EXPECT: a single row — USE CATALOG.
SHOW GRANTS `ecdh-analysts` ON CATALOG ecdh_model_dev;

-- EXPECT: two rows — USE SCHEMA and SELECT (reader tier).
SHOW GRANTS `ecdh-analysts` ON SCHEMA ecdh_model_dev.time;

-- EXPECT: two rows — USE SCHEMA and SELECT (reader tier).
SHOW GRANTS `ecdh-analysts` ON SCHEMA ecdh_model_dev.discovery;

-- EXPECT: EMPTY. Analysts must hold NO grant on _ops. This is the key
-- negative assertion of the model — _ops is engineer-only (ADR 0018).
SHOW GRANTS `ecdh-analysts` ON SCHEMA ecdh_model_dev._ops;

-- ====================================================================
-- Source catalog (ecdh_dev): USE CATALOG only for now (no analysis-layer
-- subject schemas exist yet; those grants land with subject bundles).
-- ====================================================================

-- EXPECT: a single row — USE CATALOG.
SHOW GRANTS `ecdh-analysts` ON CATALOG ecdh_dev;

-- EXPECT: EMPTY. No analyst grant on the source _ops either.
SHOW GRANTS `ecdh-analysts` ON SCHEMA ecdh_dev._ops;

-- ====================================================================
-- Cross-check: confirm the discovery view itself resolves and is readable
-- through the schema-level SELECT (the view's ownership chain is what lets
-- analysts read the underlying _ops rows without _ops access).
-- ====================================================================

-- EXPECT: the view exists and lists its columns (you are running as an
-- engineer/admin here, so this just confirms the object is in place).
DESCRIBE VIEW ecdh_model_dev.discovery.datasets;
