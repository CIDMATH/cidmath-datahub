-- Rename the ICD clinical-modification (diagnosis) tables AND their code columns to
-- disambiguate them from the procedure code systems now arriving (ICD-10-PCS; later
-- ICD-9 Vol 3):
--   codes.icd10  -> codes.icd10cm   columns icd10_code -> icd10cm_code,
--                                            parent_icd10_code -> parent_icd10cm_code
--   codes.icd9   -> codes.icd9cm     columns icd9_code  -> icd9cm_code,
--                                            parent_icd9_code  -> parent_icd9cm_code
--
-- Unlike a pure table-NAME change, this also renames COLUMNS. A Delta column rename
-- requires enabling column-mapping mode (a one-way protocol upgrade nothing else in
-- this lakehouse uses). Because `codes.icd9cm`/`codes.icd10cm` are reproducible from
-- the public CDC/NCHS source and the builds are idempotent + re-pullable, we instead
-- DROP and REBUILD from source. This supersedes the earlier metadata-only ALTER RENAME
-- approach. No downstream consumers reference these columns yet (GEMs / union views
-- are deferred), so the rebuild is safe.
--
-- Sequencing: deploy the build-script change (TABLE = "icd10cm"/"icd9cm" AND the
-- icd10cm_code / icd9cm_code columns, files renamed to build_icd*cm.py) first. Builds
-- are manual-run, so there is no race. Run DEV, verify, then PROD.
-- The DROP/cleanup handles BOTH possible current states (whether or not the earlier
-- table-name ALTER was already applied): IF EXISTS covers bare and *cm names.

-- =====================================================================
-- DEV  (ecdh_model_dev)
-- =====================================================================
-- 1. Pre-check: note current names + row counts (whichever exist).
SHOW TABLES IN ecdh_model_dev.codes LIKE 'icd*';

-- 2. Drop the old diagnosis tables (covers both pre- and post-ALTER names).
DROP TABLE IF EXISTS ecdh_model_dev.codes.icd9;
DROP TABLE IF EXISTS ecdh_model_dev.codes.icd10;
DROP TABLE IF EXISTS ecdh_model_dev.codes.icd9cm;
DROP TABLE IF EXISTS ecdh_model_dev.codes.icd10cm;

-- 3. Clear stale _ops registration + DQ rows for all four possible names; the rebuild
--    re-registers codes.icd9cm / codes.icd10cm fresh and writes new DQ results.
DELETE FROM ecdh_model_dev._ops.dataset_catalog
  WHERE full_table_name IN ('ecdh_model_dev.codes.icd9','ecdh_model_dev.codes.icd10',
                            'ecdh_model_dev.codes.icd9cm','ecdh_model_dev.codes.icd10cm');
DELETE FROM ecdh_model_dev._ops.dataset_engineering
  WHERE full_table_name IN ('ecdh_model_dev.codes.icd9','ecdh_model_dev.codes.icd10',
                            'ecdh_model_dev.codes.icd9cm','ecdh_model_dev.codes.icd10cm');
DELETE FROM ecdh_model_dev._ops.dq_results
  WHERE table_name IN ('codes.icd9','codes.icd10','codes.icd9cm','codes.icd10cm');

-- 4. REBUILD: run the renamed jobs (Databricks UI -> Workflows), all editions:
--      build_icd9cm_reference, build_icd10cm_reference
--    They recreate codes.icd9cm / codes.icd10cm with the icd9cm_code / icd10cm_code
--    columns and re-register in _ops.

-- 5. Post-check: new tables + columns present; registration repointed; DQ fresh.
DESCRIBE TABLE ecdh_model_dev.codes.icd10cm;   -- expect icd10cm_code, parent_icd10cm_code
DESCRIBE TABLE ecdh_model_dev.codes.icd9cm;     -- expect icd9cm_code,  parent_icd9cm_code
SELECT 'icd9cm' AS t, COUNT(*) AS rows FROM ecdh_model_dev.codes.icd9cm
UNION ALL SELECT 'icd10cm', COUNT(*) FROM ecdh_model_dev.codes.icd10cm;
SELECT full_table_name FROM ecdh_model_dev._ops.dataset_catalog
  WHERE full_table_name LIKE 'ecdh_model_dev.codes.icd%' ORDER BY full_table_name;
-- Expect exactly: ecdh_model_dev.codes.icd10cm, ecdh_model_dev.codes.icd9cm.

-- =====================================================================
-- PROD  (ecdh_model_prod)  -- run only after DEV verifies
-- =====================================================================
DROP TABLE IF EXISTS ecdh_model_prod.codes.icd9;
DROP TABLE IF EXISTS ecdh_model_prod.codes.icd10;
DROP TABLE IF EXISTS ecdh_model_prod.codes.icd9cm;
DROP TABLE IF EXISTS ecdh_model_prod.codes.icd10cm;
DELETE FROM ecdh_model_prod._ops.dataset_catalog
  WHERE full_table_name IN ('ecdh_model_prod.codes.icd9','ecdh_model_prod.codes.icd10',
                            'ecdh_model_prod.codes.icd9cm','ecdh_model_prod.codes.icd10cm');
DELETE FROM ecdh_model_prod._ops.dataset_engineering
  WHERE full_table_name IN ('ecdh_model_prod.codes.icd9','ecdh_model_prod.codes.icd10',
                            'ecdh_model_prod.codes.icd9cm','ecdh_model_prod.codes.icd10cm');
DELETE FROM ecdh_model_prod._ops.dq_results
  WHERE table_name IN ('codes.icd9','codes.icd10','codes.icd9cm','codes.icd10cm');
-- Then run build_icd9cm_reference + build_icd10cm_reference against prod, and repeat the post-checks.
