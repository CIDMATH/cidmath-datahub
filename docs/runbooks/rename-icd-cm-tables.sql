-- Rename the ICD clinical-modification (diagnosis) tables to disambiguate them
-- from the procedure code systems now arriving (ICD-10-PCS; later ICD-9 Vol 3):
--   codes.icd10 -> codes.icd10cm   (ICD-10-CM, diagnoses)
--   codes.icd9  -> codes.icd9cm    (ICD-9-CM,  diagnoses)
--
-- Pure table-NAME change: data, columns (icd10_code, parent_icd10_code, ...) and
-- schema are unchanged, so a Unity Catalog metadata RENAME is sufficient — no
-- rebuild (unlike the geography rename, which also changed discriminator values).
--
-- Sequencing: deploy the build-script change (TABLE = "icd10cm"/"icd9cm") together
-- with this. Builds are manual-run, so there is no race. Run the DEV section in
-- the dev SQL editor first, verify, then the PROD section.

-- =====================================================================
-- DEV  (ecdh_model_dev)
-- =====================================================================
-- 1. Pre-check: old tables exist; note row counts.
SELECT 'icd9' AS t, COUNT(*) AS rows FROM ecdh_model_dev.codes.icd9
UNION ALL SELECT 'icd10', COUNT(*) FROM ecdh_model_dev.codes.icd10;

-- 2. Rename (metadata op; preserves data + Delta history).
ALTER TABLE ecdh_model_dev.codes.icd9  RENAME TO ecdh_model_dev.codes.icd9cm;
ALTER TABLE ecdh_model_dev.codes.icd10 RENAME TO ecdh_model_dev.codes.icd10cm;

-- 3. Repoint the _ops registration + DQ rows (no rebuild).
UPDATE ecdh_model_dev._ops.dataset_catalog     SET full_table_name = 'ecdh_model_dev.codes.icd9cm'  WHERE full_table_name = 'ecdh_model_dev.codes.icd9';
UPDATE ecdh_model_dev._ops.dataset_catalog     SET full_table_name = 'ecdh_model_dev.codes.icd10cm' WHERE full_table_name = 'ecdh_model_dev.codes.icd10';
UPDATE ecdh_model_dev._ops.dataset_engineering SET full_table_name = 'ecdh_model_dev.codes.icd9cm'  WHERE full_table_name = 'ecdh_model_dev.codes.icd9';
UPDATE ecdh_model_dev._ops.dataset_engineering SET full_table_name = 'ecdh_model_dev.codes.icd10cm' WHERE full_table_name = 'ecdh_model_dev.codes.icd10';
-- DQ history: table_name is recorded as schema.table (not catalog-qualified).
UPDATE ecdh_model_dev._ops.dq_results SET table_name = 'codes.icd9cm'  WHERE table_name = 'codes.icd9';
UPDATE ecdh_model_dev._ops.dq_results SET table_name = 'codes.icd10cm' WHERE table_name = 'codes.icd10';

-- 4. Post-check: new names present, registration repointed.
SELECT 'icd9cm' AS t, COUNT(*) AS rows FROM ecdh_model_dev.codes.icd9cm
UNION ALL SELECT 'icd10cm', COUNT(*) FROM ecdh_model_dev.codes.icd10cm;
SELECT full_table_name FROM ecdh_model_dev._ops.dataset_catalog
WHERE full_table_name LIKE 'ecdh_model_dev.codes.icd%' ORDER BY full_table_name;
-- Expect: ecdh_model_dev.codes.icd10cm, ecdh_model_dev.codes.icd9cm (no bare icd9/icd10).

-- =====================================================================
-- PROD  (ecdh_model_prod)  -- run only after DEV verifies
-- =====================================================================
ALTER TABLE ecdh_model_prod.codes.icd9  RENAME TO ecdh_model_prod.codes.icd9cm;
ALTER TABLE ecdh_model_prod.codes.icd10 RENAME TO ecdh_model_prod.codes.icd10cm;
UPDATE ecdh_model_prod._ops.dataset_catalog     SET full_table_name = 'ecdh_model_prod.codes.icd9cm'  WHERE full_table_name = 'ecdh_model_prod.codes.icd9';
UPDATE ecdh_model_prod._ops.dataset_catalog     SET full_table_name = 'ecdh_model_prod.codes.icd10cm' WHERE full_table_name = 'ecdh_model_prod.codes.icd10';
UPDATE ecdh_model_prod._ops.dataset_engineering SET full_table_name = 'ecdh_model_prod.codes.icd9cm'  WHERE full_table_name = 'ecdh_model_prod.codes.icd9';
UPDATE ecdh_model_prod._ops.dataset_engineering SET full_table_name = 'ecdh_model_prod.codes.icd10cm' WHERE full_table_name = 'ecdh_model_prod.codes.icd10';
UPDATE ecdh_model_prod._ops.dq_results SET table_name = 'codes.icd9cm'  WHERE table_name = 'codes.icd9';
UPDATE ecdh_model_prod._ops.dq_results SET table_name = 'codes.icd10cm' WHERE table_name = 'codes.icd10';
