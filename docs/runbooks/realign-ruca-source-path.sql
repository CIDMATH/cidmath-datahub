-- Realign the RUCA geography reference to the source->model path + vintage_snapshot
-- (ADR 0038 Reconciliation; deltas 1-3). The data model is unchanged; this moves the
-- write path and placement:
--   * ADD  raw landings in the SOURCE catalog:  ecdh_<env>.geography_raw.us_ruca_tract / .us_ruca_zip
--   * KEEP canonical in the MODEL catalog:       ecdh_model_<env>.geography.us_ruca_tract / .us_ruca_zip
--          (now update_semantics=vintage_snapshot, atomic replaceWhere)
--   * KEEP the bridge view:                      ecdh_model_<env>.geography.us_ruca_zcta
--   * DROP the _current views:                   geography.us_ruca_tract_current / us_ruca_zip_current (ADR 0034)
--
-- RUCA is reproducible from the public USDA ERS download and the build is idempotent +
-- re-pullable, so this is a DROP + REBUILD, not a data migration (mirrors
-- docs/runbooks/rename-icd-cm-tables.sql). The canonical table SHAPES are unchanged, so
-- consumers are unaffected after the rebuild.
--
-- Sequencing: deploy the build-script change (build_ruca.py: --source-catalog/--model-catalog,
-- geography_raw landing + promote, vintage_snapshot replaceWhere, _current views removed) and the
-- vocabulary change (UpdateSemantics gains "vintage_snapshot") FIRST. Builds are manual-run, so
-- there is no race. Run DEV, verify, then PROD. IF EXISTS makes every step idempotent.

-- =====================================================================
-- DEV  (source ecdh_dev / model ecdh_model_dev)
-- =====================================================================
-- 1. Pre-check: note current canonical names + row counts (to compare post-rebuild).
SHOW TABLES IN ecdh_model_dev.geography LIKE 'us_ruca*';
SELECT 'tract' AS t, COUNT(*) rows FROM ecdh_model_dev.geography.us_ruca_tract
UNION ALL SELECT 'zip', COUNT(*) FROM ecdh_model_dev.geography.us_ruca_zip;

-- 2. Drop the retired _current views (ADR 0034 -- "current" is MAX(vintage); the views are gone).
DROP VIEW IF EXISTS ecdh_model_dev.geography.us_ruca_tract_current;
DROP VIEW IF EXISTS ecdh_model_dev.geography.us_ruca_zip_current;

-- 3. Drop the canonical tables + bridge view; the rebuild recreates them on the new write path.
DROP VIEW  IF EXISTS ecdh_model_dev.geography.us_ruca_zcta;
DROP TABLE IF EXISTS ecdh_model_dev.geography.us_ruca_tract;
DROP TABLE IF EXISTS ecdh_model_dev.geography.us_ruca_zip;
-- (No raw tables to drop yet -- geography_raw.us_ruca_* are created fresh by the rebuild.)

-- 4. Clear stale _ops registration + DQ rows in the MODEL catalog (canonicals, the dropped
--    _current views, and the bridge view). The rebuild re-registers fresh per layer.
DELETE FROM ecdh_model_dev._ops.dataset_catalog
  WHERE full_table_name IN (
    'ecdh_model_dev.geography.us_ruca_tract','ecdh_model_dev.geography.us_ruca_zip',
    'ecdh_model_dev.geography.us_ruca_tract_current','ecdh_model_dev.geography.us_ruca_zip_current',
    'ecdh_model_dev.geography.us_ruca_zcta');
DELETE FROM ecdh_model_dev._ops.dataset_engineering
  WHERE full_table_name IN (
    'ecdh_model_dev.geography.us_ruca_tract','ecdh_model_dev.geography.us_ruca_zip',
    'ecdh_model_dev.geography.us_ruca_tract_current','ecdh_model_dev.geography.us_ruca_zip_current',
    'ecdh_model_dev.geography.us_ruca_zcta');
DELETE FROM ecdh_model_dev._ops.dq_results
  WHERE table_name IN ('geography.us_ruca_tract','geography.us_ruca_zip');

-- 5. REBUILD: run the job (Databricks UI -> Workflows): build_ruca_reference, all desired vintages.
--    It creates ecdh_dev.geography_raw.us_ruca_tract / .us_ruca_zip (raw, engineer-only), promotes
--    ecdh_model_dev.geography.us_ruca_tract / .us_ruca_zip (vintage_snapshot), recreates us_ruca_zcta,
--    and re-registers each layer in its catalog's _ops.

-- 6. Post-check: raw present + 1:1 with source; canonical shapes/row counts match pre-migration;
--    no _current views; registration repointed; conformance join holds.
SHOW TABLES IN ecdh_dev.geography_raw LIKE 'us_ruca*';           -- expect us_ruca_tract (+ us_ruca_zip)
SHOW TABLES IN ecdh_model_dev.geography LIKE 'us_ruca*';          -- expect tract, zip, zcta (NO *_current)
DESCRIBE TABLE ecdh_model_dev.geography.us_ruca_tract;            -- expect geoid, vintage, state_geoid, ...
-- raw 1:1 with canonical (same logical rows per vintage):
SELECT r.vintage, COUNT(*) raw_rows, c.canon_rows
FROM ecdh_dev.geography_raw.us_ruca_tract r
JOIN (SELECT vintage, COUNT(*) canon_rows FROM ecdh_model_dev.geography.us_ruca_tract GROUP BY vintage) c
  ON r.vintage = c.vintage
GROUP BY r.vintage, c.canon_rows ORDER BY r.vintage;
-- PK uniqueness (expect 0 rows):
SELECT geoid, vintage, COUNT(*) n FROM ecdh_model_dev.geography.us_ruca_tract
GROUP BY geoid, vintage HAVING n > 1;
-- ADR 0035 conformance: RUCA (geoid, vintage) joins us_tract on the SAME key/vintage (expect ~0 unmatched).
SELECT COUNT(*) ruca_rows, COUNT(*) - COUNT(t.geoid) unmatched
FROM ecdh_model_dev.geography.us_ruca_tract r
LEFT JOIN ecdh_model_dev.geography.us_tract t ON r.geoid = t.geoid AND r.vintage = t.vintage
WHERE r.vintage = 2020;
-- bridge view still resolves:
SELECT COUNT(*) FROM ecdh_model_dev.geography.us_ruca_zcta WHERE vintage = 2020;
-- registration repointed (expect tract, zip, zcta in model; tract/zip raw in source; NO *_current):
SELECT full_table_name FROM ecdh_model_dev._ops.dataset_catalog
  WHERE full_table_name LIKE 'ecdh_model_dev.geography.us_ruca%' ORDER BY full_table_name;
SELECT full_table_name FROM ecdh_dev._ops.dataset_catalog
  WHERE full_table_name LIKE 'ecdh_dev.geography_raw.us_ruca%' ORDER BY full_table_name;

-- 7. Idempotency check: re-run a single vintage and confirm the others are untouched (atomic
--    per-vintage replaceWhere). Row counts for un-rebuilt vintages must not change.

-- =====================================================================
-- PROD  (source ecdh_prod / model ecdh_model_prod)  -- run only after DEV verifies
-- =====================================================================
DROP VIEW  IF EXISTS ecdh_model_prod.geography.us_ruca_tract_current;
DROP VIEW  IF EXISTS ecdh_model_prod.geography.us_ruca_zip_current;
DROP VIEW  IF EXISTS ecdh_model_prod.geography.us_ruca_zcta;
DROP TABLE IF EXISTS ecdh_model_prod.geography.us_ruca_tract;
DROP TABLE IF EXISTS ecdh_model_prod.geography.us_ruca_zip;
DELETE FROM ecdh_model_prod._ops.dataset_catalog
  WHERE full_table_name IN (
    'ecdh_model_prod.geography.us_ruca_tract','ecdh_model_prod.geography.us_ruca_zip',
    'ecdh_model_prod.geography.us_ruca_tract_current','ecdh_model_prod.geography.us_ruca_zip_current',
    'ecdh_model_prod.geography.us_ruca_zcta');
DELETE FROM ecdh_model_prod._ops.dataset_engineering
  WHERE full_table_name IN (
    'ecdh_model_prod.geography.us_ruca_tract','ecdh_model_prod.geography.us_ruca_zip',
    'ecdh_model_prod.geography.us_ruca_tract_current','ecdh_model_prod.geography.us_ruca_zip_current',
    'ecdh_model_prod.geography.us_ruca_zcta');
DELETE FROM ecdh_model_prod._ops.dq_results
  WHERE table_name IN ('geography.us_ruca_tract','geography.us_ruca_zip');
-- Then run build_ruca_reference against prod (--source-catalog ecdh_prod --model-catalog
-- ecdh_model_prod) and repeat the post-checks.
