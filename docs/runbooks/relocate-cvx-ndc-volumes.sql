-- Relocate the CVX + NDC revise-in-place raw Volumes to the source->model path
-- (ADR 0037 backport wave 2; ADR 0032 history model unchanged). The data model and the
-- snapshot_date history semantics are unchanged; this moves the write path, placement,
-- and the raw-snapshot Volume:
--   * ADD  raw landings in the SOURCE catalog:   ecdh_<env>.codes_raw.cvx
--                                                ecdh_<env>.codes_raw.ndc_product / .ndc_package
--   * MOVE the immutable raw snapshots onto the shared source-catalog landing Volume:
--          ecdh_<env>.codes_raw._landing   (keyed .../vintage=<snapshot_date>, ADR 0039)
--          (was the model-catalog Volumes  ecdh_model_<env>.codes.cvx_raw / .ndc_raw)
--   * KEEP canonical in the MODEL catalog:        ecdh_model_<env>.codes.cvx
--                                                 ecdh_model_<env>.codes.ndc_product / .ndc_package
--          (now update_semantics=vintage_snapshot, per-snapshot atomic replaceWhere)
--   * DROP the _current views:                    codes.cvx_current / ndc_product_current /
--                                                 ndc_package_current (ADR 0034: current = MAX(snapshot_date))
--   * DROP the old model-catalog raw Volumes:     ecdh_model_<env>.codes.cvx_raw / .ndc_raw
--
-- CVX and NDC are reproducible from the public CDC IIS / FDA downloads and the build is
-- idempotent + re-pullable, so this is a DROP + REBUILD, not a data migration (mirrors
-- docs/runbooks/realign-ruca-source-path.sql). Prior snapshots held only in the OLD
-- model-catalog Volumes are re-fetchable only as the *current* source list -- if you must
-- preserve historical raw payloads, COPY the old Volume files into the new landing Volume
-- (see the optional step) BEFORE dropping the old Volumes. The canonical table SHAPES are
-- unchanged, so consumers are unaffected after the rebuild.
--
-- Sequencing: deploy the build-script changes (build_cvx.py / build_ndc.py:
-- --source-catalog/--model-catalog, codes_raw landing + promote on build_reference,
-- vintage_snapshot replaceWhere, _current views removed) AND the reference_builder change
-- (Vintage = int|date|str; the date-typed replaceWhere predicate) FIRST. Builds are
-- manual/scheduled runs, so there is no race. Run DEV, verify, then PROD. IF EXISTS makes
-- every step idempotent.

-- =====================================================================
-- DEV  (source ecdh_dev / model ecdh_model_dev)
-- =====================================================================

-- 1. Pre-check: note current canonical names + row counts (to compare post-rebuild).
SHOW TABLES IN ecdh_model_dev.codes LIKE '{cvx,ndc_*}';
SELECT 'cvx'         AS t, COUNT(*) AS rows FROM ecdh_model_dev.codes.cvx
UNION ALL SELECT 'ndc_product', COUNT(*) FROM ecdh_model_dev.codes.ndc_product
UNION ALL SELECT 'ndc_package', COUNT(*) FROM ecdh_model_dev.codes.ndc_package;
-- Note the snapshot_date history you expect to retain:
SELECT 'cvx' AS t, snapshot_date, COUNT(*) FROM ecdh_model_dev.codes.cvx GROUP BY snapshot_date ORDER BY snapshot_date;

-- 2. (OPTIONAL) Preserve historical raw payloads. The new build only re-fetches the CURRENT
--    source list, so past raw snapshots exist only in the OLD Volumes. If you need them,
--    copy each dated file into the new landing Volume under its snapshot_date partition
--    BEFORE dropping the old Volume, e.g. (run from a notebook with dbutils):
--      dbutils.fs.cp(
--        "dbfs:/Volumes/ecdh_model_dev/codes/cvx_raw/cvx_2026-01-15.xml",
--        "dbfs:/Volumes/ecdh_dev/codes_raw/_landing/cvx/vintage=2026-01-15/cvx_xml_new.xml")
--      dbutils.fs.cp(
--        "dbfs:/Volumes/ecdh_model_dev/codes/ndc_raw/ndctext_2026-01-01.zip",
--        "dbfs:/Volumes/ecdh_dev/codes_raw/_landing/ndc_directory/vintage=2026-01-01/ndctext.zip")
--    (also write an empty _FETCH_COMPLETE marker in each dir so the builder treats it as landed.)

-- 3. Drop the retired _current views (ADR 0034 -- "current" is MAX(snapshot_date)).
DROP VIEW IF EXISTS ecdh_model_dev.codes.cvx_current;
DROP VIEW IF EXISTS ecdh_model_dev.codes.ndc_product_current;
DROP VIEW IF EXISTS ecdh_model_dev.codes.ndc_package_current;

-- 4. Run the folded builds (creates codes_raw schema + landing Volume + raw tables, promotes
--    canonicals). From the repo root with the bundle deployed to dev:
--      databricks bundle run --target dev build_cvx_reference
--      databricks bundle run --target dev build_ndc_reference

-- 5. Post-check: parity. Canonical row counts for today's snapshot should match a fresh run of
--    the old build (same source list), and the raw tables should now exist.
SELECT 'cvx'         AS t, COUNT(*) AS rows FROM ecdh_model_dev.codes.cvx
UNION ALL SELECT 'ndc_product', COUNT(*) FROM ecdh_model_dev.codes.ndc_product
UNION ALL SELECT 'ndc_package', COUNT(*) FROM ecdh_model_dev.codes.ndc_package;
SHOW TABLES IN ecdh_dev.codes_raw LIKE '{cvx,ndc_*}';

-- 6. Drop the old model-catalog raw Volumes (their payloads are re-fetchable / were copied in
--    step 2). Volumes must be empty-managed or you accept losing the archived files.
DROP VOLUME IF EXISTS ecdh_model_dev.codes.cvx_raw;
DROP VOLUME IF EXISTS ecdh_model_dev.codes.ndc_raw;

-- =====================================================================
-- PROD (source ecdh_prod / model ecdh_model_prod) -- run only after DEV verifies.
-- Requires human approval (touches prod). Mirror steps 1-6 with the prod catalogs.
-- =====================================================================
-- DROP VIEW   IF EXISTS ecdh_model_prod.codes.cvx_current;
-- DROP VIEW   IF EXISTS ecdh_model_prod.codes.ndc_product_current;
-- DROP VIEW   IF EXISTS ecdh_model_prod.codes.ndc_package_current;
-- (run build_cvx_reference / build_ndc_reference against prod, verify parity, then:)
-- DROP VOLUME IF EXISTS ecdh_model_prod.codes.cvx_raw;
-- DROP VOLUME IF EXISTS ecdh_model_prod.codes.ndc_raw;
