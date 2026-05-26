-- Cleanup SQL for the US geography table rename (us_ prefix; ADR 0006 refinement,
-- ADR 0020 implementation refinement note 2026-05-26). Run AFTER the new
-- build_geography and build_crosswalk jobs have successfully populated the
-- renamed tables, so we don't drop the old tables and then discover the new
-- ones didn't land. Run in the appropriate catalog (ecdh_model_dev or
-- ecdh_model_prod) — these statements are catalog-qualified for clarity.

-- =============================================================================
-- 1. Verify the renamed tables exist and have the expected row counts BEFORE
--    dropping anything. Eyeball that these numbers match what the old tables
--    had on their last successful run.
-- =============================================================================
SELECT 'us_state'      AS table_name, COUNT(*) AS rows FROM ecdh_model_dev.geography.us_state
UNION ALL SELECT 'us_county',    COUNT(*) FROM ecdh_model_dev.geography.us_county
UNION ALL SELECT 'us_tract',     COUNT(*) FROM ecdh_model_dev.geography.us_tract
UNION ALL SELECT 'us_zcta',      COUNT(*) FROM ecdh_model_dev.geography.us_zcta
UNION ALL SELECT 'us_hhs_region',COUNT(*) FROM ecdh_model_dev.geography.us_hhs_region
UNION ALL SELECT 'us_crosswalk', COUNT(*) FROM ecdh_model_dev.geography.us_crosswalk
UNION ALL SELECT 'boundary',     COUNT(*) FROM ecdh_model_dev.geography.boundary;

-- Spot-check that geography.boundary's geo_level discriminator values gained
-- the us_ prefix on the rebuild (the new jobs write "us_state" etc. as the
-- geo_level value).
SELECT geo_level, COUNT(*) AS rows
FROM ecdh_model_dev.geography.boundary
GROUP BY geo_level
ORDER BY geo_level;
-- Expected after rebuild: us_state, us_county, us_tract, us_zcta (and the
-- new country / country_subdivision / subnational_adm* rows once slices 3a/b/c
-- land). If you still see bare state / county / tract / zcta values here,
-- the boundary table also needs to be rebuilt — re-run build_geography.

-- =============================================================================
-- 2. Drop the old tables. Each is a full_refresh table (ADR 0007) so dropping
--    is safe; the data was reproducible from NHGIS source. DROP TABLE in Unity
--    Catalog is a soft delete; recoverable via UNDROP for 7 days if needed.
-- =============================================================================
DROP TABLE IF EXISTS ecdh_model_dev.geography.state;
DROP TABLE IF EXISTS ecdh_model_dev.geography.county;
DROP TABLE IF EXISTS ecdh_model_dev.geography.tract;
DROP TABLE IF EXISTS ecdh_model_dev.geography.zcta;
DROP TABLE IF EXISTS ecdh_model_dev.geography.hhs_region;
DROP TABLE IF EXISTS ecdh_model_dev.geography.crosswalk;

-- =============================================================================
-- 3. Remove orphaned rows from _ops.dataset_catalog and _ops.dataset_engineering
--    that point at the old table names. The renamed jobs will (and already
--    have, if they ran) registered new rows for the us_-prefixed names.
-- =============================================================================
DELETE FROM ecdh_model_dev._ops.dataset_catalog
WHERE full_table_name IN (
  'ecdh_model_dev.geography.state',
  'ecdh_model_dev.geography.county',
  'ecdh_model_dev.geography.tract',
  'ecdh_model_dev.geography.zcta',
  'ecdh_model_dev.geography.hhs_region',
  'ecdh_model_dev.geography.crosswalk'
);

DELETE FROM ecdh_model_dev._ops.dataset_engineering
WHERE full_table_name IN (
  'ecdh_model_dev.geography.state',
  'ecdh_model_dev.geography.county',
  'ecdh_model_dev.geography.tract',
  'ecdh_model_dev.geography.zcta',
  'ecdh_model_dev.geography.hhs_region',
  'ecdh_model_dev.geography.crosswalk'
);

-- =============================================================================
-- 4. Optionally: prune historical DQ results that point at the old table_name
--    values. (Leaving them is fine — they're an immutable audit trail. Pruning
--    just reduces noise in the "last-run DQ summary" view.)
-- =============================================================================
-- DELETE FROM ecdh_model_dev._ops.dq_results
-- WHERE table_name IN (
--   'geography.state',
--   'geography.county',
--   'geography.tract',
--   'geography.zcta',
--   'geography.hhs_region',
--   'geography.crosswalk'
-- );

-- =============================================================================
-- 5. For prod: identical script, replace ecdh_model_dev -> ecdh_model_prod.
-- =============================================================================
