# Runbook — geography legacy → layered cutover

Migrates the `geography` subject from the **legacy** monolithic build
(`build_geography.py` `run()`, job `build_geography_reference`) to the **layered**
per-level builder (`build_<level>_layered`, job `build_geography_layered`, ADR
0036/0037/0039). Done one level at a time, parents-first
(`us_state → us_county → us_tract → us_zcta → us_block_group → us_block`).

Schema transitions here are **deliberate, reviewed steps** — we do *not* auto-migrate
canonical schemas in the build (the build declares its target schema via
`CREATE TABLE IF NOT EXISTS` and fails loudly on a mismatch; this runbook does the
explicit migration). Geography is vintage-reproducible from NHGIS, so the migration of
a table is a `DROP` + rebuild, not an in-place data migration.

## Per-level cutover checklist

For each level `<lvl>` (and its `<lvl>_boundary`):

1. **Build + validate in dev** via the layered job (`--level <lvl>`): enriched canonical
   present, parent-FK DQ green, row counts sane.
2. **Migrate the canonical table if the enriched schema differs from the legacy one.**
   The enriched canonical adds denormalized parent-label columns (e.g. `us_county` gains
   `state_name`/`state_stusps`/`state_hhs_region`; `us_tract` will gain `county_name` +
   state labels). `CREATE TABLE IF NOT EXISTS` can't add them to the pre-existing legacy
   table, so **drop it** and let the layered build recreate it enriched:
   ```sql
   DROP TABLE IF EXISTS ecdh_model_dev.geography.<lvl>;
   ```
   (No drop needed where the schema already matches — e.g. `us_state`, which always
   carried `stusps`/`hhs_region`.) Then repair-run the `<lvl>` task.
3. **Remove the level from the legacy build** (`build_geography.py` `run()`): drop it from
   `LEVELS`, its `_register_dataset` call, and its `_comment_tables` entry, so the legacy
   build stops writing `geography.<lvl>`. (Done for `us_state` + `us_county`.) Children
   still built by the legacy build load their parent geoids from the canonical tables for
   FK checks.
4. **Retire the level's `_enriched` view** (ADR 0028, superseded by the enriched canonical):
   remove it from `build_geography_views.py` and drop it:
   ```sql
   DROP VIEW IF EXISTS ecdh_model_dev.geography.<lvl>_enriched;
   ```
5. **Clean up the polymorphic boundary.** The layered build writes a per-level
   `geography.<lvl>_boundary`; the legacy polymorphic `geography.boundary` keeps stale
   `<lvl>` rows. After confirming consumers use the per-level table, delete them:
   ```sql
   DELETE FROM ecdh_model_dev.geography.boundary WHERE geo_level = '<lvl>';
   ```
   **NOTE (perms):** `geography.boundary` is owned by the build identity; interactive users
   have `SELECT`, not `MODIFY` (grants model working as intended — humans don't mutate
   build-owned tables), so this `DELETE` is `PERMISSION_DENIED` when run interactively.
   Either run it as the build owner (notebook/job as the service principal) **or, preferred,
   defer it** — the stale rows are harmless once consumers use `<lvl>_boundary`, and the
   wholesale `DROP TABLE geography.boundary` at Final retirement removes every level's
   leftovers in one owner-context step. Treat the per-level `DELETE` as optional.
6. **Prod.** Steps 1–5 above are dev. Repeat in prod **only after** the layered build is
   deployed and run in prod for `<lvl>` — otherwise prod stops rebuilding `<lvl>` with
   nothing replacing it. (Tolerable for immutable vintages, but prod keeps the lean
   schema until the layered build runs there, so coordinate the legacy-removal deploy with
   the prod layered run.)

## Status / immediate steps

- **`us_state`** — layered build proven in dev; schema matched legacy (no drop needed);
  removed from the legacy build. ✅ (dev)
- **`us_county`** — layered proven in dev (enrichment + parent-FK green); table dropped +
  rebuilt enriched; `us_county_enriched` view dropped; removed from the legacy build. ✅ (dev)
- **`us_tract`** — layered proven in dev (county_name + state labels populated both vintages;
  4 parent-FK checks 0 orphans; 73,669 / 85,060 rows); `us_tract_enriched` view dropped +
  removed from `build_geography_views.py`; removed from legacy. ✅ (dev)
- **`us_zcta`** — non-nesting; built on the layered builder (entity + boundary +
  `us_zcta_county_xwalk`), enriched with the approximate **primary county** (largest
  land-area overlap, Census ZCTA↔county relationship file). Removed from legacy (legacy now
  builds only `us_hhs_region`). **Do to finish dev:**
  ```sql
  DROP TABLE IF EXISTS ecdh_model_dev.geography.us_zcta;  -- legacy lean 7-col -> enriched 17-col
  -- then run the us_zcta task; ensure_canonical recreates us_zcta enriched + us_zcta_county_xwalk
  DELETE FROM ecdh_model_dev.geography.boundary WHERE geo_level = 'us_zcta';  -- owner-context; optional
  ```
  **Prereq:** the job network allowlist must permit `www2.census.gov` (the relationship-file
  host), in addition to the NHGIS API. Confirm the 2010 header resolves (different column
  names + comma delimiter vs 2020 pipe) — the parser fails loud on an unmatched column.
- **`us_block_group`** — nests in tract; built on the layered builder (entity + boundary),
  enriched with county_name + state labels, FK-validated against tract/county/state. NHGIS
  national file `us_blck_grp_<year>_tl<year>` + national cenpop. **Do to finish dev:**
  ```sql
  -- no pre-existing legacy table (BG was never in the legacy build), so no DROP needed;
  -- just run the us_block_group task. ~217k/242k rows per vintage.
  ```
- **`us_block`** — atomic level; built on the layered builder (entity + boundary). **Differs:**
  NHGIS has no national block file, so it fetches ~51 per-state shapefiles
  (`<stfips>0_block_<year>_tl<year>`) in one extract and reads them **per-state + UNION**
  (driver holds one state at a time). Nests in `us_block_group` (FK block_group/county/state;
  all-water gap scoped + recorded WARN like BG). No cenpop. ~8M rows/vintage. **No DROP**
  (never in legacy). **Watch:** large-state read (CA/TX) loads into the driver — bump the
  driver if it OOMs; ~8M-polygon geometry simplification is the slow part.

## Validate after each level

```sql
-- enriched labels populated, counts sane (county example)
SELECT vintage, count(*), count(state_name), count(state_hhs_region)
FROM ecdh_model_dev.geography.us_county GROUP BY vintage ORDER BY vintage;

-- parent-FK clean (from _ops, recorded by the layered build's TableDQ.fk)
SELECT check_name, passed, failing_row_count
FROM ecdh_dev._ops.dq_results
WHERE check_name LIKE 'us_census_%_fk_%' ORDER BY checked_at DESC;
```

## Final retirement

All shapefile levels (state/county/tract/zcta) are now migrated; the legacy `run()` builds
**only `us_hhs_region`**, and its shapefile/boundary machinery (`BUILDERS`,
`_build_*_frames`, `_write_chunk`, `_check_unique`, `_reset_us_boundaries`, `_comment_tables`,
`_set_clustering`) is dead code pending retirement. To finish:

1. Extract `us_hhs_region` into its own small entrypoint (static, no shapefiles) on the
   `run_build` seam, then delete legacy `run()` + the dead helpers and the
   `build_geography_reference` job.
2. Migrate `us_block_group` + `us_block` onto the layered builder (same pattern).
3. ~~Retire `build_geography_views`~~ ✅ **done 2026-06-24** — both `_enriched` views dropped (county
   + tract); `build_geography_views.py` + `geography_views_job.yml` removed (git rm), and
   `us_enriched_view_definitions` + its tests deleted. Exemplar references repointed to
   `build_icd10cm.py` / `build_reference`. ADR 0028 marked retired-in-code.
4. Owner-context: `DROP TABLE geography.boundary` once nothing consumes the polymorphic US
   rows (the per-level `us_<lvl>_boundary` tables replace them).
