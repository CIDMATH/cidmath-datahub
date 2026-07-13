-- Relayer the ICD-10-CM + ICD-9-CM diagnosis tables onto the source->model path
-- (ADR 0037 backport wave 4). The data model and the denormalized-hierarchy shape (ADR 0030/0031)
-- are unchanged; this adds the raw layer and corrects the update_semantics:
--   * ADD  raw landings in the SOURCE catalog:   ecdh_<env>.codes_raw.icd10cm / .icd9cm
--          (built from verbatim per-edition payloads landed under codes_raw._landing, ADR 0039)
--   * KEEP canonical in the MODEL catalog:        ecdh_model_<env>.codes.icd10cm / .icd9cm
--   * SEMANTICS CHANGE: update_semantics full_refresh -> vintage_snapshot. The OLD build was
--          *mislabeled* full_refresh but actually did a per-edition DELETE+append; the folded
--          build does a per-edition atomic replaceWhere(edition_year). Net table behavior is the
--          same (per-edition replace, other editions retained), but the registered semantics and
--          the write mechanism are now correct/atomic (ADR 0034). The _ops.dataset_engineering row
--          is re-registered by the build.
--
-- These are the two most complex code sets (multi-source per edition: ICD-10-CM = base order file +
-- optional Apr-1 update + tabular XML; ICD-9-CM = DTAB + Appendix-E), but both are reproducible from
-- the public CDC/NCHS downloads and idempotent + re-pullable, so this is a DROP + REBUILD, not a data
-- migration (mirrors docs/runbooks/relocate-cvx-ndc-volumes.sql). Canonical table SHAPES are
-- unchanged, so consumers are unaffected after the rebuild.
--
-- Sequencing: deploy the build-script changes (build_icd10cm.py / build_icd9cm.py:
-- --source-catalog/--model-catalog, codes_raw landing + promote on build_reference,
-- update_semantics=vintage_snapshot) FIRST. Builds are manual-run, so there is no race. Run DEV,
-- verify parity, then PROD. IF EXISTS makes every step idempotent.
--
-- NOTE (icd9cm): the RTF de-conversion needs `striprtf` in the job environment (already a dependency
-- of the icd9cm job); no change there.

-- =====================================================================
-- DEV  (source ecdh_dev / model ecdh_model_dev)
-- =====================================================================

-- 1. Pre-check: current canonical row counts + edition coverage (to compare post-rebuild).
SELECT 'icd10cm' AS t, edition_year, COUNT(*) AS rows
FROM ecdh_model_dev.codes.icd10cm GROUP BY edition_year ORDER BY edition_year;
SELECT 'icd9cm' AS t, edition_year, COUNT(*) AS rows
FROM ecdh_model_dev.codes.icd9cm GROUP BY edition_year ORDER BY edition_year;

-- 2. Run the folded builds (creates codes_raw schema + landing Volume + raw tables, promotes
--    canonicals per edition). From the repo root with the bundle deployed to dev:
--      databricks bundle run --target dev build_icd10cm_reference
--      databricks bundle run --target dev build_icd9cm_reference
--    (Load the same editions currently in the canonical tables so parity is comparable; pass
--     --edition-year <years...> if you need more than the job default.)

-- 3. Post-check: parity. Per-edition row counts for the rebuilt edition(s) should match step 1, and
--    the raw tables should now exist with matching counts.
SELECT 'icd10cm' AS t, edition_year, COUNT(*) AS rows
FROM ecdh_model_dev.codes.icd10cm GROUP BY edition_year ORDER BY edition_year;
SELECT 'icd9cm' AS t, edition_year, COUNT(*) AS rows
FROM ecdh_model_dev.codes.icd9cm GROUP BY edition_year ORDER BY edition_year;
SHOW TABLES IN ecdh_dev.codes_raw LIKE 'icd*cm';

-- 4. Confirm the corrected semantics registered (should read 'vintage_snapshot', not 'full_refresh').
SELECT full_table_name, update_semantics
FROM ecdh_model_dev._ops.dataset_engineering
WHERE full_table_name IN ('ecdh_model_dev.codes.icd10cm', 'ecdh_model_dev.codes.icd9cm');

-- =====================================================================
-- PROD (source ecdh_prod / model ecdh_model_prod) -- run only after DEV verifies.
-- Requires human approval (touches prod). Mirror steps 1-4 with the prod catalogs:
--   databricks bundle run --target prod build_icd10cm_reference
--   databricks bundle run --target prod build_icd9cm_reference
-- =====================================================================
