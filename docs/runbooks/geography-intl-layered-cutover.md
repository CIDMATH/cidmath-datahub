# Runbook — international geography (GADM) legacy → layered cutover

Migrates the three **international geography** subjects from the legacy monolithic
`run_build` pattern (separate per-subject jobs, model-catalog only, shared polymorphic
`geography.boundary`) onto the shared layered `build_reference` builder (ADR 0036/0037/0039),
the same pattern the US geography hierarchy now uses. Done parents-first
(`country → country_subdivision → subnational`).

This is the **mirror of `geography-layered-cutover.md`** (the US cutover) for the GADM-sourced
subjects, and the last `run_build` holdout in the geography subject. When it completes, **every
geography level has its own per-level boundary table** and the shared polymorphic
`geography.boundary` can be dropped outright.

> **Status:** PLAN / pre-implementation (drafted 2026-06-30). Design decisions below are
> **PROPOSED — confirm before implementing.** No code written yet.

---

## Current state (legacy)

| Subject | Canonical table | PK | geo_level in `boundary` | Vintaged? |
|---|---|---|---|---|
| country | `geography.country` | `(country_alpha3, vintage)` | `country` (`iso_3166_1_alpha3`) | yes (GADM release yr) |
| country_subdivision | `geography.country_subdivision` | `(subdivision_code, vintage)` | `country_subdivision` (`iso_3166_2`) | yes |
| subnational | `geography.subnational` | `(gadm_gid, vintage)` | `subnational_adm2` (`gadm_gid`) | yes |

All three:
- use `run_build` + direct `grants`/`registration`/`DQRecorder`, **model-catalog only** (no
  `geography_raw` / `geography_processed` layering, no `--source-catalog`);
- each **independently download the same ~1.4 GB GADM 4.1 GeoPackage**
  (`https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-levels.zip`) and read one layer
  (`ADM_0` / `ADM_1` / `ADM_2`);
- append to the **shared** `geography.boundary` via `DELETE WHERE geo_level=… ; append`
  (subnational chunked at 10k rows);
- carry **correct** provenance already — `source_provider_code='gadm'`, `access_tier='restricted'`,
  `is_hosted=True`, `dua_required=True`, `license=GADM_LICENSE`, `update_semantics='full_refresh'`.
  (Unlike `us_hhs_region`, there is **no provenance mislabeling to fix.**)
- vintage axis = GADM release year; `gadm.GADM_VINTAGE = 2022` (GADM 4.1). Per-vintage
  `DELETE WHERE vintage = 2022 ; append`.

Pure logic (row assembly, GID parsing, ISO↔GADM matching, the gid1→subdivision_code reverse map)
already lives in `cidmath_datahub.reference.geography_intl` (ADR 0011) and
`cidmath_datahub.reference.gadm` (ADR 0023) — **reusable unchanged** in the migration. The
entrypoints are thin IO/Spark wrappers.

---

## Design decisions (PROPOSED — confirm)

**D1. Shared GADM Volume landing (the main win).** Land the ~1.4 GB GADM zip **once** per vintage
in `geography_raw._landing` (ADR 0039, `LandingRetention=PER_VINTAGE_IMMUTABLE`, fetch-once /
skip-if-present). All three levels read their layer (`ADM_0/1/2`) from the **same** stored
GeoPackage. Parents-first ordering means `country` fetches; `country_subdivision` + `subnational`
see the completion marker and skip the download. Eliminates 2× redundant 1.4 GB pulls per build.
→ *Recommend: yes.*

**D2. Raw-layer granularity. → DECIDED: B (stricter 1:1) — every source lands in the Volume, GADM
as a *fetched* payload, ISO + WHO/UN as *generated* payloads (ADR 0039 amended 2026-06-30 so
generated reference lands in the Volume too; `time` adopts the same when it migrates).**
- GADM fetched payload → Volume + 1:1 raw Delta per layer: `geography_raw.gadm_adm0`,
  `gadm_adm1`, `gadm_adm2` (source token `gadm`; canonical tables stay source-agnostic).
- pycountry ISO lists → **generator writes a parquet to the Volume**, raw built from it:
  `geography_raw.iso_3166_1` + `iso_3166_2` (1:1 of the ISO 3166-1 / 3166-2 records pycountry
  exposes; provenance = ISO 3166 via pycountry + pycountry version).
- WHO/UN `country_classifications` → **generator writes a parquet to the Volume**, raw built from
  it: `geography_raw.country_classifications` (the WHO-region / UN-M49 lookup). The in-code lookup
  IS the generator; this captures the hand-curated data as documented data rather than buried in
  code. **Provenance is documented** (resolves "ideally a documented source, not just code"):
  WHO GHO API `ParentCode` (`https://www.who.int/data/gho/info/gho-odata-api`) +
  UN Statistics Division M49 (`https://unstats.un.org/unsd/methodology/m49/`). *(Confirm exact GHO
  endpoint when wiring provenance.)*
- **Generated-payload landing:** these two use a Volume landing whose `fetch_to_volume` is a
  **generator** (writes parquet from `country_classifications` / pycountry) rather than an HTTP
  download — same `RawLanding` shape as GADM, different acquire. Keeps the workflow identical:
  Volume → 1:1 raw → processed join.
- **Vintaging of the non-GADM raw**: pycountry/classifications version on their **own** cadence
  (pycountry package version / WHO-UN publication), independent of the GADM release year. So the
  ISO + classifications raw land as **`full_refresh` snapshots stamped `ingested_at` + source
  version**, NOT GADM-vintaged. The processed step joins them as the current augmenting inputs;
  only the GADM-derived geometry/universe carries `vintage=2022`. (Dual-clock note → ADR.)

**D3. Processed → canonical.** Each subject derives `geography_processed.<subject>` (assemble +
match + FK enrichment + boundary split) and promotes the **same canonical table names**
(`geography.country` / `country_subdivision` / `subnational`) — **no canonical rename, no consumer
break** on the attribute tables. Split geometry into per-level boundary (D4).
→ *Recommend: yes (mirrors US: attrs in processed entity, geometry in processed boundary).*

**D4. Per-level boundary tables replace the shared polymorphic table.** New
`geography.country_boundary`, `geography.country_subdivision_boundary`,
`geography.subnational_boundary` (canonical-name + `_boundary`, matching `us_<lvl>_boundary`).
The `geo_level`/`geoid_system` values (`country`/`country_subdivision`/`subnational_adm2`) are
retained as columns inside each per-level table for continuity.
→ *Recommend: names above; confirm `subnational_boundary` vs `subnational_adm2_boundary`.*

**D5. Vintaged builder mode.** Use the builder's **vintaged** path (`vintage_snapshot` + atomic
`replaceWhere`), `vintages=(2022,)`, not static mode. This upgrades `country`'s current
`full_refresh`-with-a-vintage-column into proper per-vintage-immutable semantics (ADR 0034).
→ *Recommend: yes; `update_semantics=vintage_snapshot`.*

**D6. Orchestration / DAG.** Two options:
- **(a) Separate intl job** `build_geography_intl_layered` with a `country → country_subdivision →
  subnational` DAG + shared GADM env (`geopandas`/`pyogrio`/`shapely`/`pycountry`). Keeps the
  GADM source family separate from the US/census `build_geography_layered`.
- **(b) Fold into `build_geography_layered`** as three more tasks (per-task GADM env). One job for
  all geography, but mixes census + GADM source families and a heavier DAG.
→ **DECIDED: (a) separate `build_geography_intl_layered`.** Different source families (census/IPUMS
secret + shapefiles + cenpop vs GADM/no-secret + GeoPackage + pycountry), different env deps, and
different refresh cadences (US 2010/2020 census vintages frozen; GADM ~annual on new releases).
The one cross-family touchpoint — `country_subdivision`'s US-vs-`us_state` reconciliation WARN — is
a **cross-job read of an already-built canonical** (`geography.us_state`), fine under ADR 0041, not
a build edge → no reason to merge.

**D7. Final boundary retirement.** After all three GADM levels write per-level boundary tables and
the US rows are already gone (done 2026-06-30), `geography.boundary` holds nothing. **Drop it
outright** (owner context) as the closing step — the polymorphic boundary table is fully retired.
→ *Recommend: yes — this migration is what finally lets `geography.boundary` be dropped.*

---

## Target architecture (per subject)

```
geography_raw._landing/gadm_410/vintage=2022/gadm_410-levels.{zip,gpkg}   ← landed ONCE (D1)
        │  (read layer ADM_0 / ADM_1 / ADM_2 via pyogrio)
        ▼
geography_raw._landing/{iso_3166,country_classifications}/  ← generator writes parquet to Volume (D2)
        │
geography_raw.gadm_adm0 / gadm_adm1 / gadm_adm2        ← 1:1 raw of the GADM layer (fetched; vintage=2022)
geography_raw.iso_3166_1 / iso_3166_2                  ← 1:1 raw of pycountry ISO lists (generated; full_refresh)
geography_raw.country_classifications                  ← 1:1 raw WHO/UN M49 lookup (generated; full_refresh)
        │  (processed JOINs the GADM layer + ISO lists + classifications)
        ▼
geography_processed.country / country_subdivision / subnational   ← assemble + match + FK + split
        │  (entity attributes)                          (geometry → boundary)
        ▼
geography.country / country_subdivision / subnational  ← promoted canonical (names unchanged, D3)
geography.country_boundary / country_subdivision_boundary / subnational_boundary  ← per-level (D4)
```

FK plan (validate against **canonical** parents, within vintage):
- `country` — root; no parent FK.
- `country_subdivision.country_alpha2` → `geography.country` (FAIL/blocking).
- `subnational.country_alpha3` → `geography.country` (FAIL/blocking);
  `subnational.subdivision_code` → `geography.country_subdivision` (nullable; INFO, inherited gap).

Parents-first ordering is **required**: `subnational` reads the gid1→subdivision_code reverse map
from `country_subdivision`; both children FK to `country`.

---

## Per-subject migration checklist

For each subject `<s>` in order `country → country_subdivision → subnational`:

1. **Port the entrypoint to `build_reference`** — add `build_<s>_layered(...)` with a
   `ReferenceBuildSpec`: `RawLanding` for the shared GADM payload (Volume-backed,
   `fetch_to_volume`/`read_from_volume`, `PER_VINTAGE_IMMUTABLE`) reading layer `ADM_N`;
   `process` derives the processed entity (reuse `geography_intl.assemble_<s>_row` etc.) + the
   boundary split; `CanonicalOutput` promotes `geography.<s>` + `geography.<s>_boundary` with
   `validate_staging` carrying the DQ checks below. Reuse all pure logic unchanged.
2. **Migrate the DQ checks** (TableDQ on the staged tables):
   - country: alpha3 uniqueness (FAIL); iso→gadm join coverage ≥95% (WARN); cardinality 230–270 (WARN).
   - country_subdivision: subdivision_code uniqueness (FAIL); country_alpha2 FK → country (FAIL);
     adm1 join coverage ≥65% on non-nested (WARN); match precision (WARN); US↔us_state reconciliation
     (WARN); cardinality 4500–5500 (WARN).
   - subnational: gadm_gid uniqueness (FAIL); country_alpha3 FK → country (FAIL); subdivision link
     coverage ~72% (INFO); cardinality 25k–70k (WARN); rows-dropped >5% (WARN) — Ghana ADM_2
     malformed-GID + HK/Macao ADM_1-shaped accept-gaps (decision 2026-05-30), recorded as WARN via
     `ctx.recorder.record`.
3. **Build + validate in dev** via the new intl layered job task (`--level <s>`): raw landed,
   processed entity + boundary present, canonical + per-level boundary promoted, FK DQ green, row
   counts in band. Validation SQL below.
4. **Remove `<s>` from the legacy build** — its `run()` entrypoint + `_register_dataset` +
   `_dq_checks` + the shared-`geography.boundary` writes. (Children still resolve parents from the
   **canonical** tables for FK.)
5. **Owner-context: delete the migrated rows from the shared `geography.boundary`** once the
   per-level `geography.<s>_boundary` exists and consumers repoint:
   ```sql
   DELETE FROM ecdh_model_dev.geography.boundary WHERE geo_level = '<geo_level>';
   -- country -> 'country'; country_subdivision -> 'country_subdivision'; subnational -> 'subnational_adm2'
   ```
   (build-SP owned; run as SP or temporary `GRANT MODIFY` → `DELETE` → `REVOKE`.)
6. **Retire the legacy job YAML** (`geography_<s>_job.yml`) via `git rm` once the task is in the
   intl layered job and proven in dev.

---

## Final retirement (after all three)

1. **Drop the shared polymorphic boundary table** (owner context) — now empty (US rows deleted
   2026-06-30; GADM rows deleted per-subject above):
   ```sql
   DROP TABLE IF EXISTS ecdh_model_dev.geography.boundary;
   ```
   This fully retires the polymorphic boundary table; every geography level now has its own
   `<level>_boundary`.
2. **`git rm`** the three legacy `geography_{country,subdivision,subnational}_job.yml` and delete
   the legacy `run()` entrypoints / dead helpers from the three `build_geography_*` files.
3. **ADR note** — record that international geography moved onto the 0036/0037/0039 builder
   (cross-reference amendment to ADR 0022/0024; the shared GADM Volume landing under 0039).
4. **Prod** — greenfield (prod has no geography tables yet); deploy + run the intl layered DAG in
   prod after dev is clean, alongside the US geography prod deploy.

---

## Validate after each subject (dev)

```sql
-- country: cardinality + alpha3 uniqueness + boundary parity
SELECT vintage, count(*), count(DISTINCT country_alpha3) FROM ecdh_model_dev.geography.country GROUP BY vintage;
SELECT count(*) FROM ecdh_model_dev.geography.country_boundary;          -- == country row count

-- country_subdivision: cardinality + FK to country
SELECT vintage, count(*) FROM ecdh_model_dev.geography.country_subdivision GROUP BY vintage;     -- ~4.5k–5.5k

-- subnational: cardinality + FK to country (+ nullable subdivision link)
SELECT vintage, count(*), count(subdivision_code) FROM ecdh_model_dev.geography.subnational GROUP BY vintage;  -- ~45k

-- FK DQ green (from _ops, latest run per check)
SELECT check_name, passed, failing_row_count FROM ecdh_dev._ops.dq_results
WHERE check_name LIKE '%country%fk%' OR check_name LIKE '%subnational%fk%' ORDER BY checked_at DESC;

-- provenance unchanged (gadm/restricted/hosted), semantics now vintage_snapshot
SELECT source_provider_code, access_tier, update_semantics
FROM ecdh_model_dev._ops.dataset_catalog c JOIN ecdh_model_dev._ops.dataset_engineering e USING (full_table_name)
WHERE full_table_name IN ('ecdh_model_dev.geography.country',
                          'ecdh_model_dev.geography.country_subdivision',
                          'ecdh_model_dev.geography.subnational');
```

---

## Risks / watch-items

- **1.4 GB GADM download** — first task per build fetches it; the Volume skip-if-present must hold
  across tasks (shared landing path keyed by the GADM payload, not per-subject) for the
  download-once win. Verify run 2 logs "skipping fetch" with zero downloads.
- **ADM_2 scale** — ~45k polygons; subnational keeps the 10k-row chunked write (or the US
  block-style Volume-Parquet staging if Spark Connect plan-inlining bites). Subnational job
  timeout is 3 h (vs 2 h for the others).
- **Accept-gaps** — Ghana ADM_2 malformed GIDs (~260 districts) + HK/Macao ADM_1-shaped rows drop
  as level-mismatched; preserve the `subnational_rows_dropped` WARN (decision 2026-05-30). Don't
  let the rows-dropped WARN become a FAIL.
- **FK targets canonical, not processed** — children validate against `geography.country` /
  `country_subdivision`, consistent with the US migration.
- **`bundles/` not in ruff/just scope** — `python -m py_compile` the edited `build_geography_*`
  on Windows; the sandbox mount serves truncated reads of large files. Real validation = dev job run.
