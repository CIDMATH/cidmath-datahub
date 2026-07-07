# ADR note (draft) — international geography migrated onto the shared reference builder

**Status:** DRAFT — land after all three GADM levels are dev-green (append to
`docs/adr/0024-international-geography-vintaging.md` as an addendum; cross-reference
0036 / 0037 / 0039, and the amendment to 0022 / 0023).

---

## Addendum to ADR 0024 — layered-builder cutover (GADM)

International geography (`country`, `country_subdivision`, `subnational`) has moved off the
legacy per-subject `run_build` monolith onto the shared **`build_reference`** builder
(ADR 0036 shared reference-table builder; ADR 0037 raw → processed → canonical layering;
ADR 0039 raw-payload landing zone), the GADM mirror of the completed US geography migration.
It is orchestrated as one parents-first DAG, `build_geography_intl_layered`
(`country → country_subdivision → subnational`).

What changed:

- **Shared GADM Volume landing (ADR 0039).** The ~1.4 GB GADM 4.1 GeoPackage lands **once** per
  vintage in `geography_raw._landing` under a shared `volume_key` (`gadm_410_levels`). The
  `country` task fetches it; `country_subdivision` and `subnational` see the completion marker and
  skip the download (fetch-once, verified by "skipping fetch" in run-2 logs with zero bytes
  downloaded). This removes the 2× redundant 1.4 GB pulls of the legacy per-subject jobs.

- **1:1 raw + generated payloads (ADR 0039 amended 2026-06-30).** Each level lands its GADM layer
  as a 1:1 raw Delta (`geography_raw.gadm_adm0/1/2`, geometry generalized to WKB; full-res stays in
  the landed GeoPackage). The generated ISO payloads land in the Volume too — `iso_3166_1` /
  `iso_3166_2` (pycountry) and `country_classifications` (WHO GHO region + UN M49). Per-landing
  provenance overrides (`RawLanding.catalog_overrides`) stamp the generated raws with their true
  source (`pycountry`/`iso`, `who_un`) rather than inheriting the build's `gadm` provider.

- **Processed → canonical, names unchanged (ADR 0037).** `geography_processed.<level>` assembles /
  matches / FK-enriches and splits geometry; canonical `geography.country` /
  `country_subdivision` / `subnational` keep their names and schemas — no consumer break.

- **Per-level boundary tables replace the polymorphic table.** `geography.country_boundary`,
  `geography.country_subdivision_boundary`, `geography.subnational_boundary` (each retaining
  `geo_level` / `geoid_system` as columns). The shared polymorphic `geography.boundary` is deleted
  per-level and then dropped outright once every level has its own boundary table and consumers have
  repointed (see the cutover runbook).

- **Vintaged builder semantics (ADR 0034).** All three use the builder's `vintage_snapshot` path
  with atomic `replaceWhere`, `vintages=(2022,)` — upgrading `country`'s prior
  `full_refresh`-with-a-vintage-column into proper per-vintage-immutable semantics.

- **FK integrity validated against canonical parents (ADR 0041).** `country_subdivision.country_alpha2`
  and `subnational.country_alpha3` FK to `geography.country` (FAIL/blocking);
  `subnational.subdivision_code → geography.country_subdivision` is nullable (INFO, inherited gap).
  The `country_subdivision` US↔`geography.us_state` reconciliation is a cross-job read of an
  already-built US canonical (a lineage edge under ADR 0041, not a build edge), and degrades
  gracefully (skips) when `us_state` is absent.

Accepted gaps (preserved as WARN, not FAIL): `subnational` drops non-ISO GADM territories and the
GADM 4.1 malformed Ghana ADM_2 GIDs (`GHA1.1_2`, ~260 districts) + Hong Kong / Macao ADM_1-shaped
rows (decision 2026-05-30), recorded via the `subnational_rows_dropped` check.

Provenance was already correct on the canonical tables (`gadm` / `restricted` / hosted /
`dua_required`); only the generated raw rows gained true per-landing provenance. Prod is greenfield
and deploys alongside the US geography prod deploy.
