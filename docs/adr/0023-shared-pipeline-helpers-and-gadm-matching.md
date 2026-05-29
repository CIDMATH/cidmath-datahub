# 0023 — Shared pipeline helpers and ISO↔GADM subdivision matching

## Status
Accepted — 2026-05-29

## Context
ADR 0021 anticipated this decision: "if we add 3c later, that's the trigger for extracting a downloader helper." By the end of slice 3b we had three GADM-consuming build entrypoints either written or imminent (`build_geography_country.py` for ADM_0, `build_geography_subdivision.py` for ADM_1, and `build_geography_subnational.py` for ADM_2+ in 3c). The same ~1.4 GB GADM 4.1 download, the same extract-the-GeoPackage step, the same `representative_point` centroid, the same simplify-to-WKB call, the same `geography.boundary` Spark schema, and the same GADM licence/constant block had been copy-pasted across the first two. The repo's ADR backlog also carried a deferred "pipeline standardization and modular composition" item flagged for "after 2-3 pipelines exist"; that condition is now met.

Slice 3b also surfaced a second, sharper problem during its first dev run. The matcher linked ISO 3166-2 subdivisions to GADM ADM_1 polygons using only GADM's `HASC_1` and `ISO_1` code columns, on the plan-stage assumption (a prior, not a measurement) that those are "clean for most countries." The dev run measured **27.88%** non-nested coverage. A per-country breakdown showed the failure is bimodal: countries like the US, Brazil, Nigeria, Romania, China, and India matched at 80–100%, while Turkey, Japan, Mexico, France, Afghanistan, Andorra, Belgium, and many others matched at 0% — because GADM 4.1 leaves `HASC_1`/`ISO_1` blank for them. A hand-maintained fixup map cannot close a ~2,600-row gap; that would be guessing at scale, which is exactly what ADR-era guidance (and the project's "verify upstream mappings, don't guess" rule) warns against.

## Decision

### Shared GADM IO module
GADM download/extract/read/geometry helpers move to `src/cidmath_datahub/reference/gadm.py`, imported by all GADM-consuming build entrypoints. It owns: the GADM constants (`GADM_ZIP_URL`, `GADM_GPKG_NAME`, `GADM_VINTAGE`, `GADM_USER_AGENT`, `GADM_LICENSE`, `GENERALIZE_TOLERANCE_DEG`); `download_gadm_zip`, `extract_gpkg`, `read_layer`; the geometry helpers `centroid` and `simplify_to_wkb`; the `gdf_to_dict_rows` materializer that decouples downstream logic from GeoPandas; and `boundary_spark_schema()`, the shared `geography.boundary` schema factory.

Geospatial and Spark imports inside this module are **lazy** (inside the functions that need them), so importing `gadm` — and unit-testing its pure helpers (`centroid`, `gdf_to_dict_rows`) with lightweight stand-ins — needs neither geopandas/shapely nor pyspark, preserving ADR 0020's "keep the core wheel's install deps lean" property. This refines, rather than contradicts, ADR 0011: the *thin entrypoint* rule still holds (no testable business logic in `bundles/*/src/`), and shared pipeline IO is exactly the kind of reusable seam that belongs in the wheel.

### Multi-tier subdivision matching
ISO 3166-2 → GADM ADM_1 resolution (`geography_intl.resolve_subdivision_polygons`) applies three tiers per subdivision, in order:

1. **Exact code** — GADM `HASC_1` (`US.GA`) then `ISO_1` (`US-GA`). Fast and unambiguous where present.
2. **Name within country** — normalized `NAME_1` / `VARNAME_1` matched *scoped to the GADM `GID_0` (alpha-3) country*, so two countries can hold an identically-named subdivision without colliding. Names are normalized by NFKD accent-stripping, lower-casing, and collapsing non-alphanumerics (so `Côte-d'Or` ≡ `Cote d Or`, `São Paulo` ≡ `Sao Paulo`). `VARNAME_1` (GADM's pipe-delimited alternates) is indexed opportunistically and is **not** a required column — its absence never fails the build.
3. **Fixup** — the manual `GADM_ADM1_ISO_FIXUPS` map, applied last and never displacing a code or name match, for the residual handful neither tier resolves.

The name tier is the primary recovery path for the bimodal gap; the first-run data confirmed the 0%-match countries carry usable `NAME_1` values that align with the ISO grain (Andorra's 7 parishes, Afghanistan's 34 provinces, Belgium's 3 regions).

### Coverage threshold is set from data, not priors
The original 90% WARN threshold was a pre-data guess and is wrong in both directions: code-only matching floored at 28%, and even perfect name matching cannot reach 90% because some countries have a genuine **grain mismatch** between ISO 3166-2 and GADM ADM_1 (Slovenia publishes 212 ISO municipalities against ~12 GADM statistical regions; Azerbaijan 78 ISO districts against ~11 GADM economic regions). Those subdivisions legitimately keep `gadm_gid_1 = NULL` and rely on a parent polygon spatially (ADR 0022). The threshold is therefore **recalibrated from the first post-this-ADR dev run** — set a notch below the observed achievable ceiling — and is marked PROVISIONAL in code until that run lands. The DQ `details` payload continues to record the unmatched-GADM-GID sample as the ground truth for any genuinely-needed fixup entries.

## Alternatives considered
- **Leave the IO duplicated, extract at 3c.** Rejected: 3b already created the second copy and 3c would be the third; ADR 0021 already named this the trigger.
- **Put shared helpers under `bundles/_reference/src/`.** Keeps geospatial IO out of the wheel, but bends ADR 0011's thin-entrypoint rule and makes the helpers un-importable by unit tests. Rejected in favour of the wheel module with lazy heavy imports.
- **Close the coverage gap with a large hand-built fixup map.** Rejected: ~2,600 entries is guesswork at scale and violates the verify-don't-guess rule; name matching derives the links from the data GADM actually ships.
- **Strip administrative-type words ("Province of …") during name normalization.** Tempting, but blind stripping causes more false matches than it fixes; alternate spellings are handled by indexing `VARNAME_1` instead. Deferred unless a measured need appears.
- **Keep the 90% threshold and accept a permanent WARN.** Rejected: a check that always warns is noise; the threshold must reflect the achievable ceiling given real ISO-vs-GADM grain differences.

## Consequences
- One GADM IO surface shared by 3a/3b/3c; 3c starts from a deduplicated base instead of adding a third copy. Future GADM-release changes (URL, schema) are a one-file edit.
- Subdivision coverage rises substantially via name matching; the residual unmatched set is dominated by genuine grain mismatches that are correctly left `NULL`, not silently wrong.
- The `geography.boundary` schema now has a single definition (`gadm.boundary_spark_schema()`); `build_geography.py` (the US/IPUMS build) still carries its own copy and can adopt the shared factory opportunistically — out of scope for this change to limit blast radius.
- Name matching is heuristic: a wrong-but-plausible name collision within a country could mis-link a polygon. The within-country scoping and the code-tier-wins ordering bound this risk, and DQ coverage plus the unmatched sample make residuals visible. If a specific mis-link is found, a fixup entry (or a normalization tweak) is the remedy.
- The coverage threshold becomes an empirically-set value, re-confirmed whenever the GADM release or the pycountry version changes materially.
