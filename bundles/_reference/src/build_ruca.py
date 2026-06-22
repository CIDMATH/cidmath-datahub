"""Build the RUCA geography reference on the source->model path (ADR 0020, 0037, 0038).

USDA ERS Rural-Urban Commuting Area (RUCA) codes are a sub-county rural/urban classification
keyed to census tracts and (from 2010 on) ZIP codes. This entrypoint is the thin IO + Spark
layer over the pure logic in ``cidmath_datahub.reference.ruca`` (ADR 0011).

Placement follows the reworked ADR 0037 (and the ADR 0038 Reconciliation): RUCA is *sourced*
and *augments the geography subject*, so it lands **raw in the source catalog** and **promotes a
canonical table to the model catalog** -- even though it is the *simple* tier (no ``_processed``
stage; raw -> promote):

  - ``ecdh_<env>.geography_raw.us_ruca_tract`` / ``.us_ruca_zip`` -- fetched-as-is, 1:1 with the
    source rows, vintage-stamped (engineer-only).
  - ``ecdh_model_<env>.geography.us_ruca_tract`` / ``.us_ruca_zip`` -- the canonical consumer
    tables, promoted from raw (tract adds the derived ``state_geoid``/``county_geoid``); reader-tier.
  - ``geography.us_ruca_zcta`` -- the approximate ZIP->ZCTA bridge view (kept).

Versioned per RUCA decennial vintage (1990/2000/2010/2020) with ``update_semantics="vintage_snapshot"``
(ADR 0034): each run **atomically** replaces only the vintage(s) it rebuilt via Delta
``replaceWhere`` (the first build seeds the table); vintages are immutable and not comparable across
decades. No ``_current`` views -- "current" is ``MAX(vintage)`` (ADR 0034). ``vintage`` *is* the
geography vintage the codes are coded to, so the ``(geoid, vintage)`` / ``(zip_code, vintage)`` joins
to ``us_tract`` / ``us_zcta`` are ADR-0035 conformant.

Public domain (U.S. Government work) -- plain HTTPS download, no credential. The ERS slugs carry a
``?v=`` cache-buster that shifts on re-post, so ``--tract-url`` / ``--zip-url`` accept a live link per
vintage (single vintage).

Blocking DQ (FAIL, raises): PK uniqueness per table; tract GEOID is 11 digits / ZIP is 5 digits;
every ``primary_ruca`` in 1-10/99; every ``secondary_ruca`` a published code. WARN: per-vintage
cardinality; tract population present; primary-code distribution.

NOTE (ADR 0038 Reconciliation, delta 6): when the ADR 0036 shared builder lands, fold this build's
parser + a ``ReferenceTableSpec`` into ``build_reference_table`` (the simple raw->promote path).
Until then this hand-rolled skeleton carries the placement + ``vintage_snapshot`` realignment.

Usage:
    build_ruca.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --vintage 2020 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import ruca

log = get_logger(__name__)

RAW_SCHEMA = "geography_raw"  # source catalog: fetched-as-is landing (ADR 0037)
MODEL_SCHEMA = "geography"  # model catalog: canonical consumer tables
TRACT_TABLE = "us_ruca_tract"
ZIP_TABLE = "us_ruca_zip"
ZCTA_VIEW = "us_ruca_zcta"  # us_ruca_zip joined to us_zcta (approximate ZIP->ZCTA bridge)
ZCTA_TABLE = "us_zcta"
PIPELINE_REF = "bundles/_reference/src/build_ruca.py"

# Per-vintage ERS download URLs (public HTTPS; the ?v= cache-buster shifts on re-post -- override
# via --tract-url/--zip-url). CSV preferred where ERS publishes one; 2010 tract + 2000/1990 are
# Excel only (2000/1990 are legacy binary .xls, read via xlrd in the job env).
TRACT_URLS: dict[int, str] = {
    2020: "https://www.ers.usda.gov/media/5443/2020-rural-urban-commuting-area-codes-census-tracts.csv?v=68522",
    2010: "https://www.ers.usda.gov/media/5438/2010-rural-urban-commuting-area-codes-revised-732019.xlsx?v=66488",
    2000: "https://www.ers.usda.gov/media/5437/2000-rural-urban-commuting-area-codes.xls?v=85378",
    1990: "https://www.ers.usda.gov/media/5436/1990-rural-urban-commuting-area-codes.xls?v=76004",
}
# ZIP files began with the 2010 vintage; 1990/2000 have none.
ZIP_URLS: dict[int, str] = {
    2020: "https://www.ers.usda.gov/media/5444/2020-rural-urban-commuting-area-codes-zip-codes.csv?v=79637",
    2010: "https://www.ers.usda.gov/media/5440/2010-rural-urban-commuting-area-codes-zip-code-file.csv?v=19921",
}

SUPPORTED_VINTAGES = (1990, 2000, 2010, 2020)

# Generous per-vintage cardinality bands (WARN only): ~61k-90k tracts, ~40k+ ZIPs depending on
# vintage/territory coverage. A count far outside the band signals a parse/layout problem.
_TRACT_CARDINALITY_MIN, _TRACT_CARDINALITY_MAX = 50_000, 100_000
_ZIP_CARDINALITY_MIN, _ZIP_CARDINALITY_MAX = 30_000, 60_000

# Raw tract = source-fidelity (1:1 with source rows), no derived parents. The canonical adds the
# derived state_geoid/county_geoid at promote time.
RAW_TRACT_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state", T.StringType(), True),
        T.StructField("county", T.StringType(), True),
        T.StructField("primary_ruca", T.IntegerType(), False),
        T.StructField("secondary_ruca", T.StringType(), False),
        T.StructField("population", T.LongType(), True),
        T.StructField("land_area_sqmi", T.DoubleType(), True),
        T.StructField("population_density", T.DoubleType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)

# Canonical tract = raw + derived state_geoid/county_geoid; that shape is produced by the promote
# SELECT in _promote_tract (substring(geoid,...) AS state_geoid/county_geoid), not a StructType here.

# ZIP raw == ZIP canonical (no derived columns); same schema in both catalogs.
ZIP_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("zip_code", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state", T.StringType(), True),
        T.StructField("zip_code_type", T.StringType(), True),
        T.StructField("po_name", T.StringType(), True),
        T.StructField("primary_ruca", T.IntegerType(), False),
        T.StructField("secondary_ruca", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: download an ERS file and read it (CSV / XLSX / .xls) into row dicts. Kept out of the
# pure module per ADR 0011. Public HTTPS, no credential. pandas (and xlrd for legacy .xls) are
# provided by the job environment and imported lazily.
# ---------------------------------------------------------------------------


def _download(url: str) -> tuple[bytes, str]:
    """Download an ERS file; return ``(raw_bytes, filename)``. Filename feeds ``source_file``."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted ERS host
        raw = resp.read()
    filename = Path(urllib.parse.unquote(parts.path)).name
    log.info("Downloaded RUCA file", extra={"url": url, "file": filename, "bytes": len(raw)})
    return raw, filename


def _read_rows(raw: bytes, filename: str, *, sheet: Any = 0) -> list[dict[str, Any]]:
    """Read a downloaded CSV / XLSX / .xls into a list of header -> cell dicts.

    Everything is read as text (``dtype=str``) so significant leading zeros in GEOIDs / ZIP /
    FIPS codes survive (pandas would otherwise coerce them to ints). For Excel workbooks the data
    sheet is assumed to be the first sheet (override with ``--sheet`` if a vintage buries it
    behind a code-book sheet). Blank cells become empty strings, not NaN.
    """
    import io

    import pandas as pd

    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        # ERS CSVs are Latin-1 / Windows-1252, not UTF-8 (place names carry bytes like 0xF1 = n-tilde,
        # e.g. "Canon", Puerto Rico names). latin-1 maps every byte, so the read never aborts -- the
        # same encoding the ICD order-file modules use.
        df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding="latin-1")
    elif suffix in (".xlsx", ".xls"):
        # engine auto-selected by pandas: openpyxl for .xlsx, xlrd for legacy .xls (encoding is
        # handled inside the workbook format, so no encoding arg needed here).
        df = pd.read_excel(io.BytesIO(raw), sheet_name=sheet, dtype=str, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported RUCA file type {suffix!r} for {filename!r}")
    df.columns = [str(c).strip() for c in df.columns]
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking PK / id-format / code-vocab; WARN cardinality + distribution. Recorded
# against the canonical (consumer) table names; raw is the same rows landed 1:1, so the canonical
# gate covers it. (Full TableDQ (ADR 0029) adoption rides with the ADR 0036 builder fold-in.)
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    tract_records: list[ruca.RucaTractRecord],
    zip_records: list[ruca.RucaZipRecord],
    vintages: list[int],
) -> None:
    """Record DQ outcomes for both tables; raise on any blocking FAIL so a bad table never writes."""
    failures: list[str] = []

    # --- us_ruca_tract blocking ---
    tract_full = f"{MODEL_SCHEMA}.{TRACT_TABLE}"
    n_tract = len(tract_records)

    dup_tract = ruca.find_duplicate_tract_keys(tract_records)
    _record(ctx, tract_full, "us_ruca_tract_geoid_vintage_uniqueness", DQCategory.UNIQUENESS,
            not dup_tract, len(dup_tract), n_tract,
            {"sample": [list(k) for k in dup_tract[:10]]} if dup_tract else None)
    if dup_tract:
        failures.append(f"duplicate (geoid, vintage): {dup_tract[:5]}")

    bad_geoid = ruca.find_bad_tract_geoids(tract_records)
    _record(ctx, tract_full, "us_ruca_tract_geoid_is_11_digit", DQCategory.BUSINESS_RULE,
            not bad_geoid, len(bad_geoid), n_tract,
            {"sample": bad_geoid[:10]} if bad_geoid else None)
    if bad_geoid:
        failures.append(f"tract geoid not 11-digit: {bad_geoid[:5]}")

    bad_pri_t = ruca.find_invalid_primary_codes(tract_records)
    _record(ctx, tract_full, "us_ruca_tract_primary_code_valid", DQCategory.BUSINESS_RULE,
            not bad_pri_t, len(bad_pri_t), n_tract,
            {"allowed": sorted(ruca.PRIMARY_RUCA_CODES), "sample": [list(s) for s in bad_pri_t[:10]]}
            if bad_pri_t else None)
    if bad_pri_t:
        failures.append(f"tract primary out of vocab: {bad_pri_t[:5]}")

    bad_sec_t = ruca.find_invalid_secondary_codes(tract_records)
    _record(ctx, tract_full, "us_ruca_tract_secondary_code_valid", DQCategory.BUSINESS_RULE,
            not bad_sec_t, len(bad_sec_t), n_tract,
            {"sample": [list(s) for s in bad_sec_t[:10]]} if bad_sec_t else None)
    if bad_sec_t:
        failures.append(f"tract secondary out of vocab: {bad_sec_t[:5]}")

    # --- us_ruca_zip blocking ---
    zip_full = f"{MODEL_SCHEMA}.{ZIP_TABLE}"
    n_zip = len(zip_records)

    dup_zip = ruca.find_duplicate_zip_keys(zip_records)
    _record(ctx, zip_full, "us_ruca_zip_zip_code_vintage_uniqueness", DQCategory.UNIQUENESS,
            not dup_zip, len(dup_zip), n_zip,
            {"sample": [list(k) for k in dup_zip[:10]]} if dup_zip else None)
    if dup_zip:
        failures.append(f"duplicate (zip_code, vintage): {dup_zip[:5]}")

    bad_zip = ruca.find_bad_zip_codes(zip_records)
    _record(ctx, zip_full, "us_ruca_zip_zip_code_is_5_digit", DQCategory.BUSINESS_RULE,
            not bad_zip, len(bad_zip), n_zip, {"sample": bad_zip[:10]} if bad_zip else None)
    if bad_zip:
        failures.append(f"zip code not 5-digit: {bad_zip[:5]}")

    bad_pri_z = ruca.find_invalid_primary_codes(zip_records)
    _record(ctx, zip_full, "us_ruca_zip_primary_code_valid", DQCategory.BUSINESS_RULE,
            not bad_pri_z, len(bad_pri_z), n_zip,
            {"sample": [list(s) for s in bad_pri_z[:10]]} if bad_pri_z else None)
    if bad_pri_z:
        failures.append(f"zip primary out of vocab: {bad_pri_z[:5]}")

    bad_sec_z = ruca.find_invalid_secondary_codes(zip_records)
    _record(ctx, zip_full, "us_ruca_zip_secondary_code_valid", DQCategory.BUSINESS_RULE,
            not bad_sec_z, len(bad_sec_z), n_zip,
            {"sample": [list(s) for s in bad_sec_z[:10]]} if bad_sec_z else None)
    if bad_sec_z:
        failures.append(f"zip secondary out of vocab: {bad_sec_z[:5]}")

    # --- WARN: per-vintage cardinality + population presence + distribution ---
    for vintage in vintages:
        tract_v = [r for r in tract_records if r.vintage == vintage]
        if tract_v:
            ok = _TRACT_CARDINALITY_MIN <= len(tract_v) <= _TRACT_CARDINALITY_MAX
            _record(ctx, tract_full, f"us_ruca_tract_cardinality_{vintage}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, len(tract_v),
                    {"expected_range": [_TRACT_CARDINALITY_MIN, _TRACT_CARDINALITY_MAX],
                     "actual": len(tract_v)}, severity=DQSeverity.WARN)
            n_pop = sum(1 for r in tract_v if r.population is not None)
            _record(ctx, tract_full, f"us_ruca_tract_population_present_{vintage}",
                    DQCategory.NULLABILITY, n_pop == len(tract_v), len(tract_v) - n_pop, len(tract_v),
                    {"with_population": n_pop}, severity=DQSeverity.WARN)
            _record(ctx, tract_full, f"us_ruca_tract_primary_distribution_{vintage}",
                    DQCategory.BUSINESS_RULE, True, 0, len(tract_v),
                    {"distribution": ruca.primary_distribution(tract_v)}, severity=DQSeverity.INFO)

        zip_v = [r for r in zip_records if r.vintage == vintage]
        if zip_v:
            ok = _ZIP_CARDINALITY_MIN <= len(zip_v) <= _ZIP_CARDINALITY_MAX
            _record(ctx, zip_full, f"us_ruca_zip_cardinality_{vintage}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, len(zip_v),
                    {"expected_range": [_ZIP_CARDINALITY_MIN, _ZIP_CARDINALITY_MAX],
                     "actual": len(zip_v)}, severity=DQSeverity.WARN)

    if failures:
        raise ValueError("RUCA blocking DQ failed -- " + "; ".join(failures))


def _record(
    ctx: BuildContext,
    table: str,
    check_name: str,
    category: DQCategory,
    passed: bool,
    failing: int,
    total: int,
    details: dict[str, Any] | None,
    *,
    severity: DQSeverity = DQSeverity.FAIL,
) -> None:
    """Thin wrapper over ``ctx.recorder.record`` to keep the check list readable."""
    ctx.recorder.record(
        table_name=table,
        check_name=check_name,
        category=category,
        severity=severity,
        passed=passed,
        failing_row_count=failing,
        total_row_count=total,
        details=details,
    )


# ---------------------------------------------------------------------------
# Write: vintage_snapshot via atomic Delta replaceWhere (ADR 0034). The first build seeds the
# table; later runs atomically replace only the vintage(s) rebuilt and leave others intact.
# ---------------------------------------------------------------------------


def _vintage_snapshot_write(spark: SparkSession, full: str, df: Any, vintages: list[int]) -> None:
    """Atomically replace only ``vintages`` in ``full`` (seed the table on first build)."""
    if spark.catalog.tableExists(full):
        years_sql = ", ".join(str(v) for v in vintages)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"vintage IN ({years_sql})")
            .saveAsTable(full)
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full)
    log.info("vintage_snapshot write", extra={"table": full, "vintages": vintages})


def _promote_tract(spark: SparkSession, source_catalog: str, model_catalog: str,
                   vintages: list[int]) -> None:
    """Promote raw tract -> canonical (model), deriving state_geoid/county_geoid from the GEOID."""
    raw_full = f"{source_catalog}.{RAW_SCHEMA}.{TRACT_TABLE}"
    years_sql = ", ".join(str(v) for v in vintages)
    df = spark.sql(
        f"SELECT geoid, vintage, "
        f"substring(geoid, 1, 2) AS state_geoid, substring(geoid, 1, 5) AS county_geoid, "
        f"state, county, primary_ruca, secondary_ruca, population, land_area_sqmi, "
        f"population_density, source_file, ingested_at "
        f"FROM {raw_full} WHERE vintage IN ({years_sql})"
    ).sort("vintage", "geoid")
    _vintage_snapshot_write(spark, f"{model_catalog}.{MODEL_SCHEMA}.{TRACT_TABLE}", df, vintages)


def _promote_zip(spark: SparkSession, source_catalog: str, model_catalog: str,
                 vintages: list[int]) -> None:
    """Promote raw ZIP -> canonical (model); ZIP raw and canonical share one schema."""
    raw_full = f"{source_catalog}.{RAW_SCHEMA}.{ZIP_TABLE}"
    years_sql = ", ".join(str(v) for v in vintages)
    df = spark.sql(f"SELECT * FROM {raw_full} WHERE vintage IN ({years_sql})").sort(
        "vintage", "zip_code"
    )
    _vintage_snapshot_write(spark, f"{model_catalog}.{MODEL_SCHEMA}.{ZIP_TABLE}", df, vintages)


def _comment_tables(spark: SparkSession, source_catalog: str, model_catalog: str,
                    wrote_zip: bool) -> None:
    raw_t = f"{source_catalog}.{RAW_SCHEMA}.{TRACT_TABLE}"
    spark.sql(
        f"COMMENT ON TABLE {raw_t} IS 'USDA ERS RUCA census-tract codes, fetched-as-is (1:1 with "
        f"source rows), vintage-stamped. Raw landing for the canonical geography.us_ruca_tract. "
        f"vintage_snapshot. ADR 0037/0038.'"
    )
    spark.sql(
        f"COMMENT ON TABLE {model_catalog}.{MODEL_SCHEMA}.{TRACT_TABLE} IS "
        f"'USDA ERS RUCA codes by census tract. Attribute extension of geography.us_tract "
        f"(join USING (geoid, vintage)). primary_ruca (1-10/99) + secondary_ruca (verbatim) + "
        f"source population/land area/density; state_geoid/county_geoid derived from geoid. PK "
        f"(geoid, vintage); vintage_snapshot. Promoted from {source_catalog}.{RAW_SCHEMA}."
        f"{TRACT_TABLE}. Vintages NOT comparable across decades. ADR 0020/0037/0038.'"
    )
    if wrote_zip:
        raw_z = f"{source_catalog}.{RAW_SCHEMA}.{ZIP_TABLE}"
        spark.sql(
            f"COMMENT ON TABLE {raw_z} IS 'USDA ERS RUCA ZIP codes, fetched-as-is (1:1 with source "
            f"rows), vintage-stamped (>= 2010). Raw landing for geography.us_ruca_zip. "
            f"vintage_snapshot. ADR 0037/0038.'"
        )
        spark.sql(
            f"COMMENT ON TABLE {model_catalog}.{MODEL_SCHEMA}.{ZIP_TABLE} IS "
            f"'USDA ERS RUCA codes by ZIP code (>= 2010 only). primary_ruca (1-10/99) + "
            f"secondary_ruca (verbatim). ZIP is not a census GEOID, but zip_code is an approximate "
            f"FK to us_zcta.geoid (join on (zip_code = geoid, vintage)); see the us_ruca_zcta view. "
            f"PK (zip_code, vintage); vintage_snapshot. ADR 0020/0037/0038.'"
        )


def _create_zcta_view(spark: SparkSession, model_catalog: str) -> bool:
    """Materialize the approximate ZIP->ZCTA join (us_ruca_zip x us_zcta) in the model catalog.

    ZCTA is the Census areal approximation of ZIP codes, so the 5-digit zip_code is joined to
    us_zcta.geoid on (zip_code = geoid, vintage). INNER join: only ZIP rows with a matching ZCTA
    appear (point / PO-box / newer ZIPs drop out). Depends on geography.us_zcta existing (the
    geography build runs first); if absent, skip with a WARN rather than fail. Returns whether the
    view was created (so registration can match).
    """
    g = f"{model_catalog}.{MODEL_SCHEMA}"
    zcta_full = f"{g}.{ZCTA_TABLE}"
    zip_full = f"{g}.{ZIP_TABLE}"
    if not (spark.catalog.tableExists(zcta_full) and spark.catalog.tableExists(zip_full)):
        log.warning(
            "Skipping us_ruca_zcta view -- us_zcta or us_ruca_zip missing (build geography first)",
            extra={"zcta_table": zcta_full, "zip_table": zip_full},
        )
        return False
    view = f"{g}.{ZCTA_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT r.*, "
        f"z.gisjoin AS zcta_gisjoin, "
        f"z.centroid_geo_lon AS zcta_centroid_geo_lon, "
        f"z.centroid_geo_lat AS zcta_centroid_geo_lat, "
        f"z.area_land_sqm AS zcta_area_land_sqm "
        f"FROM {zip_full} r "
        f"JOIN {zcta_full} z ON r.zip_code = z.geoid AND r.vintage = z.vintage"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'us_ruca_zip joined to us_zcta on (zip_code = geoid, vintage) -- the approximate "
        f"ZIP->ZCTA bridge (ZCTA = Census ZIP approximation), with ZCTA geometry attached. INNER "
        f"join: ZIPs without a matching ZCTA (point/PO-box/newer) are omitted. Not exact identity. "
        f"ADR 0038.'"
    )
    return True


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008): raw -> source catalog (layer raw, engineer-only);
# canonical + view -> model catalog (layer reference, reader-tier). Public domain (open).
# ---------------------------------------------------------------------------

_TRACT_KNOWN_LIMITATIONS = (
    "Primary + secondary RUCA codes (stored verbatim) and the source-provided population / land "
    "area / population density only; no derived rural-urban flag (combine the two code levels "
    "downstream). Vintages (1990/2000/2010/2020) are NOT comparable across decades -- tract "
    "boundaries and the urban-core methodology change each decade. The 1990 population/land-area "
    "values reflect the ERS 12/9/2025 errata correction (RUCA codes unaffected). Code-definition "
    "labels live as constants in reference/ruca.py; a us_ruca_code_definitions lookup is deferred."
)
_ZIP_KNOWN_LIMITATIONS = (
    "Primary + secondary RUCA codes (verbatim) + ZIP labels (state, zip_code_type, po_name) only. "
    "ZIP codes are USPS routes, not census GEOIDs, but ZCTA is the Census areal approximation of "
    "ZIP codes, so zip_code joins approximately to us_zcta.geoid on (zip_code = geoid, vintage) -- "
    "see the us_ruca_zcta view. The match is not 1:1 (point/PO-box and newer ZIPs have no ZCTA; "
    "ZCTA boundaries lag ZIP changes), so it is approximate enrichment, not identity. ZIP files "
    "exist only from the 2010 vintage on. RUCA codes are transferred from tracts to ZIPs by ERS "
    "(population-share for area ZIPs; containing-tract for point ZIPs)."
)

# Provenance fields shared by every RUCA dataset entry (raw + canonical + view).
_COMMON_META = {
    "spatial_coverage": "United States",
    "source_provider_code": "usda_ers",
    "source_url": ruca.SOURCE_PRODUCT_URL,
    "source_documentation_url": ruca.SOURCE_DOC_URL,
    "source_data_dictionary_url": ruca.SOURCE_DOC_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. USDA ERS RUCA files are public domain.",
    "access_tier": "open",
    "external_maintainer_name": "USDA Economic Research Service (ERS)",
    "is_hosted": True,
    "temporal_resolution": "decennial",
}


def _register_raw(spark: SparkSession, source_catalog: str, wrote_zip: bool) -> None:
    """Register the raw landing tables in the SOURCE catalog (layer=raw, vintage_snapshot)."""
    g = f"{source_catalog}.{RAW_SCHEMA}"
    registration.register_dataset(
        spark, source_catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            subject="geography", layer="raw",
            description=(
                "Raw USDA ERS RUCA census-tract codes, fetched-as-is (1:1 with source rows), "
                "vintage-stamped. Source landing promoted to geography.us_ruca_tract."
            ),
            public_health_relevance=(
                "Raw landing for the canonical tract RUCA reference; not consumed directly."
            ),
            spatial_resolution="us_tract",
            known_limitations="Fetched-as-is source landing; derivation/validation happen at promote.",
            derived_from=None,
            **_COMMON_META,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            update_semantics="vintage_snapshot", materialization_type="table",
            cluster_columns=["vintage", "geoid"], pipeline_reference=PIPELINE_REF,
        ),
    )
    if wrote_zip:
        registration.register_dataset(
            spark, source_catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{ZIP_TABLE}",
                subject="geography", layer="raw",
                description=(
                    "Raw USDA ERS RUCA ZIP-code codes (>= 2010), fetched-as-is (1:1 with source "
                    "rows), vintage-stamped. Source landing promoted to geography.us_ruca_zip."
                ),
                public_health_relevance=(
                    "Raw landing for the canonical ZIP RUCA reference; not consumed directly."
                ),
                spatial_resolution="zip_code",
                known_limitations="Fetched-as-is source landing; validation happens at promote.",
                derived_from=None,
                **_COMMON_META,
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=f"{g}.{ZIP_TABLE}",
                update_semantics="vintage_snapshot", materialization_type="table",
                cluster_columns=["vintage", "zip_code"], pipeline_reference=PIPELINE_REF,
            ),
        )


def _register_canonical(spark: SparkSession, source_catalog: str, model_catalog: str,
                        wrote_zip: bool) -> None:
    """Register the canonical tables + the ZIP->ZCTA view in the MODEL catalog (layer=reference)."""
    g = f"{model_catalog}.{MODEL_SCHEMA}"
    raw_g = f"{source_catalog}.{RAW_SCHEMA}"

    registration.register_dataset(
        spark, model_catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            subject="geography", layer="reference",
            description=(
                "USDA ERS Rural-Urban Commuting Area (RUCA) codes by census tract. Attribute "
                "extension of geography.us_tract keyed (geoid, vintage): primary_ruca (1-10, 99), "
                "secondary_ruca (e.g. 1.0/10.3, verbatim), plus source population, land_area_sqmi, "
                "population_density, state/county labels, and derived state_geoid/county_geoid."
            ),
            public_health_relevance=(
                "Sub-county rural/urban classification: lets tract-coded surveillance/population "
                "data be made rural-aware (urban core vs commuting area vs rural) at a finer grain "
                "than county-level schemes allow."
            ),
            spatial_resolution="us_tract",
            known_limitations=_TRACT_KNOWN_LIMITATIONS,
            derived_from=[f"{raw_g}.{TRACT_TABLE}"],
            **_COMMON_META,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            update_semantics="vintage_snapshot", materialization_type="table",
            cluster_columns=["vintage", "geoid"], pipeline_reference=PIPELINE_REF,
        ),
    )

    if wrote_zip:
        registration.register_dataset(
            spark, model_catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{ZIP_TABLE}",
                subject="geography", layer="reference",
                description=(
                    "USDA ERS Rural-Urban Commuting Area (RUCA) codes by ZIP code (>= 2010). Keyed "
                    "(zip_code, vintage): primary_ruca (1-10, 99), secondary_ruca (verbatim), plus "
                    "state, zip_code_type and po_name. ZIP is not a census GEOID."
                ),
                public_health_relevance=(
                    "ZIP-grain rural/urban classification for data captured by ZIP rather than "
                    "tract (claims, registries), approximating the tract RUCA scheme."
                ),
                spatial_resolution="zip_code",
                known_limitations=_ZIP_KNOWN_LIMITATIONS,
                derived_from=[f"{raw_g}.{ZIP_TABLE}"],
                **_COMMON_META,
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=f"{g}.{ZIP_TABLE}",
                update_semantics="vintage_snapshot", materialization_type="table",
                cluster_columns=["vintage", "zip_code"], pipeline_reference=PIPELINE_REF,
            ),
        )

    # ZIP->ZCTA bridge view (only when it was actually created -- same guard as _create_zcta_view).
    if spark.catalog.tableExists(f"{g}.{ZCTA_TABLE}") and spark.catalog.tableExists(
        f"{g}.{ZIP_TABLE}"
    ):
        registration.register_dataset(
            spark, model_catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{ZCTA_VIEW}",
                subject="geography", layer="reference",
                description=(
                    "us_ruca_zip joined to us_zcta on (zip_code = geoid, vintage) -- the "
                    "approximate ZIP->ZCTA bridge with ZCTA geometry attached. INNER join; ZIPs "
                    "without a matching ZCTA are omitted. Not exact identity."
                ),
                public_health_relevance=(
                    "ZCTA-keyed RUCA classification for joining to census-geography (ZCTA) data "
                    "without hand-writing the approximate ZIP->ZCTA bridge."
                ),
                spatial_resolution="us_zcta",
                known_limitations=(
                    "Approximate ZIP->ZCTA match (not 1:1); point/PO-box and newer ZIPs absent."
                ),
                derived_from=[f"{g}.{ZIP_TABLE}", f"{g}.{ZCTA_TABLE}"],
                **{**_COMMON_META, "is_hosted": False},
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=f"{g}.{ZCTA_VIEW}",
                update_semantics="full_refresh", materialization_type="view",
                cluster_columns=None, pipeline_reference=PIPELINE_REF,
            ),
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    tract_url: str | None = None,
    zip_url: str | None = None,
    sheet: Any = 0,
) -> None:
    requested = sorted(set(vintages))
    for v in requested:
        if v not in SUPPORTED_VINTAGES:
            raise ValueError(f"Unsupported RUCA vintage {v}; expected one of {SUPPORTED_VINTAGES}")
    if (tract_url is not None or zip_url is not None) and len(requested) != 1:
        raise ValueError("--tract-url/--zip-url override a single vintage; pass exactly one --vintage")

    # Download + parse every requested vintage before the build lifecycle starts. Raw rows are the
    # source-fidelity projection (canonical derives state_geoid/county_geoid at promote time).
    tract_records: list[ruca.RucaTractRecord] = []
    zip_records: list[ruca.RucaZipRecord] = []
    raw_tract_rows: list[dict[str, Any]] = []
    raw_zip_rows: list[dict[str, Any]] = []
    now = datetime.now(tz=UTC)

    for vintage in requested:
        t_url = tract_url or TRACT_URLS[vintage]
        log.info("Downloading RUCA tract file", extra={"vintage": vintage, "url": t_url})
        raw, t_file = _download(t_url)
        t_rows = _read_rows(raw, t_file, sheet=sheet)
        t_recs = ruca.parse_tract_rows(t_rows, vintage)
        tract_records.extend(t_recs)
        raw_tract_rows.extend(_raw_tract_row(r, t_file, now) for r in t_recs)
        log.info("Parsed RUCA tracts",
                 extra={"vintage": vintage, "source_rows": len(t_rows), "rows": len(t_recs)})

        z_url = zip_url or ZIP_URLS.get(vintage)
        if z_url is None:
            log.info("No RUCA ZIP file for vintage (skipping)", extra={"vintage": vintage})
            continue
        log.info("Downloading RUCA ZIP file", extra={"vintage": vintage, "url": z_url})
        raw, z_file = _download(z_url)
        z_rows = _read_rows(raw, z_file, sheet=sheet)
        z_recs = ruca.parse_zip_rows(z_rows, vintage)
        zip_records.extend(z_recs)
        raw_zip_rows.extend(_raw_zip_row(r, z_file, now) for r in z_recs)
        log.info("Parsed RUCA ZIPs",
                 extra={"vintage": vintage, "source_rows": len(z_rows), "rows": len(z_recs)})

    tract_vintages = sorted({r["vintage"] for r in raw_tract_rows})
    zip_vintages = sorted({r["vintage"] for r in raw_zip_rows})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings that augment the geography subject "
            f"(e.g. USDA ERS RUCA). Engineer-owned; canonicals promote to ecdh_model_*.geography. "
            f"See ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical US geography reference: states, counties, tracts, ZCTAs, HHS "
            f"regions, RUCA rural-urban codes, and companion boundaries. Owned by the _reference "
            f"bundle. See ADR 0020.'"
        )

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark
        _dq_checks(ctx, tract_records, zip_records, requested)

        # 1. Land raw (source catalog), 1:1 with source, vintage_snapshot.
        raw_t_df = spark.createDataFrame(raw_tract_rows, schema=RAW_TRACT_SPARK_SCHEMA)
        _vintage_snapshot_write(
            spark, f"{source_catalog}.{RAW_SCHEMA}.{TRACT_TABLE}", raw_t_df, tract_vintages
        )
        if raw_zip_rows:
            raw_z_df = spark.createDataFrame(raw_zip_rows, schema=ZIP_SPARK_SCHEMA)
            _vintage_snapshot_write(
                spark, f"{source_catalog}.{RAW_SCHEMA}.{ZIP_TABLE}", raw_z_df, zip_vintages
            )

        # 2. Promote canonical (model catalog) from raw, vintage_snapshot.
        _promote_tract(spark, source_catalog, model_catalog, tract_vintages)
        if raw_zip_rows:
            _promote_zip(spark, source_catalog, model_catalog, zip_vintages)

        _comment_tables(spark, source_catalog, model_catalog, wrote_zip=bool(raw_zip_rows))
        _create_zcta_view(spark, model_catalog)

    def _register(spark: SparkSession) -> None:
        _register_raw(spark, source_catalog, wrote_zip=bool(raw_zip_rows))
        _register_canonical(spark, source_catalog, model_catalog, wrote_zip=bool(raw_zip_rows))

    def _grant(spark: SparkSession) -> None:
        # Raw (source catalog) is engineer-owned, not reader-exposed (ADR 0018/0037).
        grants.grant_schema_engineer(spark, source_catalog, RAW_SCHEMA, data_engineers_group)
        grants.verify_schema_engineer(spark, source_catalog, RAW_SCHEMA, data_engineers_group)
        # Canonical reference (model catalog) is reader-tier for both groups (ADR 0018).
        grants.grant_schema_reader(spark, model_catalog, MODEL_SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, model_catalog, MODEL_SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, model_catalog, MODEL_SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, model_catalog, MODEL_SCHEMA, analysts_group)

    # DQ + lifecycle are scoped to the model catalog (where the canonical consumer tables + their
    # _ops live); _work/_register/_grant reach the source catalog explicitly for the raw layer.
    run_build(
        catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=_register,
        grant=_grant,
    )


def _raw_tract_row(r: ruca.RucaTractRecord, source_file: str, now: datetime) -> dict[str, Any]:
    """Source-fidelity raw tract row (no derived parents; those are added at promote)."""
    return {
        "geoid": r.geoid,
        "vintage": r.vintage,
        "state": r.state,
        "county": r.county,
        "primary_ruca": r.primary_ruca,
        "secondary_ruca": r.secondary_ruca,
        "population": r.population,
        "land_area_sqmi": r.land_area_sqmi,
        "population_density": r.population_density,
        "source_file": source_file,
        "ingested_at": now,
    }


def _raw_zip_row(r: ruca.RucaZipRecord, source_file: str, now: datetime) -> dict[str, Any]:
    return {
        "zip_code": r.zip_code,
        "vintage": r.vintage,
        "state": r.state,
        "zip_code_type": r.zip_code_type,
        "po_name": r.po_name,
        "primary_ruca": r.primary_ruca,
        "secondary_ruca": r.secondary_ruca,
        "source_file": source_file,
        "ingested_at": now,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-catalog", required=True, help="Source-aligned catalog for raw (ecdh_<env>)."
    )
    parser.add_argument(
        "--model-catalog", required=True, help="Integrated catalog for canonical (ecdh_model_<env>)."
    )
    parser.add_argument(
        "--vintage",
        type=int,
        nargs="+",
        default=[2020],
        help="RUCA vintage(s) to load (1990/2000/2010/2020). Default: 2020.",
    )
    parser.add_argument(
        "--tract-url",
        default=None,
        help="Explicit census-tract file URL for a single vintage (overrides the templated URL).",
    )
    parser.add_argument(
        "--zip-url",
        default=None,
        help="Explicit ZIP-code file URL for a single vintage (overrides the templated URL).",
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help="Excel data sheet (name or 0-based index) for .xlsx/.xls vintages. Default: first sheet.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    sheet: Any = args.sheet
    if isinstance(sheet, str) and sheet.isdigit():
        sheet = int(sheet)
    run(
        args.source_catalog,
        args.model_catalog,
        args.vintage,
        args.data_engineers_group,
        args.analysts_group,
        tract_url=args.tract_url,
        zip_url=args.zip_url,
        sheet=sheet,
    )


if __name__ == "__main__":
    main()
