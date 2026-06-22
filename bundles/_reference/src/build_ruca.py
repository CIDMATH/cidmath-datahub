"""Build the canonical RUCA geography reference tables (ADR 0020, ADR 0038).

USDA ERS Rural-Urban Commuting Area (RUCA) codes are a sub-county rural/urban classification
keyed to census tracts and (from 2010 on) ZIP codes. This entrypoint is the thin IO + Spark
layer over the pure logic in ``cidmath_datahub.reference.ruca`` (ADR 0011). For each requested
vintage it downloads the ERS file(s) over public HTTPS, reads them (CSV / XLSX / legacy ``.xls``)
into row dicts, parses + validates, and writes two tables in the integrated catalog:

  - ``geography.us_ruca_tract`` -- PK ``(geoid, vintage)``; an attribute extension of
    ``geography.us_tract`` (joins ``USING (geoid, vintage)``), carrying primary/secondary RUCA
    codes plus the source population / land area / density.
  - ``geography.us_ruca_zip`` -- PK ``(zip_code, vintage)``; ZIP codes are not census GEOIDs and
    do not join to ``us_zcta``. Only vintages >= 2010 publish a ZIP file.

Versioned per RUCA vintage (1990/2000/2010/2020), ``snapshot_replace`` per vintage (ADR 0024):
each run replaces only the vintage(s) it rebuilt and leaves the others intact. Vintages are NOT
comparable across decades (tract boundaries + methodology change each decade), so ``vintage`` is
part of the key. Public domain (U.S. Government work) -- plain HTTPS download, no credential.

The ERS download slugs carry a ``?v=`` cache-buster that shifts when a file is re-posted, so
``--tract-url`` / ``--zip-url`` accept a live link per vintage (paste from the product page if a
templated default 404s) -- the same operator-override pattern the ICD-10-PCS build uses.

Blocking DQ (FAIL, raises): PK uniqueness per table; tract GEOID is 11 digits / ZIP is 5 digits;
every ``primary_ruca`` is in 1-10/99; every ``secondary_ruca`` is a published code. WARN:
per-vintage cardinality; tract population present; primary-code distribution.

Usage:
    build_ruca.py --catalog ecdh_model_dev --vintage 2020 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # load several vintages (ZIP is loaded only where it exists, >= 2010):
    build_ruca.py --catalog ecdh_model_dev --vintage 2020 2010 2000 1990 ...

    # paste a live ERS link if a templated default has shifted (single vintage):
    build_ruca.py --catalog ecdh_model_dev --vintage 2020 \\
        --tract-url https://www.ers.usda.gov/media/5443/<live>.csv?v=<n>
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

SCHEMA = "geography"
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

US_RUCA_TRACT_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state_geoid", T.StringType(), False),
        T.StructField("county_geoid", T.StringType(), False),
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

US_RUCA_ZIP_SPARK_SCHEMA = T.StructType(
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
# DQ (ADR 0009): blocking PK / id-format / code-vocab; WARN cardinality + distribution
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
    tract_full = f"{SCHEMA}.{TRACT_TABLE}"
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
    zip_full = f"{SCHEMA}.{ZIP_TABLE}"
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
# Write (snapshot_replace per vintage; ADR 0024 vintage semantics)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession,
    full: str,
    rows: list[dict[str, Any]],
    schema: T.StructType,
    vintages: list[int],
    sort_cols: list[str],
) -> None:
    """snapshot_replace: replace only the vintages this run rebuilt; keep other vintages."""
    df = spark.createDataFrame(rows, schema=schema).sort(*sort_cols)
    if _table_has_column(spark, full, "vintage"):
        years_sql = ", ".join(str(v) for v in vintages)
        spark.sql(f"DELETE FROM {full} WHERE vintage IN ({years_sql})")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote table", extra={"table": full, "rows": len(rows), "vintages": vintages})


def _comment_tables(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TRACT_TABLE} IS "
        f"'USDA ERS RUCA codes by census tract. Attribute extension of geography.us_tract "
        f"(join USING (geoid, vintage)). primary_ruca (1-10/99) + secondary_ruca (verbatim) + "
        f"source population/land area/density. PK (geoid, vintage); snapshot_replace. Vintages "
        f"are NOT comparable across decades. ADR 0020/0038.'"
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{ZIP_TABLE} IS "
        f"'USDA ERS RUCA codes by ZIP code (>= 2010 only). primary_ruca (1-10/99) + "
        f"secondary_ruca (verbatim). ZIP is not a census GEOID, but zip_code is an approximate FK "
        f"to us_zcta.geoid (5-digit; join on (zip_code = geoid, vintage)); see the us_ruca_zcta "
        f"view. PK (zip_code, vintage); snapshot_replace. ADR 0020/0038.'"
    )


def _create_current_views(spark: SparkSession, catalog: str) -> None:
    for table in (TRACT_TABLE, ZIP_TABLE):
        full = f"{catalog}.{SCHEMA}.{table}"
        view = f"{catalog}.{SCHEMA}.{table}_current"
        spark.sql(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * FROM {full} WHERE vintage = (SELECT MAX(vintage) FROM {full})"
        )
        spark.sql(f"COMMENT ON VIEW {view} IS 'geography.{table} restricted to the latest vintage.'")


def _create_zcta_view(spark: SparkSession, catalog: str) -> bool:
    """Materialize the approximate ZIP->ZCTA join (us_ruca_zip x us_zcta).

    ZCTA is the Census areal approximation of ZIP codes, so the 5-digit zip_code is joined to
    us_zcta.geoid on (zip_code = geoid, vintage). INNER join: only ZIP rows with a matching ZCTA
    appear (point / PO-box / newer ZIPs drop out). Depends on geography.us_zcta existing (the
    geography build runs first); if absent, skip with a WARN rather than fail. Returns whether the
    view was created (so registration can match).
    """
    zcta_full = f"{catalog}.{SCHEMA}.{ZCTA_TABLE}"
    zip_full = f"{catalog}.{SCHEMA}.{ZIP_TABLE}"
    if not (spark.catalog.tableExists(zcta_full) and spark.catalog.tableExists(zip_full)):
        log.warning(
            "Skipping us_ruca_zcta view -- us_zcta or us_ruca_zip missing (build geography first)",
            extra={"zcta_table": zcta_full, "zip_table": zip_full},
        )
        return False
    view = f"{catalog}.{SCHEMA}.{ZCTA_VIEW}"
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
# Register (_ops metadata, ADR 0008) -- public domain (open)
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


def _register(spark: SparkSession, catalog: str, vintages: list[int], *, create_views: bool) -> None:
    g = f"{catalog}.{SCHEMA}"
    common = {
        "subject": SCHEMA,
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

    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            layer="reference",
            description=(
                "USDA ERS Rural-Urban Commuting Area (RUCA) codes by census tract. Attribute "
                "extension of geography.us_tract keyed (geoid, vintage): primary_ruca (1-10, 99), "
                "secondary_ruca (e.g. 1.0/10.3, verbatim), plus source population, land_area_sqmi, "
                "population_density and state/county labels."
            ),
            public_health_relevance=(
                "Sub-county rural/urban classification: lets tract-coded surveillance/population "
                "data be made rural-aware (urban core vs commuting area vs rural) at a finer grain "
                "than county-level schemes allow."
            ),
            spatial_resolution="us_tract",
            known_limitations=_TRACT_KNOWN_LIMITATIONS,
            derived_from=[f"USDA ERS RUCA census tract file {v}" for v in vintages],
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TRACT_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["vintage", "geoid"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    zip_vintages = [v for v in vintages if v in ZIP_URLS]
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{ZIP_TABLE}",
            layer="reference",
            description=(
                "USDA ERS Rural-Urban Commuting Area (RUCA) codes by ZIP code (>= 2010 vintages). "
                "Keyed (zip_code, vintage): primary_ruca (1-10, 99), secondary_ruca (verbatim), "
                "plus state, zip_code_type and po_name. ZIP codes are not census GEOIDs."
            ),
            public_health_relevance=(
                "ZIP-grain rural/urban classification for data captured by ZIP rather than tract "
                "(claims, registries), approximating the tract RUCA scheme."
            ),
            spatial_resolution="zip_code",
            known_limitations=_ZIP_KNOWN_LIMITATIONS,
            derived_from=[f"USDA ERS RUCA ZIP code file {v}" for v in zip_vintages or vintages],
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{ZIP_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["vintage", "zip_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    if create_views:
        for table, desc in (
            (f"{TRACT_TABLE}_current", "geography.us_ruca_tract restricted to the latest vintage."),
            (f"{ZIP_TABLE}_current", "geography.us_ruca_zip restricted to the latest vintage."),
        ):
            registration.register_dataset(
                spark,
                catalog,
                registration.DatasetCatalogEntry(
                    full_table_name=f"{g}.{table}",
                    layer="reference",
                    description=desc,
                    public_health_relevance="Latest-vintage convenience view.",
                    spatial_resolution="us_tract" if table.startswith(TRACT_TABLE) else "zip_code",
                    known_limitations=None,
                    derived_from=[f"{g}.{table.removesuffix('_current')}"],
                    **{**common, "is_hosted": False},
                ),
                registration.DatasetEngineeringEntry(
                    full_table_name=f"{g}.{table}",
                    update_semantics="full_refresh",
                    materialization_type="view",
                    cluster_columns=None,
                    pipeline_reference=PIPELINE_REF,
                ),
            )

        # Register the ZIP->ZCTA bridge view only when it was actually created (same guard as
        # _create_zcta_view), so we don't catalog a view that geography hasn't enabled yet.
        if spark.catalog.tableExists(f"{g}.{ZCTA_TABLE}") and spark.catalog.tableExists(
            f"{g}.{ZIP_TABLE}"
        ):
            registration.register_dataset(
                spark,
                catalog,
                registration.DatasetCatalogEntry(
                    full_table_name=f"{g}.{ZCTA_VIEW}",
                    layer="reference",
                    description=(
                        "us_ruca_zip joined to us_zcta on (zip_code = geoid, vintage) -- the "
                        "approximate ZIP->ZCTA bridge with ZCTA geometry attached. INNER join; "
                        "ZIPs without a matching ZCTA are omitted. Not exact identity."
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
                    **{**common, "is_hosted": False},
                ),
                registration.DatasetEngineeringEntry(
                    full_table_name=f"{g}.{ZCTA_VIEW}",
                    update_semantics="full_refresh",
                    materialization_type="view",
                    cluster_columns=None,
                    pipeline_reference=PIPELINE_REF,
                ),
            )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(
    catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    tract_url: str | None = None,
    zip_url: str | None = None,
    sheet: Any = 0,
    create_views: bool = True,
) -> None:
    requested = sorted(set(vintages))
    for v in requested:
        if v not in SUPPORTED_VINTAGES:
            raise ValueError(f"Unsupported RUCA vintage {v}; expected one of {SUPPORTED_VINTAGES}")
    if (tract_url is not None or zip_url is not None) and len(requested) != 1:
        raise ValueError("--tract-url/--zip-url override a single vintage; pass exactly one --vintage")

    # Download + parse every requested vintage before the build lifecycle starts.
    tract_records: list[ruca.RucaTractRecord] = []
    zip_records: list[ruca.RucaZipRecord] = []
    tract_rows_out: list[dict[str, Any]] = []
    zip_rows_out: list[dict[str, Any]] = []
    now = datetime.now(tz=UTC)

    for vintage in requested:
        t_url = tract_url or TRACT_URLS[vintage]
        log.info("Downloading RUCA tract file", extra={"vintage": vintage, "url": t_url})
        raw, t_file = _download(t_url)
        t_recs = ruca.parse_tract_rows(_read_rows(raw, t_file, sheet=sheet), vintage)
        tract_records.extend(t_recs)
        tract_rows_out.extend(_tract_row_dict(r, t_file, now) for r in t_recs)
        log.info("Parsed RUCA tracts", extra={"vintage": vintage, "rows": len(t_recs)})

        z_url = zip_url or ZIP_URLS.get(vintage)
        if z_url is None:
            log.info("No RUCA ZIP file for vintage (skipping)", extra={"vintage": vintage})
            continue
        log.info("Downloading RUCA ZIP file", extra={"vintage": vintage, "url": z_url})
        raw, z_file = _download(z_url)
        z_recs = ruca.parse_zip_rows(_read_rows(raw, z_file, sheet=sheet), vintage)
        zip_records.extend(z_recs)
        zip_rows_out.extend(_zip_row_dict(r, z_file, now) for r in z_recs)
        log.info("Parsed RUCA ZIPs", extra={"vintage": vintage, "rows": len(z_recs)})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference: states, counties, tracts, ZCTAs, HHS "
            f"regions, RUCA rural-urban codes, and companion boundaries. Owned by the _reference "
            f"bundle. See ADR 0020.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(ctx, tract_records, zip_records, requested)
        _write_table(ctx.spark, f"{catalog}.{SCHEMA}.{TRACT_TABLE}", tract_rows_out,
                     US_RUCA_TRACT_SPARK_SCHEMA, requested, ["vintage", "geoid"])
        if zip_rows_out:
            zip_vintages = sorted({r["vintage"] for r in zip_rows_out})
            _write_table(ctx.spark, f"{catalog}.{SCHEMA}.{ZIP_TABLE}", zip_rows_out,
                         US_RUCA_ZIP_SPARK_SCHEMA, zip_vintages, ["vintage", "zip_code"])
        _comment_tables(ctx.spark, catalog)
        if create_views:
            _create_current_views(ctx.spark, catalog)
            _create_zcta_view(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        # Reference data is canonical and pipeline-owned: both groups get reader-tier (ADR 0018).
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, requested, create_views=create_views),
        grant=_grant,
    )


def _tract_row_dict(r: ruca.RucaTractRecord, source_file: str, now: datetime) -> dict[str, Any]:
    return {
        "geoid": r.geoid,
        "vintage": r.vintage,
        "state_geoid": r.state_geoid,
        "county_geoid": r.county_geoid,
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


def _zip_row_dict(r: ruca.RucaZipRecord, source_file: str, now: datetime) -> dict[str, Any]:
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
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
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
    parser.add_argument(
        "--no-current-views", action="store_true", help="Skip the *_current latest-vintage views."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    sheet: Any = args.sheet
    if isinstance(sheet, str) and sheet.isdigit():
        sheet = int(sheet)
    run(
        args.catalog,
        args.vintage,
        args.data_engineers_group,
        args.analysts_group,
        tract_url=args.tract_url,
        zip_url=args.zip_url,
        sheet=sheet,
        create_views=not args.no_current_views,
    )


if __name__ == "__main__":
    main()
