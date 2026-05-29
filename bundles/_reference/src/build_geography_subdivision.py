"""Build geography.country_subdivision + ADM_1 boundaries (ADR 0022, slice 3b).

Pulls ISO 3166-2 subdivision codes from pycountry (~5,046 entries, including
nested cases like UK constituent countries -> counties), and GADM 4.1 ADM_1
polygons from geodata.ucdavis.edu. Writes:

  - ``geography.country_subdivision`` — one row per pycountry subdivision,
    keyed by ``subdivision_code`` (ISO 3166-2). ``gadm_gid_1`` populated when
    HASC_1 / ISO_1 / fixup-map resolves to a polygon; null otherwise (nested
    subdivisions inherit their parent's polygon spatially and don't get a
    direct ADM_1 match).
  - ``geography.boundary`` (extension) — appends ``geo_level='country_subdivision'``
    rows after a DELETE-then-INSERT scoped to that level. Per-level
    ``full_refresh`` semantics on the shared boundary table.

Pure logic (subdivision-code parsing, GADM ADM_1 matching, row assembly)
lives in ``cidmath_datahub.reference.geography_intl`` (ADR 0011). This
entrypoint is the thin IO + Spark layer: download, read, assemble, write,
DQ, register.

GADM file is ~1.4 GB zipped; only the ``ADM_1`` layer is read into memory
via pyogrio. Match priority: HASC_1 -> ISO_1 -> GADM_ADM1_ISO_FIXUPS (manual
override map, ships empty; populated from first-run DQ misses, not from
training-data priors).

Usage:
    build_geography_subdivision.py --catalog ecdh_model_dev \\
        --data-engineers-group ecdh-data-engineers \\
        --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import tempfile
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants
from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
TABLE = "country_subdivision"
COUNTRY_TABLE = "country"
BOUNDARY_TABLE = "boundary"
GEO_LEVEL = "country_subdivision"

# GADM 4.1 download (same artifact as build_geography_country.py, slice 3a).
GADM_ZIP_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-levels.zip"
GADM_GPKG_NAME = "gadm_410-levels.gpkg"
GADM_ADM1_LAYER = "ADM_1"
GADM_VINTAGE = 2022
GADM_USER_AGENT = "Mozilla/5.0 cidmath-datahub/1.0 (+https://github.com/cidmath)"
GADM_LICENSE = (
    "GADM data may be used for academic and other non-commercial use. "
    "Redistribution requires explicit permission. See https://gadm.org/license.html"
)

# Generalization tolerance matches 3a / US tables (ADR 0020).
GENERALIZE_TOLERANCE_DEG = 0.005

# Join-coverage threshold: 90% of NON-NESTED subdivisions should match a GADM
# ADM_1 polygon. Nested subdivisions (parent_subdivision_code IS NOT NULL,
# ~1,300 of 5,046 pycountry entries) inherit their parent's polygon spatially
# and are excluded from the denominator. See ADR 0022 + slice 3b plan.
JOIN_COVERAGE_THRESHOLD_PCT = 90.0
CARDINALITY_MIN = 4500
CARDINALITY_MAX = 5500

SUBDIVISION_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("subdivision_code", T.StringType(), False),
        T.StructField("country_alpha2", T.StringType(), False),
        T.StructField("country_alpha3", T.StringType(), False),
        T.StructField("subdivision_local_code", T.StringType(), False),
        T.StructField("subdivision_name", T.StringType(), False),
        T.StructField("subdivision_type_label", T.StringType(), False),
        T.StructField("parent_subdivision_code", T.StringType(), True),
        T.StructField("gadm_gid_1", T.StringType(), True),
        T.StructField("centroid_geo_lon", T.DoubleType(), True),
        T.StructField("centroid_geo_lat", T.DoubleType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("source_file", T.StringType(), False),
    ]
)

# Matches geography.boundary's schema (ADR 0020 / build_geography.py).
BOUNDARY_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geo_level", T.StringType(), False),
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("resolution", T.StringType(), False),
        T.StructField("gisjoin", T.StringType(), True),
        T.StructField("geometry_wkb", T.BinaryType(), False),
    ]
)


def _download_gadm_zip(dest: Path) -> Path:
    target = dest / "gadm_410-levels.zip"
    log.info("Downloading GADM", extra={"url": GADM_ZIP_URL, "dest": str(target)})
    req = urllib.request.Request(GADM_ZIP_URL, headers={"User-Agent": GADM_USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, open(target, "wb") as out:
        chunk = resp.read(1 << 20)
        while chunk:
            out.write(chunk)
            chunk = resp.read(1 << 20)
    log.info("Downloaded GADM zip", extra={"bytes": target.stat().st_size})
    return target


def _extract_gpkg(zip_path: Path, dest: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    gpkg = dest / GADM_GPKG_NAME
    if not gpkg.exists():
        candidates = list(dest.rglob("*.gpkg"))
        if not candidates:
            raise FileNotFoundError(f"No .gpkg found under {dest}")
        gpkg = candidates[0]
    log.info("Extracted GeoPackage", extra={"path": str(gpkg)})
    return gpkg


def _read_adm1(gpkg: Path) -> Any:
    """Read the ADM_1 layer as a GeoDataFrame in EPSG:4326.

    Asserts the expected column set immediately after read so a GADM schema
    change fails locally with a clear message rather than producing silently
    empty matches downstream (per CLAUDE.md guidance).
    """
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer=GADM_ADM1_LAYER)
    gi.assert_gadm_adm1_columns(gdf.columns)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326, allow_override=True)
    else:
        gdf = gdf.to_crs(4326)
    log.info("Read GADM ADM_1", extra={"rows": len(gdf), "columns": list(gdf.columns)})
    return gdf


def _gdf_to_dict_rows(gdf: Any) -> list[dict[str, Any]]:
    """Materialize a GeoDataFrame to plain row dicts.

    Decouples the rest of the pipeline from GeoPandas so the matching logic
    in ``geography_intl.match_gadm_adm1`` stays unit-testable with dicts.
    Geometry is carried through as a shapely object.
    """
    cols = [c for c in gdf.columns if c != "geometry"]
    rows: list[dict[str, Any]] = []
    for _, r in gdf.iterrows():
        d: dict[str, Any] = {c: r[c] for c in cols}
        d["geometry"] = r.geometry
        rows.append(d)
    return rows


def _centroid(geom: Any) -> tuple[float, float] | tuple[None, None]:
    if geom is None or geom.is_empty:
        return (None, None)
    pt = geom.representative_point()
    return (float(pt.x), float(pt.y))


def _build_subdivision_rows(
    iso_to_gadm: dict[str, dict[str, Any]],
    source_file: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Iterate pycountry.subdivisions and emit (attribute_rows, boundary_rows).

    Joins each pycountry subdivision to its GADM ADM_1 polygon via the
    ``iso_to_gadm`` lookup. Subdivisions with no polygon still produce an
    attribute row (centroid / gadm_gid_1 left null); the build's DQ surfaces
    coverage as a WARN, not a FAIL.
    """
    import pycountry
    import shapely

    now = datetime.now(tz=UTC)
    attr_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []

    for sub in pycountry.subdivisions:
        code = sub.code
        country_alpha2 = sub.country_code
        country_obj = pycountry.countries.get(alpha_2=country_alpha2)
        if country_obj is None:
            log.warning(
                "Skipping subdivision with unknown country",
                extra={"code": code, "country_alpha2": country_alpha2},
            )
            continue
        country_alpha3 = country_obj.alpha_3

        gadm_row = iso_to_gadm.get(code)
        gadm_gid_1 = gadm_row.get("GID_1") if gadm_row else None
        geom = gadm_row.get("geometry") if gadm_row else None
        lon, lat = _centroid(geom) if geom is not None else (None, None)

        try:
            row = gi.assemble_subdivision_row(
                subdivision_code=code,
                country_alpha2=country_alpha2,
                country_alpha3=country_alpha3,
                subdivision_name=sub.name,
                subdivision_type_label=sub.type,
                parent_subdivision_code=sub.parent_code,
                gadm_gid_1=gadm_gid_1,
                centroid_geo_lon=lon,
                centroid_geo_lat=lat,
                source_file=source_file,
            )
        except ValueError as e:
            log.warning(
                "Skipping malformed subdivision",
                extra={"code": code, "error": str(e)},
            )
            continue
        row["ingested_at"] = now
        attr_rows.append(row)

        if geom is not None and not geom.is_empty:
            simplified = geom.simplify(GENERALIZE_TOLERANCE_DEG, preserve_topology=True)
            boundary_rows.append(
                {
                    "geo_level": GEO_LEVEL,
                    "geoid": row["subdivision_code"],
                    "vintage": GADM_VINTAGE,
                    "resolution": "generalized",
                    "gisjoin": None,
                    "geometry_wkb": shapely.to_wkb(simplified, output_dimension=2),
                }
            )

    return attr_rows, boundary_rows


def _write_subdivision_table(spark: SparkSession, catalog: str, rows: list[dict[str, Any]]) -> None:
    df = spark.createDataFrame(rows, schema=SUBDIVISION_SPARK_SCHEMA).sort("subdivision_code")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.{TABLE}"
    )
    log.info("Wrote geography.country_subdivision", extra={"rows": len(rows)})


def _write_subdivision_boundaries(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]]
) -> None:
    """Replace geography.boundary rows for this geo_level, then append.

    Per-level full_refresh semantics on a shared polymorphic table — matches
    slice 3a's _write_country_boundaries pattern.
    """
    spark.sql(f"DELETE FROM {catalog}.{SCHEMA}.{BOUNDARY_TABLE} WHERE geo_level = '{GEO_LEVEL}'")
    df = spark.createDataFrame(rows, schema=BOUNDARY_SPARK_SCHEMA)
    df.write.mode("append").saveAsTable(f"{catalog}.{SCHEMA}.{BOUNDARY_TABLE}")
    log.info(
        "Wrote country_subdivision boundaries",
        extra={"rows": len(rows), "vintage": GADM_VINTAGE},
    )


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ISO 3166-2 first-level subdivisions (subdivision_code PK like ''US-GA''). "
        f"gadm_gid_1 links to geography.boundary where a GADM ADM_1 polygon resolves; "
        f"nested subdivisions (parent_subdivision_code IS NOT NULL) inherit their "
        f"parent''s polygon spatially. Source: pycountry + GADM 4.1. ADR 0022.'"
    )


def _register_dataset(spark: SparkSession, catalog: str, pipeline_ref: str) -> None:
    """Register geography.country_subdivision in _ops.dataset_catalog + dataset_engineering.

    Mirrors the explicit-column MERGE pattern from build_geography_country
    (ADR 0008 + slice 3a). MERGE ... UPDATE SET * shorthand fails on
    _ops.dataset_catalog because the target has more columns than the source.
    """
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    desc = (
        "Global first-level subdivision reference. ISO 3166-2 PK; FK to "
        "geography.country via country_alpha2/3; optional gadm_gid_1 link "
        "to geography.boundary."
    )
    pubhealth = (
        "Subnational surveillance backbone for international data sources keyed "
        "on ISO 3166-2 (WHO subnational reporting, GBD location hierarchy)."
    )

    cat_schema = T.StructType(
        [
            T.StructField("full_table_name", T.StringType()),
            T.StructField("subject", T.StringType()),
            T.StructField("layer", T.StringType()),
            T.StructField("description", T.StringType()),
            T.StructField("public_health_relevance", T.StringType()),
            T.StructField("spatial_resolution", T.StringType()),
            T.StructField("spatial_coverage", T.StringType()),
            T.StructField("source_provider_code", T.StringType()),
            T.StructField("source_url", T.StringType()),
            T.StructField("source_documentation_url", T.StringType()),
            T.StructField("license", T.StringType()),
            T.StructField("dua_required", T.BooleanType()),
            T.StructField("dua_reference", T.StringType()),
            T.StructField("access_tier", T.StringType()),
            T.StructField("external_maintainer_name", T.StringType()),
            T.StructField("is_hosted", T.BooleanType()),
            T.StructField("owner", T.StringType()),
        ]
    )
    cat_row = [
        (
            full,
            SCHEMA,
            "reference",
            desc,
            pubhealth,
            "country_subdivision",
            "global",
            "gadm",
            "https://gadm.org/",
            "https://gadm.org/metadata.html",
            GADM_LICENSE,
            True,
            (
                "GADM citation required (Hijmans, R. GADM database of Global "
                "Administrative Areas). pycountry MIT."
            ),
            "restricted",
            "GADM, University of California, Davis",
            True,
            "cidmath-data-team",
        )
    ]
    spark.createDataFrame(cat_row, cat_schema).createOrReplaceTempView("_tmp_geo_subdivision_cat")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING _tmp_geo_subdivision_cat AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            subject = s.subject, layer = s.layer, description = s.description,
            public_health_relevance = s.public_health_relevance,
            spatial_resolution = s.spatial_resolution, spatial_coverage = s.spatial_coverage,
            source_provider_code = s.source_provider_code, source_url = s.source_url,
            source_documentation_url = s.source_documentation_url, license = s.license,
            dua_required = s.dua_required, dua_reference = s.dua_reference,
            access_tier = s.access_tier, external_maintainer_name = s.external_maintainer_name,
            is_hosted = s.is_hosted, owner = s.owner, last_validated = CURRENT_DATE()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, subject, layer, description, public_health_relevance,
             spatial_resolution, spatial_coverage, source_provider_code, source_url,
             source_documentation_url, license, dua_required, dua_reference, access_tier,
             external_maintainer_name, is_hosted, owner, last_validated)
            VALUES
            (s.full_table_name, s.subject, s.layer, s.description, s.public_health_relevance,
             s.spatial_resolution, s.spatial_coverage, s.source_provider_code, s.source_url,
             s.source_documentation_url, s.license, s.dua_required, s.dua_reference, s.access_tier,
             s.external_maintainer_name, s.is_hosted, s.owner, CURRENT_DATE())
        """
    )

    eng_schema = T.StructType(
        [
            T.StructField("full_table_name", T.StringType()),
            T.StructField("update_semantics", T.StringType()),
            T.StructField("materialization_type", T.StringType()),
            T.StructField("cluster_columns", T.ArrayType(T.StringType())),
            T.StructField("pipeline_reference", T.StringType()),
            T.StructField("schema_version", T.IntegerType()),
        ]
    )
    eng_row = [(full, "full_refresh", "table", ["subdivision_code"], pipeline_ref, 1)]
    spark.createDataFrame(eng_row, eng_schema).createOrReplaceTempView("_tmp_geo_subdivision_eng")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING _tmp_geo_subdivision_eng AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            update_semantics = s.update_semantics,
            materialization_type = s.materialization_type,
            cluster_columns = s.cluster_columns,
            pipeline_reference = s.pipeline_reference,
            last_refresh_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, update_semantics, materialization_type, cluster_columns,
             pipeline_reference, schema_version, last_refresh_at)
            VALUES
            (s.full_table_name, s.update_semantics, s.materialization_type, s.cluster_columns,
             s.pipeline_reference, s.schema_version, CURRENT_TIMESTAMP())
        """
    )
    log.info("Registered geography.country_subdivision metadata", extra={"table": full})


def _dq_checks(
    recorder: DQRecorder,
    spark: SparkSession,
    catalog: str,
    rows: list[dict[str, Any]],
    unmatched_gid_1s: list[str],
) -> None:
    """Run DQ on the assembled rows (ADR 0009).

    Three checks (per slice 3b plan):
      1. subdivision_code uniqueness — FAIL.
      2. country FK integrity to geography.country (alpha2) — WARN.
      3. ISO -> GADM ADM_1 join coverage, restricted to NON-NESTED rows — WARN.
      4. ISO 3166-2 cardinality sanity range — WARN.

    Join-coverage check also logs ``sample_unmatched_gid_1`` so future
    GADM_ADM1_ISO_FIXUPS entries have ground truth to work from.
    """
    codes = [r["subdivision_code"] for r in rows]
    dups = sorted({c for c in codes if codes.count(c) > 1})
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="subdivision_code_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dups,
        failing_row_count=len(dups),
        total_row_count=len(rows),
        details={"sample_duplicates": dups[:10]} if dups else None,
    )
    if dups:
        raise ValueError(f"Duplicate subdivision_code: {dups[:10]}")

    # FK integrity: every country_alpha2 should resolve to a geography.country row.
    country_alpha2s = {r["country_alpha2"] for r in rows}
    try:
        known_alpha2_df = spark.sql(
            f"SELECT DISTINCT country_alpha2 FROM {catalog}.{SCHEMA}.{COUNTRY_TABLE}"
        ).collect()
        known_alpha2 = {r["country_alpha2"] for r in known_alpha2_df}
    except Exception as e:  # noqa: BLE001 — country table may not exist on first deploy
        log.warning("country FK check skipped", extra={"error": str(e)})
        known_alpha2 = country_alpha2s
    fk_missing = sorted(country_alpha2s - known_alpha2)
    fk_failing = sum(1 for r in rows if r["country_alpha2"] in fk_missing)
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="subdivision_country_fk_integrity",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not fk_missing,
        failing_row_count=fk_failing,
        total_row_count=len(rows),
        details={"sample_missing_alpha2": fk_missing[:10]} if fk_missing else None,
    )

    # Join coverage — denominator restricted to NON-NESTED rows so the
    # threshold is meaningful. Nested rows inherit their parent's polygon
    # spatially and are not expected to carry gadm_gid_1.
    non_nested = [r for r in rows if r["parent_subdivision_code"] is None]
    non_nested_total = len(non_nested)
    non_nested_matched = sum(1 for r in non_nested if r["gadm_gid_1"] is not None)
    non_nested_pct = non_nested_matched / non_nested_total * 100 if non_nested_total else 0.0
    all_matched = sum(1 for r in rows if r["gadm_gid_1"] is not None)
    all_pct = (all_matched / len(rows) * 100) if rows else 0.0
    passed_cov = non_nested_pct >= JOIN_COVERAGE_THRESHOLD_PCT
    sample_missing_codes = sorted(
        r["subdivision_code"] for r in non_nested if r["gadm_gid_1"] is None
    )[:10]
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_to_gadm_adm1_join_coverage",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=passed_cov,
        failing_row_count=non_nested_total - non_nested_matched,
        total_row_count=non_nested_total,
        details={
            "non_nested_coverage_pct": round(non_nested_pct, 2),
            "all_rows_coverage_pct": round(all_pct, 2),
            "threshold_pct": JOIN_COVERAGE_THRESHOLD_PCT,
            "denominator_scope": "parent_subdivision_code IS NULL",
            "sample_missing_subdivision_codes": sample_missing_codes,
            "sample_unmatched_gid_1": unmatched_gid_1s[:20],
        },
    )

    total = len(rows)
    passed_count = CARDINALITY_MIN <= total <= CARDINALITY_MAX
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_3166_2_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=passed_count,
        failing_row_count=0 if passed_count else 1,
        total_row_count=total,
        details={"expected_range": [CARDINALITY_MIN, CARDINALITY_MAX], "actual": total},
    )


def run(
    catalog: str,
    data_engineers_group: str,
    analysts_group: str,
) -> None:
    spark = SparkSession.builder.getOrCreate()
    pipeline_ref = "bundles/_reference/src/build_geography_subdivision.py"

    log.info("Building geography.country_subdivision", extra={"catalog": catalog})

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

    workdir = Path(tempfile.mkdtemp(prefix="gadm_"))
    zip_path = _download_gadm_zip(workdir)
    gpkg = _extract_gpkg(zip_path, workdir)
    gdf = _read_adm1(gpkg)
    gadm_rows = _gdf_to_dict_rows(gdf)

    iso_to_gadm, unmatched_gid_1s = gi.match_gadm_adm1(gadm_rows, fixups=gi.GADM_ADM1_ISO_FIXUPS)
    log.info(
        "Matched GADM ADM_1 to ISO 3166-2",
        extra={
            "matched_iso_codes": len(iso_to_gadm),
            "unmatched_gadm_rows": len(unmatched_gid_1s),
            "sample_unmatched_gid_1": unmatched_gid_1s[:10],
        },
    )

    attr_rows, boundary_rows = _build_subdivision_rows(iso_to_gadm, source_file=GADM_GPKG_NAME)
    log.info(
        "Assembled subdivision rows",
        extra={"attribute_rows": len(attr_rows), "boundary_rows": len(boundary_rows)},
    )

    run_id = new_run_id()
    log.info("DQ run id assigned", extra={"run_id": run_id, "pipeline_reference": pipeline_ref})

    with DQRecorder(spark, catalog, run_id, pipeline_ref) as recorder:
        _dq_checks(recorder, spark, catalog, attr_rows, unmatched_gid_1s)
        _write_subdivision_table(spark, catalog, attr_rows)
        if boundary_rows:
            _write_subdivision_boundaries(spark, catalog, boundary_rows)

    _comment_table(spark, catalog)

    grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

    _register_dataset(spark, catalog, pipeline_ref)

    log.info("geography.country_subdivision build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(args.catalog, args.data_engineers_group, args.analysts_group)


if __name__ == "__main__":
    main()
