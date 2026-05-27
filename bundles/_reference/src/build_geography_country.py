"""Build the global geography.country table + ADM0 boundaries (ADR 0022, slice 3a).

Pulls ISO 3166-1 codes from pycountry, WHO region (GHO ParentCode form),
UN macro region, UN M49 sub-region, and UN membership from the in-repo
static lookup :mod:`cidmath_datahub.reference.country_classifications`
(neither country_converter nor pycountry expose these cleanly — see that
module's docstring), and GADM 4.1 ADM0 polygons from geodata.ucdavis.edu.
Writes:

  - ``geography.country`` — one row per ISO 3166-1 entry (~249), keyed by
    ``country_alpha3``. Centroids derived from GADM ADM0 representative
    points where the alpha-3 has a polygon; null for entries with no GADM
    match (rare — typically historical or sub-national ISO entries).
  - ``geography.boundary`` (extension) — appends ``geo_level='country'``
    rows after a DELETE-then-INSERT scoped to that level. Per-level
    ``full_refresh`` semantics on the shared boundary table.

Pure logic (validation, normalization, row assembly) lives in
``cidmath_datahub.reference.geography_intl`` (ADR 0011). This entrypoint
is the thin IO + Spark layer: download, read, assemble, write, DQ, register.

GADM file is ~1.4 GB zipped; only the ``ADM_0`` layer is read into memory
via pyogrio. GADM uses ``GID_0`` as the alpha-3 when an ISO code exists
and X-prefixed codes (``XKO``, ``XNC``, etc.) for non-ISO territories; the
latter are excluded from both the attribute table and the boundary table
because they have no canonical ISO surveillance key (ADR 0022).

Usage:
    build_geography_country.py --catalog ecdh_model_dev \\
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
from cidmath_datahub.reference import country_classifications as cclass
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
TABLE = "country"
BOUNDARY_TABLE = "boundary"

# GADM 4.1 download (verified URL pattern, gadm.org/download_world.html).
# Zipped GeoPackage containing six layers (ADM_0..ADM_5); we only read ADM_0.
GADM_ZIP_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-levels.zip"
GADM_GPKG_NAME = "gadm_410-levels.gpkg"
GADM_ADM0_LAYER = "ADM_0"
GADM_VINTAGE = 2022  # GADM 4.1 release year, recorded on each boundary row.
GADM_USER_AGENT = "Mozilla/5.0 cidmath-datahub/1.0 (+https://github.com/cidmath)"
GADM_LICENSE = (
    "GADM data may be used for academic and other non-commercial use. "
    "Redistribution requires explicit permission. See https://gadm.org/license.html"
)

# Geometry generalization tolerance matches the US tables (ADR 0020).
GENERALIZE_TOLERANCE_DEG = 0.005

COUNTRY_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("country_alpha3", T.StringType(), False),
        T.StructField("country_alpha2", T.StringType(), False),
        T.StructField("country_numeric", T.StringType(), False),
        T.StructField("country_name", T.StringType(), False),
        T.StructField("country_official_name", T.StringType(), True),
        T.StructField("who_region", T.StringType(), True),
        T.StructField("un_region", T.StringType(), True),
        T.StructField("un_subregion", T.StringType(), True),
        T.StructField("is_un_member", T.BooleanType(), False),
        T.StructField("is_sovereign", T.BooleanType(), False),
        T.StructField("iso_3166_3_predecessor", T.StringType(), True),
        T.StructField("centroid_geo_lon", T.DoubleType(), True),
        T.StructField("centroid_geo_lat", T.DoubleType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("source_file", T.StringType(), False),
    ]
)

# Matches geography.boundary's schema as defined in build_geography.py (ADR 0020).
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
    """Download the GADM 4.1 zipped GeoPackage to ``dest`` and return its path.

    Sets a real User-Agent header — geodata.ucdavis.edu has been observed to
    403 default Python user-agents.
    """
    target = dest / "gadm_410-levels.zip"
    log.info("Downloading GADM", extra={"url": GADM_ZIP_URL, "dest": str(target)})
    req = urllib.request.Request(GADM_ZIP_URL, headers={"User-Agent": GADM_USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, open(target, "wb") as out:
        chunk = resp.read(1 << 20)  # 1 MiB chunks
        while chunk:
            out.write(chunk)
            chunk = resp.read(1 << 20)
    log.info("Downloaded GADM zip", extra={"bytes": target.stat().st_size})
    return target


def _extract_gpkg(zip_path: Path, dest: Path) -> Path:
    """Unzip the GADM archive and return the path to the .gpkg file."""
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    gpkg = dest / GADM_GPKG_NAME
    if not gpkg.exists():
        # GADM occasionally nests; fall back to recursive find.
        candidates = list(dest.rglob("*.gpkg"))
        if not candidates:
            raise FileNotFoundError(f"No .gpkg found under {dest}")
        gpkg = candidates[0]
    log.info("Extracted GeoPackage", extra={"path": str(gpkg)})
    return gpkg


def _read_adm0(gpkg: Path) -> Any:
    """Read the ADM_0 layer from the GADM GeoPackage as a GeoDataFrame in EPSG:4326."""
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer=GADM_ADM0_LAYER)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326, allow_override=True)
    else:
        gdf = gdf.to_crs(4326)
    log.info(
        "Read GADM ADM_0",
        extra={"rows": len(gdf), "columns": list(gdf.columns)},
    )
    return gdf


def _gadm_alpha3_to_geometry(gdf: Any) -> dict[str, Any]:
    """Build a ``{alpha3: shapely_geometry}`` lookup from GADM ADM_0.

    Drops X-prefixed (non-ISO) GADM entries; we don't ship boundaries for
    territories without canonical ISO keys (ADR 0022).
    """
    lookup: dict[str, Any] = {}
    skipped: list[str] = []
    for _, row in gdf.iterrows():
        gid0 = row.get("GID_0")
        if not gi.is_iso_gid0(gid0):
            skipped.append(str(gid0))
            continue
        lookup[gid0] = row.geometry
    log.info(
        "GADM ADM_0 keyed by alpha3",
        extra={
            "matched": len(lookup),
            "skipped_non_iso": len(skipped),
            "sample_skipped": skipped[:10],
        },
    )
    return lookup


def _centroid(geom: Any) -> tuple[float, float] | tuple[None, None]:
    """Return (lon, lat) representative point for a (Multi)Polygon, or (None, None)."""
    if geom is None or geom.is_empty:
        return (None, None)
    pt = geom.representative_point()
    return (float(pt.x), float(pt.y))


def _build_country_rows(
    gadm_geom_by_alpha3: dict[str, Any],
    source_file: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Iterate pycountry and emit (attribute_rows, boundary_rows).

    Joins pycountry to GADM via alpha-3. Countries with no GADM polygon
    still produce an attribute row (centroid columns left null); polygons
    with no pycountry match are already excluded by ``_gadm_alpha3_to_geometry``
    (those are GADM-coined X-prefixed codes).
    """
    import pycountry

    now = datetime.now(tz=UTC)

    attr_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []

    for country in pycountry.countries:
        alpha3 = country.alpha_3
        alpha2 = country.alpha_2
        numeric = country.numeric
        name = country.name
        official_name = getattr(country, "official_name", None)

        # WHO region, UN macro region, UN M49 sub-region, and UN member
        # status all come from our in-repo static lookup
        # (cidmath_datahub.reference.country_classifications). country_converter
        # was tried first but doesn't expose a 'WHO' column at all and its
        # 'UNmember' / 'UNregion' columns don't match our controlled vocabulary
        # cleanly. The static lookup is small, deterministic, and removes the
        # runtime dependency entirely. See cclass module docstring for sources.
        who = cclass.who_region(alpha3)
        un_region = cclass.un_region(alpha3)
        un_subregion = cclass.un_subregion(alpha3)
        is_un_member = cclass.is_un_member(alpha3)

        # is_sovereign proxy: most ISO 3166-1 entries are sovereign; dependent
        # territories have an ISO alpha-2 starting with a parent's prefix in
        # some cases but ISO doesn't mark sovereignty directly. For now treat
        # UN member status as the proxy with explicit overrides for the
        # well-known non-UN-member sovereign states (Taiwan, Palestine,
        # Vatican, Kosovo). Refine in slice 3a.1 if needed.
        is_sovereign = is_un_member or alpha3 in {"TWN", "PSE", "VAT", "XKX"}

        geom = gadm_geom_by_alpha3.get(alpha3)
        lon, lat = _centroid(geom) if geom is not None else (None, None)

        # Whitelist WHO/UN values into our controlled vocabulary; anything
        # else becomes None rather than failing assembly. Defensive guard in
        # case the cclass static lookup gains a non-vocabulary value during
        # a future refresh.
        if who not in gi.WHO_REGION_CODES:
            who = None
        if un_region not in gi.UN_REGION_NAMES:
            un_region = None

        row = gi.assemble_country_row(
            alpha2=alpha2,
            alpha3=alpha3,
            numeric=numeric,
            name=name,
            official_name=official_name,
            who_region=who,
            un_region=un_region,
            un_subregion=un_subregion,
            is_un_member=is_un_member,
            is_sovereign=is_sovereign,
            iso_3166_3_predecessor=None,  # populated from pycountry.historic_countries in 3a.1
            centroid_geo_lon=lon,
            centroid_geo_lat=lat,
            source_file=source_file,
        )
        row["ingested_at"] = now
        attr_rows.append(row)

        if geom is not None and not geom.is_empty:
            import shapely

            simplified = geom.simplify(GENERALIZE_TOLERANCE_DEG, preserve_topology=True)
            boundary_rows.append(
                {
                    "geo_level": "country",
                    "geoid": alpha3,
                    "vintage": GADM_VINTAGE,
                    "resolution": "generalized",
                    "gisjoin": None,
                    "geometry_wkb": shapely.to_wkb(simplified, output_dimension=2),
                }
            )

    return attr_rows, boundary_rows


def _write_country_table(spark: SparkSession, catalog: str, rows: list[dict[str, Any]]) -> None:
    df = spark.createDataFrame(rows, schema=COUNTRY_SPARK_SCHEMA).sort("country_alpha3")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.{TABLE}"
    )
    log.info("Wrote geography.country", extra={"rows": len(rows)})


def _write_country_boundaries(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]]
) -> None:
    """Replace geography.boundary rows where geo_level='country', then append.

    Per-level full_refresh semantics on a shared polymorphic table: each
    build job owns its geo_level slice; concurrent jobs are safe because
    they touch disjoint rows.
    """
    spark.sql(f"DELETE FROM {catalog}.{SCHEMA}.{BOUNDARY_TABLE} WHERE geo_level = 'country'")
    df = spark.createDataFrame(rows, schema=BOUNDARY_SPARK_SCHEMA)
    df.write.mode("append").saveAsTable(f"{catalog}.{SCHEMA}.{BOUNDARY_TABLE}")
    log.info("Wrote country boundaries", extra={"rows": len(rows), "vintage": GADM_VINTAGE})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ISO 3166-1 countries (alpha-3 PK) with WHO and UN M49 region attributes; "
        f"centroids from GADM ADM_0 representative points. Source: pycountry "
        f"+ in-repo WHO/UN classifications + GADM 4.1. ADR 0022.'"
    )


def _register_dataset(spark: SparkSession, catalog: str, pipeline_ref: str) -> None:
    """Register geography.country in _ops.dataset_catalog + _ops.dataset_engineering."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    src = (
        "'pycountry (MIT) + in-repo country_classifications (WHO + UN M49) "
        "+ GADM 4.1 (academic non-commercial)'"
    )
    desc = (
        "Global country reference. ISO 3166-1 alpha-3 PK; alpha-2/numeric "
        "alternates; WHO + UN M49 region attributes; centroids from GADM ADM0."
    )
    pubhealth = (
        "Canonical country reference for international surveillance. ISO 3166-1 "
        "alpha-3 matches WHO, IHR, and GBD conventions; WHO region enables "
        "regional aggregation."
    )
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING (SELECT
            '{full}' AS full_table_name,
            '{desc}' AS description,
            '{pubhealth}' AS public_health_relevance,
            'country' AS spatial_resolution,
            'global' AS geographic_scope,
            {src} AS source,
            '{GADM_LICENSE.replace("'", "''")}' AS license,
            true AS dua_required,
            '{pipeline_ref}' AS pipeline_reference,
            'cidmath-data-team' AS owner
        ) AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING (SELECT
            '{full}' AS full_table_name,
            'full_refresh' AS update_semantics,
            'table' AS materialization_type,
            'country_alpha3' AS cluster_columns,
            '{pipeline_ref}' AS pipeline_reference,
            1 AS schema_version,
            CURRENT_TIMESTAMP() AS last_refresh_at
        ) AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    log.info("Registered geography.country metadata", extra={"table": full})


def _dq_checks(
    recorder: DQRecorder,
    rows: list[dict[str, Any]],
    gadm_alpha3_set: set[str],
) -> None:
    """Run DQ on the assembled rows (ADR 0009) and persist results."""
    alpha3s = [r["country_alpha3"] for r in rows]
    dups = sorted({a for a in alpha3s if alpha3s.count(a) > 1})
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="country_alpha3_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dups,
        failing_row_count=len(dups),
        total_row_count=len(rows),
        details={"sample_duplicates": dups[:10]} if dups else None,
    )
    if dups:
        raise ValueError(f"Duplicate country_alpha3: {dups[:10]}")

    matched, total, missing = gi.check_join_coverage(alpha3s, gadm_alpha3_set)
    coverage_pct = (matched / total * 100) if total else 0.0
    passed = coverage_pct >= 95.0
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_to_gadm_join_coverage",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=passed,
        failing_row_count=total - matched,
        total_row_count=total,
        details={
            "coverage_pct": round(coverage_pct, 2),
            "threshold_pct": 95.0,
            "sample_missing_alpha3": missing,
        },
    )

    # Cardinality sanity — ISO 3166-1 currently lists ~249 entries; warn outside ±10.
    passed_count = 230 <= total <= 270
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_3166_1_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=passed_count,
        failing_row_count=0 if passed_count else 1,
        total_row_count=total,
        details={"expected_range": [230, 270], "actual": total},
    )


def run(
    catalog: str,
    data_engineers_group: str,
    analysts_group: str,
) -> None:
    spark = SparkSession.builder.getOrCreate()
    pipeline_ref = "bundles/_reference/src/build_geography_country.py"

    log.info("Building geography.country", extra={"catalog": catalog})

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

    workdir = Path(tempfile.mkdtemp(prefix="gadm_"))
    zip_path = _download_gadm_zip(workdir)
    gpkg = _extract_gpkg(zip_path, workdir)
    gdf = _read_adm0(gpkg)
    gadm_by_alpha3 = _gadm_alpha3_to_geometry(gdf)

    attr_rows, boundary_rows = _build_country_rows(gadm_by_alpha3, source_file=GADM_GPKG_NAME)
    log.info(
        "Assembled country rows",
        extra={"attribute_rows": len(attr_rows), "boundary_rows": len(boundary_rows)},
    )

    run_id = new_run_id()
    log.info("DQ run id assigned", extra={"run_id": run_id, "pipeline_reference": pipeline_ref})

    with DQRecorder(spark, catalog, run_id, pipeline_ref) as recorder:
        _dq_checks(recorder, attr_rows, set(gadm_by_alpha3.keys()))
        _write_country_table(spark, catalog, attr_rows)
        if boundary_rows:
            _write_country_boundaries(spark, catalog, boundary_rows)

    _comment_table(spark, catalog)

    grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

    _register_dataset(spark, catalog, pipeline_ref)

    log.info("geography.country build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(args.catalog, args.data_engineers_group, args.analysts_group)


if __name__ == "__main__":
    main()
