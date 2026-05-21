"""Build the canonical geography reference tables in the integrated catalog.

Slice 1 (ADR 0020): state + county + HHS regions, vintages 2010 and 2020, lean
attribute tables plus companion generalized geometry in ``geography.boundary``.
Source: IPUMS NHGIS shapefiles. Update semantics: ``full_refresh`` (ADR 0007).

Scope notes (decided during wiring, see ADR 0020):
  - No crosswalk in slice 1. NHGIS publishes no direct county->county crosswalk
    (its 2010<->2020 crosswalks are sourced from block groups), and counties are
    near-stable across the decade. Crosswalks land in slice 2 at tract/BG level.
  - Centroids are population-weighted where the Census Centers of Population
    point files cover the unit (``centroid_is_pop_weighted = true``), falling
    back to the polygon interior point otherwise (e.g. units CoP doesn't cover).

Pure, testable logic (GEOID/GISJOIN parsing, HHS mapping, row assembly) lives in
``cidmath_datahub.reference.geography`` (ADR 0011); this entrypoint is the thin
IO layer: pull, read, write. Geometry/IO deps (ipumspy, geopandas, pyogrio,
shapely) are provided by the job environment, not the shared wheel (ADR 0020),
so they are imported lazily.

Usage:
    build_geography.py --catalog ecdh_model_dev --vintages 2010,2020 \\
        --data-engineers-group ecdh-data-engineers \\
        --analysts-group ecdh-analysts \\
        --ipums-secret-scope ecdh-dev-ipums --ipums-secret-key nhgis_api_key
"""

from __future__ import annotations

import argparse
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.reference import geography as geo

log = get_logger(__name__)

SCHEMA = "geography"

# IPUMS NHGIS boundary shapefile API codes, keyed by (level, vintage). Pattern is
# us_<level>_<year>_tl<tiger_basis>. Verify/extend against the live catalog with
# IpumsApiClient.get_metadata_catalog(metadata_type="shapefiles").
SHAPEFILE_NAMES: dict[tuple[str, int], str] = {
    ("state", 2010): "us_state_2010_tl2010",
    ("state", 2020): "us_state_2020_tl2020",
    ("county", 2010): "us_county_2010_tl2010",
    ("county", 2020): "us_county_2020_tl2020",
}

# Census Centers of Population point shapefiles (population-weighted centroids),
# keyed by (level, vintage). Optional: a missing entry or file falls back to the
# polygon interior point. Pattern: us_<level>_cenpop_<year>_cenpop<year>.
CENPOP_SHAPEFILE_NAMES: dict[tuple[str, int], str] = {
    ("state", 2010): "us_state_cenpop_2010_cenpop2010",
    ("state", 2020): "us_state_cenpop_2020_cenpop2020",
    ("county", 2010): "us_county_cenpop_2010_cenpop2010",
    ("county", 2020): "us_county_cenpop_2020_cenpop2020",
}

NHGIS_SOURCE_URL = "https://www.nhgis.org/"
NHGIS_DOC_URL = "https://www.nhgis.org/documentation"
NHGIS_LICENSE = (
    "IPUMS NHGIS terms of use: citation and attribution required; "
    "redistribution restricted (permission requested)."
)
NHGIS_DUA_REFERENCE = "IPUMS NHGIS citation required; see https://www.nhgis.org/ for terms."
NHGIS_MAINTAINER = "IPUMS NHGIS, University of Minnesota"

HHS_REGION_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("hhs_region", T.IntegerType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("member_states", T.ArrayType(T.StringType()), False),
    ]
)

STATE_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("stusps", T.StringType(), False),
        T.StructField("hhs_region", T.IntegerType(), False),
        T.StructField("centroid_lon", T.DoubleType(), True),
        T.StructField("centroid_lat", T.DoubleType(), True),
        T.StructField("centroid_is_pop_weighted", T.BooleanType(), False),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

COUNTY_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state_geoid", T.StringType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("centroid_lon", T.DoubleType(), True),
        T.StructField("centroid_lat", T.DoubleType(), True),
        T.StructField("centroid_is_pop_weighted", T.BooleanType(), False),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

BOUNDARY_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geo_level", T.StringType(), False),
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("resolution", T.StringType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("geometry_wkb", T.BinaryType(), False),
    ]
)


def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - depends on runtime flavor
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope=scope, key=key)


def _num(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _first_col(columns: list[str], candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def _download_shapefiles(api_key: str, shapefile_names: list[str], workdir: Path) -> None:
    from ipumspy import AggregateDataExtract, IpumsApiClient

    ipums = IpumsApiClient(api_key)
    extract = AggregateDataExtract(
        collection="nhgis",
        description="CIDMATH geography reference (state + county boundaries + cenpop)",
        shapefiles=list(shapefile_names),
    )
    log.info("Submitting NHGIS extract", extra={"shapefiles": shapefile_names})
    ipums.submit_extract(extract)
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=str(workdir))
    log.info("Downloaded NHGIS extract", extra={"workdir": str(workdir)})


def _extract_all_zips(root: Path) -> None:
    seen: set[Path] = set()
    while True:
        pending = [p for p in root.rglob("*.zip") if p not in seen]
        if not pending:
            break
        for zp in pending:
            out = zp.parent / f"{zp.stem}_unz"
            out.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zp) as zf:
                zf.extractall(out)
            seen.add(zp)


def _find_shapefile(root: Path, level: str, vintage: int, *, cenpop: bool = False) -> Path | None:
    """Locate the .shp for a level/vintage.

    Centers-of-Population files carry 'cenpop' in their path; boundary files do
    not. Returns None for a missing cenpop file (optional; caller falls back to
    geographic centroids); raises for a missing boundary file.
    """
    lvl, year = level.lower(), str(vintage)
    matches = [
        p
        for p in root.rglob("*.shp")
        if lvl in str(p).lower()
        and year in str(p).lower()
        and ("cenpop" in str(p).lower()) == cenpop
    ]
    if not matches:
        if cenpop:
            return None
        names = [p.name for p in root.rglob("*.shp")]
        raise FileNotFoundError(f"No shapefile for level={level} vintage={vintage}. Found: {names}")
    return matches[0]


def _read_gdf(shp: Path) -> Any:
    import geopandas as gpd

    gdf = gpd.read_file(shp)
    if gdf.crs is None:
        gdf = gdf.set_crs(4269, allow_override=True)
    return gdf.to_crs(4326)


def _read_cenpop_lookup(root: Path, level: str, vintage: int) -> dict[str, tuple[float, float]]:
    """Read the Centers of Population point file into ``{gisjoin: (lon, lat)}``.

    Empty dict when no cenpop file is present (caller falls back to geographic
    centroids). Keyed by GISJOIN so it joins directly to the boundary features.
    """
    shp = _find_shapefile(root, level, vintage, cenpop=True)
    if shp is None:
        log.info(
            "No Centers of Population file; using geographic centroids",
            extra={"level": level, "vintage": vintage},
        )
        return {}
    gdf = _read_gdf(shp)
    gj = _first_col(list(gdf.columns), ["GISJOIN", "gisjoin"])
    lookup: dict[str, tuple[float, float]] = {}
    if gj is None:
        return lookup
    for _, rec in gdf.iterrows():
        geom = rec.geometry
        if geom is None or geom.is_empty:
            continue
        lookup[str(rec[gj]).strip().upper()] = (float(geom.x), float(geom.y))
    log.info(
        "Loaded centers of population",
        extra={"level": level, "vintage": vintage, "points": len(lookup)},
    )
    return lookup


def _centroid_for(
    gisjoin: Any, geom: Any, cenpop: dict[str, tuple[float, float]]
) -> tuple[float, float, bool]:
    """Population-weighted centroid if available for this GISJOIN, else the
    polygon interior point. Returns ``(lon, lat, is_pop_weighted)``."""
    key = str(gisjoin).strip().upper()
    if cenpop and key in cenpop:
        lon, lat = cenpop[key]
        return lon, lat, True
    pt = geom.representative_point()
    return float(pt.x), float(pt.y), False


def _geom_to_wkb(geom: Any, tolerance: float) -> bytes:
    if tolerance > 0:
        geom = geom.simplify(tolerance, preserve_topology=True)
    return geom.wkb


def _build_state_frames(
    gdf: Any,
    vintage: int,
    tolerance: float,
    resolution: str,
    cenpop: dict[str, tuple[float, float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cols = list(gdf.columns)
    gj = _first_col(cols, ["GISJOIN", "gisjoin"])
    if gj is None:
        raise ValueError(f"state shapefile has no GISJOIN column; columns={cols}")
    aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
    awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])

    rows: list[dict[str, Any]] = []
    boundary: list[dict[str, Any]] = []
    for _, rec in gdf.iterrows():
        geom = rec.geometry
        if geom is None or geom.is_empty:
            continue
        lon, lat, pop_weighted = _centroid_for(rec[gj], geom, cenpop)
        row = geo.build_state_row(
            rec[gj],
            vintage,
            centroid_lon=lon,
            centroid_lat=lat,
            centroid_is_pop_weighted=pop_weighted,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(
            {
                "geo_level": "state",
                "geoid": row["geoid"],
                "vintage": vintage,
                "resolution": resolution,
                "gisjoin": row["gisjoin"],
                "geometry_wkb": _geom_to_wkb(geom, tolerance),
            }
        )
    return rows, boundary


def _build_county_frames(
    gdf: Any,
    vintage: int,
    tolerance: float,
    resolution: str,
    cenpop: dict[str, tuple[float, float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cols = list(gdf.columns)
    gj = _first_col(cols, ["GISJOIN", "gisjoin"])
    if gj is None:
        raise ValueError(f"county shapefile has no GISJOIN column; columns={cols}")
    name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
    aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
    awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])

    rows: list[dict[str, Any]] = []
    boundary: list[dict[str, Any]] = []
    for _, rec in gdf.iterrows():
        geom = rec.geometry
        if geom is None or geom.is_empty:
            continue
        lon, lat, pop_weighted = _centroid_for(rec[gj], geom, cenpop)
        name = str(rec[name_col]) if name_col else ""
        row = geo.build_county_row(
            rec[gj],
            vintage,
            name,
            centroid_lon=lon,
            centroid_lat=lat,
            centroid_is_pop_weighted=pop_weighted,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(
            {
                "geo_level": "county",
                "geoid": row["geoid"],
                "vintage": vintage,
                "resolution": resolution,
                "gisjoin": row["gisjoin"],
                "geometry_wkb": _geom_to_wkb(geom, tolerance),
            }
        )
    return rows, boundary


def _dq_checks(state_rows: list[dict[str, Any]], county_rows: list[dict[str, Any]]) -> None:
    def _dups(rows: list[dict[str, Any]]) -> set[tuple[str, int]]:
        seen: set[tuple[str, int]] = set()
        dups: set[tuple[str, int]] = set()
        for r in rows:
            key = (r["geoid"], r["vintage"])
            if key in seen:
                dups.add(key)
            seen.add(key)
        return dups

    state_dups, county_dups = _dups(state_rows), _dups(county_rows)
    if state_dups:
        raise ValueError(f"duplicate (geoid, vintage) in state: {sorted(state_dups)}")
    if county_dups:
        raise ValueError(f"duplicate (geoid, vintage) in county: {sorted(county_dups)}")

    state_keys = {(r["geoid"], r["vintage"]) for r in state_rows}
    orphans = sorted(
        {
            (r["geoid"], r["state_geoid"], r["vintage"])
            for r in county_rows
            if (r["state_geoid"], r["vintage"]) not in state_keys
        }
    )
    if orphans:
        raise ValueError(f"county rows with no matching state: {orphans}")
    log.info("DQ checks passed", extra={"states": len(state_rows), "counties": len(county_rows)})


def _build_hhs_region(spark: SparkSession, catalog: str) -> None:
    rows = geo.generate_hhs_regions()
    df = spark.createDataFrame(rows, schema=HHS_REGION_SPARK_SCHEMA).sort("hhs_region")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.hhs_region"
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.hhs_region IS "
        f"'The ten HHS regions (static federal grouping of states). Reference "
        f"table; full_refresh. ADR 0020.'"
    )
    log.info("Wrote hhs_region", extra={"rows": len(rows)})


def _write_table(
    spark: SparkSession,
    catalog: str,
    table: str,
    rows: list[dict[str, Any]],
    schema: T.StructType,
    sort_cols: list[str],
) -> None:
    df = spark.createDataFrame(rows, schema=schema).repartition(1).sortWithinPartitions(*sort_cols)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.{table}"
    )
    log.info("Wrote table", extra={"table": f"{catalog}.{SCHEMA}.{table}", "rows": len(rows)})


def _register_dataset(
    spark: SparkSession,
    *,
    catalog: str,
    table: str,
    description: str,
    public_health_relevance: str,
    spatial_resolution: str,
    cluster_columns: list[str] | None,
    pipeline_reference: str,
) -> None:
    full = f"{catalog}.{SCHEMA}.{table}"

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
            description,
            public_health_relevance,
            spatial_resolution,
            "United States",
            "ipums_nhgis",
            NHGIS_SOURCE_URL,
            NHGIS_DOC_URL,
            NHGIS_LICENSE,
            True,
            NHGIS_DUA_REFERENCE,
            "restricted",
            NHGIS_MAINTAINER,
            True,
            "cidmath-data-team",
        )
    ]
    spark.createDataFrame(cat_row, cat_schema).createOrReplaceTempView("_tmp_geo_cat")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING _tmp_geo_cat AS s
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
    eng_row = [(full, "full_refresh", "table", cluster_columns, pipeline_reference, 1)]
    spark.createDataFrame(eng_row, eng_schema).createOrReplaceTempView("_tmp_geo_eng")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING _tmp_geo_eng AS s
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
    log.info("Registered dataset metadata", extra={"table": full})


def run(
    catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    ipums_secret_scope: str | None = None,
    ipums_secret_key: str | None = None,
    simplify_tolerance: float = 0.005,
    full_resolution: bool = False,
) -> None:
    spark = SparkSession.builder.getOrCreate()
    pipeline_ref = "bundles/_reference/src/build_geography.py"

    log.info(
        "Building geography reference tables",
        extra={"catalog": catalog, "vintages": vintages, "full_resolution": full_resolution},
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
        f"COMMENT 'Canonical US geography reference: states, counties, HHS "
        f"regions, and companion boundaries. Owned by the _reference bundle. "
        f"Source: IPUMS NHGIS. See ADR 0020.'"
    )

    _build_hhs_region(spark, catalog)

    if not ipums_secret_scope:
        raise ValueError("--ipums-secret-scope is required to pull NHGIS shapefiles")
    api_key = _get_secret(ipums_secret_scope, ipums_secret_key or "nhgis_api_key")

    missing = [
        (lvl, v) for v in vintages for lvl in ("state", "county") if (lvl, v) not in SHAPEFILE_NAMES
    ]
    if missing:
        raise ValueError(f"no known NHGIS shapefile code for {missing}; extend SHAPEFILE_NAMES")
    boundary_names = [SHAPEFILE_NAMES[(lvl, v)] for v in vintages for lvl in ("state", "county")]
    cenpop_names = [
        CENPOP_SHAPEFILE_NAMES[(lvl, v)]
        for v in vintages
        for lvl in ("state", "county")
        if (lvl, v) in CENPOP_SHAPEFILE_NAMES
    ]
    shapefile_names = boundary_names + cenpop_names

    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    workdir = Path(tempfile.mkdtemp(prefix="nhgis_"))
    _download_shapefiles(api_key, shapefile_names, workdir)
    _extract_all_zips(workdir)

    state_rows: list[dict[str, Any]] = []
    county_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []
    for v in vintages:
        state_gdf = _read_gdf(_find_shapefile(workdir, "state", v))
        county_gdf = _read_gdf(_find_shapefile(workdir, "county", v))
        state_cenpop = _read_cenpop_lookup(workdir, "state", v)
        county_cenpop = _read_cenpop_lookup(workdir, "county", v)
        sr, sb = _build_state_frames(state_gdf, v, tolerance, resolution, state_cenpop)
        cr, cb = _build_county_frames(county_gdf, v, tolerance, resolution, county_cenpop)
        state_rows += sr
        county_rows += cr
        boundary_rows += sb + cb
        log.info(
            "Processed vintage",
            extra={
                "vintage": v,
                "states": len(sr),
                "counties": len(cr),
                "state_cenpop": len(state_cenpop),
                "county_cenpop": len(county_cenpop),
            },
        )

    _dq_checks(state_rows, county_rows)

    _write_table(spark, catalog, "state", state_rows, STATE_SPARK_SCHEMA, ["vintage", "geoid"])
    _write_table(spark, catalog, "county", county_rows, COUNTY_SPARK_SCHEMA, ["vintage", "geoid"])
    _write_table(
        spark,
        catalog,
        "boundary",
        boundary_rows,
        BOUNDARY_SPARK_SCHEMA,
        ["geo_level", "vintage", "geoid"],
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.state IS "
        f"'US states + DC (and territories), vintaged. Reference; full_refresh. "
        f"Source IPUMS NHGIS. ADR 0020.'"
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.county IS "
        f"'US counties keyed (geoid, vintage); state_geoid FK to state. Reference; "
        f"full_refresh. Source IPUMS NHGIS. ADR 0020.'"
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.boundary IS "
        f"'Companion boundary polygons (WKB) for state/county by vintage and "
        f"resolution. Source IPUMS NHGIS. ADR 0020.'"
    )

    grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
    grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)
    log.info("Access model verified", extra={"schema": f"{catalog}.{SCHEMA}"})

    _register_dataset(
        spark,
        catalog=catalog,
        table="state",
        description="US states and DC (plus territories), one row per state per vintage.",
        public_health_relevance=(
            "Canonical state spatial unit that surveillance and modeling data conform "
            "to; carries HHS region for federal regional rollups."
        ),
        spatial_resolution="state",
        cluster_columns=None,
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="county",
        description="US counties, one row per county per vintage, with state FK.",
        public_health_relevance=(
            "Canonical county spatial unit; the standard grain for U.S. infectious "
            "disease surveillance and the spatial backbone other subjects join to."
        ),
        spatial_resolution="county",
        cluster_columns=None,
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="hhs_region",
        description="The ten HHS regions (static federal grouping of states).",
        public_health_relevance=(
            "Federal regional grouping used for HHS/CDC regional reporting and rollups."
        ),
        spatial_resolution="hhs_region",
        cluster_columns=None,
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="boundary",
        description="Companion boundary polygons (WKB) for state and county by vintage.",
        public_health_relevance=(
            "Geometry for choropleth mapping and spatial-adjacency models; kept off "
            "the lean attribute tables so attribute joins stay cheap."
        ),
        spatial_resolution="multi",
        cluster_columns=["geo_level", "vintage"],
        pipeline_reference=pipeline_ref,
    )

    log.info("Geography reference build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--vintages", default="2010,2020", help="Comma-separated TIGER/Line basis years."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    parser.add_argument("--ipums-secret-scope", default=None)
    parser.add_argument("--ipums-secret-key", default="nhgis_api_key")
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.005,
        help="Generalization tolerance in degrees. Ignored with --full-resolution.",
    )
    parser.add_argument(
        "--full-resolution",
        action="store_true",
        help="Store full-resolution geometry (resolution='full') instead of generalized.",
    )
    args = parser.parse_args()

    vintages = [int(v) for v in args.vintages.split(",") if v.strip()]
    run(
        args.catalog,
        vintages,
        args.data_engineers_group,
        args.analysts_group,
        args.ipums_secret_scope,
        args.ipums_secret_key,
        args.simplify_tolerance,
        args.full_resolution,
    )


if __name__ == "__main__":
    main()
