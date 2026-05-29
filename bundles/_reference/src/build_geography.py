"""Build the canonical geography reference tables in the integrated catalog.

Slices 1 + 2a (ADR 0020): state, county, census tract, and ZCTA, plus the static
HHS regions and companion generalized geometry in ``geography.boundary``.
Vintages 2010 and 2020. Source: IPUMS NHGIS shapefiles. Update semantics:
``full_refresh`` (ADR 0007).

Scope notes (decided during wiring, see ADR 0020):
  - No crosswalk yet. NHGIS publishes no direct same-level crosswalk; its
    2010<->2020 crosswalks are sourced from block groups. Crosswalks land in
    slice 2b, shipped as published.
  - Each entity carries two centroid pairs: a geographic interior point
    (``centroid_geo_lon``/``centroid_geo_lat``, always set) and a
    population-weighted center (``centroid_pop_lon``/``centroid_pop_lat``, set
    where a Census Center of Population covers the unit — state/county/tract;
    ZCTAs have none, so the ZCTA table omits the population pair).
  - High-volume levels (tract ~74k, ZCTA ~33k per vintage) are written per
    (level, vintage) chunk rather than accumulated in driver memory: the first
    write to each table overwrites (full_refresh), later chunks append.

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
from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography as geo

log = get_logger(__name__)

SCHEMA = "geography"

# Levels built from NHGIS polygon shapefiles, in dependency order (parents first
# so tract FK checks can run against already-loaded state/county).
LEVELS = ("us_state", "us_county", "us_tract", "us_zcta")

# IPUMS NHGIS boundary shapefile API codes, keyed by (level, vintage). Pattern is
# us_<level>_<year>_tl<tiger_basis>. Verify/extend against the live catalog with
# IpumsApiClient.get_metadata_catalog(metadata_type="shapefiles").
SHAPEFILE_NAMES: dict[tuple[str, int], str] = {
    ("us_state", 2010): "us_state_2010_tl2010",
    ("us_state", 2020): "us_state_2020_tl2020",
    ("us_county", 2010): "us_county_2010_tl2010",
    ("us_county", 2020): "us_county_2020_tl2020",
    ("us_tract", 2010): "us_tract_2010_tl2010",
    ("us_tract", 2020): "us_tract_2020_tl2020",
    ("us_zcta", 2010): "us_zcta_2010_tl2010",
    ("us_zcta", 2020): "us_zcta_2020_tl2020",
}

# Census Centers of Population point shapefiles (population-weighted centroids),
# keyed by (level, vintage). Optional: a missing entry or file falls back to the
# polygon interior point. CoP exists for state/county/tract, not ZCTA.
CENPOP_SHAPEFILE_NAMES: dict[tuple[str, int], str] = {
    ("us_state", 2010): "us_state_cenpop_2010_cenpop2010",
    ("us_state", 2020): "us_state_cenpop_2020_cenpop2020",
    ("us_county", 2010): "us_county_cenpop_2010_cenpop2010",
    ("us_county", 2020): "us_county_cenpop_2020_cenpop2020",
    ("us_tract", 2010): "us_tract_cenpop_2010_cenpop2010",
    ("us_tract", 2020): "us_tract_cenpop_2020_cenpop2020",
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
        T.StructField("centroid_geo_lon", T.DoubleType(), False),
        T.StructField("centroid_geo_lat", T.DoubleType(), False),
        T.StructField("centroid_pop_lon", T.DoubleType(), True),
        T.StructField("centroid_pop_lat", T.DoubleType(), True),
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
        T.StructField("centroid_geo_lon", T.DoubleType(), False),
        T.StructField("centroid_geo_lat", T.DoubleType(), False),
        T.StructField("centroid_pop_lon", T.DoubleType(), True),
        T.StructField("centroid_pop_lat", T.DoubleType(), True),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

TRACT_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state_geoid", T.StringType(), False),
        T.StructField("county_geoid", T.StringType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("centroid_geo_lon", T.DoubleType(), False),
        T.StructField("centroid_geo_lat", T.DoubleType(), False),
        T.StructField("centroid_pop_lon", T.DoubleType(), True),
        T.StructField("centroid_pop_lat", T.DoubleType(), True),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

ZCTA_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("centroid_geo_lon", T.DoubleType(), False),
        T.StructField("centroid_geo_lat", T.DoubleType(), False),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

# geography.boundary schema is shared via gadm.boundary_spark_schema() (ADR 0023).

ENTITY_SCHEMAS: dict[str, T.StructType] = {
    "us_state": STATE_SPARK_SCHEMA,
    "us_county": COUNTY_SPARK_SCHEMA,
    "us_tract": TRACT_SPARK_SCHEMA,
    "us_zcta": ZCTA_SPARK_SCHEMA,
}


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
        description="CIDMATH geography reference (state/county/tract/zcta + cenpop)",
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
    not. The leading ``us_<level>_`` token disambiguates levels whose names are
    substrings of others (e.g. county vs cty_sub). Returns None for a missing
    cenpop file (optional); raises for a missing boundary file.
    """
    # The level identifier carries a us_ prefix (ADR 0006 refinement) but NHGIS
    # shapefile filenames only have one us_ — strip ours before composing the
    # match token.
    bare_level = level.lower().removeprefix("us_")
    token = f"us_{bare_level}_"
    year = str(vintage)
    matches = [
        p
        for p in root.rglob("*.shp")
        if token in str(p).lower()
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
) -> tuple[float, float, float | None, float | None]:
    """Return ``(geo_lon, geo_lat, pop_lon, pop_lat)``.

    The geographic interior point is always present; the population-weighted pair
    is None unless a Center of Population covers this GISJOIN.
    """
    pt = geom.representative_point()
    geo_lon, geo_lat = float(pt.x), float(pt.y)
    key = str(gisjoin).strip().upper()
    if cenpop and key in cenpop:
        pop_lon, pop_lat = cenpop[key]
        return geo_lon, geo_lat, pop_lon, pop_lat
    return geo_lon, geo_lat, None, None


def _geom_to_wkb(geom: Any, tolerance: float) -> bytes:
    if tolerance > 0:
        geom = geom.simplify(tolerance, preserve_topology=True)
    return geom.wkb


def _boundary_row(
    level: str, row: dict[str, Any], vintage: int, resolution: str, geom: Any, tolerance: float
) -> dict[str, Any]:
    return {
        "geo_level": level,
        "geoid_system": gadm.GEOID_SYSTEM_CENSUS,
        "geoid": row["geoid"],
        "vintage": vintage,
        "resolution": resolution,
        "gisjoin": row["gisjoin"],
        "geometry_wkb": _geom_to_wkb(geom, tolerance),
    }


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
        geo_lon, geo_lat, pop_lon, pop_lat = _centroid_for(rec[gj], geom, cenpop)
        row = geo.build_state_row(
            rec[gj],
            vintage,
            centroid_geo_lon=geo_lon,
            centroid_geo_lat=geo_lat,
            centroid_pop_lon=pop_lon,
            centroid_pop_lat=pop_lat,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(_boundary_row("us_state", row, vintage, resolution, geom, tolerance))
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
        geo_lon, geo_lat, pop_lon, pop_lat = _centroid_for(rec[gj], geom, cenpop)
        name = str(rec[name_col]) if name_col else ""
        row = geo.build_county_row(
            rec[gj],
            vintage,
            name,
            centroid_geo_lon=geo_lon,
            centroid_geo_lat=geo_lat,
            centroid_pop_lon=pop_lon,
            centroid_pop_lat=pop_lat,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(_boundary_row("us_county", row, vintage, resolution, geom, tolerance))
    return rows, boundary


def _build_tract_frames(
    gdf: Any,
    vintage: int,
    tolerance: float,
    resolution: str,
    cenpop: dict[str, tuple[float, float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cols = list(gdf.columns)
    gj = _first_col(cols, ["GISJOIN", "gisjoin"])
    if gj is None:
        raise ValueError(f"tract shapefile has no GISJOIN column; columns={cols}")
    aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
    awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])

    rows: list[dict[str, Any]] = []
    boundary: list[dict[str, Any]] = []
    for _, rec in gdf.iterrows():
        geom = rec.geometry
        if geom is None or geom.is_empty:
            continue
        geo_lon, geo_lat, pop_lon, pop_lat = _centroid_for(rec[gj], geom, cenpop)
        row = geo.build_tract_row(
            rec[gj],
            vintage,
            centroid_geo_lon=geo_lon,
            centroid_geo_lat=geo_lat,
            centroid_pop_lon=pop_lon,
            centroid_pop_lat=pop_lat,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(_boundary_row("us_tract", row, vintage, resolution, geom, tolerance))
    return rows, boundary


def _build_zcta_frames(
    gdf: Any,
    vintage: int,
    tolerance: float,
    resolution: str,
    cenpop: dict[str, tuple[float, float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cols = list(gdf.columns)
    gj = _first_col(cols, ["GISJOIN", "gisjoin"])
    if gj is None:
        raise ValueError(f"zcta shapefile has no GISJOIN column; columns={cols}")
    aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
    awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])

    rows: list[dict[str, Any]] = []
    boundary: list[dict[str, Any]] = []
    for _, rec in gdf.iterrows():
        geom = rec.geometry
        if geom is None or geom.is_empty:
            continue
        geo_lon, geo_lat, _pop_lon, _pop_lat = _centroid_for(rec[gj], geom, cenpop)
        row = geo.build_zcta_row(
            rec[gj],
            vintage,
            centroid_geo_lon=geo_lon,
            centroid_geo_lat=geo_lat,
            area_land_sqm=_num(rec[aland]) if aland else None,
            area_water_sqm=_num(rec[awater]) if awater else None,
        )
        rows.append(row)
        boundary.append(_boundary_row("us_zcta", row, vintage, resolution, geom, tolerance))
    return rows, boundary


BUILDERS = {
    "us_state": _build_state_frames,
    "us_county": _build_county_frames,
    "us_tract": _build_tract_frames,
    "us_zcta": _build_zcta_frames,
}


def _check_unique(
    level: str,
    vintage: int,
    rows: list[dict[str, Any]],
    *,
    recorder: DQRecorder,
    table_name: str,
) -> None:
    """Record uniqueness check on ``geoid`` for this (level, vintage) chunk; raise on fail.

    Records to ``_ops.dq_results`` for both pass and fail outcomes so the
    audit trail captures green runs as well as red ones (ADR 0009).
    """
    seen: set[str] = set()
    dups: set[str] = set()
    for r in rows:
        if r["geoid"] in seen:
            dups.add(r["geoid"])
        seen.add(r["geoid"])
    passed = not dups
    sample = sorted(dups)[:10]
    recorder.record(
        table_name=table_name,
        check_name=f"{level}_geoid_uniqueness_{vintage}",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=passed,
        failing_row_count=len(dups),
        total_row_count=len(rows),
        details={"sample_duplicates": sample, "vintage": vintage} if dups else None,
    )
    if dups:
        raise ValueError(f"duplicate geoid in {level} (vintage {vintage}): {sample}")


def _check_fk(
    level: str,
    rows: list[dict[str, Any]],
    fk_col: str,
    parent_geoids: set[str],
    vintage: int,
    *,
    recorder: DQRecorder,
    table_name: str,
) -> None:
    """Record FK integrity check; raise on fail. Records both pass and fail (ADR 0009)."""
    missing = sorted({r[fk_col] for r in rows if r[fk_col] not in parent_geoids})
    passed = not missing
    recorder.record(
        table_name=table_name,
        check_name=f"{level}_fk_{fk_col}_{vintage}",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.FAIL,
        passed=passed,
        failing_row_count=len(missing),
        total_row_count=len(rows),
        details=(
            {"sample_missing": missing[:10], "fk_column": fk_col, "vintage": vintage}
            if missing
            else None
        ),
    )
    if missing:
        raise ValueError(
            f"{level} rows referencing missing {fk_col} (vintage {vintage}): {missing[:10]}"
        )


def _build_hhs_region(spark: SparkSession, catalog: str) -> None:
    rows = geo.generate_hhs_regions()
    df = spark.createDataFrame(rows, schema=HHS_REGION_SPARK_SCHEMA).sort("hhs_region")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.us_hhs_region"
    )
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.hhs_region IS "
        f"'The ten HHS regions (static federal grouping of states). Reference "
        f"table; full_refresh. ADR 0020.'"
    )
    log.info("Wrote us_hhs_region", extra={"rows": len(rows)})


def _write_chunk(
    spark: SparkSession,
    catalog: str,
    table: str,
    rows: list[dict[str, Any]],
    schema: T.StructType,
    written: set[str],
) -> None:
    """Write one (level, vintage) chunk. The first write to a table overwrites
    (full_refresh); later chunks append. Bounds driver memory at tract/ZCTA
    volume and avoids a single-file write for large tables.
    """
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=schema)
    mode = "overwrite" if table not in written else "append"
    writer = df.write.mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        # mergeSchema lets the append evolve a new column into an existing
        # table (e.g. adding geoid_system to the shared boundary table on the
        # first re-run — ADR 0023 review P1-6); no-op once the column exists.
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(f"{catalog}.{SCHEMA}.{table}")
    written.add(table)
    log.info("Wrote chunk", extra={"table": table, "rows": len(rows), "mode": mode})


def _set_clustering(spark: SparkSession, catalog: str) -> None:
    """Best-effort Liquid Clustering on the high-volume tables (ADR 0020).

    Applied after the data lands; non-fatal if the runtime doesn't support
    ALTER ... CLUSTER BY, since clustering is a read-pruning optimization.
    """
    targets = (("boundary", "geo_level, vintage"), ("us_tract", "vintage"), ("us_zcta", "vintage"))
    for table, cols in targets:
        try:
            spark.sql(f"ALTER TABLE {catalog}.{SCHEMA}.{table} CLUSTER BY ({cols})")
        except Exception as exc:  # pragma: no cover - runtime-dependent
            log.warning("Could not set clustering", extra={"table": table, "error": str(exc)})


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


def _comment_tables(spark: SparkSession, catalog: str) -> None:
    comments = {
        "us_state": "US states + DC and territories, vintaged. Source IPUMS NHGIS. ADR 0020.",
        "us_county": "US counties (geoid, vintage); state_geoid FK. Source IPUMS NHGIS. ADR 0020.",
        "us_tract": "US tracts (geoid, vintage); county + state FKs. Source IPUMS NHGIS. ADR 0020.",
        "us_zcta": "US ZCTAs (geoid, vintage); non-nesting. Source IPUMS NHGIS. ADR 0020.",
        "boundary": "Boundary polygons (WKB) by geo_level/vintage/resolution. ADR 0020.",
    }
    for table, text in comments.items():
        spark.sql(f"COMMENT ON TABLE {catalog}.{SCHEMA}.{table} IS '{text}'")


def _reset_us_boundaries(spark: SparkSession, catalog: str) -> None:
    """Delete only this build's geo_levels from the shared boundary table.

    ``geography.boundary`` is polymorphic — the country / country_subdivision
    builds also write to it (geo_level='country', 'country_subdivision', …).
    This build must refresh only its own US levels and append, NOT overwrite the
    whole table, or it silently wipes the GADM-sourced international rows. This
    matches the per-level full_refresh contract the GADM builds already follow
    (ADR 0023 review — fixes a latent boundary-overwrite landmine). No-op on a
    fresh catalog where the table doesn't exist yet.
    """
    levels = ", ".join(f"'{lvl}'" for lvl in LEVELS)
    try:
        spark.sql(f"DELETE FROM {catalog}.{SCHEMA}.boundary WHERE geo_level IN ({levels})")
        log.info("Reset US boundary rows", extra={"geo_levels": list(LEVELS)})
    except Exception as exc:  # noqa: BLE001 — table absent on first-ever run
        log.info("boundary table not present yet; nothing to delete", extra={"error": str(exc)})


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
        f"COMMENT 'Canonical US geography reference: states, counties, tracts, "
        f"ZCTAs, HHS regions, and companion boundaries. Owned by the _reference "
        f"bundle. Source: IPUMS NHGIS. See ADR 0020.'"
    )

    _build_hhs_region(spark, catalog)

    if not ipums_secret_scope:
        raise ValueError("--ipums-secret-scope is required to pull NHGIS shapefiles")
    api_key = _get_secret(ipums_secret_scope, ipums_secret_key or "nhgis_api_key")

    missing = [(lvl, v) for v in vintages for lvl in LEVELS if (lvl, v) not in SHAPEFILE_NAMES]
    if missing:
        raise ValueError(f"no known NHGIS shapefile code for {missing}; extend SHAPEFILE_NAMES")
    boundary_names = [SHAPEFILE_NAMES[(lvl, v)] for v in vintages for lvl in LEVELS]
    cenpop_names = [
        CENPOP_SHAPEFILE_NAMES[(lvl, v)]
        for v in vintages
        for lvl in LEVELS
        if (lvl, v) in CENPOP_SHAPEFILE_NAMES
    ]
    shapefile_names = boundary_names + cenpop_names

    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    workdir = Path(tempfile.mkdtemp(prefix="nhgis_"))
    _download_shapefiles(api_key, shapefile_names, workdir)
    _extract_all_zips(workdir)

    # Process and write per (level, vintage) chunk to bound driver memory. Levels
    # run parents-first so tract FK checks see already-loaded state/county geoids.
    # DQ outcomes (uniqueness + FK) are persisted to _ops.dq_results via the
    # recorder; flushed at context-manager exit even if a write raises (ADR 0009).
    written: set[str] = set()
    state_geoids: dict[int, set[str]] = {}
    county_geoids: dict[int, set[str]] = {}
    run_id = new_run_id()
    log.info("DQ run id assigned", extra={"run_id": run_id, "pipeline_reference": pipeline_ref})

    # boundary is shared/polymorphic: refresh only this build's geo_levels and
    # always append, so we never overwrite the country / country_subdivision
    # rows. Pre-marking it "written" forces _write_chunk into append mode from
    # the first chunk (ADR 0023 review — boundary-overwrite landmine fix).
    _reset_us_boundaries(spark, catalog)
    written.add("boundary")

    with DQRecorder(spark, catalog, run_id, pipeline_ref) as recorder:
        for lvl in LEVELS:
            for v in vintages:
                gdf = _read_gdf(_find_shapefile(workdir, lvl, v))
                cenpop = (
                    _read_cenpop_lookup(workdir, lvl, v)
                    if (lvl, v) in CENPOP_SHAPEFILE_NAMES
                    else {}
                )
                rows, boundary = BUILDERS[lvl](gdf, v, tolerance, resolution, cenpop)

                table_name = f"{SCHEMA}.{lvl}"
                _check_unique(lvl, v, rows, recorder=recorder, table_name=table_name)
                if lvl == "us_state":
                    state_geoids[v] = {r["geoid"] for r in rows}
                elif lvl == "us_county":
                    county_geoids[v] = {r["geoid"] for r in rows}
                    _check_fk(
                        "us_county",
                        rows,
                        "state_geoid",
                        state_geoids.get(v, set()),
                        v,
                        recorder=recorder,
                        table_name=table_name,
                    )
                elif lvl == "us_tract":
                    _check_fk(
                        "us_tract",
                        rows,
                        "state_geoid",
                        state_geoids.get(v, set()),
                        v,
                        recorder=recorder,
                        table_name=table_name,
                    )
                    _check_fk(
                        "us_tract",
                        rows,
                        "county_geoid",
                        county_geoids.get(v, set()),
                        v,
                        recorder=recorder,
                        table_name=table_name,
                    )

                _write_chunk(spark, catalog, lvl, rows, ENTITY_SCHEMAS[lvl], written)
                _write_chunk(spark, catalog, "boundary", boundary, gadm.boundary_spark_schema(), written)
                log.info(
                    "Processed",
                    extra={"level": lvl, "vintage": v, "rows": len(rows), "cenpop": len(cenpop)},
                )

    _comment_tables(spark, catalog)
    _set_clustering(spark, catalog)

    # Grants: reader-tier for both groups, same posture as time (ADR 0018/0020).
    grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
    grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)
    log.info("Access model verified", extra={"schema": f"{catalog}.{SCHEMA}"})

    _register_dataset(
        spark,
        catalog=catalog,
        table="us_state",
        description="US states and DC (plus territories), one row per state per vintage.",
        public_health_relevance=(
            "Canonical state spatial unit that surveillance and modeling data conform "
            "to; carries HHS region for federal regional rollups."
        ),
        spatial_resolution="us_state",
        cluster_columns=None,
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="us_county",
        description="US counties, one row per county per vintage, with state FK.",
        public_health_relevance=(
            "Canonical county spatial unit; the standard grain for U.S. infectious "
            "disease surveillance and the spatial backbone other subjects join to."
        ),
        spatial_resolution="us_county",
        cluster_columns=None,
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="us_tract",
        description="US census tracts, one row per tract per vintage, with county + state FKs.",
        public_health_relevance=(
            "Fine-grained spatial unit for neighborhood-level surveillance and "
            "modeling; redrawn each decade, so vintage matters."
        ),
        spatial_resolution="us_tract",
        cluster_columns=["vintage"],
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="us_zcta",
        description="US ZIP Code Tabulation Areas, one row per ZCTA per vintage (non-nesting).",
        public_health_relevance=(
            "Approximate ZIP-code geography for joining address- or ZIP-coded health "
            "data; non-nesting, so used directly rather than via county/state."
        ),
        spatial_resolution="us_zcta",
        cluster_columns=["vintage"],
        pipeline_reference=pipeline_ref,
    )
    _register_dataset(
        spark,
        catalog=catalog,
        table="us_hhs_region",
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
        description="Companion boundary polygons (WKB) for all levels by vintage.",
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
