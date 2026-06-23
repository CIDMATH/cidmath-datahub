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
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.dq import DQRecorder
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.reference_builder import (
    CanonicalOutput,
    LandingRetention,
    RawLanding,
    ReferenceBuildSpec,
    build_reference,
    make_staging_dq,
)
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography as geo

log = get_logger(__name__)

SCHEMA = "geography"
PIPELINE_REF = "bundles/_reference/src/build_geography.py"

# Levels still built by the LEGACY whole-geography build (run(), build_geography_reference).
# us_state + us_county + us_tract are migrated to the layered builder (build_geography_layered)
# and removed here; only us_zcta remains, after which this legacy build is retired.
# See docs/runbooks/geography-layered-cutover.md.
LEVELS = ("us_zcta",)

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


def _stage_volume_payload(volume_dir: str) -> Path:
    """Copy a landing Volume dir's verbatim files to a temp dir and unzip them (ADR 0039).

    The Volume holds the immutable as-fetched payload; reads work on a temp copy so the
    Volume is never mutated (no ``*_unz`` dirs written into it).
    """
    tmp = Path(tempfile.mkdtemp(prefix="nhgis_read_"))
    for item in Path(volume_dir).glob("*"):
        if item.is_file():
            shutil.copy(item, tmp / item.name)
    _extract_all_zips(tmp)
    return tmp


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
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.us_hhs_region IS "
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
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description=description,
            public_health_relevance=public_health_relevance,
            spatial_resolution=spatial_resolution,
            spatial_coverage="United States",
            source_provider_code="ipums_nhgis",
            source_url=NHGIS_SOURCE_URL,
            source_documentation_url=NHGIS_DOC_URL,
            license=NHGIS_LICENSE,
            dua_required=True,
            dua_reference=NHGIS_DUA_REFERENCE,
            access_tier="restricted",
            external_maintainer_name=NHGIS_MAINTAINER,
            is_hosted=True,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="full_refresh",
            materialization_type="table",
            cluster_columns=cluster_columns,
            pipeline_reference=pipeline_reference,
        ),
    )


def _comment_tables(spark: SparkSession, catalog: str) -> None:
    # us_state + us_county + us_tract are owned by the layered build now (cutover).
    comments = {
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
    log.info(
        "Building geography reference tables",
        extra={"catalog": catalog, "vintages": vintages, "full_resolution": full_resolution},
    )

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference: states, counties, tracts, "
            f"ZCTAs, HHS regions, and companion boundaries. Owned by the _reference "
            f"bundle. Source: IPUMS NHGIS. See ADR 0020.'"
        )

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark
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

        # Process and write per (level, vintage) chunk to bound driver memory.
        # Levels run parents-first so tract FK checks see already-loaded
        # state/county geoids. DQ outcomes (uniqueness + FK) are recorded via
        # ctx.recorder and flushed by the run_build seam even on failure (ADR 0009).
        written: set[str] = set()

        # boundary is shared/polymorphic: refresh only this build's geo_levels and
        # always append, so we never overwrite the country / country_subdivision
        # rows. Pre-marking it "written" forces _write_chunk into append mode from
        # the first chunk (ADR 0023 review -- boundary-overwrite landmine fix).
        _reset_us_boundaries(spark, catalog)
        written.add("boundary")

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
                _check_unique(lvl, v, rows, recorder=ctx.recorder, table_name=table_name)

                _write_chunk(spark, catalog, lvl, rows, ENTITY_SCHEMAS[lvl], written)
                _write_chunk(
                    spark, catalog, "boundary", boundary, gadm.boundary_spark_schema(), written
                )
                log.info(
                    "Processed",
                    extra={"level": lvl, "vintage": v, "rows": len(rows), "cenpop": len(cenpop)},
                )

    def _register(spark: SparkSession) -> None:
        _comment_tables(spark, catalog)
        _set_clustering(spark, catalog)
        # us_state + us_county + us_tract are registered by the layered build now (cutover).
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
            pipeline_reference=PIPELINE_REF,
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
            pipeline_reference=PIPELINE_REF,
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
            pipeline_reference=PIPELINE_REF,
        )

    def _grant(spark: SparkSession) -> None:
        # Grants: reader-tier for both groups, same posture as time (ADR 0018/0020).
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)
        log.info("Access model verified", extra={"schema": f"{catalog}.{SCHEMA}"})

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=_register,
        grant=_grant,
    )
    log.info("Geography reference build complete", extra={"catalog": catalog})


# ---------------------------------------------------------------------------
# Layered build on the shared reference builder (ADR 0036/0037) — us_state.
# ---------------------------------------------------------------------------
# Migration of us_state onto the raw → processed → canonical path via
# build_reference(). Raw is a strict 1:1 copy of each source FILE (the TIGER/NHGIS
# shapefile, and the CenPop file); the attribute/geometry split and the CenPop join
# happen in `processed`; two canonicals are promoted (us_state attributes,
# us_state_boundary geometry). State first proves the builder; county/tract/zcta and
# block-group/block follow (parents-first). The old per-level path above stays until
# the cutover removes each migrated level. Source-catalog tables carry the `census`
# token (ADR 0006 refinement); the model canonical stays source-agnostic.

# Raw = 1:1 source copies (stable column names across vintages; values untouched).
RAW_SHAPEFILE_SCHEMA = T.StructType(
    [
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("src_name", T.StringType(), True),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
        T.StructField("geometry_wkb", T.BinaryType(), False),  # full-resolution, as-is
    ]
)

RAW_CENPOP_SCHEMA = T.StructType(
    [
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("centroid_pop_lon", T.DoubleType(), False),
        T.StructField("centroid_pop_lat", T.DoubleType(), False),
    ]
)

# Per-level boundary (ADR 0036/0037 decision: one boundary table per level, vs the
# old polymorphic geography.boundary). geo_level is implied by the table name.
BOUNDARY_LEVEL_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("geoid_system", T.StringType(), False),
        T.StructField("resolution", T.StringType(), False),
        T.StructField("geometry_wkb", T.BinaryType(), False),
    ]
)


def _state_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw shapefile (+ cenpop join) → processed us_state attribute rows.

    Module-level (picklable for Spark). Reuses the pure ``geo.build_state_row`` so
    the entity logic is identical to the legacy build; the geographic centroid is the
    geometry's representative point, the population centroid comes from the cenpop
    left-join (null where absent).
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in STATE_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            pop_lon = None if pd.isna(r.centroid_pop_lon) else float(r.centroid_pop_lon)
            pop_lat = None if pd.isna(r.centroid_pop_lat) else float(r.centroid_pop_lat)
            out.append(
                geo.build_state_row(
                    r.gisjoin,
                    int(r.vintage),
                    centroid_geo_lon=float(pt.x),
                    centroid_geo_lat=float(pt.y),
                    centroid_pop_lon=pop_lon,
                    centroid_pop_lat=pop_lat,
                    area_land_sqm=None if pd.isna(r.area_land_sqm) else float(r.area_land_sqm),
                    area_water_sqm=None if pd.isna(r.area_water_sqm) else float(r.area_water_sqm),
                )
            )
        yield pd.DataFrame(out, columns=cols)


def _county_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw county shapefile (+ cenpop join) → lean processed county rows.

    Reuses the pure ``geo.build_county_row`` (county name from the shapefile; ``state_geoid``
    derived from the geoid). The state *labels* are joined on in ``process`` from the
    same-catalog parent ``us_census_state``, not here.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in COUNTY_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            pop_lon = None if pd.isna(r.centroid_pop_lon) else float(r.centroid_pop_lon)
            pop_lat = None if pd.isna(r.centroid_pop_lat) else float(r.centroid_pop_lat)
            out.append(
                geo.build_county_row(
                    r.gisjoin,
                    int(r.vintage),
                    "" if pd.isna(r.src_name) else str(r.src_name),
                    centroid_geo_lon=float(pt.x),
                    centroid_geo_lat=float(pt.y),
                    centroid_pop_lon=pop_lon,
                    centroid_pop_lat=pop_lat,
                    area_land_sqm=None if pd.isna(r.area_land_sqm) else float(r.area_land_sqm),
                    area_water_sqm=None if pd.isna(r.area_water_sqm) else float(r.area_water_sqm),
                )
            )
        yield pd.DataFrame(out, columns=cols)


def _tract_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw tract shapefile (+ cenpop join) → lean processed tract rows.

    Reuses the pure ``geo.build_tract_row`` (geoid + parent state_geoid/county_geoid derived
    from the GISJOIN; tracts have no name of their own). The county_name + state labels are
    joined on in ``process`` from the same-catalog parents, not here.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in TRACT_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            pop_lon = None if pd.isna(r.centroid_pop_lon) else float(r.centroid_pop_lon)
            pop_lat = None if pd.isna(r.centroid_pop_lat) else float(r.centroid_pop_lat)
            out.append(
                geo.build_tract_row(
                    r.gisjoin,
                    int(r.vintage),
                    centroid_geo_lon=float(pt.x),
                    centroid_geo_lat=float(pt.y),
                    centroid_pop_lon=pop_lon,
                    centroid_pop_lat=pop_lat,
                    area_land_sqm=None if pd.isna(r.area_land_sqm) else float(r.area_land_sqm),
                    area_water_sqm=None if pd.isna(r.area_water_sqm) else float(r.area_water_sqm),
                )
            )
        yield pd.DataFrame(out, columns=cols)


def _boundary_map(iterator: Any) -> Any:
    """mapInPandas: raw shapefile geometry → processed ``<level>_boundary`` rows (any level).

    Module-level (picklable). ``level_param`` (the bare gisjoin level — ``state`` /
    ``county`` / ``tract`` / …), ``res_param`` and ``tol_param`` ride in as columns added
    by the caller, avoiding closed-over driver state — names avoid a leading underscore,
    which ``DataFrame.itertuples`` would rename away.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in BOUNDARY_LEVEL_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            tol = float(r.tol_param)
            if tol > 0:
                geom = geom.simplify(tol, preserve_topology=True)
            out.append(
                {
                    "geoid": geo.gisjoin_to_geoid(r.gisjoin, str(r.level_param)),
                    "vintage": int(r.vintage),
                    "gisjoin": str(r.gisjoin),
                    "geoid_system": gadm.GEOID_SYSTEM_CENSUS,
                    "resolution": str(r.res_param),
                    "geometry_wkb": geom.wkb,
                }
            )
        yield pd.DataFrame(out, columns=cols)


def build_state_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    api_key: str,
    simplify_tolerance: float = 0.005,
    full_resolution: bool = False,
) -> tuple[str, str]:
    """Build us_state via the shared builder (ADR 0036) — the first layered adopter."""
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_state = f"{source_catalog}.geography_raw.us_census_state"
    raw_cenpop = f"{source_catalog}.geography_raw.us_census_state_cenpop"
    proc_state = f"{source_catalog}.geography_processed.us_census_state"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_state_boundary"

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_state} (gisjoin STRING, vintage INT, src_name STRING, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_state} (geoid STRING, vintage INT, gisjoin STRING, "
            f"name STRING, stusps STRING, hhs_region INT, centroid_geo_lon DOUBLE, "
            f"centroid_geo_lat DOUBLE, centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, gisjoin STRING, "
            f"geoid_system STRING, resolution STRING, geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_state (geoid STRING, "
            f"vintage INT, gisjoin STRING, name STRING, stusps STRING, hhs_region INT, "
            f"centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, centroid_pop_lon DOUBLE, "
            f"centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, area_water_sqm DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_state_boundary (geoid STRING, "
            f"vintage INT, gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    # ADR 0039: fetch the verbatim NHGIS extract into the landing Volume (per vintage,
    # immutable — the builder skips this when the vintage is already present), then read
    # the 1:1 raw frame from a temp copy of that payload.
    def _fetch_state_shapefile(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [SHAPEFILE_NAMES[("us_state", v)]], Path(vdir))

    def _read_state_shapefile(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        gdf = _read_gdf(_find_shapefile(staged, "us_state", v))
        cols = list(gdf.columns)
        gj = _first_col(cols, ["GISJOIN", "gisjoin"])
        name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
        aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
        awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
        if gj is None:
            raise ValueError(f"state shapefile has no GISJOIN column; columns={cols}")
        rows = [
            {
                "gisjoin": str(rec[gj]).strip().upper(),
                "vintage": int(v),
                "src_name": str(rec[name_col]) if name_col else None,
                "area_land_sqm": _num(rec[aland]) if aland else None,
                "area_water_sqm": _num(rec[awater]) if awater else None,
                "geometry_wkb": rec.geometry.wkb,
            }
            for _, rec in gdf.iterrows()
            if rec.geometry is not None and not rec.geometry.is_empty
        ]
        return ctx.spark.createDataFrame(rows, RAW_SHAPEFILE_SCHEMA)

    def _fetch_state_cenpop(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [CENPOP_SHAPEFILE_NAMES[("us_state", v)]], Path(vdir))

    def _read_state_cenpop(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        lookup = _read_cenpop_lookup(staged, "us_state", v)
        rows = [
            {"gisjoin": gj, "vintage": int(v), "centroid_pop_lon": lon, "centroid_pop_lat": lat}
            for gj, (lon, lat) in lookup.items()
        ]
        return ctx.spark.createDataFrame(rows, RAW_CENPOP_SCHEMA)

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(f"SELECT * FROM {raw_state} WHERE vintage = {int(v)}")
        cen = ctx.spark.sql(f"SELECT * FROM {raw_cenpop} WHERE vintage = {int(v)}")
        joined = raw.join(cen, ["gisjoin", "vintage"], "left")
        return joined.mapInPandas(_state_entity_map, schema=STATE_SPARK_SCHEMA)

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'state' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_state} "
            f"WHERE vintage = {int(v)}"
        )
        return raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_state} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.us_census_state")
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_state_pk_unique")
        dq.not_null(
            columns=["geoid", "name", "stusps", "hhs_region"],
            check_name="us_census_state_core_not_null",
        )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_state_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_state_boundary_pk_unique")
        dq.not_null(columns=["geometry_wkb"], check_name="us_census_state_boundary_geom_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US states, vintaged.",
        public_health_relevance="Canonical state spatial unit for surveillance/modeling rollups.",
        spatial_resolution="us_state",
        spatial_coverage="United States",
        source_provider_code="ipums_nhgis",
        source_origin_code="census",
        source_url=NHGIS_SOURCE_URL,
        source_documentation_url=NHGIS_DOC_URL,
        license=NHGIS_LICENSE,
        dua_required=True,
        dua_reference=NHGIS_DUA_REFERENCE,
        access_tier="restricted",
        external_maintainer_name=NHGIS_MAINTAINER,
        is_hosted=True,
    )

    spec = ReferenceBuildSpec(
        subject=SCHEMA,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=base_entry,
        raw_landings=[
            RawLanding(
                table="us_census_state",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_state_shapefile,
                read_from_volume=_read_state_shapefile,
                description="IPUMS NHGIS state boundary shapefile, as-is (attributes + geometry).",
            ),
            RawLanding(
                table="us_census_state_cenpop",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_state_cenpop,
                read_from_volume=_read_state_cenpop,
                description="Census Centers of Population for states, as-is.",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_state",
                reads=("us_census_state", "us_census_state_cenpop"),
                process=_process_entity,
                processed_table="us_census_state",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description="US states and DC (plus territories), one row per state per vintage.",
                public_health_relevance=(
                    "Canonical state spatial unit that surveillance and modeling data conform to; "
                    "carries HHS region for federal regional rollups."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_state_boundary",
                reads=("us_census_state",),
                process=_process_boundary,
                processed_table="us_census_state_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US state boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


def build_county_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    api_key: str,
    simplify_tolerance: float = 0.005,
    full_resolution: bool = False,
) -> tuple[str, str]:
    """Build us_county via the shared builder (ADR 0036) — parents-first, after us_state.

    The processed entity joins the same-catalog ``us_census_state`` for the denormalized
    state labels (ADR 0037 decision 7), so ``us_state`` must already be built. The enriched
    canonical ``us_county`` supersedes the ``us_county_enriched`` view (ADR 0028 retired).
    """
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_county = f"{source_catalog}.geography_raw.us_census_county"
    raw_cenpop = f"{source_catalog}.geography_raw.us_census_county_cenpop"
    proc_county = f"{source_catalog}.geography_processed.us_census_county"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_county_boundary"
    proc_state = f"{source_catalog}.geography_processed.us_census_state"  # parent (labels)

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_county} (gisjoin STRING, vintage INT, src_name STRING, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_county} (geoid STRING, vintage INT, state_geoid STRING, "
            f"gisjoin STRING, name STRING, centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, "
            f"area_water_sqm DOUBLE, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, gisjoin STRING, "
            f"geoid_system STRING, resolution STRING, geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_county (geoid STRING, "
            f"vintage INT, state_geoid STRING, gisjoin STRING, name STRING, centroid_geo_lon DOUBLE, "
            f"centroid_geo_lat DOUBLE, centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_county_boundary (geoid STRING, "
            f"vintage INT, gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    def _fetch_shapefile(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [SHAPEFILE_NAMES[("us_county", v)]], Path(vdir))

    def _read_shapefile(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        gdf = _read_gdf(_find_shapefile(staged, "us_county", v))
        cols = list(gdf.columns)
        gj = _first_col(cols, ["GISJOIN", "gisjoin"])
        name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
        aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
        awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
        if gj is None:
            raise ValueError(f"county shapefile has no GISJOIN column; columns={cols}")
        rows = [
            {
                "gisjoin": str(rec[gj]).strip().upper(),
                "vintage": int(v),
                "src_name": str(rec[name_col]) if name_col else None,
                "area_land_sqm": _num(rec[aland]) if aland else None,
                "area_water_sqm": _num(rec[awater]) if awater else None,
                "geometry_wkb": rec.geometry.wkb,
            }
            for _, rec in gdf.iterrows()
            if rec.geometry is not None and not rec.geometry.is_empty
        ]
        return ctx.spark.createDataFrame(rows, RAW_SHAPEFILE_SCHEMA)

    def _fetch_cenpop(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [CENPOP_SHAPEFILE_NAMES[("us_county", v)]], Path(vdir))

    def _read_cenpop(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        lookup = _read_cenpop_lookup(staged, "us_county", v)
        rows = [
            {"gisjoin": gj, "vintage": int(v), "centroid_pop_lon": lon, "centroid_pop_lat": lat}
            for gj, (lon, lat) in lookup.items()
        ]
        return ctx.spark.createDataFrame(rows, RAW_CENPOP_SCHEMA)

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(f"SELECT * FROM {raw_county} WHERE vintage = {int(v)}")
        cen = ctx.spark.sql(f"SELECT * FROM {raw_cenpop} WHERE vintage = {int(v)}")
        lean = raw.join(cen, ["gisjoin", "vintage"], "left").mapInPandas(
            _county_entity_map, schema=COUNTY_SPARK_SCHEMA
        )
        # Enrich with state labels from the same-catalog parent (parents-first; ADR 0037 #7).
        state = ctx.spark.sql(
            f"SELECT geoid AS state_geoid, name AS state_name, stusps AS state_stusps, "
            f"hhs_region AS state_hhs_region FROM {proc_state} WHERE vintage = {int(v)}"
        )
        return lean.join(state, ["state_geoid"], "left").select(
            "geoid", "vintage", "state_geoid", "gisjoin", "name",
            "centroid_geo_lon", "centroid_geo_lat", "centroid_pop_lon", "centroid_pop_lat",
            "area_land_sqm", "area_water_sqm", "state_name", "state_stusps", "state_hhs_region",
        )

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'county' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_county} "
            f"WHERE vintage = {int(v)}"
        )
        return raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_county} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.us_census_county")
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_county_pk_unique")
        dq.not_null(
            columns=["geoid", "state_geoid", "name"], check_name="us_census_county_core_not_null"
        )
        # Parent-FK within vintage: every county's state_geoid resolves in us_census_state.
        for v in vintages:
            make_staging_dq(
                ctx,
                staging_fqn,
                record_table="geography_processed.us_census_county",
                where=f"vintage = {int(v)}",
            ).fk(
                key="state_geoid",
                parent_table=proc_state,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_county_state_fk_{v}",
            )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_county_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_county_boundary_pk_unique")
        dq.not_null(columns=["geometry_wkb"], check_name="us_census_county_boundary_geom_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US counties, vintaged.",
        public_health_relevance="Canonical county spatial unit; the standard US surveillance grain.",
        spatial_resolution="us_county",
        spatial_coverage="United States",
        source_provider_code="ipums_nhgis",
        source_origin_code="census",
        source_url=NHGIS_SOURCE_URL,
        source_documentation_url=NHGIS_DOC_URL,
        license=NHGIS_LICENSE,
        dua_required=True,
        dua_reference=NHGIS_DUA_REFERENCE,
        access_tier="restricted",
        external_maintainer_name=NHGIS_MAINTAINER,
        is_hosted=True,
    )

    spec = ReferenceBuildSpec(
        subject=SCHEMA,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=base_entry,
        raw_landings=[
            RawLanding(
                table="us_census_county",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_shapefile,
                read_from_volume=_read_shapefile,
                description="IPUMS NHGIS county boundary shapefile, as-is (attributes + geometry).",
            ),
            RawLanding(
                table="us_census_county_cenpop",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_cenpop,
                read_from_volume=_read_cenpop,
                description="Census Centers of Population for counties, as-is.",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_county",
                reads=("us_census_county", "us_census_county_cenpop"),
                process=_process_entity,
                processed_table="us_census_county",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description="US counties, one row per county per vintage, enriched with state labels.",
                public_health_relevance=(
                    "Canonical county spatial unit; the standard grain for U.S. infectious-disease "
                    "surveillance and the spatial backbone other subjects join to."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_county_boundary",
                reads=("us_census_county",),
                process=_process_boundary,
                processed_table="us_census_county_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US county boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


def build_tract_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    vintages: list[int],
    data_engineers_group: str,
    analysts_group: str,
    api_key: str,
    simplify_tolerance: float = 0.005,
    full_resolution: bool = False,
) -> tuple[str, str]:
    """Build us_tract via the shared builder (ADR 0036) — parents-first, after us_county.

    The deepest cross-level case: ``process`` joins same-catalog ``us_census_county`` for
    ``county_name`` (not derivable from the GISJOIN) and ``us_census_state`` for the state
    labels (ADR 0037 decision 7), so both parents must already be built. The enriched
    canonical ``us_tract`` supersedes the ``us_tract_enriched`` view (ADR 0028 retired).
    """
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_tract = f"{source_catalog}.geography_raw.us_census_tract"
    raw_cenpop = f"{source_catalog}.geography_raw.us_census_tract_cenpop"
    proc_tract = f"{source_catalog}.geography_processed.us_census_tract"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_tract_boundary"
    proc_county = f"{source_catalog}.geography_processed.us_census_county"  # parent (county_name)
    proc_state = f"{source_catalog}.geography_processed.us_census_state"  # parent (state labels)

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_tract} (gisjoin STRING, vintage INT, src_name STRING, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_tract} (geoid STRING, vintage INT, state_geoid STRING, "
            f"county_geoid STRING, gisjoin STRING, centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, "
            f"area_water_sqm DOUBLE, county_name STRING, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, gisjoin STRING, "
            f"geoid_system STRING, resolution STRING, geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_tract (geoid STRING, "
            f"vintage INT, state_geoid STRING, county_geoid STRING, gisjoin STRING, "
            f"centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, centroid_pop_lon DOUBLE, "
            f"centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, area_water_sqm DOUBLE, "
            f"county_name STRING, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_tract_boundary (geoid STRING, "
            f"vintage INT, gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    def _fetch_shapefile(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [SHAPEFILE_NAMES[("us_tract", v)]], Path(vdir))

    def _read_shapefile(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        gdf = _read_gdf(_find_shapefile(staged, "us_tract", v))
        cols = list(gdf.columns)
        gj = _first_col(cols, ["GISJOIN", "gisjoin"])
        name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
        aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
        awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
        if gj is None:
            raise ValueError(f"tract shapefile has no GISJOIN column; columns={cols}")
        rows = [
            {
                "gisjoin": str(rec[gj]).strip().upper(),
                "vintage": int(v),
                "src_name": str(rec[name_col]) if name_col else None,
                "area_land_sqm": _num(rec[aland]) if aland else None,
                "area_water_sqm": _num(rec[awater]) if awater else None,
                "geometry_wkb": rec.geometry.wkb,
            }
            for _, rec in gdf.iterrows()
            if rec.geometry is not None and not rec.geometry.is_empty
        ]
        return ctx.spark.createDataFrame(rows, RAW_SHAPEFILE_SCHEMA)

    def _fetch_cenpop(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [CENPOP_SHAPEFILE_NAMES[("us_tract", v)]], Path(vdir))

    def _read_cenpop(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        lookup = _read_cenpop_lookup(staged, "us_tract", v)
        rows = [
            {"gisjoin": gj, "vintage": int(v), "centroid_pop_lon": lon, "centroid_pop_lat": lat}
            for gj, (lon, lat) in lookup.items()
        ]
        return ctx.spark.createDataFrame(rows, RAW_CENPOP_SCHEMA)

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(f"SELECT * FROM {raw_tract} WHERE vintage = {int(v)}")
        cen = ctx.spark.sql(f"SELECT * FROM {raw_cenpop} WHERE vintage = {int(v)}")
        lean = raw.join(cen, ["gisjoin", "vintage"], "left").mapInPandas(
            _tract_entity_map, schema=TRACT_SPARK_SCHEMA
        )
        # Enrich from same-catalog parents (parents-first; ADR 0037 #7): county_name from
        # county, state labels from state.
        county = ctx.spark.sql(
            f"SELECT geoid AS county_geoid, name AS county_name "
            f"FROM {proc_county} WHERE vintage = {int(v)}"
        )
        state = ctx.spark.sql(
            f"SELECT geoid AS state_geoid, name AS state_name, stusps AS state_stusps, "
            f"hhs_region AS state_hhs_region FROM {proc_state} WHERE vintage = {int(v)}"
        )
        return (
            lean.join(county, ["county_geoid"], "left")
            .join(state, ["state_geoid"], "left")
            .select(
                "geoid", "vintage", "state_geoid", "county_geoid", "gisjoin",
                "centroid_geo_lon", "centroid_geo_lat", "centroid_pop_lon", "centroid_pop_lat",
                "area_land_sqm", "area_water_sqm", "county_name", "state_name", "state_stusps",
                "state_hhs_region",
            )
        )

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'tract' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_tract} "
            f"WHERE vintage = {int(v)}"
        )
        return raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_tract} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.us_census_tract")
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_tract_pk_unique")
        dq.not_null(
            columns=["geoid", "state_geoid", "county_geoid"],
            check_name="us_census_tract_core_not_null",
        )
        # Parent-FK within vintage: tract.county_geoid -> county, tract.state_geoid -> state.
        for v in vintages:
            make_staging_dq(
                ctx, staging_fqn, record_table="geography_processed.us_census_tract",
                where=f"vintage = {int(v)}",
            ).fk(
                key="county_geoid", parent_table=proc_county, parent_key="geoid",
                parent_where=f"vintage = {int(v)}", check_name=f"us_census_tract_county_fk_{v}",
            )
            make_staging_dq(
                ctx, staging_fqn, record_table="geography_processed.us_census_tract",
                where=f"vintage = {int(v)}",
            ).fk(
                key="state_geoid", parent_table=proc_state, parent_key="geoid",
                parent_where=f"vintage = {int(v)}", check_name=f"us_census_tract_state_fk_{v}",
            )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_tract_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_tract_boundary_pk_unique")
        dq.not_null(columns=["geometry_wkb"], check_name="us_census_tract_boundary_geom_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US census tracts, vintaged.",
        public_health_relevance="Fine-grained spatial unit for neighborhood-level surveillance.",
        spatial_resolution="us_tract",
        spatial_coverage="United States",
        source_provider_code="ipums_nhgis",
        source_origin_code="census",
        source_url=NHGIS_SOURCE_URL,
        source_documentation_url=NHGIS_DOC_URL,
        license=NHGIS_LICENSE,
        dua_required=True,
        dua_reference=NHGIS_DUA_REFERENCE,
        access_tier="restricted",
        external_maintainer_name=NHGIS_MAINTAINER,
        is_hosted=True,
    )

    spec = ReferenceBuildSpec(
        subject=SCHEMA,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=base_entry,
        raw_landings=[
            RawLanding(
                table="us_census_tract",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_shapefile,
                read_from_volume=_read_shapefile,
                description="IPUMS NHGIS tract boundary shapefile, as-is (attributes + geometry).",
            ),
            RawLanding(
                table="us_census_tract_cenpop",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_cenpop,
                read_from_volume=_read_cenpop,
                description="Census Centers of Population for tracts, as-is.",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_tract",
                reads=("us_census_tract", "us_census_tract_cenpop"),
                process=_process_entity,
                processed_table="us_census_tract",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description="US census tracts per vintage, enriched with county + state labels.",
                public_health_relevance=(
                    "Fine-grained spatial unit for neighborhood-level surveillance and modeling; "
                    "redrawn each decade, so vintage matters."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_tract_boundary",
                reads=("us_census_tract",),
                process=_process_boundary,
                processed_table="us_census_tract_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US tract boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


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
    parser.add_argument(
        "--source-catalog",
        default=None,
        help="Source catalog for raw/processed (ecdh_<env>). Defaults to --catalog with the "
        "'model_' segment removed (ecdh_model_dev -> ecdh_dev). Used only with --layered-state.",
    )
    parser.add_argument(
        "--layered-state",
        action="store_true",
        help="Build us_state only via the shared reference builder (ADR 0036), on the layered "
        "raw/processed/canonical path, instead of the legacy whole-geography build.",
    )
    parser.add_argument(
        "--layered",
        action="store_true",
        help="Build the layered geography chain parents-first (us_state -> us_county; more "
        "levels as migrated) in ONE process. Dev convenience; production uses --level + the job DAG.",
    )
    parser.add_argument(
        "--level",
        choices=["us_state", "us_county", "us_tract"],
        default=None,
        help="Build a single geography level via the shared builder (ADR 0036). Its parent levels "
        "must already be built (their processed tables are joined for enrichment). This is the "
        "per-level entry the job DAG calls, one task per level, ordered by depends_on.",
    )
    args = parser.parse_args()

    vintages = [int(v) for v in args.vintages.split(",") if v.strip()]

    if args.level or args.layered or args.layered_state:
        if not args.ipums_secret_scope:
            raise ValueError("--ipums-secret-scope is required to pull NHGIS shapefiles")
        source_catalog = args.source_catalog or args.catalog.replace("ecdh_model_", "ecdh_")
        api_key = _get_secret(args.ipums_secret_scope, args.ipums_secret_key)
        level_kwargs = {
            "source_catalog": source_catalog,
            "model_catalog": args.catalog,
            "vintages": vintages,
            "data_engineers_group": args.data_engineers_group,
            "analysts_group": args.analysts_group,
            "api_key": api_key,
            "simplify_tolerance": args.simplify_tolerance,
            "full_resolution": args.full_resolution,
        }
        # One build function per level; the job DAG (depends_on) enforces parents-first.
        builders = {
            "us_state": build_state_layered,
            "us_county": build_county_layered,
            "us_tract": build_tract_layered,
        }
        if args.level:
            builders[args.level](**level_kwargs)
        elif args.layered:  # whole chain in one process (dev convenience), parents-first
            build_state_layered(**level_kwargs)
            build_county_layered(**level_kwargs)
            build_tract_layered(**level_kwargs)
        else:  # --layered-state
            build_state_layered(**level_kwargs)
        return

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
