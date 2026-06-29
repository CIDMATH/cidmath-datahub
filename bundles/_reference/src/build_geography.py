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

from cidmath_datahub.common import registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext
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

# The whole geography subject (us_hhs_region + us_state/county/tract/zcta/block_group/block) is
# built on the shared layered builder (build_*_layered, job build_geography_layered; ADR
# 0036/0037/0039). The legacy monolithic run() and its build_geography_reference job are retired;
# us_hhs_region is the builder's static (non-vintaged) shape. See
# docs/runbooks/geography-layered-cutover.md.

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
    # Block groups: NHGIS national files (verified in the shapefile metadata catalog). NHGIS
    # abbreviates the level as "blck_grp" in filenames (handled by _NHGIS_FILE_TOKEN). NB:
    # blocks are per-state only (no us_block_* national file) -- the block level will differ.
    ("us_block_group", 2010): "us_blck_grp_2010_tl2010",
    ("us_block_group", 2020): "us_blck_grp_2020_tl2020",
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
    ("us_block_group", 2010): "us_blck_grp_cenpop_2010_cenpop2010",
    ("us_block_group", 2020): "us_blck_grp_cenpop_2020_cenpop2020",
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

# Lean block group (mapInPandas output): tract's shape + the parent tract_geoid (12-digit
# geoid nests in an 11-digit tract). county_name + state labels are joined in `process`.
BLOCK_GROUP_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state_geoid", T.StringType(), False),
        T.StructField("county_geoid", T.StringType(), False),
        T.StructField("tract_geoid", T.StringType(), False),
        T.StructField("gisjoin", T.StringType(), False),
        T.StructField("centroid_geo_lon", T.DoubleType(), False),
        T.StructField("centroid_geo_lat", T.DoubleType(), False),
        T.StructField("centroid_pop_lon", T.DoubleType(), True),
        T.StructField("centroid_pop_lat", T.DoubleType(), True),
        T.StructField("area_land_sqm", T.DoubleType(), True),
        T.StructField("area_water_sqm", T.DoubleType(), True),
    ]
)

# Lean block (mapInPandas output): block group's shape + block_group_geoid; no pop centroid
# (blocks are the atomic unit, Census publishes no block Center of Population).
BLOCK_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state_geoid", T.StringType(), False),
        T.StructField("county_geoid", T.StringType(), False),
        T.StructField("tract_geoid", T.StringType(), False),
        T.StructField("block_group_geoid", T.StringType(), False),
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
    "us_block_group": BLOCK_GROUP_SPARK_SCHEMA,
    "us_block": BLOCK_SPARK_SCHEMA,
}

# NHGIS abbreviates some level names in shapefile filenames (us_block_group -> us_blck_grp);
# _find_shapefile matches on this token. Levels not listed derive the token from the name.
_NHGIS_FILE_TOKEN: dict[str, str] = {"us_block_group": "blck_grp"}

# Census blocks are NOT distributed as a national NHGIS file — only per-state (e.g.
# "010_block_2020_tl2020" for Alabama). State code = 2-digit FIPS + "0". 50 states + DC + PR.
_BLOCK_STATE_FIPS: tuple[str, ...] = (
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "72",
)


def _block_shapefile_names(vintage: int) -> list[str]:
    """NHGIS per-state block shapefile names for a vintage (one per state, ``NN0_block_…``)."""
    y = int(vintage)
    return [f"{fips}0_block_{y}_tl{y}" for fips in _BLOCK_STATE_FIPS]


# --- ZCTA↔county relationship files (Census, provider=census; a 2nd source in this build) ---
# ZCTAs do not nest in counties, so there is no parent GISJOIN. We approximate a parent by
# the county of largest land-area overlap, from Census's published ZCTA-to-county
# relationship files (land-area overlap per ZCTA×county part). Format differs by vintage
# (2010 comma-delimited vs 2020 pipe-delimited; different column names) so the reader sniffs
# the delimiter and resolves columns alias-tolerantly, failing loud on an unmatched column.
# See docs/reviews + the dataset_catalog source_documentation_url.
ZCTA_COUNTY_REL_URLS: dict[int, str] = {
    2010: "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt",
    2020: (
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
        "zcta520/tab20_zcta520_county20_natl.txt"
    ),
}
ZCTA_COUNTY_REL_DOC_URL = (
    "https://www.census.gov/programs-surveys/geography/technical-documentation/"
    "records-layout/2020-zcta-record-layout.html"
)
# Canonical field -> accepted source header names across the 2010 / 2020 layouts.
_REL_ZCTA5_ALIASES = ["GEOID_ZCTA5_20", "GEOID_ZCTA5_10", "ZCTA5", "ZCTA5CE10", "ZCTA5CE20"]
_REL_COUNTY_GEOID_ALIASES = ["GEOID_COUNTY_20", "GEOID_COUNTY_10", "GEOID", "COUNTY_GEOID"]
_REL_AREALAND_PART_ALIASES = ["AREALAND_PART", "AREALANDPT"]
_REL_ZCTA_AREALAND_ALIASES = ["AREALAND_ZCTA5_20", "AREALAND_ZCTA5_10", "ZAREALAND"]

ZCTA_REL_RAW_SCHEMA = T.StructType(
    [
        T.StructField("zcta5", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("county_geoid", T.StringType(), False),
        T.StructField("area_land_part_sqm", T.DoubleType(), True),
        T.StructField("zcta_area_land_sqm", T.DoubleType(), True),
    ]
)

# Enriched us_zcta = lean ZCTA + approximate primary county (largest land overlap) + that
# county's state labels + overlap diagnostics. Preserves 1 row per ZCTA (granularity).
ZCTA_ENRICHED_COLS = (
    "geoid STRING, vintage INT, gisjoin STRING, centroid_geo_lon DOUBLE, "
    "centroid_geo_lat DOUBLE, area_land_sqm DOUBLE, area_water_sqm DOUBLE, "
    "primary_county_geoid STRING, primary_county_name STRING, state_geoid STRING, "
    "state_name STRING, state_stusps STRING, state_hhs_region INT, "
    "primary_county_overlap_land_sqm DOUBLE, primary_county_overlap_fraction DOUBLE, "
    "county_overlap_count INT, spans_multiple_counties BOOLEAN"
)
# Final projection order for the enriched entity (must match ZCTA_ENRICHED_COLS).
_ZCTA_ENRICHED_SELECT = [
    "geoid",
    "vintage",
    "gisjoin",
    "centroid_geo_lon",
    "centroid_geo_lat",
    "area_land_sqm",
    "area_water_sqm",
    "primary_county_geoid",
    "primary_county_name",
    "state_geoid",
    "state_name",
    "state_stusps",
    "state_hhs_region",
    "primary_county_overlap_land_sqm",
    "primary_county_overlap_fraction",
    "county_overlap_count",
    "spans_multiple_counties",
]
# Full "any overlap" crosswalk (does not preserve 1:1) — every ZCTA×county overlap part.
ZCTA_XWALK_COLS = (
    "geoid STRING, vintage INT, county_geoid STRING, county_name STRING, "
    "state_geoid STRING, overlap_land_sqm DOUBLE, overlap_fraction DOUBLE, "
    "is_primary BOOLEAN"
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


def _download_census_rel_file(url: str, workdir: Path) -> None:
    """Download a Census relationship file (.txt) verbatim into the landing dir (ADR 0039).

    NOTE (ops): the job environment's network allowlist must include ``www2.census.gov``
    (separate from the NHGIS API host). Without it this raises at fetch time — loud, not
    silent. The payload is stored as-is; parsing happens on a temp copy at read time.
    """
    import urllib.request

    dest = workdir / url.rsplit("/", 1)[-1]
    workdir.mkdir(parents=True, exist_ok=True)
    log.info("Downloading Census relationship file", extra={"url": url, "dest": str(dest)})
    urllib.request.urlretrieve(url, dest)  # noqa: S310 - fixed census.gov URL, not user input


def _read_zcta_county_rel(ctx: BuildContext, v: int, vdir: str) -> Any:
    """Parse a ZCTA↔county relationship file into the 1:1 raw frame (ZCTA_REL_RAW_SCHEMA).

    Delimiter is sniffed (2010 comma vs 2020 pipe) and columns are resolved alias-tolerantly
    across the 2010/2020 layouts; an unmatched required column raises (fail loud, do not
    guess). Keeps only the fields the crosswalk needs; the Volume holds the verbatim file.
    """
    import csv

    staged = _stage_volume_payload(vdir)
    txts = [p for p in staged.glob("*") if p.suffix.lower() in (".txt", ".csv")]
    if not txts:
        raise FileNotFoundError(f"no ZCTA-county relationship .txt in {vdir}")
    path = txts[0]
    with open(path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(8192)
        fh.seek(0)
        delimiter = "|" if sample.count("|") > sample.count(",") else ","
        reader = csv.DictReader(fh, delimiter=delimiter)
        header = reader.fieldnames or []
        zc = _first_col(header, _REL_ZCTA5_ALIASES)
        cc = _first_col(header, _REL_COUNTY_GEOID_ALIASES)
        ap = _first_col(header, _REL_AREALAND_PART_ALIASES)
        zl = _first_col(header, _REL_ZCTA_AREALAND_ALIASES)
        missing = [
            name
            for name, col in [("zcta5", zc), ("county_geoid", cc), ("area_land_part", ap)]
            if col is None
        ]
        if missing:
            raise ValueError(
                f"ZCTA-county rel {path.name} missing required columns {missing}; header={header}"
            )
        rows: list[dict[str, Any]] = []
        for rec in reader:
            z = str(rec[zc]).strip()
            c = str(rec[cc]).strip()
            if not z or not c:
                continue
            rows.append(
                {
                    "zcta5": z.zfill(5),
                    "vintage": int(v),
                    "county_geoid": c.zfill(5),
                    "area_land_part_sqm": _num(rec[ap]),
                    "zcta_area_land_sqm": _num(rec[zl]) if zl else None,
                }
            )
    log.info("Parsed ZCTA-county relationship file", extra={"vintage": v, "rows": len(rows)})
    return ctx.spark.createDataFrame(rows, ZCTA_REL_RAW_SCHEMA)


def _find_shapefile(root: Path, level: str, vintage: int, *, cenpop: bool = False) -> Path | None:
    """Locate the .shp for a level/vintage.

    Centers-of-Population files carry 'cenpop' in their path; boundary files do
    not. The leading ``us_<level>_`` token disambiguates levels whose names are
    substrings of others (e.g. county vs cty_sub). Returns None for a missing
    cenpop file (optional); raises for a missing boundary file.
    """
    # The level identifier carries a us_ prefix (ADR 0006 refinement) but NHGIS
    # shapefile filenames only have one us_ — strip ours before composing the
    # match token. NHGIS also abbreviates some levels (block_group -> blck_grp),
    # so prefer the explicit override where one exists.
    bare_level = _NHGIS_FILE_TOKEN.get(level.lower(), level.lower().removeprefix("us_"))
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


def _geom_to_wkb(geom: Any, tolerance: float) -> bytes:
    if tolerance > 0:
        geom = geom.simplify(tolerance, preserve_topology=True)
    return geom.wkb


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


def _block_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw block shapefile → lean processed block rows.

    Reuses the pure ``geo.build_block_row`` (15-digit geoid + parent block_group/tract/county/
    state geoids derived from the GISJOIN; no Center of Population for blocks). county_name +
    state labels are joined on in ``process`` from the same-catalog parents, not here.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in BLOCK_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            out.append(
                geo.build_block_row(
                    r.gisjoin,
                    int(r.vintage),
                    centroid_geo_lon=float(pt.x),
                    centroid_geo_lat=float(pt.y),
                    area_land_sqm=None if pd.isna(r.area_land_sqm) else float(r.area_land_sqm),
                    area_water_sqm=None if pd.isna(r.area_water_sqm) else float(r.area_water_sqm),
                )
            )
        yield pd.DataFrame(out, columns=cols)


def _block_group_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw block-group shapefile (+ cenpop join) → lean processed BG rows.

    Reuses the pure ``geo.build_block_group_row`` (12-digit geoid + parent state/county/tract
    geoids derived from the GISJOIN). county_name + state labels are joined on in ``process``
    from the same-catalog parents, not here.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in BLOCK_GROUP_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            pop_lon = None if pd.isna(r.centroid_pop_lon) else float(r.centroid_pop_lon)
            pop_lat = None if pd.isna(r.centroid_pop_lat) else float(r.centroid_pop_lat)
            out.append(
                geo.build_block_group_row(
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


def _zcta_entity_map(iterator: Any) -> Any:
    """mapInPandas: raw ZCTA shapefile → lean processed us_zcta rows.

    Reuses the pure ``geo.build_zcta_row`` (geoid from the GISJOIN; no parent geoids — ZCTAs
    do not nest — and no population centroid, as Census publishes no ZCTA Center of
    Population). The approximate primary-county + state labels are joined on in ``process``
    from the same-catalog parents + the Census ZCTA↔county relationship file, not here.
    """
    import pandas as pd
    from shapely import wkb as _wkb

    cols = [f.name for f in ZCTA_SPARK_SCHEMA.fields]
    for pdf in iterator:
        out: list[dict[str, Any]] = []
        for r in pdf.itertuples(index=False):
            geom = _wkb.loads(bytes(r.geometry_wkb))
            pt = geom.representative_point()
            out.append(
                geo.build_zcta_row(
                    r.gisjoin,
                    int(r.vintage),
                    centroid_geo_lon=float(pt.x),
                    centroid_geo_lat=float(pt.y),
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
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_state} (gisjoin STRING, vintage INT, "
            f"src_name STRING, area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) "
            f"USING DELTA"
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
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
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
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_county} (gisjoin STRING, vintage INT, "
            f"src_name STRING, area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_county} (geoid STRING, vintage INT, "
            f"state_geoid STRING, gisjoin STRING, name STRING, centroid_geo_lon DOUBLE, "
            f"centroid_geo_lat DOUBLE, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, "
            f"area_water_sqm DOUBLE, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_county (geoid STRING, "
            f"vintage INT, state_geoid STRING, gisjoin STRING, name STRING, "
            f"centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, centroid_pop_lon DOUBLE, "
            f"centroid_pop_lat DOUBLE, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_county_boundary "
            f"(geoid STRING, vintage INT, gisjoin STRING, geoid_system STRING, resolution STRING, "
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
            "geoid",
            "vintage",
            "state_geoid",
            "gisjoin",
            "name",
            "centroid_geo_lon",
            "centroid_geo_lat",
            "centroid_pop_lon",
            "centroid_pop_lat",
            "area_land_sqm",
            "area_water_sqm",
            "state_name",
            "state_stusps",
            "state_hhs_region",
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
        public_health_relevance=(
            "Canonical county spatial unit; the standard US surveillance grain."
        ),
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
                description=(
                    "US counties, one row per county per vintage, enriched with state labels."
                ),
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
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_tract} (gisjoin STRING, vintage INT, "
            f"src_name STRING, area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_tract} (geoid STRING, vintage INT, "
            f"state_geoid STRING, county_geoid STRING, gisjoin STRING, centroid_geo_lon DOUBLE, "
            f"centroid_geo_lat DOUBLE, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, "
            f"area_water_sqm DOUBLE, county_name STRING, state_name STRING, state_stusps STRING, "
            f"state_hhs_region INT) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
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
                "geoid",
                "vintage",
                "state_geoid",
                "county_geoid",
                "gisjoin",
                "centroid_geo_lon",
                "centroid_geo_lat",
                "centroid_pop_lon",
                "centroid_pop_lat",
                "area_land_sqm",
                "area_water_sqm",
                "county_name",
                "state_name",
                "state_stusps",
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
                ctx,
                staging_fqn,
                record_table="geography_processed.us_census_tract",
                where=f"vintage = {int(v)}",
            ).fk(
                key="county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_tract_county_fk_{v}",
            )
            make_staging_dq(
                ctx,
                staging_fqn,
                record_table="geography_processed.us_census_tract",
                where=f"vintage = {int(v)}",
            ).fk(
                key="state_geoid",
                parent_table=proc_state,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_tract_state_fk_{v}",
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


def build_zcta_layered(
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
    """Build us_zcta via the shared builder (ADR 0036) — non-nesting, after us_county.

    ZCTAs do not nest in counties/states, so there is no parent GISJOIN. We approximate a
    parent by the county of **largest land-area overlap**, from Census's published ZCTA↔county
    relationship file (a 2nd source, provider=census, alongside the NHGIS shapefile). The
    enriched ``us_zcta`` keeps one row per ZCTA with that primary county + its state labels +
    overlap diagnostics (``spans_multiple_counties``); a separate ``us_zcta_county_xwalk``
    preserves **every** ZCTA×county overlap (any overlap, not 1:1) for spatial allocation.
    Parents-first: ``process`` joins same-catalog ``us_census_county`` + ``us_census_state``.
    """
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_zcta = f"{source_catalog}.geography_raw.us_census_zcta"
    raw_rel = f"{source_catalog}.geography_raw.us_census_zcta_county_rel"
    proc_zcta = f"{source_catalog}.geography_processed.us_census_zcta"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_zcta_boundary"
    proc_xwalk = f"{source_catalog}.geography_processed.us_census_zcta_county_xwalk"
    proc_county = f"{source_catalog}.geography_processed.us_census_county"  # parent
    proc_state = f"{source_catalog}.geography_processed.us_census_state"  # parent (state labels)

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_zcta} (gisjoin STRING, vintage INT, src_name STRING, "
            f"area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_rel} (zcta5 STRING, vintage INT, "
            f"county_geoid STRING, area_land_part_sqm DOUBLE, zcta_area_land_sqm DOUBLE) "
            f"USING DELTA"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_zcta} ({ZCTA_ENRICHED_COLS}) USING DELTA")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_xwalk} ({ZCTA_XWALK_COLS}) USING DELTA")

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_zcta ({ZCTA_ENRICHED_COLS}) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_zcta_boundary (geoid STRING, "
            f"vintage INT, gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_zcta_county_xwalk "
            f"({ZCTA_XWALK_COLS}) USING DELTA"
        )

    def _fetch_zcta_shapefile(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [SHAPEFILE_NAMES[("us_zcta", v)]], Path(vdir))

    def _read_zcta_shapefile(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        gdf = _read_gdf(_find_shapefile(staged, "us_zcta", v))
        cols = list(gdf.columns)
        gj = _first_col(cols, ["GISJOIN", "gisjoin"])
        name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
        aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
        awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
        if gj is None:
            raise ValueError(f"zcta shapefile has no GISJOIN column; columns={cols}")
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

    def _fetch_zcta_rel(v: int, vdir: str) -> None:
        if v not in ZCTA_COUNTY_REL_URLS:
            raise ValueError(f"no ZCTA↔county relationship file URL configured for vintage {v}")
        _download_census_rel_file(ZCTA_COUNTY_REL_URLS[v], Path(vdir))

    def _rel_primary_and_counts(ctx: BuildContext, v: int) -> tuple[Any, Any]:
        """(primary-county-per-ZCTA, distinct-county-count-per-ZCTA) from the rel raw."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        rel = ctx.spark.sql(
            f"SELECT zcta5 AS geoid, county_geoid, area_land_part_sqm, zcta_area_land_sqm "
            f"FROM {raw_rel} WHERE vintage = {int(v)}"
        )
        counts = rel.groupBy("geoid").agg(
            # countDistinct returns BIGINT; the DDL declares county_overlap_count INT, and the
            # per-vintage write won't silently widen — cast to match (counts are tiny).
            F.countDistinct("county_geoid").cast("int").alias("county_overlap_count")
        )
        w = Window.partitionBy("geoid").orderBy(
            F.col("area_land_part_sqm").desc_nulls_last(), F.col("county_geoid")
        )
        primary = (
            rel.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .select(
                "geoid",
                F.col("county_geoid").alias("primary_county_geoid"),
                F.col("area_land_part_sqm").alias("primary_county_overlap_land_sqm"),
                F.when(
                    F.col("zcta_area_land_sqm") > 0,
                    F.col("area_land_part_sqm") / F.col("zcta_area_land_sqm"),
                ).alias("primary_county_overlap_fraction"),
            )
        )
        return primary, counts

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        from pyspark.sql import functions as F

        lean = ctx.spark.sql(f"SELECT * FROM {raw_zcta} WHERE vintage = {int(v)}").mapInPandas(
            _zcta_entity_map, schema=ZCTA_SPARK_SCHEMA
        )
        primary, counts = _rel_primary_and_counts(ctx, v)
        county = ctx.spark.sql(
            f"SELECT geoid AS primary_county_geoid, name AS primary_county_name, state_geoid "
            f"FROM {proc_county} WHERE vintage = {int(v)}"
        )
        state = ctx.spark.sql(
            f"SELECT geoid AS state_geoid, name AS state_name, stusps AS state_stusps, "
            f"hhs_region AS state_hhs_region FROM {proc_state} WHERE vintage = {int(v)}"
        )
        return (
            lean.join(primary, ["geoid"], "left")
            .join(counts, ["geoid"], "left")
            .join(county, ["primary_county_geoid"], "left")
            .join(state, ["state_geoid"], "left")
            .withColumn(
                "spans_multiple_counties",
                F.coalesce(F.col("county_overlap_count") > 1, F.lit(False)),
            )
            .select(*_ZCTA_ENRICHED_SELECT)
        )

    def _process_xwalk(ctx: BuildContext, v: int) -> Any:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        rel = ctx.spark.sql(
            f"SELECT zcta5 AS geoid, vintage, county_geoid, area_land_part_sqm, zcta_area_land_sqm "
            f"FROM {raw_rel} WHERE vintage = {int(v)}"
        )
        county = ctx.spark.sql(
            f"SELECT geoid AS county_geoid, name AS county_name, state_geoid "
            f"FROM {proc_county} WHERE vintage = {int(v)}"
        )
        w = Window.partitionBy("geoid").orderBy(
            F.col("area_land_part_sqm").desc_nulls_last(), F.col("county_geoid")
        )
        return (
            rel.withColumn(
                "overlap_fraction",
                F.when(
                    F.col("zcta_area_land_sqm") > 0,
                    F.col("area_land_part_sqm") / F.col("zcta_area_land_sqm"),
                ),
            )
            .withColumn("is_primary", F.row_number().over(w) == 1)
            # INNER join scopes the xwalk to our county universe (50 states + DC + PR). The
            # Census rel file also covers the Island Areas (AS/GU/MP/VI) that NHGIS's
            # us_county/us_zcta exclude; those overlaps are dropped here and recorded as a
            # non-blocking accepted gap in _validate_xwalk (cf. GADM subnational_rows_dropped).
            .join(county, ["county_geoid"], "inner")
            .select(
                "geoid",
                "vintage",
                "county_geoid",
                "county_name",
                "state_geoid",
                F.col("area_land_part_sqm").alias("overlap_land_sqm"),
                "overlap_fraction",
                "is_primary",
            )
        )

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'zcta' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_zcta} "
            f"WHERE vintage = {int(v)}"
        )
        return raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_zcta} WHERE vintage = {int(v)}")

    def _promote_xwalk(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_xwalk} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.us_census_zcta")
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_zcta_pk_unique")
        dq.not_null(columns=["geoid"], check_name="us_census_zcta_geoid_not_null")
        # Approximate-parent FK (non-null only — a few all-water ZCTAs may have no overlap):
        # primary_county_geoid -> county, within vintage.
        for v in vintages:
            make_staging_dq(
                ctx,
                staging_fqn,
                record_table="geography_processed.us_census_zcta",
                where=f"vintage = {int(v)} AND primary_county_geoid IS NOT NULL",
            ).fk(
                key="primary_county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_zcta_primary_county_fk_{v}",
            )

    def _validate_xwalk(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_zcta_county_xwalk"
        )
        dq.unique(
            keys=["geoid", "county_geoid", "vintage"],
            check_name="us_census_zcta_county_xwalk_pk_unique",
        )
        dq.not_null(
            columns=["geoid", "county_geoid"],
            check_name="us_census_zcta_county_xwalk_keys_not_null",
        )
        for v in vintages:
            make_staging_dq(
                ctx,
                staging_fqn,
                record_table="geography_processed.us_census_zcta_county_xwalk",
                where=f"vintage = {int(v)}",
            ).fk(
                key="county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_zcta_county_xwalk_county_fk_{v}",
            )
            # Accepted gap: record (non-blocking WARN) how many raw rel overlaps reference an
            # off-scope county (Island Areas AS/GU/MP/VI) that the INNER join dropped, so the
            # gap is tracked in _ops, not silent. Counts the rel rows, not the kept xwalk rows.
            make_staging_dq(
                ctx,
                raw_rel,
                record_table="geography_processed.us_census_zcta_county_xwalk",
                where=f"vintage = {int(v)}",
            ).fk(
                key="county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=f"vintage = {int(v)}",
                check_name=f"us_census_zcta_county_xwalk_offscope_county_dropped_{v}",
                severity=DQSeverity.WARN,
                raise_on_fail=False,
            )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_zcta_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_zcta_boundary_pk_unique")
        dq.not_null(columns=["geometry_wkb"], check_name="us_census_zcta_boundary_geom_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US ZIP Code Tabulation Areas, vintaged (non-nesting).",
        public_health_relevance=(
            "ZCTA spatial unit for ZIP-keyed health data; carries an approximate primary "
            "county (largest land-area overlap) + state for rollups and readable filtering."
        ),
        spatial_resolution="us_zcta",
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
                table="us_census_zcta",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_zcta_shapefile,
                read_from_volume=_read_zcta_shapefile,
                description="IPUMS NHGIS ZCTA boundary shapefile, as-is (attributes + geometry).",
            ),
            RawLanding(
                table="us_census_zcta_county_rel",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_zcta_rel,
                read_from_volume=_read_zcta_county_rel,
                description=(
                    "Census ZCTA↔county relationship file (land-area overlap per part), as-is. "
                    f"Provider=census; layout {ZCTA_COUNTY_REL_DOC_URL}"
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_zcta",
                reads=("us_census_zcta", "us_census_zcta_county_rel"),
                process=_process_entity,
                processed_table="us_census_zcta",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=(
                    "US ZCTAs per vintage (1 row/ZCTA), enriched with the approximate primary "
                    "county (largest land-area overlap) + that county's state labels."
                ),
                public_health_relevance=(
                    "ZIP-keyed health data conforms here; the primary county/state give "
                    "readable rollups, with spans_multiple_counties flagging the approximation."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_zcta_county_xwalk",
                reads=("us_census_zcta_county_rel",),
                process=_process_xwalk,
                processed_table="us_census_zcta_county_xwalk",
                promote=_promote_xwalk,
                validate_staging=_validate_xwalk,
                canonical_cluster_columns=["vintage"],
                description=(
                    "Every ZCTA×county land-area overlap (any overlap, not 1:1) with fraction "
                    "and is_primary flag, for spatial allocation/apportionment. Source=census; "
                    "scoped to our county universe (50 states + DC + PR) — the rel file's Island "
                    "Areas (AS/GU/MP/VI) are dropped to match us_county/us_zcta coverage."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_zcta_boundary",
                reads=("us_census_zcta",),
                process=_process_boundary,
                processed_table="us_census_zcta_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US ZCTA boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


def build_block_group_layered(
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
    """Build us_block_group via the shared builder (ADR 0036) — nests in tract, after us_tract.

    The deepest nesting level: ``process`` joins same-catalog ``us_census_county`` for
    ``county_name`` and ``us_census_state`` for the state labels (ADR 0037 decision 7), and
    the staging is FK-validated against tract (its direct parent), county, and state — so all
    three must already be built. NHGIS national file ``us_blck_grp_<year>_tl<year>``.
    """
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_bg = f"{source_catalog}.geography_raw.us_census_block_group"
    raw_cenpop = f"{source_catalog}.geography_raw.us_census_block_group_cenpop"
    proc_bg = f"{source_catalog}.geography_processed.us_census_block_group"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_block_group_boundary"
    proc_tract = f"{source_catalog}.geography_processed.us_census_tract"  # parent (nesting)
    proc_county = f"{source_catalog}.geography_processed.us_census_county"  # parent (county_name)
    proc_state = f"{source_catalog}.geography_processed.us_census_state"  # parent (state labels)

    bg_cols = (
        "geoid STRING, vintage INT, state_geoid STRING, county_geoid STRING, "
        "tract_geoid STRING, gisjoin STRING, centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, "
        "centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE, area_land_sqm DOUBLE, "
        "area_water_sqm DOUBLE, county_name STRING, state_name STRING, state_stusps STRING, "
        "state_hhs_region INT"
    )
    bg_select = [
        "geoid",
        "vintage",
        "state_geoid",
        "county_geoid",
        "tract_geoid",
        "gisjoin",
        "centroid_geo_lon",
        "centroid_geo_lat",
        "centroid_pop_lon",
        "centroid_pop_lat",
        "area_land_sqm",
        "area_water_sqm",
        "county_name",
        "state_name",
        "state_stusps",
        "state_hhs_region",
    ]

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_bg} (gisjoin STRING, vintage INT, "
            f"src_name STRING, area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_cenpop} (gisjoin STRING, vintage INT, "
            f"centroid_pop_lon DOUBLE, centroid_pop_lat DOUBLE) USING DELTA"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_bg} ({bg_cols}) USING DELTA")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_block_group ({bg_cols}) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_block_group_boundary "
            f"(geoid STRING, vintage INT, gisjoin STRING, geoid_system STRING, "
            f"resolution STRING, geometry_wkb BINARY) USING DELTA"
        )

    def _fetch_shapefile(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, [SHAPEFILE_NAMES[("us_block_group", v)]], Path(vdir))

    def _read_shapefile(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        gdf = _read_gdf(_find_shapefile(staged, "us_block_group", v))
        cols = list(gdf.columns)
        gj = _first_col(cols, ["GISJOIN", "gisjoin"])
        name_col = _first_col(cols, ["NAME", "NAMELSAD", "NHGISNAM", "NAME10", "NAME20", "name"])
        aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
        awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
        if gj is None:
            raise ValueError(f"block group shapefile has no GISJOIN column; columns={cols}")
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
        _download_shapefiles(api_key, [CENPOP_SHAPEFILE_NAMES[("us_block_group", v)]], Path(vdir))

    def _read_cenpop(ctx: BuildContext, v: int, vdir: str) -> Any:
        staged = _stage_volume_payload(vdir)
        lookup = _read_cenpop_lookup(staged, "us_block_group", v)
        rows = [
            {"gisjoin": gj, "vintage": int(v), "centroid_pop_lon": lon, "centroid_pop_lat": lat}
            for gj, (lon, lat) in lookup.items()
        ]
        return ctx.spark.createDataFrame(rows, RAW_CENPOP_SCHEMA)

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(f"SELECT * FROM {raw_bg} WHERE vintage = {int(v)}")
        cen = ctx.spark.sql(f"SELECT * FROM {raw_cenpop} WHERE vintage = {int(v)}")
        lean = raw.join(cen, ["gisjoin", "vintage"], "left").mapInPandas(
            _block_group_entity_map, schema=BLOCK_GROUP_SPARK_SCHEMA
        )
        # Scope to tracts in our universe (left_semi): NHGIS erases coastal water, so a few
        # all-water tracts come back empty and are dropped by the tract build; their (sliver)
        # block groups would otherwise dangle. The drop count is recorded in _validate_entity
        # (accepted gap, cf. zcta Island Areas / GADM subnational_rows_dropped).
        tract_keys = ctx.spark.sql(
            f"SELECT geoid AS tract_geoid FROM {proc_tract} WHERE vintage = {int(v)}"
        )
        lean = lean.join(tract_keys, ["tract_geoid"], "left_semi")
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
            .select(*bg_select)
        )

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'bg' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_bg} "
            f"WHERE vintage = {int(v)}"
        )
        bdf = raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)
        # Scope to the entity (drops the same off-universe all-water BGs; boundary ⊆ entity).
        # The builder writes the entity processed table before the boundary, per vintage.
        entity_keys = ctx.spark.sql(f"SELECT geoid FROM {proc_bg} WHERE vintage = {int(v)}")
        return bdf.join(entity_keys, ["geoid"], "left_semi")

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_bg} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_block_group"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_block_group_pk_unique")
        dq.not_null(
            columns=["geoid", "state_geoid", "county_geoid", "tract_geoid"],
            check_name="us_census_block_group_core_not_null",
        )
        # Parent-FK within vintage: bg.tract_geoid -> tract (nesting), county, state.
        rec = "geography_processed.us_census_block_group"
        for v in vintages:
            where = f"vintage = {int(v)}"
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="tract_geoid",
                parent_table=proc_tract,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_group_tract_fk_{v}",
            )
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_group_county_fk_{v}",
            )
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="state_geoid",
                parent_table=proc_state,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_group_state_fk_{v}",
            )
            # Accepted gap (non-blocking WARN): BG rows dropped in process for nesting in an
            # off-universe tract (all-water tract erased by NHGIS). raw - kept = dropped.
            raw_n = ctx.spark.sql(f"SELECT count(*) AS n FROM {raw_bg} WHERE {where}").collect()[0][
                "n"
            ]
            kept_n = ctx.spark.sql(
                f"SELECT count(*) AS n FROM {staging_fqn} WHERE {where}"
            ).collect()[0]["n"]
            dropped = int(raw_n) - int(kept_n)
            ctx.recorder.record(
                table_name=rec,
                check_name=f"us_census_block_group_offscope_tract_dropped_{v}",
                category=DQCategory.REFERENTIAL,
                severity=DQSeverity.WARN,
                passed=dropped == 0,
                failing_row_count=dropped,
                total_row_count=int(raw_n),
                details={"dropped_bg_offscope_tract": dropped} if dropped else None,
            )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_block_group_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_block_group_boundary_pk_unique")
        dq.not_null(
            columns=["geometry_wkb"], check_name="us_census_block_group_boundary_geom_not_null"
        )

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US census block groups, vintaged (nest within tracts).",
        public_health_relevance=(
            "Finest standard census tabulation unit for neighborhood-level surveillance, "
            "small-area estimation, and SDOH linkage."
        ),
        spatial_resolution="us_block_group",
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
                table="us_census_block_group",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_shapefile,
                read_from_volume=_read_shapefile,
                description="IPUMS NHGIS block-group boundary shapefile, as-is.",
            ),
            RawLanding(
                table="us_census_block_group_cenpop",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_cenpop,
                read_from_volume=_read_cenpop,
                description="Census Centers of Population for block groups, as-is.",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_block_group",
                reads=("us_census_block_group", "us_census_block_group_cenpop"),
                process=_process_entity,
                processed_table="us_census_block_group",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=(
                    "US census block groups per vintage, enriched with county + state labels; "
                    "every BG nests in a us_tract (a few all-water BGs whose tract NHGIS "
                    "water-erased are dropped + recorded)."
                ),
                public_health_relevance=(
                    "Finest standard tabulation unit; the grain for neighborhood SDOH and "
                    "small-area work, redrawn each decade so vintage matters."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_block_group_boundary",
                reads=("us_census_block_group",),
                process=_process_boundary,
                processed_table="us_census_block_group_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US block-group boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


def build_block_layered(
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
    """Build us_block via the shared builder (ADR 0036) — the atomic level, after us_block_group.

    Census blocks have no national NHGIS file, so the raw landing is the ~51 per-state
    shapefiles fetched in one extract and read per-state then UNION-ed (driver memory is
    bounded to one state at a time — big states like CA may still need a sizeable driver).
    ~8M rows/vintage, written per-(level,vintage) chunk. No Center of Population for blocks.
    Every block nests in a us_block_group; the all-water gap (cf. block group) is scoped +
    recorded. ``process`` joins same-catalog ``us_census_county`` + ``us_census_state``.
    """
    resolution = "full" if full_resolution else "generalized"
    tolerance = 0.0 if full_resolution else simplify_tolerance

    raw_block = f"{source_catalog}.geography_raw.us_census_block"
    proc_block = f"{source_catalog}.geography_processed.us_census_block"
    proc_boundary = f"{source_catalog}.geography_processed.us_census_block_boundary"
    proc_bg = f"{source_catalog}.geography_processed.us_census_block_group"  # parent (nesting)
    proc_county = f"{source_catalog}.geography_processed.us_census_county"  # parent (county_name)
    proc_state = f"{source_catalog}.geography_processed.us_census_state"  # parent (state labels)

    block_cols = (
        "geoid STRING, vintage INT, state_geoid STRING, county_geoid STRING, "
        "tract_geoid STRING, block_group_geoid STRING, gisjoin STRING, "
        "centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, area_land_sqm DOUBLE, "
        "area_water_sqm DOUBLE, county_name STRING, state_name STRING, state_stusps STRING, "
        "state_hhs_region INT"
    )
    block_select = [
        "geoid",
        "vintage",
        "state_geoid",
        "county_geoid",
        "tract_geoid",
        "block_group_geoid",
        "gisjoin",
        "centroid_geo_lon",
        "centroid_geo_lat",
        "area_land_sqm",
        "area_water_sqm",
        "county_name",
        "state_name",
        "state_stusps",
        "state_hhs_region",
    ]

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source "
            f"files). ADR 0037.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Source-catalog processed/derived geography (engineer-only). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_block} (gisjoin STRING, vintage INT, "
            f"src_name STRING, area_land_sqm DOUBLE, area_water_sqm DOUBLE, geometry_wkb BINARY) "
            f"USING DELTA"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_block} ({block_cols}) USING DELTA")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {proc_boundary} (geoid STRING, vintage INT, "
            f"gisjoin STRING, geoid_system STRING, resolution STRING, "
            f"geometry_wkb BINARY) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_block ({block_cols}) "
            f"USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_block_boundary "
            f"(geoid STRING, vintage INT, gisjoin STRING, geoid_system STRING, "
            f"resolution STRING, geometry_wkb BINARY) USING DELTA"
        )

    def _fetch_block(v: int, vdir: str) -> None:
        _download_shapefiles(api_key, _block_shapefile_names(v), Path(vdir))

    def _read_block(ctx: BuildContext, v: int, vdir: str) -> Any:
        # ~8M block rows can't go through createDataFrame: Spark Connect inlines a local
        # relation into the query plan, which blows past spark.rpc.message.maxSize (a single
        # big state ~1.4GB). Instead, stage each state to Parquet on the Volume (vectorized,
        # one state in driver memory at a time) and read it back DISTRIBUTED.
        import numpy as np
        import pandas as pd

        staged = _stage_volume_payload(vdir)
        shps = sorted(
            p
            for p in staged.rglob("*.shp")
            if "block" in p.name.lower()
            and "blck_grp" not in p.name.lower()
            and str(int(v)) in p.name.lower()
        )
        if not shps:
            names = [p.name for p in staged.rglob("*.shp")]
            raise FileNotFoundError(f"no block shapefiles for vintage={v}; found {names}")
        # Volume staging dir (engineer-only landing volume; rewritten each run).
        pq_dir = (
            Path(vdir).parent.parent / "_read_parquet" / "us_census_block" / f"vintage={int(v)}"
        )
        if pq_dir.exists():
            shutil.rmtree(pq_dir)
        pq_dir.mkdir(parents=True, exist_ok=True)
        for i, shp in enumerate(shps):
            gdf = _read_gdf(shp)
            cols = list(gdf.columns)
            gj = _first_col(cols, ["GISJOIN", "gisjoin"])
            aland = _first_col(cols, ["ALAND", "ALAND10", "ALAND20", "aland"])
            awater = _first_col(cols, ["AWATER", "AWATER10", "AWATER20", "awater"])
            if gj is None:
                raise ValueError(f"block shapefile {shp.name} has no GISJOIN; columns={cols}")
            g = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
            n = len(g)
            # Explicit, identical dtypes per part (positional numpy arrays) so Spark's merged
            # Parquet read sees one consistent schema: ALAND/AWATER are integers in some states
            # and float (with nulls) in others -> force float64; vintage int64; src_name object.
            if aland is not None:
                land = pd.to_numeric(g[aland], errors="coerce").astype("float64").to_numpy()
            else:
                land = np.full(n, np.nan, dtype="float64")
            if awater is not None:
                water = pd.to_numeric(g[awater], errors="coerce").astype("float64").to_numpy()
            else:
                water = np.full(n, np.nan, dtype="float64")
            pd.DataFrame(
                {
                    "gisjoin": g[gj].astype(str).str.strip().str.upper().to_numpy(),
                    "vintage": np.full(n, int(v), dtype="int64"),
                    "src_name": np.array([None] * n, dtype=object),
                    "area_land_sqm": land,
                    "area_water_sqm": water,
                    "geometry_wkb": g.geometry.to_wkb(),
                }
            ).to_parquet(pq_dir / f"part_{i:03d}.parquet", index=False)
        return ctx.spark.read.parquet(str(pq_dir)).selectExpr(
            "CAST(gisjoin AS STRING) AS gisjoin",
            "CAST(vintage AS INT) AS vintage",
            "CAST(src_name AS STRING) AS src_name",
            "CAST(area_land_sqm AS DOUBLE) AS area_land_sqm",
            "CAST(area_water_sqm AS DOUBLE) AS area_water_sqm",
            "CAST(geometry_wkb AS BINARY) AS geometry_wkb",
        )

    def _process_entity(ctx: BuildContext, v: int) -> Any:
        lean = ctx.spark.sql(f"SELECT * FROM {raw_block} WHERE vintage = {int(v)}").mapInPandas(
            _block_entity_map, schema=BLOCK_SPARK_SCHEMA
        )
        # Scope to block groups in our universe (left_semi): the all-water block groups dropped
        # by the BG build would otherwise leave their (all-water) blocks dangling. Recorded in
        # _validate_entity as an accepted gap.
        bg_keys = ctx.spark.sql(
            f"SELECT geoid AS block_group_geoid FROM {proc_bg} WHERE vintage = {int(v)}"
        )
        lean = lean.join(bg_keys, ["block_group_geoid"], "left_semi")
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
            .select(*block_select)
        )

    def _process_boundary(ctx: BuildContext, v: int) -> Any:
        raw = ctx.spark.sql(
            f"SELECT *, 'block' AS level_param, '{resolution}' AS res_param, "
            f"CAST({float(tolerance)} AS DOUBLE) AS tol_param FROM {raw_block} "
            f"WHERE vintage = {int(v)}"
        )
        bdf = raw.mapInPandas(_boundary_map, schema=BOUNDARY_LEVEL_SPARK_SCHEMA)
        # Scope to the entity (boundary ⊆ entity); entity processed table is written first.
        entity_keys = ctx.spark.sql(f"SELECT geoid FROM {proc_block} WHERE vintage = {int(v)}")
        return bdf.join(entity_keys, ["geoid"], "left_semi")

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_block} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.us_census_block")
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_block_pk_unique")
        dq.not_null(
            columns=["geoid", "state_geoid", "county_geoid", "tract_geoid", "block_group_geoid"],
            check_name="us_census_block_core_not_null",
        )
        rec = "geography_processed.us_census_block"
        for v in vintages:
            where = f"vintage = {int(v)}"
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="block_group_geoid",
                parent_table=proc_bg,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_block_group_fk_{v}",
            )
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="county_geoid",
                parent_table=proc_county,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_county_fk_{v}",
            )
            make_staging_dq(ctx, staging_fqn, record_table=rec, where=where).fk(
                key="state_geoid",
                parent_table=proc_state,
                parent_key="geoid",
                parent_where=where,
                check_name=f"us_census_block_state_fk_{v}",
            )
            # Accepted gap (non-blocking WARN): blocks dropped for nesting in an off-universe
            # (all-water) block group. raw - kept = dropped.
            raw_n = ctx.spark.sql(f"SELECT count(*) AS n FROM {raw_block} WHERE {where}").collect()[
                0
            ]["n"]
            kept_n = ctx.spark.sql(
                f"SELECT count(*) AS n FROM {staging_fqn} WHERE {where}"
            ).collect()[0]["n"]
            dropped = int(raw_n) - int(kept_n)
            ctx.recorder.record(
                table_name=rec,
                check_name=f"us_census_block_offscope_bg_dropped_{v}",
                category=DQCategory.REFERENTIAL,
                severity=DQSeverity.WARN,
                passed=dropped == 0,
                failing_row_count=dropped,
                total_row_count=int(raw_n),
                details={"dropped_block_offscope_bg": dropped} if dropped else None,
            )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.us_census_block_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="us_census_block_boundary_pk_unique")
        dq.not_null(columns=["geometry_wkb"], check_name="us_census_block_boundary_geom_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description="US census blocks, vintaged (the atomic tabulation unit; nest in BGs).",
        public_health_relevance=(
            "Atomic census geography; the building block for custom aggregations and the "
            "finest spatial resolution for allocation/apportionment."
        ),
        spatial_resolution="us_block",
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
                table="us_census_block",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_block,
                read_from_volume=_read_block,
                description="IPUMS NHGIS per-state block boundary shapefiles, as-is (~51 files).",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_block",
                reads=("us_census_block",),
                process=_process_entity,
                processed_table="us_census_block",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=(
                    "US census blocks per vintage, enriched with county + state labels; every "
                    "block nests in a us_block_group (all-water orphans dropped + recorded)."
                ),
                public_health_relevance=(
                    "Atomic tabulation unit; the grain for building custom geographies and "
                    "high-resolution spatial allocation."
                ),
            ),
            CanonicalOutput(
                canonical_table="us_block_boundary",
                reads=("us_census_block",),
                process=_process_boundary,
                processed_table="us_census_block_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                canonical_cluster_columns=["vintage"],
                description="US block boundary polygons (WKB) by vintage/resolution.",
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
    )
    return build_reference(spec, vintages=vintages)


# ---------------------------------------------------------------------------
# Layered build — us_hhs_region (static, generated; ADR 0036 static build).
# ---------------------------------------------------------------------------
# The ten HHS regions are a non-vintaged federal grouping generated in code (no
# shapefiles, no vintage, no source payload to land in a Volume). It uses the shared
# builder's STATIC shape: one generated raw landing promoted 1:1 to the canonical
# us_hhs_region. This is the generated/static "builder bend" recorded in ADR 0036.
HHS_REGION_SOURCE_URL = "https://www.hhs.gov/about/agencies/iea/regional-offices/index.html"


def build_hhs_region_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
) -> tuple[str, str]:
    """Build the static us_hhs_region via the shared builder's static path (ADR 0036)."""
    raw_hhs = f"{source_catalog}.geography_raw.us_hhs_region"
    hhs_desc = "The ten HHS regions (static federal grouping of states)."
    hhs_phr = "Federal regional grouping used for HHS/CDC regional reporting and rollups."

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source). ADR 0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_hhs} (hhs_region INT, name STRING, "
            f"member_states ARRAY<STRING>) USING DELTA"
        )

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical US geography reference (source-agnostic). ADR 0020/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.us_hhs_region (hhs_region INT, "
            f"name STRING, member_states ARRAY<STRING>) USING DELTA"
        )

    def _acquire(ctx: BuildContext, _v: int) -> Any:
        # Generated reference: the ten HHS regions, materialized 1:1 as the raw landing.
        rows = geo.generate_hhs_regions()
        return ctx.spark.createDataFrame(rows, HHS_REGION_SPARK_SCHEMA).sort("hhs_region")

    def _promote(ctx: BuildContext, _v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {raw_hhs}")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_raw.us_hhs_region")
        dq.unique(keys=["hhs_region"], check_name="us_hhs_region_pk_unique")
        dq.not_null(
            columns=["hhs_region", "name", "member_states"],
            check_name="us_hhs_region_core_not_null",
        )

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description=hhs_desc,
        public_health_relevance=hhs_phr,
        spatial_resolution="hhs_region",
        spatial_coverage="United States",
        source_provider_code="hhs",
        source_origin_code="hhs",
        source_url=HHS_REGION_SOURCE_URL,
        source_documentation_url=HHS_REGION_SOURCE_URL,
        license="Public domain (U.S. Government work).",
        dua_required=False,
        dua_reference="",
        access_tier="open",
        external_maintainer_name="U.S. Department of Health & Human Services",
        is_hosted=False,
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
                table="us_hhs_region",
                acquire=_acquire,
                description="The ten HHS regions, generated in code (1:1 raw, no external source).",
            )
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="us_hhs_region",
                reads=("us_hhs_region",),
                promote=_promote,
                validate_staging=_validate,
                description=hhs_desc,
                public_health_relevance=hhs_phr,
            )
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="full_refresh",
        static=True,
    )
    return build_reference(spec)


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
        "levels as migrated) in ONE process. Dev convenience; production uses --level + the "
        "job DAG.",
    )
    parser.add_argument(
        "--level",
        choices=[
            "us_hhs_region",
            "us_state",
            "us_county",
            "us_tract",
            "us_zcta",
            "us_block_group",
            "us_block",
        ],
        default=None,
        help="Build a single geography level via the shared builder (ADR 0036). Its parent levels "
        "must already be built (their processed tables are joined for enrichment). This is the "
        "per-level entry the job DAG calls, one task per level, ordered by depends_on. "
        "us_hhs_region is static/generated (no parents, no NHGIS secret).",
    )
    args = parser.parse_args()

    vintages = [int(v) for v in args.vintages.split(",") if v.strip()]
    source_catalog = args.source_catalog or args.catalog.replace("ecdh_model_", "ecdh_")

    if not (args.level or args.layered or args.layered_state):
        raise ValueError("pass --level <name> (job DAG), --layered (dev chain), or --layered-state")

    # us_hhs_region is static/generated — no shapefiles, so it needs no NHGIS secret.
    if args.level == "us_hhs_region":
        build_hhs_region_layered(
            source_catalog=source_catalog,
            model_catalog=args.catalog,
            data_engineers_group=args.data_engineers_group,
            analysts_group=args.analysts_group,
        )
        return

    # Every other level pulls NHGIS shapefiles.
    if not args.ipums_secret_scope:
        raise ValueError("--ipums-secret-scope is required to pull NHGIS shapefiles")
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
    # One build function per shapefile level; the job DAG (depends_on) enforces parents-first.
    builders = {
        "us_state": build_state_layered,
        "us_county": build_county_layered,
        "us_tract": build_tract_layered,
        "us_zcta": build_zcta_layered,
        "us_block_group": build_block_group_layered,
        "us_block": build_block_layered,
    }
    if args.level:
        builders[args.level](**level_kwargs)
    elif args.layered:  # whole subject in one process (dev convenience), parents-first
        build_hhs_region_layered(
            source_catalog=source_catalog,
            model_catalog=args.catalog,
            data_engineers_group=args.data_engineers_group,
            analysts_group=args.analysts_group,
        )
        build_state_layered(**level_kwargs)
        build_county_layered(**level_kwargs)
        build_tract_layered(**level_kwargs)
        build_zcta_layered(**level_kwargs)
        build_block_group_layered(**level_kwargs)
        build_block_layered(**level_kwargs)
    else:  # --layered-state
        build_state_layered(**level_kwargs)


if __name__ == "__main__":
    main()
