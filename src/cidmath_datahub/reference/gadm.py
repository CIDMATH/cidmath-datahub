"""Shared GADM download / IO helpers for the geography reference builds.

Slices 3a (country / ADM_0) and 3b (country_subdivision / ADM_1) — and 3c
(subnational / ADM_2+) when it lands — all pull the same ~1.4 GB GADM 4.1
zipped GeoPackage from geodata.ucdavis.edu, extract it, read one layer, and
turn polygons into generalized WKB. That IO surface was copy-pasted across
``build_geography_country.py`` and ``build_geography_subdivision.py``; ADR
0021 flagged the third copy (3c) as the trigger to extract it, and ADR 0023
makes this module that extraction.

What lives here: the GADM download constants, the download/extract/read
helpers, the geometry helpers (representative-point centroid, simplify→WKB),
the GeoDataFrame→dict-rows materializer, and the shared ``geography.boundary``
Spark schema. Geospatial and Spark imports are **lazy** (inside the functions
that need them) so importing this module — and unit-testing the pure helpers —
does not require geopandas/shapely/pyspark, keeping the core wheel's install
deps lean (ADR 0020). The deterministic match/parse logic stays in
``geography_intl`` (ADR 0011); this module is the IO seam.
"""

from __future__ import annotations

import urllib.request
import zipfile
from pathlib import Path
from typing import Any

from cidmath_datahub.common.logging import get_logger

log = get_logger(__name__)

# GADM 4.1 download (gadm.org/download_world.html). Zipped GeoPackage with six
# layers (ADM_0..ADM_5); each build reads only the layer it needs.
GADM_ZIP_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/gadm_410-levels.zip"
GADM_ZIP_NAME = "gadm_410-levels.zip"
GADM_GPKG_NAME = "gadm_410-levels.gpkg"
GADM_VINTAGE = 2022  # GADM 4.1 release year, recorded on each boundary row.
GADM_RELEASE = "4.1"  # GADM release identifier, stamped into source_file (ADR 0023 review P1-7).
# geodata.ucdavis.edu 403s default Python user-agents; send a real one.
GADM_USER_AGENT = "Mozilla/5.0 cidmath-datahub/1.0 (+https://github.com/cidmath)"
GADM_LICENSE = (
    "GADM data may be used for academic and other non-commercial use. "
    "Redistribution requires explicit permission. See https://gadm.org/license.html"
)

# Geometry generalization tolerance (degrees) — matches the US tables (ADR 0020).
GENERALIZE_TOLERANCE_DEG = 0.005


def download_gadm_zip(dest: Path) -> Path:
    """Download the GADM 4.1 zipped GeoPackage to ``dest`` and return its path."""
    target = dest / GADM_ZIP_NAME
    log.info("Downloading GADM", extra={"url": GADM_ZIP_URL, "dest": str(target)})
    req = urllib.request.Request(GADM_ZIP_URL, headers={"User-Agent": GADM_USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, open(target, "wb") as out:
        chunk = resp.read(1 << 20)  # 1 MiB chunks
        while chunk:
            out.write(chunk)
            chunk = resp.read(1 << 20)
    log.info("Downloaded GADM zip", extra={"bytes": target.stat().st_size})
    return target


def extract_gpkg(zip_path: Path, dest: Path) -> Path:
    """Unzip the GADM archive and return the path to the .gpkg file."""
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    gpkg = dest / GADM_GPKG_NAME
    if not gpkg.exists():
        # GADM occasionally nests; fall back to a recursive find.
        candidates = list(dest.rglob("*.gpkg"))
        if not candidates:
            raise FileNotFoundError(f"No .gpkg found under {dest}")
        gpkg = candidates[0]
    log.info("Extracted GeoPackage", extra={"path": str(gpkg)})
    return gpkg


def read_layer(gpkg: Path, layer: str) -> Any:
    """Read one layer of the GADM GeoPackage as a GeoDataFrame in EPSG:4326.

    Lazy ``geopandas`` import so this module loads without the geospatial
    stack. Column-set assertions are the caller's job (e.g.
    ``geography_intl.assert_gadm_adm1_columns``) so each build fails loudly on
    a GADM schema change it actually depends on.
    """
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer=layer)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326, allow_override=True)
    else:
        gdf = gdf.to_crs(4326)
    log.info(
        "Read GADM layer", extra={"layer": layer, "rows": len(gdf), "columns": list(gdf.columns)}
    )
    return gdf


def gdf_to_dict_rows(gdf: Any) -> list[dict[str, Any]]:
    """Materialize a GeoDataFrame to plain row dicts (geometry carried through).

    Decouples downstream matching/assembly from GeoPandas so the logic in
    ``geography_intl`` stays unit-testable with plain dicts. The shapely
    geometry object is preserved under the ``"geometry"`` key.
    """
    cols = [c for c in gdf.columns if c != "geometry"]
    rows: list[dict[str, Any]] = []
    for _, r in gdf.iterrows():
        d: dict[str, Any] = {c: r[c] for c in cols}
        d["geometry"] = r.geometry
        rows.append(d)
    return rows


def centroid(geom: Any) -> tuple[float, float] | tuple[None, None]:
    """Return the ``(lon, lat)`` representative point of a geometry.

    Uses shapely ``representative_point`` (guaranteed inside the polygon,
    unlike a true centroid for concave/multipart shapes). Returns
    ``(None, None)`` for a missing or empty geometry.
    """
    if geom is None or geom.is_empty:
        return (None, None)
    pt = geom.representative_point()
    return (float(pt.x), float(pt.y))


def simplify_to_wkb(geom: Any, tolerance: float = GENERALIZE_TOLERANCE_DEG) -> bytes:
    """Simplify a geometry (topology-preserving) and return 2D WKB bytes."""
    import shapely

    simplified = geom.simplify(tolerance, preserve_topology=True)
    return shapely.to_wkb(simplified, output_dimension=2)


def boundary_spark_schema() -> Any:
    """Return the Spark schema for ``geography.boundary`` (ADR 0020).

    Lazy ``pyspark`` import so this module loads without Spark. Mirrors the
    definition originally in ``build_geography.py``; the GADM builds (3a/3b/3c)
    append their ``geo_level`` slices through this shared schema.
    """
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("geo_level", T.StringType(), False),
            T.StructField("geoid", T.StringType(), False),
            T.StructField("vintage", T.IntegerType(), False),
            T.StructField("resolution", T.StringType(), False),
            T.StructField("gisjoin", T.StringType(), True),
            T.StructField("geometry_wkb", T.BinaryType(), False),
        ]
    )
