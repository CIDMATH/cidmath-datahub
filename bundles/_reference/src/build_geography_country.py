"""Build `geography.country` + per-level country boundaries on the shared builder.

International geography, slice 3a (ADR 0022), migrated from the legacy `run_build`
monolith onto the shared `build_reference` builder (ADR 0036/0037/0039) — the GADM
mirror of the US geography migration. Vintaged on the GADM release year (2022).

Sources (all land in the Volume, ADR 0039 amended 2026-06-30):
  - **GADM 4.1 ADM_0** — *fetched* payload; the ~1.4 GB GeoPackage lands ONCE under a
    shared `volume_key` (`gadm_410_levels`) reused by country/subdivision/subnational, so
    only the first level downloads it. Raw `geography_raw.gadm_adm0` (1:1; geometry
    generalized to bound Delta size — the full-res polygons stay in the landed GeoPackage).
  - **ISO 3166-1** (pycountry) — *generated* payload; raw `geography_raw.iso_3166_1`.
  - **WHO region / UN M49** (`country_classifications`) — *generated* payload; raw
    `geography_raw.country_classifications` (documented: WHO GHO `ParentCode`, UN M49).

Processed joins the three → `geography_processed.country` (attributes + centroid) +
`geography_processed.country_boundary` (geometry); promoted to `geography.country` +
`geography.country_boundary`. Pure logic stays in `cidmath_datahub.reference.geography_intl`
(ADR 0011) and `.gadm` (ADR 0023); `.country_classifications` is the WHO/UN lookup.

Usage:
    build_geography_country.py --catalog ecdh_model_dev --source-catalog ecdh_dev \\
        --vintages 2022 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
from cidmath_datahub.reference import country_classifications as cclass
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
PIPELINE_REF = "bundles/_reference/src/build_geography_country.py"
GADM_ADM0_LAYER = "ADM_0"
# All three GADM levels share this Volume payload key so the GeoPackage lands once.
GADM_VOLUME_KEY = "gadm_410_levels"
SOURCE_FILE = f"GADM {gadm.GADM_RELEASE} ADM_0"

COUNTRY_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("country_alpha3", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
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

RAW_GADM_ADM0_SCHEMA = T.StructType(
    [
        T.StructField("gid_0", T.StringType(), False),
        T.StructField("country_name", T.StringType(), True),
        T.StructField("geometry_wkb", T.BinaryType(), True),
        T.StructField("vintage", T.IntegerType(), False),
    ]
)

# DDL column lists (declared once; kept in lockstep with the schemas above).
_COUNTRY_DDL = (
    "country_alpha3 STRING, vintage INT, country_alpha2 STRING, country_numeric STRING, "
    "country_name STRING, country_official_name STRING, who_region STRING, un_region STRING, "
    "un_subregion STRING, is_un_member BOOLEAN, is_sovereign BOOLEAN, "
    "iso_3166_3_predecessor STRING, centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, "
    "ingested_at TIMESTAMP, source_file STRING"
)
_BOUNDARY_DDL = (
    "geo_level STRING, geoid_system STRING, geoid STRING, vintage INT, resolution STRING, "
    "gisjoin STRING, geometry_wkb BINARY"
)
_RAW_GADM_ADM0_DDL = "gid_0 STRING, country_name STRING, geometry_wkb BINARY, vintage INT"
_RAW_ISO_DDL = (
    "alpha2 STRING, alpha3 STRING, numeric STRING, name STRING, official_name STRING, vintage INT"
)
_RAW_CLASS_DDL = (
    "alpha3 STRING, who_region STRING, un_region STRING, un_subregion STRING, "
    "is_un_member BOOLEAN, vintage INT"
)


def _find_gpkg(volume_dir: Path) -> Path:
    """Locate the extracted GeoPackage under a landed GADM Volume dir."""
    direct = volume_dir / gadm.GADM_GPKG_NAME
    if direct.exists():
        return direct
    candidates = list(volume_dir.rglob("*.gpkg"))
    if not candidates:
        raise FileNotFoundError(f"No .gpkg found under {volume_dir}")
    return candidates[0]


# --- GADM landing (fetched once; shared across the three GADM levels via volume_key) -------
def _fetch_gadm(_v: int, volume_dir: str) -> None:
    d = Path(volume_dir)
    zip_path = gadm.download_gadm_zip(d)
    gadm.extract_gpkg(zip_path, d)
    try:  # keep the extracted .gpkg; drop the zip to save Volume space
        zip_path.unlink()
    except OSError:
        pass


def _read_gadm_adm0(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    gpkg = _find_gpkg(Path(volume_dir))
    gdf = gadm.read_layer(gpkg, GADM_ADM0_LAYER)
    rows: list[dict[str, Any]] = []
    for r in gadm.gdf_to_dict_rows(gdf):
        geom = r.get("geometry")
        wkb = gadm.simplify_to_wkb(geom) if (geom is not None and not geom.is_empty) else None
        rows.append(
            {
                "gid_0": r.get("GID_0"),
                "country_name": r.get("COUNTRY"),
                "geometry_wkb": wkb,
                "vintage": int(v),
            }
        )
    return ctx.spark.createDataFrame(rows, RAW_GADM_ADM0_SCHEMA)


# --- generated landings (pycountry ISO + WHO/UN classifications -> parquet) ----------------
def _fetch_iso(_v: int, volume_dir: str) -> None:
    import pycountry

    rows = [
        {
            "alpha2": c.alpha_2,
            "alpha3": c.alpha_3,
            "numeric": c.numeric,
            "name": c.name,
            "official_name": getattr(c, "official_name", None),
        }
        for c in pycountry.countries
    ]
    cols = ["alpha2", "alpha3", "numeric", "name", "official_name"]
    pd.DataFrame(rows)[cols].to_parquet(f"{volume_dir}/iso_3166_1.parquet", index=False)


def _read_iso(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    df = ctx.spark.read.parquet(f"{volume_dir}/iso_3166_1.parquet")
    return df.select(
        F.col("alpha2").cast("string").alias("alpha2"),
        F.col("alpha3").cast("string").alias("alpha3"),
        F.col("numeric").cast("string").alias("numeric"),
        F.col("name").cast("string").alias("name"),
        F.col("official_name").cast("string").alias("official_name"),
        F.lit(int(v)).cast("int").alias("vintage"),
    )


def _fetch_classifications(_v: int, volume_dir: str) -> None:
    import pycountry

    rows = [
        {
            "alpha3": c.alpha_3,
            "who_region": cclass.who_region(c.alpha_3),
            "un_region": cclass.un_region(c.alpha_3),
            "un_subregion": cclass.un_subregion(c.alpha_3),
            "is_un_member": bool(cclass.is_un_member(c.alpha_3)),
        }
        for c in pycountry.countries
    ]
    cols = ["alpha3", "who_region", "un_region", "un_subregion", "is_un_member"]
    pd.DataFrame(rows)[cols].to_parquet(f"{volume_dir}/country_classifications.parquet", index=False)


def _read_classifications(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    df = ctx.spark.read.parquet(f"{volume_dir}/country_classifications.parquet")
    return df.select(
        F.col("alpha3").cast("string").alias("alpha3"),
        F.col("who_region").cast("string").alias("who_region"),
        F.col("un_region").cast("string").alias("un_region"),
        F.col("un_subregion").cast("string").alias("un_subregion"),
        F.col("is_un_member").cast("boolean").alias("is_un_member"),
        F.lit(int(v)).cast("int").alias("vintage"),
    )


def build_country_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    vintages: tuple[int, ...] = (gadm.GADM_VINTAGE,),
) -> tuple[str, str]:
    """Build geography.country + geography.country_boundary via the shared builder."""
    raw_gadm = f"{source_catalog}.geography_raw.gadm_adm0"
    raw_iso = f"{source_catalog}.geography_raw.iso_3166_1"
    raw_class = f"{source_catalog}.geography_raw.country_classifications"
    proc_country = f"{source_catalog}.geography_processed.country"
    proc_boundary = f"{source_catalog}.geography_processed.country_boundary"

    c_desc = (
        "Global country reference. ISO 3166-1 alpha-3 PK (vintaged); alpha-2/numeric alternates; "
        "WHO + UN M49 region attributes; centroids from GADM ADM_0."
    )
    c_phr = (
        "Canonical country reference for international surveillance. ISO 3166-1 alpha-3 matches "
        "WHO, IHR, and GBD conventions; WHO region enables regional aggregation."
    )
    b_desc = "Country (ADM_0) generalized boundary geometry (per-level; replaces the polymorphic table)."

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source). ADR 0037/0039.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Derived geography staging (assemble/join/split). ADR 0037.'"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_gadm} ({_RAW_GADM_ADM0_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_iso} ({_RAW_ISO_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_class} ({_RAW_CLASS_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_country} ({_COUNTRY_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_boundary} ({_BOUNDARY_DDL}) USING DELTA")

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical geography reference (source-agnostic). ADR 0020/0022/0037.'"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.country ({_COUNTRY_DDL}) USING DELTA")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.country_boundary "
            f"({_BOUNDARY_DDL}) USING DELTA"
        )

    def _process_country_entity(ctx: BuildContext, v: int) -> Any:
        import shapely

        spark = ctx.spark
        iso = {r["alpha3"]: r for r in spark.sql(f"SELECT * FROM {raw_iso} WHERE vintage = {int(v)}").collect()}
        cls = {r["alpha3"]: r for r in spark.sql(f"SELECT * FROM {raw_class} WHERE vintage = {int(v)}").collect()}
        geom_by_a3: dict[str, Any] = {}
        for gr in spark.sql(
            f"SELECT gid_0, geometry_wkb FROM {raw_gadm} WHERE vintage = {int(v)}"
        ).collect():
            gid0 = gr["gid_0"]
            if gi.is_iso_gid0(gid0) and gr["geometry_wkb"] is not None:
                geom_by_a3[gid0] = shapely.from_wkb(bytes(gr["geometry_wkb"]))

        now = datetime.now(tz=UTC)
        attr_rows: list[dict[str, Any]] = []
        for alpha3, rec in iso.items():
            c = cls.get(alpha3)
            who = c["who_region"] if c else None
            un_region = c["un_region"] if c else None
            un_subregion = c["un_subregion"] if c else None
            is_un_member = bool(c["is_un_member"]) if (c and c["is_un_member"] is not None) else False
            # Whitelist into the controlled vocabularies (defensive; same as the legacy build).
            if who not in gi.WHO_REGION_CODES:
                who = None
            if un_region not in gi.UN_REGION_NAMES:
                un_region = None
            is_sovereign = is_un_member or alpha3 in {"TWN", "PSE", "VAT", "XKX"}
            geom = geom_by_a3.get(alpha3)
            lon, lat = gadm.centroid(geom) if geom is not None else (None, None)
            row = gi.assemble_country_row(
                alpha2=rec["alpha2"],
                alpha3=alpha3,
                numeric=rec["numeric"],
                name=rec["name"],
                official_name=rec["official_name"],
                who_region=who,
                un_region=un_region,
                un_subregion=un_subregion,
                is_un_member=is_un_member,
                is_sovereign=is_sovereign,
                iso_3166_3_predecessor=None,
                centroid_geo_lon=lon,
                centroid_geo_lat=lat,
                source_file=SOURCE_FILE,
            )
            row["ingested_at"] = now
            row["vintage"] = int(v)
            attr_rows.append(row)
        return spark.createDataFrame(attr_rows, COUNTRY_SPARK_SCHEMA).sort("country_alpha3")

    def _process_country_boundary(ctx: BuildContext, v: int) -> Any:
        spark = ctx.spark
        brows: list[dict[str, Any]] = []
        for r in spark.sql(
            f"SELECT gid_0, geometry_wkb FROM {raw_gadm} WHERE vintage = {int(v)}"
        ).collect():
            if not gi.is_iso_gid0(r["gid_0"]) or r["geometry_wkb"] is None:
                continue
            brows.append(
                {
                    "geo_level": "country",
                    "geoid_system": gadm.GEOID_SYSTEM_ISO_ALPHA3,
                    "geoid": r["gid_0"],
                    "vintage": int(v),
                    "resolution": "generalized",
                    "gisjoin": None,
                    "geometry_wkb": bytes(r["geometry_wkb"]),
                }
            )
        return spark.createDataFrame(brows, gadm.boundary_spark_schema())

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_country} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.country")
        dq.unique(keys=["country_alpha3", "vintage"], check_name="country_pk_uniqueness")
        dq.not_null(
            columns=["country_alpha3", "country_alpha2", "country_numeric", "country_name",
                     "is_un_member", "is_sovereign", "vintage"],
            check_name="country_core_not_null",
        )
        # Coverage + cardinality WARNs (non-blocking; mirror the legacy build's expectations).
        total = ctx.spark.sql(f"SELECT count(*) AS c FROM {staging_fqn}").collect()[0]["c"]
        with_geom = ctx.spark.sql(
            f"SELECT count(*) AS c FROM {staging_fqn} WHERE centroid_geo_lat IS NOT NULL"
        ).collect()[0]["c"]
        coverage = (with_geom / total) if total else 0.0
        ctx.recorder.record(
            table_name="geography_processed.country",
            check_name="country_iso_to_gadm_join_coverage",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.WARN,
            passed=coverage >= 0.95,
            failing_row_count=total - with_geom,
            total_row_count=total,
            details={"coverage": round(coverage, 4)},
        )
        ctx.recorder.record(
            table_name="geography_processed.country",
            check_name="country_iso_3166_1_cardinality",
            category=DQCategory.CARDINALITY,
            severity=DQSeverity.WARN,
            passed=230 <= total <= 270,
            total_row_count=total,
        )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.country_boundary")
        dq.unique(keys=["geoid", "vintage"], check_name="country_boundary_pk_uniqueness")
        dq.not_null(columns=["geo_level", "geoid", "geometry_wkb", "vintage"],
                    check_name="country_boundary_core_not_null")

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description=c_desc,
        public_health_relevance=c_phr,
        spatial_resolution="country",
        spatial_coverage="global",
        source_provider_code="gadm",
        source_origin_code="gadm",
        source_url="https://gadm.org/",
        source_documentation_url="https://gadm.org/metadata.html",
        license=gadm.GADM_LICENSE,
        dua_required=True,
        dua_reference=(
            "GADM citation required (Hijmans, R. GADM database of Global Administrative Areas). "
            "pycountry MIT; country_classifications from WHO GHO ParentCode + UN M49 (public)."
        ),
        access_tier="restricted",
        external_maintainer_name="GADM, University of California, Davis",
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
                table="gadm_adm0",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=GADM_VOLUME_KEY,
                fetch_to_volume=_fetch_gadm,
                read_from_volume=_read_gadm_adm0,
                description=(
                    "GADM 4.1 ADM_0 (country polygons), 1:1 raw; geometry generalized to bound "
                    "Delta size (full-res GeoPackage preserved in the landing Volume)."
                ),
            ),
            RawLanding(
                table="iso_3166_1",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_iso,
                read_from_volume=_read_iso,
                description="ISO 3166-1 country codes from pycountry (generated payload, parquet).",
            ),
            RawLanding(
                table="country_classifications",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_classifications,
                read_from_volume=_read_classifications,
                description=(
                    "WHO region / UN M49 sub-region lookup (generated payload; sources: WHO GHO "
                    "ParentCode, UN Statistics Division M49)."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="country",
                reads=("gadm_adm0", "iso_3166_1", "country_classifications"),
                process=_process_country_entity,
                processed_table="country",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=c_desc,
                public_health_relevance=c_phr,
                canonical_cluster_columns=["country_alpha3"],
            ),
            CanonicalOutput(
                canonical_table="country_boundary",
                reads=("gadm_adm0",),
                process=_process_country_boundary,
                processed_table="country_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                description=b_desc,
                public_health_relevance=c_phr,
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    return build_reference(spec, vintages=tuple(vintages))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Model catalog (ecdh_model_<env>).")
    parser.add_argument("--source-catalog", required=True, help="Source catalog (ecdh_<env>).")
    parser.add_argument("--vintages", default=str(gadm.GADM_VINTAGE), help="Comma-separated, e.g. 2022")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    vintages = tuple(int(x) for x in args.vintages.split(","))
    build_country_layered(
        source_catalog=args.source_catalog,
        model_catalog=args.catalog,
        data_engineers_group=args.data_engineers_group,
        analysts_group=args.analysts_group,
        vintages=vintages,
    )
    log.info("Country reference build complete", extra={"catalog": args.catalog, "vintages": vintages})


if __name__ == "__main__":
    main()
