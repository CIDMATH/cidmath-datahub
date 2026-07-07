"""Build `geography.subnational` + per-level ADM_2 boundaries on the shared builder.

International geography, slice 3c (ADR 0022), migrated from the legacy `run_build`
monolith onto the shared `build_reference` builder (ADR 0036/0037/0039) — the GADM
mirror of the US geography migration, following `build_geography_country.py` /
`build_geography_subdivision.py` as the worked templates. Vintaged on the GADM release
year (2022).

Sources (land in the Volume, ADR 0039 amended 2026-06-30):
  - **GADM 4.1 ADM_2** — *fetched* payload; reuses the shared `volume_key`
    (`gadm_410_levels`) so the ~1.4 GB GeoPackage lands ONCE across the three levels (country
    fetched it; this task skips the download). Raw `geography_raw.gadm_adm2` (1:1; geometry
    generalized to WKB — full-res polygons stay in the landed GeoPackage).
  - No new generated source: `subdivision_code` is resolved from `geography.country_subdivision`
    (slice 3b) via the `gadm_gid_1 -> subdivision_code` reverse map — a cross-level read of the
    already-built parent (like `us_tract` reading `us_county`).

Processed assembles `geography_processed.subnational` (attributes + centroid; non-ISO GADM
territories and malformed GIDs dropped and recorded) + `geography_processed.subnational_boundary`
(geometry, split from the entity); promoted to `geography.subnational` +
`geography.subnational_boundary`.

Known gaps (recorded as the `subnational_rows_dropped` WARN, not FAIL): GADM 4.1 codes Ghana's
ADM_2 GIDs malformed (`GHA1.1_2` rather than `GHA.1.1_2`) so ~260 districts drop as
level-mismatched; Hong Kong / Macao appear as ADM_1-shaped rows and are likewise dropped
(decision 2026-05-30).

Usage:
    build_geography_subnational.py --catalog ecdh_model_dev --source-catalog ecdh_dev \\
        --vintages 2022 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
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
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
PIPELINE_REF = "bundles/_reference/src/build_geography_subnational.py"
GADM_ADM2_LAYER = "ADM_2"
GADM_LEVEL = 2
# All three GADM levels share this Volume payload key so the GeoPackage lands once.
GADM_VOLUME_KEY = "gadm_410_levels"
GEO_LEVEL = "subnational_adm2"

# Cardinality sanity range for GADM 4.1 ADM_2 worldwide (~45k); wide WARN band.
CARDINALITY_MIN = 25000
CARDINALITY_MAX = 70000
# Drop-rate WARN threshold (steady-state drops = non-ISO territories + accepted malformed GIDs).
DROP_PCT_WARN_THRESHOLD = 5.0

SUBNATIONAL_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("gadm_gid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("gadm_level", T.IntegerType(), False),
        T.StructField("subnational_name", T.StringType(), False),
        T.StructField("subnational_type_label", T.StringType(), False),
        T.StructField("parent_gid", T.StringType(), True),
        T.StructField("country_alpha3", T.StringType(), False),
        T.StructField("subdivision_code", T.StringType(), True),
        T.StructField("centroid_geo_lon", T.DoubleType(), True),
        T.StructField("centroid_geo_lat", T.DoubleType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("source_file", T.StringType(), False),
    ]
)

RAW_GADM_ADM2_SCHEMA = T.StructType(
    [
        T.StructField("gid_0", T.StringType(), False),
        T.StructField("gid_1", T.StringType(), True),
        T.StructField("gid_2", T.StringType(), False),
        T.StructField("name_2", T.StringType(), True),
        T.StructField("type_2", T.StringType(), True),
        T.StructField("engtype_2", T.StringType(), True),
        T.StructField("geometry_wkb", T.BinaryType(), True),
        T.StructField("vintage", T.IntegerType(), False),
    ]
)

# DDL column lists (declared once; kept in lockstep with the schemas above).
_SUBNATIONAL_DDL = (
    "gadm_gid STRING, vintage INT, gadm_level INT, subnational_name STRING, "
    "subnational_type_label STRING, parent_gid STRING, country_alpha3 STRING, "
    "subdivision_code STRING, centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, "
    "ingested_at TIMESTAMP, source_file STRING"
)
_BOUNDARY_DDL = (
    "geo_level STRING, geoid_system STRING, geoid STRING, vintage INT, resolution STRING, "
    "gisjoin STRING, geometry_wkb BINARY"
)
_RAW_GADM_ADM2_DDL = (
    "gid_0 STRING, gid_1 STRING, gid_2 STRING, name_2 STRING, type_2 STRING, "
    "engtype_2 STRING, geometry_wkb BINARY, vintage INT"
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


# --- GADM landing (shared payload; the country task fetches it, this one skips) ------------
def _fetch_gadm(_v: int, volume_dir: str) -> None:
    d = Path(volume_dir)
    zip_path = gadm.download_gadm_zip(d)
    gadm.extract_gpkg(zip_path, d)
    try:  # keep the extracted .gpkg; drop the zip to save Volume space
        zip_path.unlink()
    except OSError:
        pass


def _read_gadm_adm2(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    gpkg = _find_gpkg(Path(volume_dir))
    gdf = gadm.read_layer(gpkg, GADM_ADM2_LAYER)
    gi.assert_gadm_adm2_columns(gdf.columns)
    rows: list[dict[str, Any]] = []
    for r in gadm.gdf_to_dict_rows(gdf):
        geom = r.get("geometry")
        wkb = gadm.simplify_to_wkb(geom) if (geom is not None and not geom.is_empty) else None
        rows.append(
            {
                "gid_0": r.get("GID_0"),
                "gid_1": r.get("GID_1"),
                "gid_2": r.get("GID_2"),
                "name_2": r.get("NAME_2"),
                "type_2": r.get("TYPE_2"),
                "engtype_2": r.get("ENGTYPE_2"),
                "geometry_wkb": wkb,
                "vintage": int(v),
            }
        )
    return ctx.spark.createDataFrame(rows, RAW_GADM_ADM2_SCHEMA)


def build_subnational_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    vintages: tuple[int, ...] = (gadm.GADM_VINTAGE,),
) -> tuple[str, str]:
    """Build geography.subnational + geography.subnational_boundary."""
    raw_gadm = f"{source_catalog}.geography_raw.gadm_adm2"
    proc_sub = f"{source_catalog}.geography_processed.subnational"
    proc_boundary = f"{source_catalog}.geography_processed.subnational_boundary"
    country_fqn = f"{model_catalog}.{SCHEMA}.country"
    subdivision_fqn = f"{model_catalog}.{SCHEMA}.country_subdivision"

    s_desc = (
        "GADM subnational administrative units (ADM_2 in slice 3c). PK gadm_gid + vintage; "
        "parent_gid to the ADM_1 parent; optional subdivision_code (ISO 3166-2 of the parent)."
    )
    s_phr = (
        "County-equivalent subnational backbone for international surveillance and modeling below "
        "the first administrative level, where no ISO standard exists."
    )
    b_desc = (
        "Subnational (ADM_2) generalized boundary geometry (per-level; replaces the polymorphic "
        "table)."
    )

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_raw "
            f"COMMENT 'Source-catalog raw landings for geography (1:1 with source). ADR 0037/0039.'"
        )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.geography_processed "
            f"COMMENT 'Derived geography staging (assemble/join/split). ADR 0037.'"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_gadm} ({_RAW_GADM_ADM2_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_sub} ({_SUBNATIONAL_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_boundary} ({_BOUNDARY_DDL}) USING DELTA")

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical geography reference (source-agnostic). ADR 0020/0022/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.subnational "
            f"({_SUBNATIONAL_DDL}) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.subnational_boundary "
            f"({_BOUNDARY_DDL}) USING DELTA"
        )

    def _load_gid1_to_subdivision_code(spark: SparkSession) -> dict[str, str]:
        """Build the ``{gadm_gid_1 -> subdivision_code}`` map from slice 3b's canonical table.

        Cross-level read of the already-built parent (subnational depends_on country_subdivision).
        Returns an empty map (all subdivision_code NULL) if that table isn't built yet.
        """
        if not spark.catalog.tableExists(subdivision_fqn):
            log.warning(
                "country_subdivision not available; subdivision_code will be NULL",
                extra={"table": subdivision_fqn},
            )
            return {}
        rows = spark.sql(
            f"SELECT gadm_gid_1, subdivision_code FROM {subdivision_fqn} WHERE gadm_gid_1 IS NOT NULL"
        ).collect()
        mapping = gi.build_gid1_to_subdivision_code(
            {"gadm_gid_1": r["gadm_gid_1"], "subdivision_code": r["subdivision_code"]} for r in rows
        )
        log.info("Loaded gid1->subdivision_code map", extra={"entries": len(mapping)})
        return mapping

    def _process_subnational_entity(ctx: BuildContext, v: int) -> Any:
        import shapely

        spark = ctx.spark
        gid1_to_subcode = _load_gid1_to_subdivision_code(spark)
        source_file = f"{gadm.GADM_GPKG_NAME} (GADM {gadm.GADM_RELEASE})"
        now = datetime.now(tz=UTC)

        attr_rows: list[dict[str, Any]] = []
        skipped_non_iso = 0
        skipped_malformed = 0
        # Generalized geometry keeps this driver-side collect bounded (~45k small polygons);
        # only the entity's centroid needs geometry — the boundary split is a pure-Spark join.
        for r in spark.sql(f"SELECT * FROM {raw_gadm} WHERE vintage = {int(v)}").collect():
            gid0 = r["gid_0"]
            if not gi.is_iso_gid0(gid0):
                skipped_non_iso += 1
                continue
            gid2 = r["gid_2"]
            parent_gid = r["gid_1"]
            wkb = r["geometry_wkb"]
            geom = shapely.from_wkb(bytes(wkb)) if wkb is not None else None
            lon, lat = gadm.centroid(geom) if geom is not None else (None, None)
            subdivision_code = (
                gid1_to_subcode.get(parent_gid) if isinstance(parent_gid, str) else None
            )
            try:
                row = gi.assemble_subnational_row(
                    gadm_gid=gid2,
                    gadm_level=GADM_LEVEL,
                    subnational_name=str(r["name_2"]) if r["name_2"] else "",
                    subnational_type_label=str(r["type_2"]) if r["type_2"] else "",
                    parent_gid=parent_gid,
                    country_alpha3=gid0,
                    subdivision_code=subdivision_code,
                    centroid_geo_lon=lon,
                    centroid_geo_lat=lat,
                    source_file=source_file,
                )
            except ValueError as e:
                skipped_malformed += 1
                log.warning("Skipping malformed ADM_2 row", extra={"gid_2": gid2, "error": str(e)})
                continue
            row["ingested_at"] = now
            row["vintage"] = int(v)
            attr_rows.append(row)

        log.info(
            "Assembled subnational rows",
            extra={
                "attribute_rows": len(attr_rows),
                "skipped_non_iso": skipped_non_iso,
                "skipped_malformed": skipped_malformed,
            },
        )
        return spark.createDataFrame(attr_rows, SUBNATIONAL_SPARK_SCHEMA).sort("gadm_gid")

    def _process_subnational_boundary(ctx: BuildContext, v: int) -> Any:
        """Split geometry to the per-level boundary in pure Spark.

        Joins the already-written processed entity (its kept gadm_gid) to the raw ADM_2 geometry
        — no driver-side geometry materialization, so the ~45k polygons write without hitting the
        Spark-Connect createDataFrame plan-inlining limit that bit us_block.
        """
        return ctx.spark.sql(
            f"""
            SELECT '{GEO_LEVEL}' AS geo_level,
                   '{gadm.GEOID_SYSTEM_GADM}' AS geoid_system,
                   s.gadm_gid AS geoid,
                   CAST({int(v)} AS INT) AS vintage,
                   'generalized' AS resolution,
                   CAST(NULL AS STRING) AS gisjoin,
                   r.geometry_wkb AS geometry_wkb
            FROM {proc_sub} s
            JOIN {raw_gadm} r
              ON r.gid_2 = s.gadm_gid AND r.vintage = {int(v)}
            WHERE s.vintage = {int(v)}
              AND r.geometry_wkb IS NOT NULL
            """
        )

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_sub} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        spark = ctx.spark
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.subnational")
        dq.unique(keys=["gadm_gid", "vintage"], check_name="subnational_gadm_gid_uniqueness")
        dq.not_null(
            columns=["gadm_gid", "gadm_level", "subnational_name", "subnational_type_label",
                     "country_alpha3", "vintage"],
            check_name="subnational_core_not_null",
        )
        # Country FK (blocking): every country_alpha3 must resolve to geography.country.
        if spark.catalog.tableExists(country_fqn):
            dq.fk(
                key="country_alpha3",
                parent_table=country_fqn,
                parent_key="country_alpha3",
                check_name="subnational_country_fk_integrity",
            )
        else:
            log.warning("country FK check skipped (parent not built yet)", extra={"parent": country_fqn})

        # Cardinality (WARN).
        dq.cardinality(
            check_name="adm2_cardinality",
            min_rows=CARDINALITY_MIN,
            max_rows=CARDINALITY_MAX,
            severity=DQSeverity.WARN,
        )

        total = spark.sql(f"SELECT count(*) AS c FROM {staging_fqn}").collect()[0]["c"]

        # subdivision_code link coverage (INFO): inherits 3b's ~72% match; honest, not gated.
        linked = spark.sql(
            f"SELECT count(*) AS c FROM {staging_fqn} WHERE subdivision_code IS NOT NULL"
        ).collect()[0]["c"]
        link_pct = (linked / total * 100) if total else 0.0
        ctx.recorder.record(
            table_name="geography_processed.subnational",
            check_name="subnational_subdivision_link_coverage",
            category=DQCategory.REFERENTIAL,
            severity=DQSeverity.INFO,
            passed=True,
            failing_row_count=total - linked,
            total_row_count=total,
            details={"linked": linked, "coverage_pct": round(link_pct, 2)},
        )

        # Dropped-row visibility (WARN if the drop rate spikes; the steady-state drops — non-ISO
        # GADM territories + the accepted Ghana/HK/Macao malformed-GID gaps — stay well under 5%).
        # Denominator is the raw ADM_2 count; kept is the staged (successfully assembled) count.
        total_read = spark.sql(
            f"SELECT count(*) AS c FROM {raw_gadm} WHERE vintage = "
            f"(SELECT max(vintage) FROM {staging_fqn})"
        ).collect()[0]["c"]
        dropped = max(total_read - total, 0)
        drop_pct = (dropped / total_read * 100) if total_read else 0.0
        ctx.recorder.record(
            table_name="geography_processed.subnational",
            check_name="subnational_rows_dropped",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.WARN,
            passed=drop_pct < DROP_PCT_WARN_THRESHOLD,
            failing_row_count=dropped,
            total_row_count=total_read,
            details={
                "kept": total,
                "dropped": dropped,
                "drop_pct": round(drop_pct, 2),
                "known_gap": (
                    "Steady-state drops are non-ISO GADM territories plus accepted GADM 4.1 gaps: "
                    "malformed Ghana ADM_2 GIDs (GHA1.1_2, ~260 districts) and Hong Kong / Macao "
                    "ADM_1-shaped rows. Accepted gap (2026-05-30); revisit on GADM fix. Per-bucket "
                    "counts + samples are in the build logs."
                ),
            },
        )

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.subnational_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="subnational_boundary_pk_uniqueness")
        dq.not_null(
            columns=["geo_level", "geoid", "geometry_wkb", "vintage"],
            check_name="subnational_boundary_core_not_null",
        )

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description=s_desc,
        public_health_relevance=s_phr,
        spatial_resolution="subnational_adm2",
        spatial_coverage="global",
        source_provider_code="gadm",
        source_origin_code="gadm",
        source_url="https://gadm.org/",
        source_documentation_url="https://gadm.org/metadata.html",
        license=gadm.GADM_LICENSE,
        dua_required=True,
        dua_reference=(
            "GADM citation required (Hijmans, R. GADM database of Global Administrative Areas)."
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
                table="gadm_adm2",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=GADM_VOLUME_KEY,
                fetch_to_volume=_fetch_gadm,
                read_from_volume=_read_gadm_adm2,
                description=(
                    "GADM 4.1 ADM_2 (second-level subnational polygons), 1:1 raw; geometry "
                    "generalized to bound Delta size (full-res GeoPackage preserved in the Volume)."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="subnational",
                reads=("gadm_adm2",),
                process=_process_subnational_entity,
                processed_table="subnational",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=s_desc,
                public_health_relevance=s_phr,
                canonical_cluster_columns=["country_alpha3", "vintage"],
            ),
            CanonicalOutput(
                canonical_table="subnational_boundary",
                reads=("gadm_adm2",),
                process=_process_subnational_boundary,
                processed_table="subnational_boundary",
                promote=_promote_boundary,
                validate_staging=_validate_boundary,
                description=b_desc,
                public_health_relevance=s_phr,
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
    build_subnational_layered(
        source_catalog=args.source_catalog,
        model_catalog=args.catalog,
        data_engineers_group=args.data_engineers_group,
        analysts_group=args.analysts_group,
        vintages=vintages,
    )
    log.info(
        "Subnational reference build complete",
        extra={"catalog": args.catalog, "vintages": vintages},
    )


if __name__ == "__main__":
    main()
