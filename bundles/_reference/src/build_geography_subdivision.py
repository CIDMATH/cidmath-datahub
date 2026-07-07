"""Build `geography.country_subdivision` + per-level ADM_1 boundaries on the shared builder.

International geography, slice 3b (ADR 0022), migrated from the legacy `run_build`
monolith onto the shared `build_reference` builder (ADR 0036/0037/0039) — the GADM
mirror of the US geography migration, following `build_geography_country.py` as the
worked template. Vintaged on the GADM release year (2022).

Sources (all land in the Volume, ADR 0039 amended 2026-06-30):
  - **GADM 4.1 ADM_1** — *fetched* payload; reuses the shared `volume_key`
    (`gadm_410_levels`) so the ~1.4 GB GeoPackage lands ONCE across country/subdivision/
    subnational (the `country` task fetched it; this task skips the download). Raw
    `geography_raw.gadm_adm1` (1:1; geometry generalized to WKB — the full-res polygons
    stay in the landed GeoPackage).
  - **ISO 3166-2** (pycountry) — *generated* payload; raw `geography_raw.iso_3166_2`.

Processed resolves each ISO 3166-2 subdivision to a GADM ADM_1 polygon (HASC_1 → ISO_1 →
name → fixups; pure logic in `cidmath_datahub.reference.geography_intl`) →
`geography_processed.country_subdivision` (attributes + centroid) +
`geography_processed.country_subdivision_boundary` (geometry, split from the entity);
promoted to `geography.country_subdivision` + `geography.country_subdivision_boundary`.

Usage:
    build_geography_subdivision.py --catalog ecdh_model_dev --source-catalog ecdh_dev \\
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
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
PIPELINE_REF = "bundles/_reference/src/build_geography_subdivision.py"
GADM_ADM1_LAYER = "ADM_1"
# All three GADM levels share this Volume payload key so the GeoPackage lands once.
GADM_VOLUME_KEY = "gadm_410_levels"
GEO_LEVEL = "country_subdivision"

# Join-coverage threshold: share of NON-NESTED subdivisions that should resolve to a GADM
# ADM_1 polygon (nested subdivisions inherit their parent's polygon spatially and are
# excluded from the denominator). Set from data (name-based matching measured ~72% on the
# 2026-05-29 dev run; genuine ISO-vs-GADM grain mismatches cap it below 90%). 65% leaves
# headroom so the WARN fires on a real regression, not normal source drift.
JOIN_COVERAGE_THRESHOLD_PCT = 65.0
CARDINALITY_MIN = 4500
CARDINALITY_MAX = 5500

# Per-landing provenance (over the build's base `gadm` entry) so the generated ISO 3166-2
# raw records its true source rather than inheriting 'gadm' (ADR 0039; RawLanding.catalog_overrides).
_ISO_3166_2_PROVENANCE = {
    "source_provider_code": "pycountry",  # distributor the codes are pulled from
    "source_origin_code": "iso",  # ISO 3166-2 defines the codes
    "source_url": "https://www.iso.org/iso-3166-country-codes.html",
    "source_documentation_url": "https://www.iso.org/iso-3166-country-codes.html",
    "license": "Public (ISO 3166-2 subdivision codes); pycountry MIT.",
    "dua_required": False,
    "dua_reference": "",
    "access_tier": "open",
    "external_maintainer_name": "ISO / pycountry",
    "is_hosted": False,
}

SUBDIVISION_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("subdivision_code", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("country_alpha2", T.StringType(), False),
        T.StructField("country_alpha3", T.StringType(), False),
        T.StructField("subdivision_local_code", T.StringType(), False),
        T.StructField("subdivision_name", T.StringType(), False),
        T.StructField("subdivision_type_label", T.StringType(), False),
        T.StructField("parent_subdivision_code", T.StringType(), True),
        T.StructField("gadm_gid_1", T.StringType(), True),
        T.StructField("gadm_match_method", T.StringType(), False),
        T.StructField("centroid_geo_lon", T.DoubleType(), True),
        T.StructField("centroid_geo_lat", T.DoubleType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("source_file", T.StringType(), False),
    ]
)

RAW_GADM_ADM1_SCHEMA = T.StructType(
    [
        T.StructField("gid_0", T.StringType(), False),
        T.StructField("gid_1", T.StringType(), False),
        T.StructField("name_1", T.StringType(), True),
        T.StructField("type_1", T.StringType(), True),
        T.StructField("engtype_1", T.StringType(), True),
        T.StructField("hasc_1", T.StringType(), True),
        T.StructField("iso_1", T.StringType(), True),
        T.StructField("varname_1", T.StringType(), True),
        T.StructField("geometry_wkb", T.BinaryType(), True),
        T.StructField("vintage", T.IntegerType(), False),
    ]
)

# DDL column lists (declared once; kept in lockstep with the schemas above).
_SUBDIVISION_DDL = (
    "subdivision_code STRING, vintage INT, country_alpha2 STRING, country_alpha3 STRING, "
    "subdivision_local_code STRING, subdivision_name STRING, subdivision_type_label STRING, "
    "parent_subdivision_code STRING, gadm_gid_1 STRING, gadm_match_method STRING, "
    "centroid_geo_lon DOUBLE, centroid_geo_lat DOUBLE, ingested_at TIMESTAMP, source_file STRING"
)
_BOUNDARY_DDL = (
    "geo_level STRING, geoid_system STRING, geoid STRING, vintage INT, resolution STRING, "
    "gisjoin STRING, geometry_wkb BINARY"
)
_RAW_GADM_ADM1_DDL = (
    "gid_0 STRING, gid_1 STRING, name_1 STRING, type_1 STRING, engtype_1 STRING, "
    "hasc_1 STRING, iso_1 STRING, varname_1 STRING, geometry_wkb BINARY, vintage INT"
)
_RAW_ISO_3166_2_DDL = (
    "subdivision_code STRING, country_alpha2 STRING, country_alpha3 STRING, name STRING, "
    "type_label STRING, parent_code STRING, vintage INT"
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


def _read_gadm_adm1(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    gpkg = _find_gpkg(Path(volume_dir))
    gdf = gadm.read_layer(gpkg, GADM_ADM1_LAYER)
    gi.assert_gadm_adm1_columns(gdf.columns)
    rows: list[dict[str, Any]] = []
    for r in gadm.gdf_to_dict_rows(gdf):
        geom = r.get("geometry")
        wkb = gadm.simplify_to_wkb(geom) if (geom is not None and not geom.is_empty) else None
        rows.append(
            {
                "gid_0": r.get("GID_0"),
                "gid_1": r.get("GID_1"),
                "name_1": r.get("NAME_1"),
                "type_1": r.get("TYPE_1"),
                "engtype_1": r.get("ENGTYPE_1"),
                "hasc_1": r.get("HASC_1"),
                "iso_1": r.get("ISO_1"),
                "varname_1": r.get("VARNAME_1"),
                "geometry_wkb": wkb,
                "vintage": int(v),
            }
        )
    return ctx.spark.createDataFrame(rows, RAW_GADM_ADM1_SCHEMA)


# --- generated landing (pycountry ISO 3166-2 subdivisions -> parquet) ----------------------
def _fetch_iso_3166_2(_v: int, volume_dir: str) -> None:
    import pycountry

    rows: list[dict[str, Any]] = []
    for sub in pycountry.subdivisions:
        country_obj = pycountry.countries.get(alpha_2=sub.country_code)
        if country_obj is None:
            log.warning(
                "Skipping subdivision with unknown country",
                extra={"code": sub.code, "country_alpha2": sub.country_code},
            )
            continue
        rows.append(
            {
                "subdivision_code": sub.code,
                "country_alpha2": sub.country_code,
                "country_alpha3": country_obj.alpha_3,
                "name": sub.name,
                "type_label": sub.type,
                "parent_code": sub.parent_code,
            }
        )
    cols = ["subdivision_code", "country_alpha2", "country_alpha3", "name", "type_label", "parent_code"]
    pd.DataFrame(rows)[cols].to_parquet(f"{volume_dir}/iso_3166_2.parquet", index=False)


def _read_iso_3166_2(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    df = ctx.spark.read.parquet(f"{volume_dir}/iso_3166_2.parquet")
    return df.select(
        F.col("subdivision_code").cast("string").alias("subdivision_code"),
        F.col("country_alpha2").cast("string").alias("country_alpha2"),
        F.col("country_alpha3").cast("string").alias("country_alpha3"),
        F.col("name").cast("string").alias("name"),
        F.col("type_label").cast("string").alias("type_label"),
        F.col("parent_code").cast("string").alias("parent_code"),
        F.lit(int(v)).cast("int").alias("vintage"),
    )


def build_subdivision_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    vintages: tuple[int, ...] = (gadm.GADM_VINTAGE,),
) -> tuple[str, str]:
    """Build geography.country_subdivision + geography.country_subdivision_boundary."""
    raw_gadm = f"{source_catalog}.geography_raw.gadm_adm1"
    raw_iso = f"{source_catalog}.geography_raw.iso_3166_2"
    proc_sub = f"{source_catalog}.geography_processed.country_subdivision"
    proc_boundary = f"{source_catalog}.geography_processed.country_subdivision_boundary"
    country_fqn = f"{model_catalog}.{SCHEMA}.country"
    us_state_fqn = f"{model_catalog}.{SCHEMA}.us_state"

    s_desc = (
        "Global first-level subdivision reference. ISO 3166-2 PK (vintaged); FK to "
        "geography.country via country_alpha2/3; optional gadm_gid_1 link to the ADM_1 polygon."
    )
    s_phr = (
        "Subnational surveillance backbone for international data sources keyed on ISO 3166-2 "
        "(WHO subnational reporting, GBD location hierarchy)."
    )
    b_desc = (
        "Country subdivision (ADM_1) generalized boundary geometry (per-level; replaces the "
        "polymorphic table)."
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
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_gadm} ({_RAW_GADM_ADM1_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_iso} ({_RAW_ISO_3166_2_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_sub} ({_SUBDIVISION_DDL}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {proc_boundary} ({_BOUNDARY_DDL}) USING DELTA")

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical geography reference (source-agnostic). ADR 0020/0022/0037.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.country_subdivision "
            f"({_SUBDIVISION_DDL}) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.country_subdivision_boundary "
            f"({_BOUNDARY_DDL}) USING DELTA"
        )

    def _load_gadm_rows(ctx: BuildContext, v: int) -> list[dict[str, Any]]:
        """Reconstruct GADM ADM_1 dict rows (uppercase keys + shapely geometry) from raw."""
        import shapely

        rows: list[dict[str, Any]] = []
        for r in ctx.spark.sql(f"SELECT * FROM {raw_gadm} WHERE vintage = {int(v)}").collect():
            wkb = r["geometry_wkb"]
            geom = shapely.from_wkb(bytes(wkb)) if wkb is not None else None
            rows.append(
                {
                    "GID_0": r["gid_0"],
                    "GID_1": r["gid_1"],
                    "NAME_1": r["name_1"],
                    "TYPE_1": r["type_1"],
                    "ENGTYPE_1": r["engtype_1"],
                    "HASC_1": r["hasc_1"],
                    "ISO_1": r["iso_1"],
                    "VARNAME_1": r["varname_1"],
                    "geometry": geom,
                }
            )
        return rows

    def _process_subdivision_entity(ctx: BuildContext, v: int) -> Any:
        import pycountry

        spark = ctx.spark
        gadm_rows = _load_gadm_rows(ctx, v)
        subdivisions = [
            {
                "subdivision_code": r["subdivision_code"],
                "country_alpha2": r["country_alpha2"],
                "country_alpha3": r["country_alpha3"],
                "name": r["name"],
                "type_label": r["type_label"],
                "parent_code": r["parent_code"],
            }
            for r in spark.sql(f"SELECT * FROM {raw_iso} WHERE vintage = {int(v)}").collect()
        ]
        resolved, methods, unmatched_gid_1s = gi.resolve_subdivision_polygons(
            gadm_rows, subdivisions, fixups=gi.GADM_ADM1_ISO_FIXUPS
        )
        log.info(
            "Resolved ISO 3166-2 subdivisions to GADM ADM_1 polygons",
            extra={
                "subdivisions": len(subdivisions),
                "matched": len(resolved),
                "unmatched_gadm_rows": len(unmatched_gid_1s),
                "sample_unmatched_gid_1": unmatched_gid_1s[:10],
            },
        )

        # Stamp the data-defining versions for reproducibility (ADR 0023 review P0-4).
        source_file = (
            f"{gadm.GADM_GPKG_NAME} (GADM {gadm.GADM_RELEASE}); pycountry {pycountry.__version__}"
        )
        now = datetime.now(tz=UTC)
        attr_rows: list[dict[str, Any]] = []
        for rec in subdivisions:
            code = rec["subdivision_code"]
            gadm_row = resolved.get(code)
            gadm_gid_1 = gadm_row.get("GID_1") if gadm_row else None
            geom = gadm_row.get("geometry") if gadm_row else None
            lon, lat = gadm.centroid(geom) if geom is not None else (None, None)
            try:
                row = gi.assemble_subdivision_row(
                    subdivision_code=code,
                    country_alpha2=rec["country_alpha2"],
                    country_alpha3=rec["country_alpha3"],
                    subdivision_name=rec["name"],
                    subdivision_type_label=rec["type_label"],
                    parent_subdivision_code=rec["parent_code"],
                    gadm_gid_1=gadm_gid_1,
                    gadm_match_method=methods.get(code, "none"),
                    centroid_geo_lon=lon,
                    centroid_geo_lat=lat,
                    source_file=source_file,
                )
            except ValueError as e:
                log.warning("Skipping malformed subdivision", extra={"code": code, "error": str(e)})
                continue
            row["ingested_at"] = now
            row["vintage"] = int(v)
            attr_rows.append(row)
        return spark.createDataFrame(attr_rows, SUBDIVISION_SPARK_SCHEMA).sort("subdivision_code")

    def _process_subdivision_boundary(ctx: BuildContext, v: int) -> Any:
        """Split geometry to the per-level boundary in pure Spark.

        Joins the already-written processed entity (its resolved gadm_gid_1) to the raw
        ADM_1 geometry — no driver-side geometry materialization, so it scales cleanly.
        """
        return ctx.spark.sql(
            f"""
            SELECT '{GEO_LEVEL}' AS geo_level,
                   '{gadm.GEOID_SYSTEM_ISO_3166_2}' AS geoid_system,
                   s.subdivision_code AS geoid,
                   CAST({int(v)} AS INT) AS vintage,
                   'generalized' AS resolution,
                   CAST(NULL AS STRING) AS gisjoin,
                   r.geometry_wkb AS geometry_wkb
            FROM {proc_sub} s
            JOIN {raw_gadm} r
              ON r.gid_1 = s.gadm_gid_1 AND r.vintage = {int(v)}
            WHERE s.vintage = {int(v)}
              AND s.gadm_gid_1 IS NOT NULL
              AND r.geometry_wkb IS NOT NULL
            """
        )

    def _promote_entity(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_sub} WHERE vintage = {int(v)}")

    def _promote_boundary(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {proc_boundary} WHERE vintage = {int(v)}")

    def _validate_entity(ctx: BuildContext, staging_fqn: str) -> None:
        spark = ctx.spark
        dq = make_staging_dq(ctx, staging_fqn, record_table="geography_processed.country_subdivision")
        dq.unique(keys=["subdivision_code", "vintage"], check_name="subdivision_code_uniqueness")
        dq.not_null(
            columns=["subdivision_code", "country_alpha2", "country_alpha3",
                     "subdivision_local_code", "subdivision_name", "subdivision_type_label",
                     "gadm_match_method", "vintage"],
            check_name="country_subdivision_core_not_null",
        )
        # Country FK (blocking): every country_alpha2 must resolve to geography.country. The
        # parent is a hard DAG dependency; guard existence so a genuinely-absent parent skips
        # (first deploy) rather than erroring, mirroring the legacy build's resilience.
        if spark.catalog.tableExists(country_fqn):
            dq.fk(
                key="country_alpha2",
                parent_table=country_fqn,
                parent_key="country_alpha2",
                check_name="subdivision_country_fk_integrity",
            )
        else:
            log.warning("country FK check skipped (parent not built yet)", extra={"parent": country_fqn})

        # Cardinality (WARN).
        dq.cardinality(
            check_name="iso_3166_2_cardinality",
            min_rows=CARDINALITY_MIN,
            max_rows=CARDINALITY_MAX,
            severity=DQSeverity.WARN,
        )

        # ISO -> GADM ADM_1 join coverage on NON-NESTED rows (WARN). Nested subdivisions
        # inherit their parent's polygon spatially and are excluded from the denominator.
        nn_total = spark.sql(
            f"SELECT count(*) AS c FROM {staging_fqn} WHERE parent_subdivision_code IS NULL"
        ).collect()[0]["c"]
        nn_matched = spark.sql(
            f"SELECT count(*) AS c FROM {staging_fqn} "
            f"WHERE parent_subdivision_code IS NULL AND gadm_gid_1 IS NOT NULL"
        ).collect()[0]["c"]
        nn_pct = (nn_matched / nn_total * 100) if nn_total else 0.0
        sample_missing = [
            r["subdivision_code"]
            for r in spark.sql(
                f"SELECT subdivision_code FROM {staging_fqn} "
                f"WHERE parent_subdivision_code IS NULL AND gadm_gid_1 IS NULL "
                f"ORDER BY subdivision_code LIMIT 10"
            ).collect()
        ]
        ctx.recorder.record(
            table_name="geography_processed.country_subdivision",
            check_name="iso_to_gadm_adm1_join_coverage",
            category=DQCategory.REFERENTIAL,
            severity=DQSeverity.WARN,
            passed=nn_pct >= JOIN_COVERAGE_THRESHOLD_PCT,
            failing_row_count=nn_total - nn_matched,
            total_row_count=nn_total,
            details={
                "non_nested_coverage_pct": round(nn_pct, 2),
                "threshold_pct": JOIN_COVERAGE_THRESHOLD_PCT,
                "denominator_scope": "parent_subdivision_code IS NULL",
                "sample_missing_subdivision_codes": sample_missing,
            },
        )

        # Match-precision visibility (WARN): flag the low-confidence name_ambiguous subset.
        method_counts = {
            r["gadm_match_method"]: r["c"]
            for r in spark.sql(
                f"SELECT gadm_match_method, count(*) AS c FROM {staging_fqn} GROUP BY gadm_match_method"
            ).collect()
        }
        ambiguous = [
            r["subdivision_code"]
            for r in spark.sql(
                f"SELECT subdivision_code FROM {staging_fqn} "
                f"WHERE gadm_match_method = 'name_ambiguous' ORDER BY subdivision_code"
            ).collect()
        ]
        total = spark.sql(f"SELECT count(*) AS c FROM {staging_fqn}").collect()[0]["c"]
        ctx.recorder.record(
            table_name="geography_processed.country_subdivision",
            check_name="subdivision_match_precision",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.WARN,
            passed=not ambiguous,
            failing_row_count=len(ambiguous),
            total_row_count=total,
            details={
                "method_counts": method_counts,
                "ambiguous_count": len(ambiguous),
                "sample_ambiguous": ambiguous[:20],
            },
        )

        # US / international reconciliation (WARN): symmetric diff of the US rows of
        # country_subdivision (ISO 3166-2) vs geography.us_state (NHGIS) — a cross-job read of
        # an already-built US canonical (ADR 0041). Non-empty diff is a review prompt, not a block.
        if spark.catalog.tableExists(us_state_fqn):
            us_local = {
                r["subdivision_local_code"]
                for r in spark.sql(
                    f"SELECT subdivision_local_code FROM {staging_fqn} WHERE country_alpha2 = 'US'"
                ).collect()
            }
            us_state = {
                r["stusps"]
                for r in spark.sql(f"SELECT DISTINCT stusps FROM {us_state_fqn}").collect()
            }
            only_in_subdivision = sorted(us_local - us_state)
            only_in_us_state = sorted(us_state - us_local)
            reconciled = not only_in_subdivision and not only_in_us_state
            ctx.recorder.record(
                table_name="geography_processed.country_subdivision",
                check_name="us_subdivision_vs_us_state_reconciliation",
                category=DQCategory.REFERENTIAL,
                severity=DQSeverity.WARN,
                passed=reconciled,
                failing_row_count=len(only_in_subdivision) + len(only_in_us_state),
                total_row_count=len(us_local),
                details=(
                    {
                        "only_in_country_subdivision": only_in_subdivision,
                        "only_in_us_state": only_in_us_state,
                    }
                    if not reconciled
                    else None
                ),
            )
        else:
            log.warning("us_state reconciliation skipped (not built yet)", extra={"table": us_state_fqn})

    def _validate_boundary(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(
            ctx, staging_fqn, record_table="geography_processed.country_subdivision_boundary"
        )
        dq.unique(keys=["geoid", "vintage"], check_name="country_subdivision_boundary_pk_uniqueness")
        dq.not_null(
            columns=["geo_level", "geoid", "geometry_wkb", "vintage"],
            check_name="country_subdivision_boundary_core_not_null",
        )

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description=s_desc,
        public_health_relevance=s_phr,
        spatial_resolution="country_subdivision",
        spatial_coverage="global",
        source_provider_code="gadm",
        source_origin_code="gadm",
        source_url="https://gadm.org/",
        source_documentation_url="https://gadm.org/metadata.html",
        license=gadm.GADM_LICENSE,
        dua_required=True,
        dua_reference=(
            "GADM citation required (Hijmans, R. GADM database of Global Administrative Areas). "
            "pycountry MIT."
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
                table="gadm_adm1",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=GADM_VOLUME_KEY,
                fetch_to_volume=_fetch_gadm,
                read_from_volume=_read_gadm_adm1,
                description=(
                    "GADM 4.1 ADM_1 (first-level subdivision polygons), 1:1 raw; geometry "
                    "generalized to bound Delta size (full-res GeoPackage preserved in the Volume)."
                ),
            ),
            RawLanding(
                table="iso_3166_2",
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch_iso_3166_2,
                read_from_volume=_read_iso_3166_2,
                description="ISO 3166-2 subdivision codes from pycountry (generated payload, parquet).",
                catalog_overrides=_ISO_3166_2_PROVENANCE,
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="country_subdivision",
                reads=("gadm_adm1", "iso_3166_2"),
                process=_process_subdivision_entity,
                processed_table="country_subdivision",
                promote=_promote_entity,
                validate_staging=_validate_entity,
                description=s_desc,
                public_health_relevance=s_phr,
                canonical_cluster_columns=["subdivision_code"],
            ),
            CanonicalOutput(
                canonical_table="country_subdivision_boundary",
                reads=("gadm_adm1",),
                process=_process_subdivision_boundary,
                processed_table="country_subdivision_boundary",
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
    build_subdivision_layered(
        source_catalog=args.source_catalog,
        model_catalog=args.catalog,
        data_engineers_group=args.data_engineers_group,
        analysts_group=args.analysts_group,
        vintages=vintages,
    )
    log.info(
        "Country subdivision reference build complete",
        extra={"catalog": args.catalog, "vintages": vintages},
    )


if __name__ == "__main__":
    main()
