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
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.dq import DQRecorder
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import country_classifications as cclass
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
TABLE = "country"
BOUNDARY_TABLE = "boundary"
PIPELINE_REF = "bundles/_reference/src/build_geography_country.py"

# GADM ADM_0 layer in the shared GADM 4.1 GeoPackage. Download / extract /
# read helpers, the GADM constants (URL, vintage, license, generalization
# tolerance), and the geography.boundary schema live in
# cidmath_datahub.reference.gadm (ADR 0023). We read only the ADM_0 layer.
GADM_ADM0_LAYER = "ADM_0"

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

# geography.boundary schema is provided by gadm.boundary_spark_schema() (ADR 0023).


def _read_adm0(gpkg: Path) -> Any:
    """Read the ADM_0 layer from the GADM GeoPackage as a GeoDataFrame in EPSG:4326."""
    return gadm.read_layer(gpkg, GADM_ADM0_LAYER)


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
        lon, lat = gadm.centroid(geom) if geom is not None else (None, None)

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
        row["vintage"] = gadm.GADM_VINTAGE
        attr_rows.append(row)

        if geom is not None and not geom.is_empty:
            boundary_rows.append(
                {
                    "geo_level": "country",
                    "geoid_system": gadm.GEOID_SYSTEM_ISO_ALPHA3,
                    "geoid": alpha3,
                    "vintage": gadm.GADM_VINTAGE,
                    "resolution": "generalized",
                    "gisjoin": None,
                    "geometry_wkb": gadm.simplify_to_wkb(geom),
                }
            )

    return attr_rows, boundary_rows


def _write_country_table(spark: SparkSession, catalog: str, rows: list[dict[str, Any]]) -> None:
    df = spark.createDataFrame(rows, schema=COUNTRY_SPARK_SCHEMA).sort("country_alpha3")
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    if gadm.table_has_column(spark, full, "vintage"):
        # Per-vintage refresh (ADR 0024): replace only this release's vintage,
        # leaving any other loaded vintages intact.
        spark.sql(f"DELETE FROM {full} WHERE vintage = {gadm.GADM_VINTAGE}")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        # Migration / first build: establish the table and the vintage column.
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote geography.country", extra={"rows": len(rows), "vintage": gadm.GADM_VINTAGE})


def _write_country_boundaries(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]]
) -> None:
    """Replace geography.boundary rows where geo_level='country', then append.

    Per-level full_refresh semantics on a shared polymorphic table: each
    build job owns its geo_level slice; concurrent jobs are safe because
    they touch disjoint rows.
    """
    spark.sql(f"DELETE FROM {catalog}.{SCHEMA}.{BOUNDARY_TABLE} WHERE geo_level = 'country'")
    df = spark.createDataFrame(rows, schema=gadm.boundary_spark_schema())
    # mergeSchema evolves geoid_system into the existing boundary table on the
    # first re-run (ADR 0023 review P1-6); no-op once the column exists.
    df.write.option("mergeSchema", "true").mode("append").saveAsTable(
        f"{catalog}.{SCHEMA}.{BOUNDARY_TABLE}"
    )
    log.info("Wrote country boundaries", extra={"rows": len(rows), "vintage": gadm.GADM_VINTAGE})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ISO 3166-1 countries (PK country_alpha3, vintage) with WHO and UN "
        f"M49 region attributes; "
        f"centroids from GADM ADM_0 representative points. Source: pycountry "
        f"+ in-repo WHO/UN classifications + GADM 4.1. ADR 0022.'"
    )


def _register_dataset(spark: SparkSession, catalog: str, pipeline_ref: str) -> None:
    """Register geography.country in _ops.dataset_catalog + _ops.dataset_engineering.

    Mirrors the explicit-column pattern from build_geography.py (`MERGE ... UPDATE
    SET col = s.col, ...`). MERGE ... UPDATE SET * shorthand fails on
    _ops.dataset_catalog because the target has 17 columns and the source
    SELECT only supplies a subset.
    """
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description=(
                "Global country reference. ISO 3166-1 alpha-3 PK; alpha-2/numeric "
                "alternates; WHO + UN M49 region attributes; centroids from GADM ADM0."
            ),
            public_health_relevance=(
                "Canonical country reference for international surveillance. ISO 3166-1 "
                "alpha-3 matches WHO, IHR, and GBD conventions; WHO region enables "
                "regional aggregation."
            ),
            spatial_resolution="country",
            spatial_coverage="global",
            source_provider_code="gadm",
            source_url="https://gadm.org/",
            source_documentation_url="https://gadm.org/metadata.html",
            license=gadm.GADM_LICENSE,
            dua_required=True,
            dua_reference=(
                "GADM citation required (Hijmans, R. GADM database of Global "
                "Administrative Areas). pycountry MIT; country_classifications "
                "derived from WHO GHO + UN M49 (public)."
            ),
            access_tier="restricted",
            external_maintainer_name="GADM, University of California, Davis",
            is_hosted=True,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="full_refresh",
            materialization_type="table",
            cluster_columns=["country_alpha3"],
            pipeline_reference=pipeline_ref,
        ),
    )


def _dq_checks(
    recorder: DQRecorder,
    rows: list[dict[str, Any]],
    gadm_alpha3_set: set[str],
) -> None:
    """Run DQ on the assembled rows (ADR 0009) and persist results."""
    alpha3s = [r["country_alpha3"] for r in rows]
    alpha3_counts = Counter(alpha3s)
    dups = sorted(a for a, n in alpha3_counts.items() if n > 1)
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
    log.info("Building geography.country", extra={"catalog": catalog})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark
        workdir = Path(tempfile.mkdtemp(prefix="gadm_"))
        zip_path = gadm.download_gadm_zip(workdir)
        gpkg = gadm.extract_gpkg(zip_path, workdir)
        gdf = _read_adm0(gpkg)
        gadm_by_alpha3 = _gadm_alpha3_to_geometry(gdf)

        # Stamp data-defining versions for reproducibility (ADR 0023 review P1-7):
        # the row set comes from pycountry; the boundaries from GADM 4.1.
        import pycountry

        source_file = (
            f"{gadm.GADM_GPKG_NAME} (GADM {gadm.GADM_RELEASE}); pycountry {pycountry.__version__}"
        )
        attr_rows, boundary_rows = _build_country_rows(gadm_by_alpha3, source_file=source_file)
        log.info(
            "Assembled country rows",
            extra={"attribute_rows": len(attr_rows), "boundary_rows": len(boundary_rows)},
        )

        # DQ runs pre-write on the assembled in-memory rows (uniqueness via Counter,
        # ISO->GADM join coverage, ISO 3166-1 cardinality) -- bespoke checks over
        # Python data, not table queries, so they stay inline (ADR 0029).
        _dq_checks(ctx.recorder, attr_rows, set(gadm_by_alpha3.keys()))
        _write_country_table(spark, catalog, attr_rows)
        if boundary_rows:
            _write_country_boundaries(spark, catalog, boundary_rows)

    def _register(spark: SparkSession) -> None:
        _comment_table(spark, catalog)
        _register_dataset(spark, catalog, PIPELINE_REF)

    def _grant(spark: SparkSession) -> None:
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=_register,
        grant=_grant,
    )
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
