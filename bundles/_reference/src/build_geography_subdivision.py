"""Build geography.country_subdivision + ADM_1 boundaries (ADR 0022, slice 3b).

Pulls ISO 3166-2 subdivision codes from pycountry (~5,046 entries, including
nested cases like UK constituent countries -> counties), and GADM 4.1 ADM_1
polygons from geodata.ucdavis.edu. Writes:

  - ``geography.country_subdivision`` — one row per pycountry subdivision,
    keyed by ``subdivision_code`` (ISO 3166-2). ``gadm_gid_1`` populated when
    HASC_1 / ISO_1 / fixup-map resolves to a polygon; null otherwise (nested
    subdivisions inherit their parent's polygon spatially and don't get a
    direct ADM_1 match).
  - ``geography.boundary`` (extension) — appends ``geo_level='country_subdivision'``
    rows after a DELETE-then-INSERT scoped to that level. Per-level
    ``full_refresh`` semantics on the shared boundary table.

Pure logic (subdivision-code parsing, GADM ADM_1 matching, row assembly)
lives in ``cidmath_datahub.reference.geography_intl`` (ADR 0011). This
entrypoint is the thin IO + Spark layer: download, read, assemble, write,
DQ, register.

GADM file is ~1.4 GB zipped; only the ``ADM_1`` layer is read into memory
via pyogrio. Match priority: HASC_1 -> ISO_1 -> GADM_ADM1_ISO_FIXUPS (manual
override map, ships empty; populated from first-run DQ misses, not from
training-data priors).

Usage:
    build_geography_subdivision.py --catalog ecdh_model_dev \\
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

from cidmath_datahub.common import grants
from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import gadm
from cidmath_datahub.reference import geography_intl as gi

log = get_logger(__name__)

SCHEMA = "geography"
TABLE = "country_subdivision"
COUNTRY_TABLE = "country"
BOUNDARY_TABLE = "boundary"
GEO_LEVEL = "country_subdivision"

# GADM ADM_1 layer in the shared GADM 4.1 GeoPackage. Download / extract /
# read helpers, the GADM constants (URL, vintage, license, generalization
# tolerance), and the geography.boundary schema all live in
# cidmath_datahub.reference.gadm (ADR 0023).
GADM_ADM1_LAYER = "ADM_1"

# Join-coverage threshold: share of NON-NESTED subdivisions that should match a
# GADM ADM_1 polygon (nested subdivisions inherit their parent's polygon
# spatially and are excluded from the denominator). Set from data: code-only
# matching floored at 27.88%; name-based matching (ADR 0023) measured 72.04%
# (2,586/3,590) on the 2026-05-29 dev run. It cannot reach 90% because of
# genuine ISO-vs-GADM grain mismatches (e.g. Slovenia's 212 ISO municipalities
# vs ~12 GADM ADM_1 regions). 65% leaves ~7pt headroom below the observed
# ceiling so the WARN fires on a real regression, not normal GADM/pycountry
# drift. Re-confirm if either source version changes materially.
JOIN_COVERAGE_THRESHOLD_PCT = 65.0
CARDINALITY_MIN = 4500
CARDINALITY_MAX = 5500

SUBDIVISION_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("subdivision_code", T.StringType(), False),
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

# geography.boundary schema is provided by gadm.boundary_spark_schema() (ADR 0023).


def _read_adm1(gpkg: Path) -> Any:
    """Read the ADM_1 layer and assert the columns we depend on.

    Column assertion runs immediately after read so a GADM schema change
    fails locally with a clear message (per CLAUDE.md guidance) rather than
    producing silently empty matches downstream.
    """
    gdf = gadm.read_layer(gpkg, GADM_ADM1_LAYER)
    gi.assert_gadm_adm1_columns(gdf.columns)
    return gdf


def _collect_subdivisions() -> list[dict[str, Any]]:
    """Collect pycountry subdivisions as plain dicts for matching + assembly.

    One dict per ISO 3166-2 subdivision with the fields the resolver
    (``geography_intl.resolve_subdivision_polygons``) and
    :func:`assemble_subdivision_row` need. Skips any subdivision whose country
    prefix doesn't resolve to a pycountry country (shouldn't happen, but fail
    soft rather than crash the build).
    """
    import pycountry

    records: list[dict[str, Any]] = []
    for sub in pycountry.subdivisions:
        country_obj = pycountry.countries.get(alpha_2=sub.country_code)
        if country_obj is None:
            log.warning(
                "Skipping subdivision with unknown country",
                extra={"code": sub.code, "country_alpha2": sub.country_code},
            )
            continue
        records.append(
            {
                "subdivision_code": sub.code,
                "country_alpha2": sub.country_code,
                "country_alpha3": country_obj.alpha_3,
                "name": sub.name,
                "type_label": sub.type,
                "parent_code": sub.parent_code,
            }
        )
    return records


def _build_subdivision_rows(
    subdivisions: list[dict[str, Any]],
    resolved: dict[str, dict[str, Any]],
    methods: dict[str, str],
    source_file: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Assemble (attribute_rows, boundary_rows) from subdivisions + resolved polygons.

    ``resolved`` maps ``subdivision_code → gadm_row`` and ``methods`` maps
    ``subdivision_code → match-method`` (both from
    ``resolve_subdivision_polygons``). Subdivisions with no polygon still
    produce an attribute row (centroid / gadm_gid_1 null, method ``none``); the
    build's DQ surfaces coverage as a WARN, not a FAIL.
    """
    now = datetime.now(tz=UTC)
    attr_rows: list[dict[str, Any]] = []
    boundary_rows: list[dict[str, Any]] = []

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
            log.warning(
                "Skipping malformed subdivision",
                extra={"code": code, "error": str(e)},
            )
            continue
        row["ingested_at"] = now
        attr_rows.append(row)

        if geom is not None and not geom.is_empty:
            boundary_rows.append(
                {
                    "geo_level": GEO_LEVEL,
                    "geoid": row["subdivision_code"],
                    "vintage": gadm.GADM_VINTAGE,
                    "resolution": "generalized",
                    "gisjoin": None,
                    "geometry_wkb": gadm.simplify_to_wkb(geom),
                }
            )

    return attr_rows, boundary_rows


def _write_subdivision_table(spark: SparkSession, catalog: str, rows: list[dict[str, Any]]) -> None:
    df = spark.createDataFrame(rows, schema=SUBDIVISION_SPARK_SCHEMA).sort("subdivision_code")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{SCHEMA}.{TABLE}"
    )
    log.info("Wrote geography.country_subdivision", extra={"rows": len(rows)})


def _write_subdivision_boundaries(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]]
) -> None:
    """Replace geography.boundary rows for this geo_level, then append.

    Per-level full_refresh semantics on a shared polymorphic table — matches
    slice 3a's _write_country_boundaries pattern.
    """
    spark.sql(f"DELETE FROM {catalog}.{SCHEMA}.{BOUNDARY_TABLE} WHERE geo_level = '{GEO_LEVEL}'")
    df = spark.createDataFrame(rows, schema=gadm.boundary_spark_schema())
    df.write.mode("append").saveAsTable(f"{catalog}.{SCHEMA}.{BOUNDARY_TABLE}")
    log.info(
        "Wrote country_subdivision boundaries",
        extra={"rows": len(rows), "vintage": gadm.GADM_VINTAGE},
    )


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ISO 3166-2 first-level subdivisions (subdivision_code PK like ''US-GA''). "
        f"gadm_gid_1 links to geography.boundary where a GADM ADM_1 polygon resolves; "
        f"nested subdivisions (parent_subdivision_code IS NOT NULL) inherit their "
        f"parent''s polygon spatially. Source: pycountry + GADM 4.1. ADR 0022.'"
    )


def _register_dataset(spark: SparkSession, catalog: str, pipeline_ref: str) -> None:
    """Register geography.country_subdivision in _ops.dataset_catalog + dataset_engineering.

    Mirrors the explicit-column MERGE pattern from build_geography_country
    (ADR 0008 + slice 3a). MERGE ... UPDATE SET * shorthand fails on
    _ops.dataset_catalog because the target has more columns than the source.
    """
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    desc = (
        "Global first-level subdivision reference. ISO 3166-2 PK; FK to "
        "geography.country via country_alpha2/3; optional gadm_gid_1 link "
        "to geography.boundary."
    )
    pubhealth = (
        "Subnational surveillance backbone for international data sources keyed "
        "on ISO 3166-2 (WHO subnational reporting, GBD location hierarchy)."
    )

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
            desc,
            pubhealth,
            "country_subdivision",
            "global",
            "gadm",
            "https://gadm.org/",
            "https://gadm.org/metadata.html",
            gadm.GADM_LICENSE,
            True,
            (
                "GADM citation required (Hijmans, R. GADM database of Global "
                "Administrative Areas). pycountry MIT."
            ),
            "restricted",
            "GADM, University of California, Davis",
            True,
            "cidmath-data-team",
        )
    ]
    spark.createDataFrame(cat_row, cat_schema).createOrReplaceTempView("_tmp_geo_subdivision_cat")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING _tmp_geo_subdivision_cat AS s
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
    eng_row = [(full, "full_refresh", "table", ["subdivision_code"], pipeline_ref, 1)]
    spark.createDataFrame(eng_row, eng_schema).createOrReplaceTempView("_tmp_geo_subdivision_eng")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING _tmp_geo_subdivision_eng AS s
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
    log.info("Registered geography.country_subdivision metadata", extra={"table": full})


def _dq_checks(
    recorder: DQRecorder,
    spark: SparkSession,
    catalog: str,
    rows: list[dict[str, Any]],
    unmatched_gid_1s: list[str],
) -> None:
    """Run DQ on the assembled rows (ADR 0009).

    Three checks (per slice 3b plan):
      1. subdivision_code uniqueness — FAIL.
      2. country FK integrity to geography.country (alpha2) — WARN.
      3. ISO -> GADM ADM_1 join coverage, restricted to NON-NESTED rows — WARN.
      4. ISO 3166-2 cardinality sanity range — WARN.

    Join-coverage check also logs ``sample_unmatched_gid_1`` so future
    GADM_ADM1_ISO_FIXUPS entries have ground truth to work from.
    """
    code_counts = Counter(r["subdivision_code"] for r in rows)
    dups = sorted(c for c, n in code_counts.items() if n > 1)
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="subdivision_code_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dups,
        failing_row_count=len(dups),
        total_row_count=len(rows),
        details={"sample_duplicates": dups[:10]} if dups else None,
    )
    if dups:
        raise ValueError(f"Duplicate subdivision_code: {dups[:10]}")

    # FK integrity: every country_alpha2 should resolve to a geography.country row.
    country_alpha2s = {r["country_alpha2"] for r in rows}
    try:
        known_alpha2_df = spark.sql(
            f"SELECT DISTINCT country_alpha2 FROM {catalog}.{SCHEMA}.{COUNTRY_TABLE}"
        ).collect()
        known_alpha2 = {r["country_alpha2"] for r in known_alpha2_df}
    except Exception as e:  # noqa: BLE001 — country table may not exist on first deploy
        log.warning("country FK check skipped", extra={"error": str(e)})
        known_alpha2 = country_alpha2s
    fk_missing = sorted(country_alpha2s - known_alpha2)
    fk_failing = sum(1 for r in rows if r["country_alpha2"] in fk_missing)
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="subdivision_country_fk_integrity",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.FAIL,
        passed=not fk_missing,
        failing_row_count=fk_failing,
        total_row_count=len(rows),
        details={"sample_missing_alpha2": fk_missing[:10]} if fk_missing else None,
    )
    # Blocking on a canonical reference others FK against (ADR 0023 review P0-3):
    # a real referential break should stop the publish, not just warn. The
    # check above no-ops (known_alpha2 = our own set) when geography.country
    # doesn't exist yet, so this only fires on a genuine break.
    if fk_missing:
        raise ValueError(f"Subdivisions reference unknown countries: {fk_missing[:10]}")

    # Join coverage — denominator restricted to NON-NESTED rows so the
    # threshold is meaningful. Nested rows inherit their parent's polygon
    # spatially and are not expected to carry gadm_gid_1.
    non_nested = [r for r in rows if r["parent_subdivision_code"] is None]
    non_nested_total = len(non_nested)
    non_nested_matched = sum(1 for r in non_nested if r["gadm_gid_1"] is not None)
    non_nested_pct = non_nested_matched / non_nested_total * 100 if non_nested_total else 0.0
    all_matched = sum(1 for r in rows if r["gadm_gid_1"] is not None)
    all_pct = (all_matched / len(rows) * 100) if rows else 0.0
    passed_cov = non_nested_pct >= JOIN_COVERAGE_THRESHOLD_PCT
    sample_missing_codes = sorted(
        r["subdivision_code"] for r in non_nested if r["gadm_gid_1"] is None
    )[:10]
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_to_gadm_adm1_join_coverage",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=passed_cov,
        failing_row_count=non_nested_total - non_nested_matched,
        total_row_count=non_nested_total,
        details={
            "non_nested_coverage_pct": round(non_nested_pct, 2),
            "all_rows_coverage_pct": round(all_pct, 2),
            "threshold_pct": JOIN_COVERAGE_THRESHOLD_PCT,
            "denominator_scope": "parent_subdivision_code IS NULL",
            "sample_missing_subdivision_codes": sample_missing_codes,
            "sample_unmatched_gid_1": unmatched_gid_1s[:20],
        },
    )

    total = len(rows)
    passed_count = CARDINALITY_MIN <= total <= CARDINALITY_MAX
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="iso_3166_2_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=passed_count,
        failing_row_count=0 if passed_count else 1,
        total_row_count=total,
        details={"expected_range": [CARDINALITY_MIN, CARDINALITY_MAX], "actual": total},
    )

    # Match-precision visibility (ADR 0023 review P0-1/P0-2). Coverage counts how
    # many matched; this reports HOW, and flags the low-confidence subset. Match
    # is country-scoped so cross-country mis-links can't happen; the residual
    # risk is within-country name ambiguity (name_ambiguous). WARN + a reviewable
    # sample so a human can spot-check the heuristic matches rather than trusting
    # a coverage number alone.
    method_counts = Counter(r["gadm_match_method"] for r in rows)
    ambiguous = sorted(r["subdivision_code"] for r in rows if r["gadm_match_method"] == "name_ambiguous")
    name_review_sample = [
        {
            "subdivision_code": r["subdivision_code"],
            "subdivision_name": r["subdivision_name"],
            "gadm_gid_1": r["gadm_gid_1"],
            "method": r["gadm_match_method"],
        }
        for r in rows
        if r["gadm_match_method"] in ("name", "name_ambiguous")
    ][:20]
    recorder.record(
        table_name=f"{SCHEMA}.{TABLE}",
        check_name="subdivision_match_precision",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not ambiguous,
        failing_row_count=len(ambiguous),
        total_row_count=len(rows),
        details={
            "method_counts": dict(method_counts),
            "ambiguous_count": len(ambiguous),
            "sample_ambiguous": ambiguous[:20],
            "sample_name_matches_for_review": name_review_sample,
        },
    )

    # US / international reconciliation (ADR 0023 review P1-8). The US rows of
    # country_subdivision (ISO 3166-2, from pycountry) and geography.us_state
    # (from NHGIS) describe the same real-world units via two independent
    # sources; this surfaces any symmetric difference so drift is visible. WARN,
    # not FAIL — ISO 3166-2:US and NHGIS legitimately differ on some outlying
    # territories, so a non-empty diff is a review prompt, not a publish-blocker.
    us_local = {r["subdivision_local_code"] for r in rows if r["country_alpha2"] == "US"}
    try:
        us_state_codes: set[str] | None = {
            r["stusps"]
            for r in spark.sql(f"SELECT DISTINCT stusps FROM {catalog}.{SCHEMA}.us_state").collect()
        }
    except Exception as e:  # noqa: BLE001 — us_state may not be built yet
        log.warning("us_state reconciliation skipped", extra={"error": str(e)})
        us_state_codes = None
    if us_state_codes is not None:
        only_in_subdivision = sorted(us_local - us_state_codes)
        only_in_us_state = sorted(us_state_codes - us_local)
        reconciled = not only_in_subdivision and not only_in_us_state
        recorder.record(
            table_name=f"{SCHEMA}.{TABLE}",
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


def run(
    catalog: str,
    data_engineers_group: str,
    analysts_group: str,
) -> None:
    spark = SparkSession.builder.getOrCreate()
    pipeline_ref = "bundles/_reference/src/build_geography_subdivision.py"

    log.info("Building geography.country_subdivision", extra={"catalog": catalog})

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

    workdir = Path(tempfile.mkdtemp(prefix="gadm_"))
    zip_path = gadm.download_gadm_zip(workdir)
    gpkg = gadm.extract_gpkg(zip_path, workdir)
    gdf = _read_adm1(gpkg)
    gadm_rows = gadm.gdf_to_dict_rows(gdf)

    subdivisions = _collect_subdivisions()
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

    # Stamp the data-defining versions on every row for reproducibility (ADR
    # 0023 review P0-4): the row set is determined by the pycountry release and
    # the GADM 4.1 download. GADM is pinned by URL; pycountry is captured here.
    import pycountry

    source_file = f"{gadm.GADM_GPKG_NAME} (GADM {gadm.GADM_RELEASE}); pycountry {pycountry.__version__}"
    attr_rows, boundary_rows = _build_subdivision_rows(
        subdivisions, resolved, methods, source_file=source_file
    )
    log.info(
        "Assembled subdivision rows",
        extra={"attribute_rows": len(attr_rows), "boundary_rows": len(boundary_rows)},
    )

    run_id = new_run_id()
    log.info("DQ run id assigned", extra={"run_id": run_id, "pipeline_reference": pipeline_ref})

    with DQRecorder(spark, catalog, run_id, pipeline_ref) as recorder:
        _dq_checks(recorder, spark, catalog, attr_rows, unmatched_gid_1s)
        _write_subdivision_table(spark, catalog, attr_rows)
        if boundary_rows:
            _write_subdivision_boundaries(spark, catalog, boundary_rows)

    _comment_table(spark, catalog)

    grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

    _register_dataset(spark, catalog, pipeline_ref)

    log.info("geography.country_subdivision build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(args.catalog, args.data_engineers_group, args.analysts_group)


if __name__ == "__main__":
    main()
