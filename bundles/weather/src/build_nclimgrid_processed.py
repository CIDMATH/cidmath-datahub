"""Conform weather_raw.noaa_nclimgrid_daily into weather_processed.noaa_nclimgrid_daily.

ADR 0025 slice 2 (processed). Reads the faithful raw landing (NCEI region codes,
sentinel-cleared values) and conforms it to the shared reference dimensions:

  - NCEI region codes -> FIPS ``geoid`` via the NOAA ``us-state-codes_ncei-to-fips``
    cross-reference, pulled at runtime from NOAA's ``doc/`` folder (docs-first:
    the mapping is published, not inferred). State -> 2-digit FIPS; county ->
    FIPS state (2) + FIPS county suffix (3). The cross-reference is keyed on the
    numeric NCEI/FIPS columns only — its ``state_name`` column has a known
    Illinois/Indiana swap, so names are ignored.
  - ``geoid`` foreign-keys to ``geography.us_county`` / ``geography.us_state``
    (vintage 2020), ``obs_date`` to ``time.calendar_date``.
  - Units made explicit per the nClimGrid user guide: prcp in mm, temps in degC.

Catalog topology (ADR 0025 / databricks.yml): weather_processed stays in the
**source-aligned** catalog (``ecdh_<env>``) alongside weather_raw, and registers
into that catalog's ``_ops`` / surfaces in its ``discovery``. The geography and
time reference live in the **integrated** catalog (``ecdh_model_<env>``) and are
referenced cross-catalog for the FK validation only — the processed table is not
moved into the integrated model. ``merge_upsert`` on
(geo_level, geoid, variable, obs_date) absorbs the prelim->scaled revisions that
flow through raw.

Conformance arithmetic is the unit-tested ``cidmath_datahub.weather.nclimgrid``
(``parse_ncei_fips_crosswalk`` / ``conform_region``) applied to the small set of
distinct region codes (~3155) and joined back — so the geoid rule is single-
sourced in Python, not duplicated in SQL. This is a thin IO + Spark entrypoint
(ADR 0011).

Usage:
    build_nclimgrid_processed.py --catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --start-year 2024 --end-year 2026 --data-engineers-group ecdh-data-engineers
"""

from __future__ import annotations

import argparse
import urllib.request
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.dq import DQRecorder, TableDQ
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.weather import nclimgrid as ncl

log = get_logger(__name__)

# Raw (faithful) and processed (conformed) both live in the source-aligned catalog.
SOURCE_SCHEMA = "weather_raw"
SOURCE_TABLE = "noaa_nclimgrid_daily"
SCHEMA = "weather_processed"
TABLE = "noaa_nclimgrid_daily"
FULL_TABLE_REL = f"{SCHEMA}.{TABLE}"
PIPELINE_REF = "bundles/weather/src/build_nclimgrid_processed.py"

# Reference dimensions (integrated catalog) this layer FKs against, cross-catalog.
GEOGRAPHY_SCHEMA = "geography"
TIME_SCHEMA = "time"
US_VINTAGE = 2020  # current Census vintage the geoids FK against (ADR 0022).

# NOAA nClimGrid-Daily documentation folder (docs-first, ADR 0025). The NCEI->FIPS
# cross-reference and the readme/data-dictionary live here; recorded in the
# catalog so downstream users can find them.
DOC_BASE = "https://www.ncei.noaa.gov/data/nclimgrid-daily/doc"
DEFAULT_CROSSWALK_URL = f"{DOC_BASE}/us-state-codes_ncei-to-fips.csv"
README_URL = f"{DOC_BASE}/nclimgrid-daily_v1-0-0_readme-web.txt"
SOURCE_URL = "https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages"
SOURCE_DOI = "https://doi.org/10.25921/c4gt-r169"

USER_AGENT = "Mozilla/5.0 cidmath-datahub/1.0 (+https://github.com/cidmath)"
HTTP_TIMEOUT = 120

# Sanity bounds for the value-range DQ (degrees C / mm). Wide WARN bands — the
# data is authoritative NOAA, so this flags conformance/unit regressions, not
# real weather.
TEMP_MIN_C, TEMP_MAX_C = -90.0, 60.0

# Conform-map DF: distinct (region_type, region_code) -> (geo_level, geoid).
_CONFORM_SCHEMA = T.StructType(
    [
        T.StructField("region_type", T.StringType(), False),
        T.StructField("region_code", T.StringType(), False),
        T.StructField("geo_level", T.StringType(), False),
        T.StructField("geoid", T.StringType(), False),
    ]
)


def _http_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8-sig", errors="replace")


def _unit_case() -> str:
    """SQL CASE mapping variable -> unit, single-sourced from ``ncl.UNITS``."""
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ncl.UNITS.items())
    return f"CASE variable {whens} ELSE NULL END"


def _ensure_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{FULL_TABLE_REL} (
            geo_level STRING, geoid STRING, region_name STRING,
            variable STRING, unit STRING, obs_date DATE, value DOUBLE,
            status STRING, source_file STRING, processed_at TIMESTAMP
        ) USING delta
        CLUSTER BY (variable, geo_level, obs_date)
        """
    )


def _build_conform_map(
    spark: SparkSession,
    catalog: str,
    start_year: int,
    end_year: int,
    ncei_to_fips: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Conform the distinct (region_type, region_code) in the raw window.

    Returns (conformed_rows, unconformed). ``conformed_rows`` feed the join;
    ``unconformed`` (conform_region -> None) drive the blocking coverage DQ. Only
    distinct codes are conformed (~3155), so the geoid rule runs in Python once
    per code, not per daily row.
    """
    src = f"{catalog}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    distinct = spark.sql(
        f"""
        SELECT DISTINCT region_type, region_code
        FROM {src}
        WHERE year(obs_date) BETWEEN {start_year} AND {end_year}
        """
    ).collect()
    conformed: list[dict[str, Any]] = []
    unconformed: list[dict[str, Any]] = []
    for r in distinct:
        rt, code = r["region_type"], r["region_code"]
        geoid = ncl.conform_region(rt, code, ncei_to_fips)
        geo_level = ncl.GEO_LEVEL_BY_REGION_TYPE.get(rt)
        if geoid is None or geo_level is None:
            unconformed.append({"region_type": rt, "region_code": code})
        else:
            conformed.append(
                {"region_type": rt, "region_code": code, "geo_level": geo_level, "geoid": geoid}
            )
    log.info(
        "Conformed region codes",
        extra={"conformed": len(conformed), "unconformed": len(unconformed)},
    )
    return conformed, unconformed


def _write_processed(
    spark: SparkSession,
    catalog: str,
    start_year: int,
    end_year: int,
    conformed: list[dict[str, Any]],
) -> None:
    """Join raw -> conform-map, attach unit, and ``merge_upsert`` the window.

    merge_upsert on (geo_level, geoid, variable, obs_date) so prelim->scaled
    revisions flowing through raw update in place rather than duplicating.
    """
    src = f"{catalog}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    full = f"{catalog}.{FULL_TABLE_REL}"

    spark.createDataFrame(conformed, _CONFORM_SCHEMA).createOrReplaceTempView("_tmp_conform_map")
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _tmp_nclimgrid_processed AS
        SELECT
            c.geo_level                       AS geo_level,
            c.geoid                           AS geoid,
            r.region_name                     AS region_name,
            r.variable                        AS variable,
            {_unit_case()}                    AS unit,
            r.obs_date                        AS obs_date,
            r.value                           AS value,
            r.status                          AS status,
            r.source_file                     AS source_file,
            current_timestamp()               AS processed_at
        FROM {src} r
        JOIN _tmp_conform_map c
          ON r.region_type = c.region_type AND r.region_code = c.region_code
        WHERE year(r.obs_date) BETWEEN {start_year} AND {end_year}
        """
    )
    spark.sql(
        f"""
        MERGE INTO {full} AS t
        USING _tmp_nclimgrid_processed AS s
        ON t.geo_level = s.geo_level AND t.geoid = s.geoid
           AND t.variable = s.variable AND t.obs_date = s.obs_date
        WHEN MATCHED THEN UPDATE SET
            region_name = s.region_name, unit = s.unit, value = s.value,
            status = s.status, source_file = s.source_file, processed_at = s.processed_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _dq_checks(
    recorder: DQRecorder,
    spark: SparkSession,
    catalog: str,
    model_catalog: str,
    start_year: int,
    end_year: int,
    unconformed: list[dict[str, Any]],
    conformed: list[dict[str, Any]],
) -> None:
    """Post-conformance DQ on weather_processed.noaa_nclimgrid_daily (ADR 0009).

    1. NCEI->FIPS coverage — FAIL (blocking): any raw region code that didn't
       conform to a geoid. A new/renamed NCEI code must be mapped, not dropped.
    2. geoid FK to geography.us_county / us_state (vintage 2020, cross-catalog)
       — FAIL (blocking, ADR 0023 P0-3): conformed geoids must exist in the
       reference.
    3. obs_date FK to time.calendar_date — WARN: time covers 1900-2099, so
       nClimGrid's 1951-present history resolves cleanly; the WARN guards
       against an unexpected out-of-range obs_date, not an expected gap.
    4. value ranges — WARN: prcp >= 0 mm; temps within [-90, 60] degC. Flags a
       conformance/unit regression, not real weather.
    5. natural-key uniqueness over the window — FAIL.
    6. stale geoids — WARN: (geo_level, geoid) present in the window that the
       current conformance run did not produce — leftovers from a changed
       conformance mapping, since merge_upsert never deletes. Recorded before
       the blocking checks so it survives a later raise. Remediation: a
       targeted DELETE of the stale keys, then re-run.
    """
    full = f"{catalog}.{FULL_TABLE_REL}"
    where = f"year(obs_date) BETWEEN {start_year} AND {end_year}"
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {full} WHERE {where}").collect()[0]["n"]

    # 6. Stale-geoid guard (WARN, recorded first so it survives a later blocking
    # raise). merge_upsert keys on (geo_level, geoid, variable, obs_date) and
    # never deletes, so if a conformance change remaps a code to a different
    # geoid, the old geoid's rows linger. The blocking FK check (2) catches stale
    # *invalid* geoids; this catches stale *valid* ones, which would otherwise
    # double-count silently. Remediation: targeted DELETE of the stale keys then
    # re-run (cheaper than a full drop+rebuild on the full-history table).
    expected_pairs = {(c["geo_level"], c["geoid"]) for c in conformed}
    in_table = spark.sql(f"SELECT DISTINCT geo_level, geoid FROM {full} WHERE {where}").collect()
    stale = sorted(
        (r["geo_level"], r["geoid"])
        for r in in_table
        if (r["geo_level"], r["geoid"]) not in expected_pairs
    )
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_processed_stale_geoids",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not stale,
        failing_row_count=len(stale),
        total_row_count=total,
        details={
            "sample_stale": stale[:10],
            "remediation": "targeted DELETE of the stale (geo_level, geoid), then re-run",
        }
        if stale
        else None,
    )

    # 1. NCEI->FIPS coverage (blocking).
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_processed_ncei_fips_coverage",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.FAIL,
        passed=not unconformed,
        failing_row_count=len(unconformed),
        total_row_count=total,
        details={"sample_unconformed": unconformed[:10]} if unconformed else None,
    )
    if unconformed:
        raise ValueError(f"Unconformed NCEI region codes (no FIPS mapping): {unconformed[:10]}")

    # 2. geoid FK to geography reference (cross-catalog), per level (blocking).
    fk_missing_total = 0
    fk_samples: dict[str, list[str]] = {}
    for geo_level in ncl.GEO_LEVEL_BY_REGION_TYPE.values():
        ref = f"{model_catalog}.{GEOGRAPHY_SCHEMA}.{geo_level}"
        missing = spark.sql(
            f"""
            SELECT DISTINCT p.geoid
            FROM {full} p
            LEFT ANTI JOIN (
                SELECT geoid FROM {ref} WHERE vintage = {US_VINTAGE}
            ) g ON p.geoid = g.geoid
            WHERE {where} AND p.geo_level = '{geo_level}'
            """
        ).collect()
        miss = [m["geoid"] for m in missing]
        if miss:
            fk_samples[geo_level] = miss[:10]
            fk_missing_total += len(miss)
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_processed_geoid_fk_integrity",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.FAIL,
        passed=fk_missing_total == 0,
        failing_row_count=fk_missing_total,
        total_row_count=total,
        details={"sample_missing_geoids": fk_samples, "vintage": US_VINTAGE}
        if fk_missing_total
        else None,
    )
    if fk_missing_total:
        raise ValueError(
            f"Conformed geoids absent from geography vintage {US_VINTAGE}: {fk_samples}"
        )

    # 3. obs_date FK to time.calendar_date (WARN — time covers 1900-2099; guard).
    cal = f"{model_catalog}.{TIME_SCHEMA}.calendar_date"
    try:
        out_of_time = spark.sql(
            f"""
            SELECT COUNT(*) AS n FROM {full} p
            LEFT ANTI JOIN {cal} d ON p.obs_date = d.date
            WHERE {where}
            """
        ).collect()[0]["n"]
    except Exception as e:  # noqa: BLE001 — time may not be built yet
        log.warning("time FK check skipped", extra={"error": str(e)})
        out_of_time = 0
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_processed_date_in_time_dim",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=out_of_time == 0,
        failing_row_count=int(out_of_time),
        total_row_count=total,
        details={"note": "obs_date outside time.calendar_date (covers 1900-2099)"}
        if out_of_time
        else None,
    )

    # 4. value ranges (WARN).
    bad_values = spark.sql(
        f"""
        SELECT COUNT(*) AS n FROM {full}
        WHERE {where} AND value IS NOT NULL AND (
            (variable = 'prcp' AND value < 0)
            OR (
                variable IN ('tavg', 'tmax', 'tmin')
                AND (value < {TEMP_MIN_C} OR value > {TEMP_MAX_C})
            )
        )
        """
    ).collect()[0]["n"]
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_processed_value_ranges",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=bad_values == 0,
        failing_row_count=int(bad_values),
        total_row_count=total,
        details={"prcp_min": 0, "temp_range_c": [TEMP_MIN_C, TEMP_MAX_C]} if bad_values else None,
    )

    # 5. natural-key uniqueness (blocking) — shared helper (ADR 0029).
    TableDQ(
        recorder=recorder, spark=spark, query_table=full, record_table=FULL_TABLE_REL, where=where
    ).unique(
        keys=["geo_level", "geoid", "variable", "obs_date"],
        check_name="nclimgrid_processed_key_uniqueness",
    )


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{FULL_TABLE_REL} IS "
        f"'NOAA nClimGrid-Daily area-averages conformed to FIPS geography "
        f"(geoid FK to geography.us_county/us_state vintage {US_VINTAGE}) and "
        f"time. Long-form: one row per geo_level/geoid/variable/obs_date; units "
        f"mm (prcp) / degC (temps). CONUS-only. Docs: {DOC_BASE}. ADR 0025.'"
    )


def _register_dataset(
    spark: SparkSession,
    catalog: str,
    pipeline_ref: str,
    crosswalk_url: str,
    cov_start: Any,
    cov_end: Any,
) -> None:
    """Register weather_processed.noaa_nclimgrid_daily metadata (ADR 0008).

    The first weather table to register: spatial + temporal + externally sourced,
    so it exercises the registration temporal/doc-URL extension (ADR 0025).
    """
    full = f"{catalog}.{FULL_TABLE_REL}"
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject="weather",
            layer="processed",
            description=(
                "NOAA nClimGrid-Daily area-averaged precipitation and temperature "
                "(prcp/tavg/tmax/tmin) conformed to FIPS geography and the time "
                "dimension. Long-form, daily, state + county. Units mm / degC."
            ),
            public_health_relevance=(
                "Daily temperature and precipitation covariates at state/county "
                "resolution for infectious-disease modeling (seasonality, "
                "environmental drivers of transmission)."
            ),
            spatial_resolution="us_state, us_county",
            spatial_coverage="conus",
            source_provider_code="noaa",
            source_url=SOURCE_URL,
            source_documentation_url=DOC_BASE,
            license="public domain (U.S. Government work, 17 U.S.C. 105)",
            dua_required=False,
            dua_reference=(
                f"No DUA. NOAA requests citation of nClimGrid-Daily ({SOURCE_DOI}). "
                f"NCEI->FIPS cross-reference: {crosswalk_url}"
            ),
            access_tier="open",
            external_maintainer_name="NOAA National Centers for Environmental Information",
            is_hosted=True,
            source_data_dictionary_url=README_URL,
            temporal_coverage_start=cov_start,
            temporal_coverage_end=cov_end,
            temporal_resolution="daily",
            known_limitations=(
                "CONUS-only: excludes Alaska, Hawaii, and US territories "
                "(nClimGrid-Daily area-averages cover the contiguous US only)."
            ),
            # Catalog-level lineage: derived from the raw landing. geography/time
            # are referenced (FK) dimensions, not derivation sources, so excluded.
            derived_from=[f"{catalog}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"],
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="merge_upsert",
            materialization_type="table",
            cluster_columns=["variable", "geo_level", "obs_date"],
            pipeline_reference=pipeline_ref,
        ),
    )


def run(
    catalog: str,
    model_catalog: str,
    start_year: int,
    end_year: int,
    data_engineers_group: str,
    crosswalk_url: str = DEFAULT_CROSSWALK_URL,
) -> None:
    log.info(
        "Conforming nClimGrid processed",
        extra={
            "catalog": catalog,
            "model_catalog": model_catalog,
            "start_year": start_year,
            "end_year": end_year,
        },
    )

    def _ensure(spark: SparkSession) -> None:
        _ensure_table(spark, catalog)

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark
        # Docs-first: pull NOAA's published NCEI->FIPS cross-reference at runtime.
        log.info("Fetching NCEI->FIPS cross-reference", extra={"url": crosswalk_url})
        ncei_to_fips = ncl.parse_ncei_fips_crosswalk(_http_text(crosswalk_url).splitlines())
        if not ncei_to_fips:
            raise ValueError(f"Empty NCEI->FIPS cross-reference from {crosswalk_url}")
        log.info("Loaded cross-reference", extra={"states": len(ncei_to_fips)})

        conformed, unconformed = _build_conform_map(
            spark, catalog, start_year, end_year, ncei_to_fips
        )
        if conformed:
            _write_processed(spark, catalog, start_year, end_year, conformed)
        _dq_checks(
            ctx.recorder,
            spark,
            catalog,
            model_catalog,
            start_year,
            end_year,
            unconformed,
            conformed,
        )

    def _register(spark: SparkSession) -> None:
        _comment_table(spark, catalog)
        # Materialized temporal coverage for the catalog (what actually landed).
        cov = spark.sql(
            f"SELECT MIN(obs_date) AS lo, MAX(obs_date) AS hi FROM {catalog}.{FULL_TABLE_REL}"
        ).collect()[0]
        _register_dataset(spark, catalog, PIPELINE_REF, crosswalk_url, cov["lo"], cov["hi"])

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=_register,
        # Processed is engineer-tier internal staging (ADR 0018): no analyst grant.
        grant=lambda spark: grants.grant_schema_engineer(
            spark, catalog, SCHEMA, data_engineers_group
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog", required=True, help="Source-aligned catalog holding weather_* (ecdh_<env>)."
    )
    parser.add_argument(
        "--model-catalog",
        required=True,
        help="Integrated catalog holding the geography/time reference (ecdh_model_<env>).",
    )
    parser.add_argument("--start-year", type=int, default=None,
                        help="Explicit window start (with --end-year); omit if using --recent-years.")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Explicit window end (with --start-year); omit if using --recent-years.")
    parser.add_argument(
        "--recent-years", type=int, default=None,
        help="Rolling window: conform [current_year - N, current_year]. Used by the scheduled monthly "
             "refresh instead of --start-year/--end-year.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument(
        "--crosswalk-url",
        default=DEFAULT_CROSSWALK_URL,
        help="NOAA NCEI->FIPS cross-reference URL (defaults to the nClimGrid doc/ folder).",
    )
    args = parser.parse_args()
    start_year, end_year = ncl.resolve_year_window(
        start_year=args.start_year, end_year=args.end_year, recent_years=args.recent_years
    )
    run(
        args.catalog,
        args.model_catalog,
        start_year,
        end_year,
        args.data_engineers_group,
        args.crosswalk_url,
    )


if __name__ == "__main__":
    main()
