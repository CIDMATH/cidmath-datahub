"""Build the analysis-ready wide daily weather table `weather.daily`.

ADR 0025 slice 3 (the deferred analysis layer). Reshapes the long-form
`weather_processed.noaa_nclimgrid_daily` (one row per geo_level/geoid/variable/
obs_date) into a **wide, analysis-ready** table — one row per (geo_level, geoid,
obs_date) with prcp/tavg/tmax/tmin as columns — and joins the geography labels
(county/state name, state context, HHS region) and time labels (epi-week, ISO week)
so consumers can align it to surveillance data with a single join.

Layering: lives in the **model catalog** `weather` schema (consumer-facing canonical,
analyst-readable), built FROM the source-catalog `weather_processed` table, and joined
to the `geography`/`time` references (both in the model catalog, vintage 2020 for
geography — the fixed nClimGrid county set, per ADR 0025). `merge_upsert` so prelim->
scaled revisions in processed propagate here on the next run.

This is a thin Spark/SQL entrypoint (ADR 0011); it does no HTTP (reads processed).

Usage:
    build_nclimgrid_analysis.py --catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --start-year 1951 --end-year 2026 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
from typing import Any

from pyspark.sql import SparkSession

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.dq import TableDQ
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build

log = get_logger(__name__)

SCHEMA = "weather"  # model-catalog analysis schema
TABLE = "daily"
FULL_TABLE_REL = f"{SCHEMA}.{TABLE}"
PIPELINE_REF = "bundles/weather/src/build_nclimgrid_analysis.py"

# Source (long-form processed) — in the source-aligned catalog.
SOURCE_SCHEMA = "weather_processed"
SOURCE_TABLE = "noaa_nclimgrid_daily"

# Geography vintage: nClimGrid applies a fixed modern county set 1951-present; the 2020
# Census vintage covers the whole series (ADR 0025). Matches the processed FK vintage.
GEO_VINTAGE = 2020

SOURCE_URL = "https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages"
DOC_BASE = "https://www.ncei.noaa.gov/data/nclimgrid-daily/doc"
SOURCE_DOI = "10.25921/c4gt-r169"


def _ensure_table(spark: SparkSession, model_catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {model_catalog}.{FULL_TABLE_REL} (
            geo_level STRING, geoid STRING, obs_date DATE,
            geo_name STRING, state_geoid STRING, state_name STRING, state_stusps STRING,
            state_hhs_region INT, state_hhs_region_name STRING,
            year INT, month INT, iso_week INT, epi_year INT, epi_week INT, epi_week_id STRING,
            is_weekend BOOLEAN,
            prcp DOUBLE, tavg DOUBLE, tmax DOUBLE, tmin DOUBLE,
            is_prelim BOOLEAN, built_at TIMESTAMP
        ) USING delta
        CLUSTER BY (geo_level, state_stusps, obs_date)
        """
    )


def _wide_select(catalog: str, model_catalog: str, start_year: int, end_year: int) -> str:
    """SELECT that pivots processed to wide and joins geography + time labels.

    County rows label from geography.us_county (its own `name` + parent-state columns);
    state rows from geography.us_state (its `name`/stusps/hhs_region). COALESCE unifies
    them so a single row shape covers both grains.
    """
    proc = f"{catalog}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"
    us_county = f"{model_catalog}.geography.us_county"
    us_state = f"{model_catalog}.geography.us_state"
    cal = f"{model_catalog}.time.calendar_date"
    return f"""
        WITH wide AS (
            SELECT geo_level, geoid, obs_date,
                MAX(CASE WHEN variable = 'prcp' THEN value END) AS prcp,
                MAX(CASE WHEN variable = 'tavg' THEN value END) AS tavg,
                MAX(CASE WHEN variable = 'tmax' THEN value END) AS tmax,
                MAX(CASE WHEN variable = 'tmin' THEN value END) AS tmin,
                MAX(CASE WHEN status = 'prelim' THEN 1 ELSE 0 END) = 1 AS is_prelim
            FROM {proc}
            WHERE year(obs_date) BETWEEN {start_year} AND {end_year}
            GROUP BY geo_level, geoid, obs_date
        )
        SELECT
            w.geo_level, w.geoid, w.obs_date,
            COALESCE(c.name, s.name) AS geo_name,
            COALESCE(c.state_geoid, s.geoid) AS state_geoid,
            COALESCE(c.state_name, s.name) AS state_name,
            COALESCE(c.state_stusps, s.stusps) AS state_stusps,
            COALESCE(c.state_hhs_region, s.hhs_region) AS state_hhs_region,
            COALESCE(c.state_hhs_region_name, s.hhs_region_name) AS state_hhs_region_name,
            t.year, t.month, t.iso_week, t.epi_year, t.epi_week, t.epi_week_id, t.is_weekend,
            w.prcp, w.tavg, w.tmax, w.tmin, w.is_prelim,
            current_timestamp() AS built_at
        FROM wide w
        LEFT JOIN {us_county} c
            ON w.geo_level = 'us_county' AND c.geoid = w.geoid AND c.vintage = {GEO_VINTAGE}
        LEFT JOIN {us_state} s
            ON w.geo_level = 'us_state' AND s.geoid = w.geoid AND s.vintage = {GEO_VINTAGE}
        LEFT JOIN {cal} t ON t.date = w.obs_date
    """


def _merge(spark: SparkSession, model_catalog: str, select_sql: str) -> None:
    """merge_upsert the wide rows on the natural key (geo_level, geoid, obs_date)."""
    spark.sql(select_sql).createOrReplaceTempView("_tmp_weather_daily")
    spark.sql(
        f"""
        MERGE INTO {model_catalog}.{FULL_TABLE_REL} AS t
        USING _tmp_weather_daily AS s
        ON t.geo_level = s.geo_level AND t.geoid = s.geoid AND t.obs_date = s.obs_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _dq_checks(
    ctx: BuildContext, model_catalog: str, start_year: int, end_year: int
) -> None:
    full = f"{model_catalog}.{FULL_TABLE_REL}"
    where = f"year(obs_date) BETWEEN {start_year} AND {end_year}"
    dq = TableDQ(
        recorder=ctx.recorder,
        spark=ctx.spark,
        query_table=full,
        record_table=FULL_TABLE_REL,
        where=where,
    )
    # PK uniqueness (blocking) — one row per geo-day.
    dq.unique(
        keys=["geo_level", "geoid", "obs_date"],
        check_name="weather_daily_key_uniqueness",
    )
    # Every row must carry its geography + time labels (blocking): processed already FK-validates
    # geoid ∈ geography and obs_date ∈ time, so a null label here is a join regression.
    dq.not_null(
        columns=["geo_level", "geoid", "obs_date", "geo_name", "state_stusps", "epi_week_id"],
        check_name="weather_daily_labels_not_null",
    )


def _register_dataset(
    spark: SparkSession, catalog: str, model_catalog: str, cov_start: Any, cov_end: Any
) -> None:
    full = f"{model_catalog}.{FULL_TABLE_REL}"
    registration.register_dataset(
        spark,
        model_catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject="weather",
            layer="analysis",
            description=(
                "Analysis-ready daily weather: one row per (geo_level, geoid, obs_date) with "
                "prcp/tavg/tmax/tmin as columns, plus geography (county/state name, state context, "
                "HHS region) and time (epi-week, ISO week) labels. Units mm / degC. Wide form of "
                "weather_processed.noaa_nclimgrid_daily; state + county; CONUS-only."
            ),
            public_health_relevance=(
                "Join-ready temperature and precipitation covariates at state/county resolution, "
                "pre-labeled with epi-week and HHS region for direct alignment to surveillance "
                "time series."
            ),
            spatial_resolution="us_state, us_county",
            spatial_coverage="conus",
            source_provider_code="noaa",
            source_url=SOURCE_URL,
            source_documentation_url=DOC_BASE,
            license="public domain (U.S. Government work, 17 U.S.C. 105)",
            dua_required=False,
            dua_reference=f"No DUA. NOAA requests citation of nClimGrid-Daily ({SOURCE_DOI}).",
            access_tier="open",
            external_maintainer_name="NOAA National Centers for Environmental Information",
            is_hosted=True,
            source_data_dictionary_url=f"{DOC_BASE}/nclimgrid-daily_v1-0-0_readme-web.txt",
            temporal_coverage_start=cov_start,
            temporal_coverage_end=cov_end,
            temporal_resolution="daily",
            known_limitations=(
                "CONUS-only: excludes Alaska, Hawaii, and US territories. Geography labels at the "
                "2020 Census vintage (nClimGrid's fixed modern county set)."
            ),
            # Derived from the processed long-form table; geography/time are FK label dimensions.
            derived_from=[f"{catalog}.{SOURCE_SCHEMA}.{SOURCE_TABLE}"],
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="merge_upsert",
            materialization_type="table",
            cluster_columns=["geo_level", "state_stusps", "obs_date"],
            pipeline_reference=PIPELINE_REF,
        ),
    )


def run(
    catalog: str,
    model_catalog: str,
    start_year: int,
    end_year: int,
    data_engineers_group: str,
    analysts_group: str,
) -> None:
    log.info(
        "Building analysis-ready weather.daily",
        extra={"catalog": catalog, "model_catalog": model_catalog,
               "start_year": start_year, "end_year": end_year},
    )

    def _ensure(spark: SparkSession) -> None:
        _ensure_table(spark, model_catalog)

    def _work(ctx: BuildContext) -> None:
        select_sql = _wide_select(catalog, model_catalog, start_year, end_year)
        _merge(ctx.spark, model_catalog, select_sql)
        _dq_checks(ctx, model_catalog, start_year, end_year)

    def _register(spark: SparkSession) -> None:
        cov = spark.sql(
            f"SELECT MIN(obs_date) AS lo, MAX(obs_date) AS hi FROM {model_catalog}.{FULL_TABLE_REL}"
        ).collect()[0]
        _register_dataset(spark, catalog, model_catalog, cov["lo"], cov["hi"])

    def _grant(spark: SparkSession) -> None:
        # Consumer-facing analysis layer: reader-tier for BOTH engineers and analysts (ADR 0018).
        for group in (data_engineers_group, analysts_group):
            grants.grant_schema_reader(spark, model_catalog, SCHEMA, group)
            grants.verify_schema_reader(spark, model_catalog, SCHEMA, group)

    run_build(
        catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=_register,
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog", required=True, help="Source-aligned catalog holding weather_processed (ecdh_<env>)."
    )
    parser.add_argument(
        "--model-catalog", required=True,
        help="Integrated catalog: writes weather.daily, reads geography/time (ecdh_model_<env>).",
    )
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.model_catalog,
        args.start_year,
        args.end_year,
        args.data_engineers_group,
        args.analysts_group,
    )


if __name__ == "__main__":
    main()
