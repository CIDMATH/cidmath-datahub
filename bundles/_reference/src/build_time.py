"""Build the canonical time reference tables in the integrated catalog.

Generates `time.calendar_date` and `time.epi_week` in `ecdh_model_<env>` using
the deterministic logic in `cidmath_datahub.reference.time`. Time is
computational reference data (ADR 0014), so update semantics is `full_refresh`
(ADR 0007) — each run regenerates and overwrites.

After writing, registers metadata rows in `_ops.dataset_catalog` and
`_ops.dataset_engineering` (ADR 0008) and applies reader-tier read grants to
both the engineers and analysts groups (ADR 0018). Reference data is canonical
and pipeline-owned, so human groups consume it read-only.

Usage:
    build_time.py --catalog ecdh_model_dev --start-year 2015 --end-year 2035 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants
from cidmath_datahub.common.dq import TableDQ
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.reference import time as rt

log = get_logger(__name__)

SCHEMA = "time"
PIPELINE_REF = "bundles/_reference/src/build_time.py"

CALENDAR_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("date", T.DateType(), False),
        T.StructField("year", T.IntegerType(), False),
        T.StructField("quarter", T.IntegerType(), False),
        T.StructField("month", T.IntegerType(), False),
        T.StructField("month_name", T.StringType(), False),
        T.StructField("day_of_month", T.IntegerType(), False),
        T.StructField("day_of_week_iso", T.IntegerType(), False),
        T.StructField("day_name", T.StringType(), False),
        T.StructField("day_of_year", T.IntegerType(), False),
        T.StructField("iso_year", T.IntegerType(), False),
        T.StructField("iso_week", T.IntegerType(), False),
        T.StructField("epi_year", T.IntegerType(), False),
        T.StructField("epi_week", T.IntegerType(), False),
        T.StructField("epi_week_id", T.StringType(), False),
        T.StructField("is_weekend", T.BooleanType(), False),
    ]
)

EPI_WEEK_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("epi_week_id", T.StringType(), False),
        T.StructField("epi_year", T.IntegerType(), False),
        T.StructField("epi_week", T.IntegerType(), False),
        T.StructField("start_date", T.DateType(), False),
        T.StructField("end_date", T.DateType(), False),
        T.StructField("label", T.StringType(), False),
    ]
)


def _register_dataset(
    spark: SparkSession,
    *,
    catalog: str,
    table: str,
    description: str,
    public_health_relevance: str,
    temporal_coverage_start: date,
    temporal_coverage_end: date,
    pipeline_reference: str,
) -> None:
    """Upsert metadata rows for a reference table into `_ops` (ADR 0008).

    Idempotent via MERGE on full_table_name. Reference tables are canonical, so
    layer is recorded as 'reference' and is_hosted is True.
    """
    full = f"{catalog}.{SCHEMA}.{table}"

    catalog_src = spark.createDataFrame(
        [
            (
                full,
                SCHEMA,
                "reference",
                description,
                public_health_relevance,
                temporal_coverage_start,
                temporal_coverage_end,
                "annual",  # temporal_resolution placeholder; calendar is daily, weeks weekly
                True,  # is_hosted
                "cidmath-data-team",  # owner
            )
        ],
        T.StructType(
            [
                T.StructField("full_table_name", T.StringType()),
                T.StructField("subject", T.StringType()),
                T.StructField("layer", T.StringType()),
                T.StructField("description", T.StringType()),
                T.StructField("public_health_relevance", T.StringType()),
                T.StructField("temporal_coverage_start", T.DateType()),
                T.StructField("temporal_coverage_end", T.DateType()),
                T.StructField("temporal_resolution", T.StringType()),
                T.StructField("is_hosted", T.BooleanType()),
                T.StructField("owner", T.StringType()),
            ]
        ),
    )
    catalog_src.createOrReplaceTempView("_tmp_catalog_src")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING _tmp_catalog_src AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            subject = s.subject, layer = s.layer, description = s.description,
            public_health_relevance = s.public_health_relevance,
            temporal_coverage_start = s.temporal_coverage_start,
            temporal_coverage_end = s.temporal_coverage_end,
            temporal_resolution = s.temporal_resolution,
            is_hosted = s.is_hosted, owner = s.owner, last_validated = CURRENT_DATE()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, subject, layer, description, public_health_relevance,
             temporal_coverage_start, temporal_coverage_end, temporal_resolution,
             is_hosted, owner, last_validated)
            VALUES
            (s.full_table_name, s.subject, s.layer, s.description, s.public_health_relevance,
             s.temporal_coverage_start, s.temporal_coverage_end, s.temporal_resolution,
             s.is_hosted, s.owner, CURRENT_DATE())
        """
    )

    eng_src = spark.createDataFrame(
        [(full, "full_refresh", "table", pipeline_reference, 1)],
        T.StructType(
            [
                T.StructField("full_table_name", T.StringType()),
                T.StructField("update_semantics", T.StringType()),
                T.StructField("materialization_type", T.StringType()),
                T.StructField("pipeline_reference", T.StringType()),
                T.StructField("schema_version", T.IntegerType()),
            ]
        ),
    )
    eng_src.createOrReplaceTempView("_tmp_eng_src")
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING _tmp_eng_src AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            update_semantics = s.update_semantics,
            materialization_type = s.materialization_type,
            pipeline_reference = s.pipeline_reference,
            last_refresh_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, update_semantics, materialization_type,
             pipeline_reference, schema_version, last_refresh_at)
            VALUES
            (s.full_table_name, s.update_semantics, s.materialization_type,
             s.pipeline_reference, s.schema_version, CURRENT_TIMESTAMP())
        """
    )
    log.info("Registered dataset metadata", extra={"table": full})


def run(
    catalog: str,
    start_year: int,
    end_year: int,
    data_engineers_group: str,
    analysts_group: str,
) -> None:
    log.info(
        "Building time reference tables",
        extra={"catalog": catalog, "start_year": start_year, "end_year": end_year},
    )

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical time reference: calendar dates and MMWR epi-weeks. "
            f"Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark

        # --- calendar_date ---
        # Sort ascending and write as a single file so the table is physically
        # ordered by date. Consumers should still ORDER BY date for a guaranteed
        # order, but the on-disk layout makes the common case sorted.
        cal_rows = rt.generate_calendar(date(start_year, 1, 1), date(end_year, 12, 31))
        cal_df = (
            spark.createDataFrame(cal_rows, schema=CALENDAR_SPARK_SCHEMA)
            .repartition(1)
            .sortWithinPartitions("date")
        )
        cal_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"{catalog}.{SCHEMA}.calendar_date"
        )
        log.info("Wrote calendar_date", extra={"rows": len(cal_rows)})

        # --- epi_week ---
        wk_rows = rt.generate_epi_weeks(start_year, end_year)
        wk_df = (
            spark.createDataFrame(wk_rows, schema=EPI_WEEK_SPARK_SCHEMA)
            .repartition(1)
            .sortWithinPartitions("start_date")
        )
        wk_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"{catalog}.{SCHEMA}.epi_week"
        )
        log.info("Wrote epi_week", extra={"rows": len(wk_rows)})

        # Natural-key uniqueness on the canonical PKs (ADR 0029). Generated
        # deterministically, so a duplicate is a generator regression -- FAIL +
        # raise. These are this build's first DQ records (previously none).
        TableDQ(
            recorder=ctx.recorder,
            spark=spark,
            query_table=f"{catalog}.{SCHEMA}.calendar_date",
            record_table=f"{SCHEMA}.calendar_date",
        ).unique(keys=["date"], check_name="calendar_date_pk_uniqueness")
        TableDQ(
            recorder=ctx.recorder,
            spark=spark,
            query_table=f"{catalog}.{SCHEMA}.epi_week",
            record_table=f"{SCHEMA}.epi_week",
        ).unique(keys=["epi_week_id"], check_name="epi_week_pk_uniqueness")

    def _register(spark: SparkSession) -> None:
        # --- table comments ---
        spark.sql(
            f"COMMENT ON TABLE {catalog}.{SCHEMA}.calendar_date IS "
            f"'One row per calendar date with ISO and MMWR epi-week attributes. "
            f"Reference table; full_refresh. ADR 0014.'"
        )
        spark.sql(
            f"COMMENT ON TABLE {catalog}.{SCHEMA}.epi_week IS "
            f"'One row per MMWR epidemiological week (Sunday-Saturday). "
            f"Reference table; full_refresh. ADR 0014.'"
        )

        # --- metadata registration ---
        _register_dataset(
            spark,
            catalog=catalog,
            table="calendar_date",
            description="One row per calendar date with ISO week and MMWR epi-week attributes.",
            public_health_relevance=(
                "Canonical date dimension for joining and aligning time series across "
                "all subjects; provides epi-week mapping used throughout surveillance."
            ),
            temporal_coverage_start=date(start_year, 1, 1),
            temporal_coverage_end=date(end_year, 12, 31),
            pipeline_reference=PIPELINE_REF,
        )
        _register_dataset(
            spark,
            catalog=catalog,
            table="epi_week",
            description="One row per MMWR epidemiological week with Sunday start and Saturday end.",
            public_health_relevance=(
                "Canonical epi-week dimension; the standard temporal grain for U.S. "
                "infectious disease surveillance reporting."
            ),
            temporal_coverage_start=date(start_year, 1, 1),
            temporal_coverage_end=date(end_year, 12, 31),
            pipeline_reference=PIPELINE_REF,
        )

    def _grant(spark: SparkSession) -> None:
        # --- grants: reader-tier (USE SCHEMA + SELECT) on the time schema ---
        # The time schema is canonical reference data, owned and written by this
        # bundle's deploy SP. Human groups -- both engineers and analysts -- consume
        # it read-only; neither hand-edits generated reference data (ADR 0018).
        # Both groups also need USE CATALOG on the catalog to traverse here; that
        # is granted by an admin in scripts/setup/grant_catalog_permissions.sql
        # (the deploy SP can't grant catalog-level privileges). See ADR 0018.
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

        # Verify the applied grants (deploy-time access gate; ADR 0018). Both groups
        # must hold exactly reader-tier on the time schema -- confirms the reads work
        # and that neither group was accidentally over-granted write access.
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
    log.info("Time reference build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--start-year", type=int, default=1900)
    parser.add_argument("--end-year", type=int, default=2099)
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.start_year,
        args.end_year,
        args.data_engineers_group,
        args.analysts_group,
    )


if __name__ == "__main__":
    main()
