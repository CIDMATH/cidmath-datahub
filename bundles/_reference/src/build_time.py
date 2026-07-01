"""Build the canonical time reference (`time.calendar_date` + `time.epi_week`).

Generated reference (ADR 0014): the deterministic calendar + MMWR epi-week logic
lives in `cidmath_datahub.reference.time` (pure functions returning plain Python
rows). This entrypoint is the thin Spark/IO layer.

Migrated onto the shared `build_reference` builder (ADR 0036) via the **static**
(non-vintaged) path with **Volume-backed generated landings** (ADR 0039, amended
2026-06-30 — generated reference lands in the Volume too, removing the old
"generated = no Volume" carve-out): the generator writes a parquet payload to the
landing Volume, the 1:1 raw Delta (`time_raw.*`) is built from it, and the
canonical (`time.*`) is promoted to the model catalog. `full_refresh`
(SNAPSHOT_PER_RUN landing: same-day re-runs skip the regenerate, a new day refreshes).

Usage:
    build_time.py --catalog ecdh_model_dev --source-catalog ecdh_dev \\
        --start-year 1900 --end-year 2099 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
from datetime import date
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
from cidmath_datahub.reference import time as rt

log = get_logger(__name__)

SCHEMA = "time"
PIPELINE_REF = "bundles/_reference/src/build_time.py"

# Time is generated, not externally sourced. The data is computed in-house; the
# definitions follow the CDC MMWR epidemiological-week convention (the surveillance-
# authoritative rule: Sun-Sat weeks, week 1 = the first with >=4 days in the year) and
# ISO 8601 for the Gregorian/ISO-week calendar columns. Recorded as generated provenance
# (ADR 0006/0008/0039 amendment). Verified against the CDC MMWR Week Log 2025-2026
# (105/105 weeks exact, incl. the 53-week 2025 case) on 2026-06-30.
MMWR_DOC_URL = "https://ndc.services.cdc.gov/wp-content/uploads/MMWR_week_overview.pdf"
ISO_8601_URL = "https://www.iso.org/iso-8601-date-and-time-format.html"

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

# DDL column lists (kept in lockstep with the schemas above; declared once, fail loud).
_CALENDAR_DDL_COLS = (
    "date DATE, year INT, quarter INT, month INT, month_name STRING, day_of_month INT, "
    "day_of_week_iso INT, day_name STRING, day_of_year INT, iso_year INT, iso_week INT, "
    "epi_year INT, epi_week INT, epi_week_id STRING, is_weekend BOOLEAN"
)
_EPI_WEEK_DDL_COLS = (
    "epi_week_id STRING, epi_year INT, epi_week INT, start_date DATE, end_date DATE, label STRING"
)


def _write_payload_parquet(rows: list[dict[str, Any]], schema: T.StructType, path: str) -> None:
    """Write generated rows to a single parquet payload in the landing Volume.

    Runs in Phase 0 (`fetch_to_volume`) with **no Spark** — so the generator output
    (plain Python dicts) is materialized via pandas/pyarrow. Column order is pinned to
    the Spark schema; `read_from_volume` re-asserts types on the way back in.
    """
    columns = [f.name for f in schema.fields]
    pdf = pd.DataFrame(rows)[columns]
    pdf.to_parquet(path, index=False)
    log.info("wrote generated payload", extra={"path": path, "rows": len(rows)})


def _read_payload_parquet(ctx: BuildContext, path: str, schema: T.StructType) -> Any:
    """Read the parquet payload back and cast to the canonical schema (type-faithful)."""
    df = ctx.spark.read.parquet(path)
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def build_time_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    start_year: int,
    end_year: int,
    data_engineers_group: str,
    analysts_group: str,
) -> tuple[str, str]:
    """Build `time.calendar_date` + `time.epi_week` via the shared builder's static path."""
    raw_cal = f"{source_catalog}.{SCHEMA}_raw.calendar_date"
    raw_wk = f"{source_catalog}.{SCHEMA}_raw.epi_week"
    cov_start = date(start_year, 1, 1)
    cov_end = date(end_year, 12, 31)

    cal_desc = "One row per calendar date with ISO week and MMWR epi-week attributes."
    cal_phr = (
        "Canonical date dimension for joining and aligning time series across all subjects; "
        "provides the epi-week mapping used throughout surveillance."
    )
    wk_desc = "One row per MMWR epidemiological week (Sunday start, Saturday end)."
    wk_phr = (
        "Canonical epi-week dimension; the standard temporal grain for U.S. infectious "
        "disease surveillance reporting."
    )

    def _ensure_staging(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{SCHEMA}_raw "
            f"COMMENT 'Source-catalog raw landings for time (1:1 generator output). ADR 0037/0039.'"
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_cal} ({_CALENDAR_DDL_COLS}) USING DELTA")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {raw_wk} ({_EPI_WEEK_DDL_COLS}) USING DELTA")

    def _ensure_canonical(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{SCHEMA} "
            f"COMMENT 'Canonical time reference: calendar dates and MMWR epi-weeks. ADR 0014.'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.calendar_date "
            f"({_CALENDAR_DDL_COLS}) USING DELTA"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{SCHEMA}.epi_week "
            f"({_EPI_WEEK_DDL_COLS}) USING DELTA"
        )

    # --- Volume-backed generated landings (ADR 0039 amendment): generator -> parquet -> raw ---
    def _fetch_calendar(_v: int, vdir: str) -> None:
        rows = rt.generate_calendar(cov_start, cov_end)
        _write_payload_parquet(rows, CALENDAR_SPARK_SCHEMA, f"{vdir}/calendar_date.parquet")

    def _read_calendar(ctx: BuildContext, _v: int, vdir: str) -> Any:
        return _read_payload_parquet(ctx, f"{vdir}/calendar_date.parquet", CALENDAR_SPARK_SCHEMA)

    def _fetch_epi(_v: int, vdir: str) -> None:
        rows = rt.generate_epi_weeks(start_year, end_year)
        _write_payload_parquet(rows, EPI_WEEK_SPARK_SCHEMA, f"{vdir}/epi_week.parquet")

    def _read_epi(ctx: BuildContext, _v: int, vdir: str) -> Any:
        return _read_payload_parquet(ctx, f"{vdir}/epi_week.parquet", EPI_WEEK_SPARK_SCHEMA)

    # --- promote: raw is already canonical-shaped; identity copy, physically date-ordered ---
    def _promote_calendar(ctx: BuildContext, _v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {raw_cal} ORDER BY date")

    def _promote_epi(ctx: BuildContext, _v: int) -> Any:
        return ctx.spark.sql(f"SELECT * FROM {raw_wk} ORDER BY start_date")

    # --- staging DQ: natural-key uniqueness on the canonical PKs (ADR 0029); a duplicate is
    #     a generator regression -> FAIL + raise (gates the promote). ---
    def _validate_calendar(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table=f"{SCHEMA}_raw.calendar_date")
        dq.unique(keys=["date"], check_name="calendar_date_pk_uniqueness")
        dq.not_null(
            columns=[f.name for f in CALENDAR_SPARK_SCHEMA.fields],
            check_name="calendar_date_core_not_null",
        )

    def _validate_epi(ctx: BuildContext, staging_fqn: str) -> None:
        dq = make_staging_dq(ctx, staging_fqn, record_table=f"{SCHEMA}_raw.epi_week")
        dq.unique(keys=["epi_week_id"], check_name="epi_week_pk_uniqueness")
        dq.not_null(
            columns=[f.name for f in EPI_WEEK_SPARK_SCHEMA.fields],
            check_name="epi_week_core_not_null",
        )

    base_entry = registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SCHEMA,
        layer="reference",
        description=cal_desc,
        public_health_relevance=cal_phr,
        spatial_resolution="n/a",
        spatial_coverage="n/a (temporal reference)",
        source_provider_code="generated",
        source_origin_code="cdc",  # MMWR epi-week definition is CDC's; the data is generated from it
        source_url=MMWR_DOC_URL,
        source_documentation_url=MMWR_DOC_URL,
        source_data_dictionary_url=ISO_8601_URL,  # calendar/ISO-week column definitions
        license="Generated computational reference (no external license; CDC MMWR epi-weeks + ISO 8601 calendar).",
        dua_required=False,
        dua_reference="",
        access_tier="open",
        external_maintainer_name="cidmath-datahub (generated)",
        is_hosted=False,
        temporal_coverage_start=cov_start,
        temporal_coverage_end=cov_end,
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
                table="calendar_date",
                landing_retention=LandingRetention.SNAPSHOT_PER_RUN,
                fetch_to_volume=_fetch_calendar,
                read_from_volume=_read_calendar,
                description="Generated calendar payload (1:1 raw; generator output landed as parquet).",
            ),
            RawLanding(
                table="epi_week",
                landing_retention=LandingRetention.SNAPSHOT_PER_RUN,
                fetch_to_volume=_fetch_epi,
                read_from_volume=_read_epi,
                description="Generated MMWR epi-week payload (1:1 raw; generator output landed as parquet).",
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table="calendar_date",
                reads=("calendar_date",),
                promote=_promote_calendar,
                validate_staging=_validate_calendar,
                description=cal_desc,
                public_health_relevance=cal_phr,
            ),
            CanonicalOutput(
                canonical_table="epi_week",
                reads=("epi_week",),
                promote=_promote_epi,
                validate_staging=_validate_epi,
                description=wk_desc,
                public_health_relevance=wk_phr,
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="full_refresh",
        static=True,
    )
    return build_reference(spec)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Model catalog (ecdh_model_<env>).")
    parser.add_argument("--source-catalog", required=True, help="Source catalog (ecdh_<env>).")
    parser.add_argument("--start-year", type=int, default=1900)
    parser.add_argument("--end-year", type=int, default=2099)
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    build_time_layered(
        source_catalog=args.source_catalog,
        model_catalog=args.catalog,
        start_year=args.start_year,
        end_year=args.end_year,
        data_engineers_group=args.data_engineers_group,
        analysts_group=args.analysts_group,
    )
    log.info("Time reference build complete", extra={"catalog": args.catalog})


if __name__ == "__main__":
    main()
