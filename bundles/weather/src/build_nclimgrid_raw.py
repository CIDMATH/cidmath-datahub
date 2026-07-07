"""Land NOAA nClimGrid-Daily area averages into weather_raw.noaa_nclimgrid_daily.

ADR 0025 slice 1 (raw only). Faithful ingest: discover the cty + ste monthly
CSVs under ``access/averages/<year>/``, download them politely, **land each verbatim
in the ``weather_raw._landing`` UC Volume (ADR 0039) before parsing** so the raw
table is reproducible from the stored payloads without re-fetching NOAA (which
revises prelim->scaled and rewrites files); the landing is SNAPSHOT_PER_RUN (a fresh
verbatim snapshot per run). Then parse via ``cidmath_datahub.weather.nclimgrid`` (NCEI
region codes preserved exactly as published) and ``merge_upsert`` into
``weather_raw.noaa_nclimgrid_daily`` keyed on
(region_type, region_code, variable, obs_date). There is **no** NCEI->FIPS
conformance here — that is the processed layer (a later slice); raw stays
faithful to the source so it can be reviewed before processing is designed.

Parameterized by ``--start-year`` / ``--end-year`` so a recent window can be
pulled for review before the full 1951-present backfill. ``merge_upsert``
(ADR 0025) absorbs the prelim->scaled revision when recent months are
re-pulled. CONUS-only (no AK/HI/territories) — a documented limitation.

Discovery lists the year directory and matches files via
``nclimgrid.parse_average_filename`` rather than constructing names, so the
scaled-vs-prelim suffix is handled without guessing. Downloads are throttled
(``--request-delay``) to be polite to NCEI's web-accessible folder.

This is a thin IO + Spark entrypoint (ADR 0011); all parsing logic is in the
unit-tested ``cidmath_datahub.weather.nclimgrid`` module.

Usage:
    build_nclimgrid_raw.py --catalog ecdh_dev --start-year 2024 --end-year 2026 \\
        --data-engineers-group ecdh-data-engineers
"""

from __future__ import annotations

import argparse
import os
import time
import urllib.request
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants
from cidmath_datahub.common.dq import DQRecorder, TableDQ
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.weather import nclimgrid as ncl

log = get_logger(__name__)

SCHEMA = "weather_raw"
TABLE = "noaa_nclimgrid_daily"
FULL_TABLE_REL = f"{SCHEMA}.{TABLE}"
PIPELINE_REF = "bundles/weather/src/build_nclimgrid_raw.py"

BASE_URL = "https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages"
# geodata/NCEI 403 default Python user-agents on some endpoints; send a real one.
USER_AGENT = "Mozilla/5.0 cidmath-datahub/1.0 (+https://github.com/cidmath)"
HTTP_TIMEOUT = 120
DEFAULT_REQUEST_DELAY = 0.5  # seconds between file downloads (politeness)

RAW_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("region_type", T.StringType(), False),
        T.StructField("region_code", T.StringType(), False),  # NCEI code, verbatim
        T.StructField("region_name", T.StringType(), False),
        T.StructField("variable", T.StringType(), False),  # prcp/tavg/tmax/tmin
        T.StructField("obs_date", T.DateType(), False),
        T.StructField("value", T.DoubleType(), True),  # sentinel -> NULL
        T.StructField("status", T.StringType(), False),  # scaled | prelim
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


def _http_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _discover_year_files(year: int, region_types: set[str]) -> list[tuple[str, dict[str, Any]]]:
    """List a year directory and return [(filename, parsed-meta)] for target files.

    Keeps only our region types and the four variables; ignores version files and
    other region types (cen/div/hc1/...).
    """
    html = _http_text(f"{BASE_URL}/{year}/")
    out: list[tuple[str, dict[str, Any]]] = []
    for name in ncl.extract_csv_links(html):
        meta = ncl.parse_average_filename(name)
        if meta and meta["region_type"] in region_types and meta["variable"] in ncl.VARIABLES:
            out.append((name, meta))
    log.info("Discovered nClimGrid files", extra={"year": year, "files": len(out)})
    return out


LANDING_VOLUME = "_landing"  # weather_raw._landing UC Volume (verbatim payloads, ADR 0039)


def _ensure_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{FULL_TABLE_REL} (
            region_type STRING, region_code STRING, region_name STRING,
            variable STRING, obs_date DATE, value DOUBLE, status STRING,
            source_file STRING, ingested_at TIMESTAMP
        ) USING delta
        CLUSTER BY (variable, region_type, obs_date)
        """
    )


def _ensure_landing_volume(spark: SparkSession, catalog: str, data_engineers_group: str) -> None:
    """Create the engineer-only landing Volume and grant file access (ADR 0039).

    READ/WRITE VOLUME is volume-scoped and is NOT conferred by ownership for FUSE file
    access, so grant it explicitly to the running principal (its own writes/reads) and the
    engineer group (inspection) — same pattern as the shared reference builder's Phase 0.
    """
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{SCHEMA}.{LANDING_VOLUME} "
        f"COMMENT 'Verbatim NOAA nClimGrid CSV payloads — engineer-only landing zone. ADR 0039.'"
    )
    principal = spark.sql("SELECT current_user()").collect()[0][0]
    for grantee in {principal, data_engineers_group}:
        grants.grant_volume_engineer(spark, catalog, SCHEMA, LANDING_VOLUME, grantee)


def _landing_dir(catalog: str, run_date: str) -> str:
    """Per-run snapshot dir under the landing Volume (SNAPSHOT_PER_RUN — NOAA revises
    prelim→scaled, so each run captures a fresh verbatim snapshot; ADR 0039)."""
    return f"/Volumes/{catalog}/{SCHEMA}/{LANDING_VOLUME}/snapshot_date={run_date}"


def _merge_file(spark: SparkSession, catalog: str, rows: list[dict[str, Any]]) -> None:
    """merge_upsert one file's rows on the natural key (ADR 0025).

    Per-file batches (county ~ 97k rows) keep driver memory bounded and make
    the load idempotent — re-pulling a month (prelim -> scaled) upserts the
    revised values rather than duplicating.
    """
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=RAW_SPARK_SCHEMA)
    df.createOrReplaceTempView("_tmp_nclimgrid_raw")
    spark.sql(
        f"""
        MERGE INTO {catalog}.{FULL_TABLE_REL} AS t
        USING _tmp_nclimgrid_raw AS s
        ON t.region_type = s.region_type AND t.region_code = s.region_code
           AND t.variable = s.variable AND t.obs_date = s.obs_date
        WHEN MATCHED THEN UPDATE SET
            region_name = s.region_name, value = s.value, status = s.status,
            source_file = s.source_file, ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _dq_checks(
    recorder: DQRecorder,
    spark: SparkSession,
    catalog: str,
    start_year: int,
    end_year: int,
    files_loaded: int,
) -> None:
    """Post-load DQ on weather_raw.noaa_nclimgrid_daily for the loaded year range (ADR 0009).

    Query-based (not in-memory) so it scales to the full backfill:
      1. natural-key uniqueness over the loaded range — FAIL.
      2. region_type / variable vocabulary — FAIL (faithful-landing guard).
      3. value null rate — INFO (sentinel-derived missingness, expected).
      4. files-discovered sanity — WARN (zero files for a non-empty range is wrong).
      5. cell completeness/density over the loaded window — WARN (each elapsed
         year x variable x region_type should be days_in_year x full region set).
    """
    full = f"{catalog}.{FULL_TABLE_REL}"
    where = f"year(obs_date) BETWEEN {start_year} AND {end_year}"
    dq = TableDQ(
        recorder=recorder, spark=spark, query_table=full, record_table=FULL_TABLE_REL, where=where
    )

    # 1. Natural-key uniqueness (blocking) — shared helper (ADR 0029); records the
    # UNIQUENESS check and raises on a duplicate key, replacing the hand-written
    # dup-count + record + raise.
    dq.unique(
        keys=["region_type", "region_code", "variable", "obs_date"],
        check_name="nclimgrid_raw_key_uniqueness",
    )

    total = spark.sql(f"SELECT COUNT(*) AS n FROM {full} WHERE {where}").collect()[0]["n"]

    bad_vocab = spark.sql(
        f"""
        SELECT COUNT(*) AS n FROM {full} WHERE {where}
        AND (region_type NOT IN ('cty', 'ste') OR variable NOT IN ('prcp', 'tavg', 'tmax', 'tmin'))
        """
    ).collect()[0]["n"]
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_raw_vocab",
        category=DQCategory.SCHEMA,
        severity=DQSeverity.FAIL,
        passed=bad_vocab == 0,
        failing_row_count=int(bad_vocab),
        total_row_count=int(total),
    )
    if bad_vocab:
        raise ValueError(f"Unexpected region_type/variable values: {bad_vocab}")

    nulls = spark.sql(
        f"SELECT COUNT(*) AS n FROM {full} WHERE {where} AND value IS NULL"
    ).collect()[0]["n"]
    null_pct = (nulls / total * 100) if total else 0.0
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_raw_value_null_rate",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.INFO,
        passed=True,
        failing_row_count=int(nulls),
        total_row_count=int(total),
        details={"null_pct": round(null_pct, 3)},
    )

    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_raw_files_discovered",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=files_loaded > 0,
        failing_row_count=0 if files_loaded > 0 else 1,
        total_row_count=files_loaded,
        details={"files_loaded": files_loaded, "year_range": [start_year, end_year]},
    )

    # 5. Cell completeness / density (WARN, CARDINALITY). Each fully-elapsed
    # (year, variable, region_type) cell in the loaded window should be dense:
    # every calendar day of the year x the full region set. Catches missing
    # months / days / regions that a month-count check misses. The current
    # calendar year is exempt (legitimately partial). Expected region cardinality
    # is the union across the whole table (the fixed nClimGrid county/state set),
    # so even a single-year run compares against the canonical set, not itself.
    # WARN, not FAIL: a genuine early-source gap should surface, not abort a
    # long backfill chunk; re-run the chunk if it's an ingest gap.
    incomplete = spark.sql(
        f"""
        WITH full_set AS (
            SELECT region_type, COUNT(DISTINCT region_code) AS n_regions
            FROM {full}
            GROUP BY region_type
        ),
        cell AS (
            SELECT year(obs_date) AS yr, variable, region_type,
                   COUNT(*) AS actual_rows,
                   COUNT(DISTINCT obs_date) AS distinct_days,
                   COUNT(DISTINCT region_code) AS distinct_regions
            FROM {full}
            WHERE {where} AND year(obs_date) < year(current_date())
            GROUP BY year(obs_date), variable, region_type
        )
        SELECT c.yr, c.variable, c.region_type, c.actual_rows, c.distinct_days,
               datediff(make_date(c.yr + 1, 1, 1), make_date(c.yr, 1, 1)) AS days_in_year,
               c.distinct_regions, f.n_regions AS expected_regions
        FROM cell c JOIN full_set f ON c.region_type = f.region_type
        WHERE c.distinct_days <> datediff(make_date(c.yr + 1, 1, 1), make_date(c.yr, 1, 1))
           OR c.distinct_regions <> f.n_regions
           OR c.actual_rows
              <> datediff(make_date(c.yr + 1, 1, 1), make_date(c.yr, 1, 1)) * f.n_regions
        ORDER BY c.yr, c.variable, c.region_type
        """
    ).collect()
    incomplete_cells = [
        {
            "year": r["yr"],
            "variable": r["variable"],
            "region_type": r["region_type"],
            "actual_rows": int(r["actual_rows"]),
            "expected_rows": int(r["days_in_year"]) * int(r["expected_regions"]),
        }
        for r in incomplete
    ]
    recorder.record(
        table_name=FULL_TABLE_REL,
        check_name="nclimgrid_raw_cell_completeness",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=not incomplete_cells,
        failing_row_count=len(incomplete_cells),
        total_row_count=int(total),
        details={"sample_incomplete_cells": incomplete_cells[:10]} if incomplete_cells else None,
    )


def run(
    catalog: str,
    start_year: int,
    end_year: int,
    data_engineers_group: str,
    region_types: set[str] | None = None,
    request_delay: float = DEFAULT_REQUEST_DELAY,
) -> None:
    region_types = region_types or {"cty", "ste"}
    log.info(
        "Ingesting nClimGrid raw",
        extra={
            "catalog": catalog,
            "start_year": start_year,
            "end_year": end_year,
            "region_types": sorted(region_types),
        },
    )

    def _ensure(spark: SparkSession) -> None:
        _ensure_table(spark, catalog)
        _ensure_landing_volume(spark, catalog, data_engineers_group)

    def _work(ctx: BuildContext) -> None:
        spark = ctx.spark
        run_date = datetime.now(tz=UTC).date().isoformat()
        landing = _landing_dir(catalog, run_date)
        os.makedirs(landing, exist_ok=True)
        files_loaded = 0
        for year in range(start_year, end_year + 1):
            for name, meta in _discover_year_files(year, region_types):
                text = _http_text(f"{BASE_URL}/{year}/{name}")
                # Land the payload verbatim in the Volume BEFORE parsing (ADR 0039), then build
                # the raw rows FROM the landed file — so the raw table is reproducible from the
                # Volume without re-fetching NOAA (which revises prelim→scaled and rewrites files).
                payload_path = os.path.join(landing, name)
                with open(payload_path, "w", encoding="utf-8") as fh:
                    fh.write(text)
                with open(payload_path, encoding="utf-8") as fh:
                    lines = fh.read().splitlines()
                rows = ncl.parse_average_csv(lines, source_file=name)
                now = datetime.now(tz=UTC)
                for r in rows:
                    r["status"] = meta["status"]
                    r["ingested_at"] = now
                _merge_file(spark, catalog, rows)
                files_loaded += 1
                time.sleep(request_delay)
            log.info("Year complete", extra={"year": year, "files_loaded_so_far": files_loaded})
        log.info("Landed payloads in Volume", extra={"dir": landing, "files": files_loaded})
        _dq_checks(ctx.recorder, spark, catalog, start_year, end_year, files_loaded)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        # Raw is engineer-tier internal staging (ADR 0018): not catalogued, no analyst grant.
        register=None,
        grant=lambda spark: grants.grant_schema_engineer(
            spark, catalog, SCHEMA, data_engineers_group
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Source-aligned catalog (ecdh_<env>).")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument(
        "--region-types",
        default="cty,ste",
        help="Comma-separated nClimGrid region types to ingest (default cty,ste).",
    )
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY)
    args = parser.parse_args()

    region_types = {r.strip() for r in args.region_types.split(",") if r.strip()}
    run(
        args.catalog,
        args.start_year,
        args.end_year,
        args.data_engineers_group,
        region_types,
        args.request_delay,
    )


if __name__ == "__main__":
    main()
