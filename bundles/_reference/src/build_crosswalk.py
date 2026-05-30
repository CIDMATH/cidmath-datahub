"""Build geography.us_crosswalk: NHGIS bg-sourced 2010<->2020 crosswalks as published.

Slice 2b (ADR 0021). Downloads the six bg-sourced crosswalk file sets from
NHGIS's supplemental-data endpoint (no extract submission), normalizes them
into the long-form ``geography.us_crosswalk`` table (one row per source × target ×
weight_kind), applies the standard weight-sum DQ per (file, weight_kind),
registers metadata, and applies Liquid Clustering.

The exact NHGIS directory/filename for each crosswalk is verified on first
download — a 404 fails loud with the attempted URL. Weight kinds present in
each file are mapped through ``geo.NHGIS_WEIGHT_COLUMNS`` into our controlled
vocabulary; files carrying different subsets are absorbed naturally by the
long-form shape.

Runs as a separate job (``crosswalk_job.yml``) with a lighter environment than
the geometry build — just ``ipumspy`` and ``pandas``, no geopandas/shapely.
Reuses the ``ipums_secret_*`` bundle variables already added in slice 1.

Usage:
    build_crosswalk.py --catalog ecdh_model_dev \\
        --ipums-secret-scope ecdh-dev-ipums --ipums-secret-key nhgis_api_key
"""

from __future__ import annotations

import argparse
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import registration
from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import geography as geo

log = get_logger(__name__)

SCHEMA = "geography"
TABLE = "us_crosswalk"

# The six bg-sourced 2010<->2020 NHGIS crosswalk file sets to ship (ADR 0021).
# NHGIS national crosswalk files live directly under ``/crosswalks/{filename}``
# on the supplemental-data endpoint (verified pattern from blk1990_blk2010,
# blk2010_blk2020, etc.). The loader builds the URL via ipums.base_url and
# downloads via ipums.get(); a 404 fails loud with the attempted URL.
CROSSWALK_FILES: list[dict[str, Any]] = [
    {
        "source_level": "bg",
        "source_vintage": 2010,
        "target_level": "bg",
        "target_vintage": 2020,
        "filename": "nhgis_bg2010_bg2020.zip",
        "csv_stem": "nhgis_bg2010_bg2020",
    },
    {
        "source_level": "bg",
        "source_vintage": 2010,
        "target_level": "tract",
        "target_vintage": 2020,
        "filename": "nhgis_bg2010_tr2020.zip",
        "csv_stem": "nhgis_bg2010_tr2020",
    },
    {
        "source_level": "bg",
        "source_vintage": 2010,
        "target_level": "county",
        "target_vintage": 2020,
        "filename": "nhgis_bg2010_co2020.zip",
        "csv_stem": "nhgis_bg2010_co2020",
    },
    {
        "source_level": "bg",
        "source_vintage": 2020,
        "target_level": "bg",
        "target_vintage": 2010,
        "filename": "nhgis_bg2020_bg2010.zip",
        "csv_stem": "nhgis_bg2020_bg2010",
    },
    {
        "source_level": "bg",
        "source_vintage": 2020,
        "target_level": "tract",
        "target_vintage": 2010,
        "filename": "nhgis_bg2020_tr2010.zip",
        "csv_stem": "nhgis_bg2020_tr2010",
    },
    {
        "source_level": "bg",
        "source_vintage": 2020,
        "target_level": "county",
        "target_vintage": 2010,
        "filename": "nhgis_bg2020_co2010.zip",
        "csv_stem": "nhgis_bg2020_co2010",
    },
]

# NHGIS column-name prefix per geographic level (for {prefix}{year}gj headers).
LEVEL_GJ_PREFIX = {"bg": "bg", "tract": "tr", "county": "co"}

NHGIS_SOURCE_URL = "https://www.nhgis.org/"
NHGIS_DOC_URL = "https://www.nhgis.org/geographic-crosswalks"
NHGIS_LICENSE = (
    "IPUMS NHGIS terms of use: citation and attribution required; "
    "redistribution restricted (permission requested)."
)
NHGIS_DUA_REFERENCE = "IPUMS NHGIS citation required; see https://www.nhgis.org/ for terms."
NHGIS_MAINTAINER = "IPUMS NHGIS, University of Minnesota"

CROSSWALK_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("source_level", T.StringType(), False),
        T.StructField("source_vintage", T.IntegerType(), False),
        T.StructField("source_geoid", T.StringType(), False),
        T.StructField("source_gisjoin", T.StringType(), False),
        T.StructField("target_level", T.StringType(), False),
        T.StructField("target_vintage", T.IntegerType(), False),
        T.StructField("target_geoid", T.StringType(), False),
        T.StructField("target_gisjoin", T.StringType(), False),
        T.StructField("weight_kind", T.StringType(), False),
        T.StructField("weight", T.DoubleType(), False),
    ]
)

# Streaming read batch size — bounds driver memory; raw rows pivot ~5x on
# normalization so the Spark chunk is up to ~5 * BATCH rows.
BATCH = 50_000
# Weight-sum DQ tolerance (matches validate_crosswalk_weights default).
DQ_TOLERANCE = 1e-3


def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - depends on runtime flavor
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope=scope, key=key)


def _ipums_base_url(api_key: str) -> tuple[str, Any]:
    """Return (base_url, IpumsApiClient) for building supplemental URLs."""
    from ipumspy import IpumsApiClient

    ipums = IpumsApiClient(api_key)
    return ipums.base_url, ipums


def _download_zip(ipums: Any, base_url: str, filename: str, dest: Path) -> Path:
    """Download a supplemental-data crosswalk zip via the IPUMS API client."""
    url = f"{base_url}/supplemental-data/nhgis/crosswalks/{filename}"
    target = dest / filename
    log.info("Downloading crosswalk", extra={"url": url, "dest": str(target)})
    with ipums.get(url, stream=True) as response:
        with open(target, "wb") as out:
            for chunk in response.iter_content(chunk_size=65536):
                out.write(chunk)
    return target


def _extract_zip(zip_path: Path, dest: Path) -> Path:
    out_dir = dest / zip_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    return out_dir


def _find_gj_csv(unzipped: Path, csv_stem: str) -> Path:
    """Locate the GISJOIN-keyed CSV (preferred). Fall back to any CSV with 'gj'."""
    explicit = list(unzipped.rglob(f"{csv_stem}_gj.csv"))
    if explicit:
        return explicit[0]
    csvs = list(unzipped.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV found under {unzipped}")
    gjs = [p for p in csvs if "gj" in p.name.lower()]
    return gjs[0] if gjs else csvs[0]


def _identify_columns(
    header: list[str],
    source_level: str,
    source_vintage: int,
    target_level: str,
    target_vintage: int,
) -> tuple[str, str, dict[str, str]]:
    """Pick source/target GISJOIN columns and the present weight columns from
    the actual CSV header. NHGIS column naming: ``{prefix}{year}gj`` for the
    GISJOIN column; weights are ``parea`` and ``wt_*``.
    """
    src_pref = LEVEL_GJ_PREFIX[source_level]
    tgt_pref = LEVEL_GJ_PREFIX[target_level]
    src_col = f"{src_pref}{source_vintage}gj"
    tgt_col = f"{tgt_pref}{target_vintage}gj"
    lower = {c.lower(): c for c in header}
    if src_col not in lower:
        raise ValueError(f"source GJ column {src_col!r} not in header {header}")
    if tgt_col not in lower:
        raise ValueError(f"target GJ column {tgt_col!r} not in header {header}")
    weight_columns = {raw: kind for raw, kind in geo.NHGIS_WEIGHT_COLUMNS.items() if raw in lower}
    return lower[src_col], lower[tgt_col], weight_columns


def _stream_csv_rows(csv_path: Path) -> tuple[list[str], Any]:
    """Return ``(header, iterator-of-dict-rows)`` reading the CSV in pandas chunks
    with all columns kept as strings (preserves GISJOIN leading zeros).
    """
    import pandas as pd

    head_df = pd.read_csv(csv_path, nrows=0)
    header = list(head_df.columns)

    def _iter() -> Any:
        for chunk in pd.read_csv(csv_path, chunksize=BATCH, dtype=str):
            yield from chunk.to_dict(orient="records")

    return header, _iter()


def _write_chunk(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], written: set[str]
) -> None:
    """Append one normalized chunk. The first write per session overwrites
    (full_refresh, with schema replacement); subsequent chunks append.
    """
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=CROSSWALK_SPARK_SCHEMA)
    mode = "overwrite" if TABLE not in written else "append"
    writer = df.write.mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(f"{catalog}.{SCHEMA}.{TABLE}")
    written.add(TABLE)
    log.info("Wrote crosswalk chunk", extra={"rows": len(rows), "mode": mode})


def _update_running_sums(
    running: dict[tuple[str, str], float], normalized: list[dict[str, Any]]
) -> None:
    """Maintain ``{(source_gisjoin, weight_kind) -> sum_weight}`` across chunks
    so the per-file weight-sum DQ can run without holding all rows in memory.
    """
    for r in normalized:
        key = (r["source_gisjoin"], r["weight_kind"])
        running[key] = running.get(key, 0.0) + r["weight"]


def _check_running_sums(
    desc: str,
    running: dict[tuple[str, str], float],
    tolerance: float,
    *,
    recorder: DQRecorder,
    table_name: str,
    spec: dict[str, Any],
) -> None:
    """Per-(source_gisjoin, weight_kind) weight sums should be ~1.0 (ADR 0009).

    Records one row per (file, weight_kind) in ``_ops.dq_results`` so the
    audit trail is granular enough to spot weight-specific drift, then
    raises (with all offenders summarized) if any kind failed tolerance.
    """
    # Partition the offender list by weight_kind so we record one row per kind.
    per_kind_offenders: dict[str, list[tuple[str, float]]] = {}
    per_kind_totals: dict[str, int] = {}
    for (src, kind), total in running.items():
        per_kind_totals[kind] = per_kind_totals.get(kind, 0) + 1
        if abs(total - 1.0) > tolerance:
            per_kind_offenders.setdefault(kind, []).append((src, total))

    any_failed = False
    for kind in sorted(per_kind_totals):
        offenders = per_kind_offenders.get(kind, [])
        passed = not offenders
        sample = sorted(offenders)[:5]
        recorder.record(
            table_name=table_name,
            check_name=(
                f"crosswalk_weight_sum_"
                f"{spec['source_level']}{spec['source_vintage']}_to_"
                f"{spec['target_level']}{spec['target_vintage']}_{kind}"
            ),
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.FAIL,
            passed=passed,
            failing_row_count=len(offenders),
            total_row_count=per_kind_totals[kind],
            details=(
                {
                    "crosswalk": desc,
                    "weight_kind": kind,
                    "tolerance": tolerance,
                    "sample_offenders": [[src, total] for src, total in sample],
                }
                if offenders
                else None
            ),
        )
        if offenders:
            any_failed = True

    if any_failed:
        # Report a flat summary across all weight kinds in the exception, matching
        # the prior behaviour so anything reading job logs sees the same shape.
        all_offenders = sorted(
            (src, kind, total) for kind, lst in per_kind_offenders.items() for src, total in lst
        )[:5]
        raise ValueError(f"weight sums != 1.0 in {desc}; first offenders: {all_offenders}")
    log.info("DQ weight sums OK", extra={"crosswalk": desc, "source_units": len(running)})


def _set_clustering(spark: SparkSession, catalog: str) -> None:
    """Best-effort Liquid Clustering on the dominant filter columns (ADR 0021).

    Non-fatal if the runtime doesn't support ``ALTER ... CLUSTER BY`` — clustering
    is a read-pruning optimization, not a correctness requirement.
    """
    try:
        spark.sql(
            f"ALTER TABLE {catalog}.{SCHEMA}.{TABLE} "
            f"CLUSTER BY (source_level, source_vintage, target_level, target_vintage)"
        )
    except Exception as exc:  # pragma: no cover - runtime-dependent
        log.warning("Could not set clustering on crosswalk", extra={"error": str(exc)})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'NHGIS bg-sourced 2010<->2020 crosswalks, long-form. ADR 0021.'"
    )


def _register_dataset(spark: SparkSession, catalog: str, pipeline_reference: str) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description="Cross-vintage geographic crosswalks (NHGIS bg-sourced 2010<->2020).",
            public_health_relevance=(
                "Translate data between 2010 and 2020 census geographies for "
                "cross-vintage comparability of surveillance and modeling time series."
            ),
            spatial_resolution="multi",
            spatial_coverage="United States",
            source_provider_code="ipums_nhgis",
            source_url=NHGIS_SOURCE_URL,
            source_documentation_url=NHGIS_DOC_URL,
            license=NHGIS_LICENSE,
            dua_required=True,
            dua_reference=NHGIS_DUA_REFERENCE,
            access_tier="restricted",
            external_maintainer_name=NHGIS_MAINTAINER,
            is_hosted=True,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="full_refresh",
            materialization_type="table",
            cluster_columns=["source_level", "source_vintage", "target_level", "target_vintage"],
            pipeline_reference=pipeline_reference,
        ),
    )


def _process_one(
    spark: SparkSession,
    catalog: str,
    spec: dict[str, Any],
    ipums: Any,
    base_url: str,
    workdir: Path,
    written: set[str],
    recorder: DQRecorder,
) -> None:
    """Download, normalize, write, and DQ-check one crosswalk file set."""
    desc = (
        f"{spec['source_level']}{spec['source_vintage']}"
        f"->{spec['target_level']}{spec['target_vintage']}"
    )
    log.info("Processing crosswalk", extra={"crosswalk": desc})

    zip_path = _download_zip(ipums, base_url, spec["filename"], workdir)
    unz = _extract_zip(zip_path, workdir)
    csv = _find_gj_csv(unz, spec["csv_stem"])
    header, row_iter = _stream_csv_rows(csv)
    src_col, tgt_col, weight_cols = _identify_columns(
        header,
        spec["source_level"],
        spec["source_vintage"],
        spec["target_level"],
        spec["target_vintage"],
    )
    log.info(
        "Crosswalk columns identified",
        extra={
            "crosswalk": desc,
            "src": src_col,
            "tgt": tgt_col,
            "weights": list(weight_cols.values()),
        },
    )

    running: dict[tuple[str, str], float] = {}
    total_written = 0
    raw_batch: list[dict[str, Any]] = []

    def _flush(batch: list[dict[str, Any]]) -> int:
        if not batch:
            return 0
        normalized = geo.normalize_crosswalk_rows(
            batch,
            source_level=spec["source_level"],
            source_vintage=spec["source_vintage"],
            target_level=spec["target_level"],
            target_vintage=spec["target_vintage"],
            source_gj_col=src_col,
            target_gj_col=tgt_col,
            weight_columns=weight_cols,
        )
        _update_running_sums(running, normalized)
        _write_chunk(spark, catalog, normalized, written)
        return len(normalized)

    for raw in row_iter:
        raw_batch.append(raw)
        if len(raw_batch) >= BATCH:
            total_written += _flush(raw_batch)
            raw_batch = []
    total_written += _flush(raw_batch)

    _check_running_sums(
        desc,
        running,
        DQ_TOLERANCE,
        recorder=recorder,
        table_name=f"{SCHEMA}.{TABLE}",
        spec=spec,
    )
    log.info(
        "Completed crosswalk",
        extra={"crosswalk": desc, "rows": total_written, "source_units": len(running)},
    )


def run(catalog: str, ipums_secret_scope: str | None, ipums_secret_key: str | None) -> None:
    spark = SparkSession.builder.getOrCreate()
    pipeline_ref = "bundles/_reference/src/build_crosswalk.py"

    if not ipums_secret_scope:
        raise ValueError("--ipums-secret-scope is required to pull NHGIS crosswalks")
    api_key = _get_secret(ipums_secret_scope, ipums_secret_key or "nhgis_api_key")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")

    base_url, ipums = _ipums_base_url(api_key)
    workdir = Path(tempfile.mkdtemp(prefix="nhgis_xw_"))
    log.info("Crosswalk build starting", extra={"catalog": catalog, "files": len(CROSSWALK_FILES)})

    written: set[str] = set()
    run_id = new_run_id()
    log.info("DQ run id assigned", extra={"run_id": run_id, "pipeline_reference": pipeline_ref})

    with DQRecorder(spark, catalog, run_id, pipeline_ref) as recorder:
        for spec in CROSSWALK_FILES:
            _process_one(spark, catalog, spec, ipums, base_url, workdir, written, recorder)

    _comment_table(spark, catalog)
    _set_clustering(spark, catalog)
    _register_dataset(spark, catalog, pipeline_ref)

    log.info("Crosswalk build complete", extra={"catalog": catalog})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--ipums-secret-scope", default=None)
    parser.add_argument("--ipums-secret-key", default="nhgis_api_key")
    args = parser.parse_args()
    run(args.catalog, args.ipums_secret_scope, args.ipums_secret_key)


if __name__ == "__main__":
    main()
