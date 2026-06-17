"""Build the canonical ``codes.icd9_procedures`` reference table (ADR 0014).

ICD-9-CM Volume 3 procedure codes (CMS) -- the ICD-9 procedure counterpart to
``codes.icd10pcs`` and a sibling of ``codes.icd9cm`` (diagnoses). This entrypoint is the
thin IO + Spark layer over the pure logic in ``cidmath_datahub.reference.icd9_procedures``
(ADR 0011). It downloads the CMS ICD-9-CM Version 32 master-descriptions zip, extracts the
procedure (``SG``) long + short title files, joins them by code, and writes
``ecdh_model_<env>.codes.icd9_procedures`` keyed by ``(icd9_procedure_code, edition_year)``
(ADR 0006; ADR 0015: reference table, no Kimball suffix).

**Flat**: code + short/long title + a small chapter grouping (``category`` + ``chapter``).
Like ``codes.icd10pcs`` and unlike ``codes.icd9cm``, there is **no** classification hierarchy
(parent/ancestors/level) -- just the flat code list with chapter labels.

ICD-9-CM is **frozen**: Version 32 (effective 2014-10-01) is the final release, valid through
2015-09-30. So this is a single edition (``edition_year`` defaults to 2015) written with
``snapshot_replace`` -- re-pullable, vintage-reproducible (ADR 0024). Public domain (CMS) --
plain HTTPS download, no credential.

The CMS listing page is JavaScript-rendered and the direct-download slug can shift, so
``--source-url`` accepts the live link (grab it from the CMS ICD-9-CM code-titles page if the
templated default 404s) -- the same operator-override pattern the ICD-10-PCS build uses.

Blocking DQ (FAIL, raises): ``(icd9_procedure_code, edition_year)`` uniqueness; non-null
``long_title``; canonical ``NN[.NN]`` code format; every ``chapter`` resolves (category in
00-99). WARN: cardinality (~2.6k codes), chapter distribution, billable share.

Usage:
    build_icd9_procedures.py --catalog ecdh_model_dev --edition-year 2015 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import tempfile
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import icd9_procedures

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "icd9_procedures"
CURRENT_VIEW = "icd9_procedures_current"
PIPELINE_REF = "bundles/_reference/src/build_icd9_procedures.py"

# ICD-9-CM Volume 3 has ~2,600 procedure codes (V32). WARN outside a generous band -- a real
# edition should never be this small, and a runaway count signals a parse/layout problem.
_CARDINALITY_MIN = 2_000
_CARDINALITY_MAX = 4_500

ICD9_PROCEDURE_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("icd9_procedure_code", T.StringType(), False),
        T.StructField("edition_year", T.IntegerType(), False),
        T.StructField("short_title", T.StringType(), True),
        T.StructField("long_title", T.StringType(), False),
        T.StructField("is_billable", T.BooleanType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("chapter_code", T.StringType(), True),
        T.StructField("chapter_name", T.StringType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: download the CMS zip + extract the two procedure (SG) members (ADR 0011
# keeps the member knowledge in the pure module). Public HTTPS, no credential.
# ---------------------------------------------------------------------------


def _fetch_zip(url: str) -> bytes:
    """Download the CMS master-descriptions zip (public, no auth)."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CMS host
        raw = resp.read()
    # A real release is a zip ("PK" signature). A 200 with non-zip bytes means CMS served an
    # error/HTML page (a shifted slug); fail loudly so --source-url can be supplied.
    if raw[:2] != b"PK":
        preview = raw[:300].decode("utf-8", "replace")
        raise ValueError(
            f"CMS download did not return a zip ({len(raw)} bytes) from {url!r}. The "
            f"direct-download slug likely shifted -- pass the live link via --source-url "
            f"(grab it from {icd9_procedures.SOURCE_LANDING_URL}). Response began: {preview!r}"
        )
    log.info("Downloaded ICD-9-CM v32 descriptions zip", extra={"url": url, "bytes": len(raw)})
    return raw


def _extract_sg_members(zip_bytes: bytes) -> tuple[str, str, str]:
    """Return ``(long_text, short_text, long_member_name)`` for the procedure (SG) files."""
    with tempfile.TemporaryDirectory(prefix="icd9proc_") as tmp:
        zip_path = Path(tmp) / "icd9.zip"
        zip_path.write_bytes(zip_bytes)
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            long_member = icd9_procedures.select_long_member(names)
            short_member = icd9_procedures.select_short_member(names)
            long_text = zf.read(long_member).decode(icd9_procedures.SOURCE_ENCODING)
            short_text = zf.read(short_member).decode(icd9_procedures.SOURCE_ENCODING)
    log.info("Extracted SG members", extra={"long": long_member, "short": short_member})
    return long_text, short_text, long_member.replace("\\", "/").split("/")[-1]


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness / non-null / format / chapter; WARN cardinality
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext, records: list[icd9_procedures.Icd9ProcedureRecord], edition_year: int
) -> None:
    """Record DQ; raise on any blocking FAIL so a bad table never writes."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(records)

    dup_keys = icd9_procedures.find_duplicate_keys(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_procedure_code_edition_year_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_keys,
        failing_row_count=len(dup_keys),
        total_row_count=total,
        details={"sample_duplicates": [list(k) for k in dup_keys[:10]]} if dup_keys else None,
    )

    missing_title = icd9_procedures.find_missing_long_titles(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_procedure_long_title_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing_title,
        failing_row_count=len(missing_title),
        total_row_count=total,
        details=(
            {"sample_missing": [list(k) for k in missing_title[:10]]} if missing_title else None
        ),
    )

    bad_format = icd9_procedures.find_format_violations(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_procedure_code_format",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_format,
        failing_row_count=len(bad_format),
        total_row_count=total,
        details={"sample_violations": bad_format[:10]} if bad_format else None,
    )

    bad_chapters = icd9_procedures.find_bad_chapters(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_procedure_chapter_resolved",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_chapters,
        failing_row_count=len(bad_chapters),
        total_row_count=total,
        details={"sample": [list(c) for c in bad_chapters[:10]]} if bad_chapters else None,
    )

    # --- WARN checks ---
    ok = _CARDINALITY_MIN <= total <= _CARDINALITY_MAX
    ctx.recorder.record(
        table_name=table,
        check_name=f"icd9_procedure_cardinality_{edition_year}",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=ok,
        failing_row_count=0 if ok else 1,
        total_row_count=total,
        details={"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": total},
    )

    ctx.recorder.record(
        table_name=table,
        check_name=f"icd9_procedure_chapter_distribution_{edition_year}",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.INFO,
        passed=True,
        total_row_count=total,
        details={
            "distribution": icd9_procedures.chapter_distribution(records),
            "billable_share": round(icd9_procedures.billable_share(records), 4),
        },
    )

    failures: list[str] = []
    if dup_keys:
        failures.append(f"duplicate (icd9_procedure_code, edition_year): {dup_keys[:5]}")
    if missing_title:
        failures.append(f"null long_title: {missing_title[:5]}")
    if bad_format:
        failures.append(f"malformed icd9_procedure_code: {bad_format[:5]}")
    if bad_chapters:
        failures.append(f"unresolved chapter: {bad_chapters[:5]}")
    if failures:
        raise ValueError("ICD-9 procedure blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace per edition; ADR 0024 vintage semantics)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], edition_year: int
) -> None:
    """snapshot_replace: replace only this edition's rows; keep other vintages."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=ICD9_PROCEDURE_SPARK_SCHEMA).sort(
        "edition_year", "icd9_procedure_code"
    )
    if _table_has_column(spark, full, "edition_year"):
        spark.sql(f"DELETE FROM {full} WHERE edition_year = {edition_year}")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.icd9_procedures", extra={"rows": len(rows), "edition": edition_year})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ICD-9-CM Volume 3 procedure code system (CMS), flat: code + short/long title + "
        f"chapter grouping (category, chapter). One row per code per edition; PK "
        f"(icd9_procedure_code, edition_year); snapshot_replace. Frozen (Version 32, final). "
        f"No classification hierarchy (contrast codes.icd9cm). ADR 0014/0015.'"
    )


def _create_current_view(spark: SparkSession, catalog: str) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    view = f"{catalog}.{SCHEMA}.{CURRENT_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {full} WHERE edition_year = (SELECT MAX(edition_year) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'codes.icd9_procedures restricted to the latest edition_year (the final V32 release).'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008) -- public domain (open)
# ---------------------------------------------------------------------------

_KNOWN_LIMITATIONS = (
    "Code list + chapter grouping only: icd9_procedure_code, short/long title, is_billable "
    "(leaf-of-set), and category + chapter (the 18 fixed Volume-3 chapters). No classification "
    "hierarchy (parent/ancestors/level) -- contrast codes.icd9cm, which carries the ADR-0031 "
    "tree; this table mirrors the flat codes.icd10pcs shape. ICD-9-CM is frozen at Version 32 "
    "(effective 2014-10-01, final), valid for US coding through 2015-09-30; there are no newer "
    "editions and no mid-year updates."
)


def _register(spark: SparkSession, catalog: str, edition_year: int, *, create_view: bool) -> None:
    g = f"{catalog}.{SCHEMA}"
    # Version 32 is effective Oct 1 of the prior calendar year through Sep 30 of the FY.
    cov_start = date(edition_year - 1, 10, 1)
    cov_end = date(edition_year, 9, 30)
    common = {
        "subject": SCHEMA,
        "layer": "reference",
        "public_health_relevance": (
            "Canonical legacy inpatient-procedure code standard for U.S. data predating the "
            "2015-10-01 ICD-10 transition; lets historical encounter/procedure feeds conform "
            "ICD-9 procedure codes to a shared reference (e.g. 47.01 Laparoscopic appendectomy). "
            "The ICD-9 counterpart to codes.icd10pcs."
        ),
        "spatial_resolution": "none",
        "spatial_coverage": "United States",
        "source_provider_code": "cms",
        "source_url": icd9_procedures.SOURCE_LANDING_URL,
        "source_documentation_url": icd9_procedures.SOURCE_LANDING_URL,
        "source_data_dictionary_url": icd9_procedures.SOURCE_LANDING_URL,
        "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
        "dua_required": False,
        "dua_reference": "No DUA. CMS ICD-9-CM files are public domain.",
        "access_tier": "open",
        "external_maintainer_name": "Centers for Medicare & Medicaid Services (CMS)",
        "is_hosted": True,
        "temporal_coverage_start": cov_start,
        "temporal_coverage_end": cov_end,
        "temporal_resolution": "annual",
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": [f"CMS ICD-9-CM Version 32 master descriptions (FY{edition_year})"],
    }
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TABLE}",
            description=(
                "ICD-9-CM Volume 3 (procedure) codes from CMS. Flat: icd9_procedure_code "
                "(2-digit category + up to 2 decimals, e.g. 47.01), short/long title, "
                "is_billable (leaf code), and a chapter grouping (category + chapter_code/name). "
                "Frozen at Version 32 (final). PK (icd9_procedure_code, edition_year)."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["edition_year", "icd9_procedure_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )
    if create_view:
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{CURRENT_VIEW}",
                description="codes.icd9_procedures restricted to the latest edition_year.",
                **{**common, "is_hosted": False},
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=f"{g}.{CURRENT_VIEW}",
                update_semantics="full_refresh",
                materialization_type="view",
                cluster_columns=None,
                pipeline_reference=PIPELINE_REF,
            ),
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(
    catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    edition_year: int = 2015,
    source_url: str | None = None,
    create_view: bool = True,
) -> None:
    # Download + parse before the build lifecycle starts. ICD-9-CM is frozen, so there is one
    # release zip (Version 32); we join its SG long + short title files into the flat records.
    zip_bytes = _fetch_zip(source_url or icd9_procedures.SOURCE_ZIP_URL)
    long_text, short_text, long_member = _extract_sg_members(zip_bytes)
    long_pairs = icd9_procedures.parse_titles(long_text)
    short_pairs = icd9_procedures.parse_titles(short_text)
    records = icd9_procedures.assemble_records(long_pairs, short_pairs, edition_year)

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-10-PCS, "
            f"ICD-9-CM, ICD-9 procedures, CVX, NDC, LOINC, SNOMED CT, RxNorm, ...). Owned by "
            f"the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(ctx, records, edition_year)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "icd9_procedure_code": r.icd9_procedure_code,
                "edition_year": r.edition_year,
                "short_title": r.short_title,
                "long_title": r.long_title,
                "is_billable": r.is_billable,
                "category": r.category,
                "chapter_code": r.chapter_code,
                "chapter_name": r.chapter_name,
                "source_file": long_member,
                "ingested_at": now,
            }
            for r in records
        ]
        _write_table(ctx.spark, catalog, rows, edition_year)
        _comment_table(ctx.spark, catalog)
        if create_view:
            _create_current_view(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        # Reference data is canonical and pipeline-owned: both groups get reader-tier only
        # (ADR 0018). Verify the applied grants as a deploy-time access gate.
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, edition_year, create_view=create_view),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--edition-year",
        type=int,
        default=2015,
        help="Fiscal-year edition to stamp (Version 32 is the final release -> 2015). Default: 2015.",
    )
    parser.add_argument(
        "--source-url",
        default=None,
        help="Override the CMS Version 32 master-descriptions zip URL (paste the live link if "
        "the templated default has shifted).",
    )
    parser.add_argument(
        "--no-current-view",
        action="store_true",
        help="Skip the codes.icd9_procedures_current view.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.data_engineers_group,
        args.analysts_group,
        edition_year=args.edition_year,
        source_url=args.source_url,
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
