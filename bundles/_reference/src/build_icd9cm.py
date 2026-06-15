"""Build the canonical ``codes.icd9cm`` reference table (ADR 0014/0031).

ICD-9-CM diagnosis codes (NCHS Tabular List of Diseases, Volume 1, incl. the V
and E supplementary classifications) for U.S. coding before the 2015-10-01
ICD-10 transition. This entrypoint is the thin IO + Spark layer over the pure
logic in ``cidmath_datahub.reference.icd9cm`` (ADR 0011). For each fiscal-year
edition it:

  1. downloads the ``DTAB`` (tabular list) and ``APPNDX`` (Appendix E) zips,
     converts the RTF members to text, and parses them;
  2. assembles records (``is_billable`` = leaf-of-set) and builds the hierarchy
     (prefix-rule adjacency + Appendix-E chapter/block; ADR 0031) -- the same
     column shape and semantics as ``codes.icd10cm``.

It then runs DQ and writes ``ecdh_model_<env>.codes.icd9cm`` keyed by
``(icd9cm_code, edition_year)`` (ADR 0006; ADR 0015: reference table, no Kimball
suffix). ICD-9-CM is frozen, so editions are pure annual base releases (no
mid-year overlay). ``--hierarchy`` (``build`` / ``skip``) controls the Appendix-E
download; adjacency is always computed from the code set.

The NCHS FTP archive's latest full RTF release is FY2012 (directory ``2011``);
FY2013/FY2014 were not redistributed there (the partial code freeze). Available
editions run roughly FY1997-FY2012.

Usage:
    build_icd9cm.py --catalog ecdh_model_dev --edition-year 2012 2011 2010 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import tempfile
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import icd9cm

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "icd9cm"
PIPELINE_REF = "bundles/_reference/src/build_icd9cm.py"

# ICD-9-CM diagnosis codes incl. V/E are ~13k-17k per edition. WARN outside a
# generous band -- a real edition should never be this small.
_CARDINALITY_MIN = 10_000
_CARDINALITY_MAX = 22_000

#: Mirrors codes.icd10cm's shape (ADR 0031 contract): flat columns + hierarchy.
ICD9_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("icd9cm_code", T.StringType(), False),
        T.StructField("edition_year", T.IntegerType(), False),
        T.StructField("description", T.StringType(), False),
        T.StructField("is_billable", T.BooleanType(), False),
        T.StructField("parent_icd9cm_code", T.StringType(), True),
        T.StructField("node_level", T.IntegerType(), False),
        T.StructField("ancestor_codes", T.ArrayType(T.StringType()), False),
        T.StructField("chapter_code", T.StringType(), True),
        T.StructField("chapter_name", T.StringType(), True),
        T.StructField("block_code", T.StringType(), True),
        T.StructField("block_name", T.StringType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: download a zip member and convert the RTF to text (kept out of the pure
# module per ADR 0011; striprtf is lazily imported so the module loads without it)
# ---------------------------------------------------------------------------


def _fetch_rtf_text(url: str, selector: Callable[[list[str]], str]) -> tuple[str, str]:
    """Download a zip, extract the selected RTF member, and return ``(text, member)``.

    The NCHS files are RTF; ``striprtf`` converts to indented plain text that the
    pure parsers consume. Decoded latin-1 (the RTF body is ASCII-ish).
    """
    from striprtf.striprtf import rtf_to_text

    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with tempfile.TemporaryDirectory(prefix="icd9_") as tmp:
        zip_path = Path(tmp) / "icd9cm.zip"
        with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC NCHS host
            zip_path.write_bytes(resp.read())
        with zipfile.ZipFile(zip_path) as zf:
            member = selector(zf.namelist())
            raw = zf.read(member)
    text = rtf_to_text(raw.decode("latin-1"))
    log.info("Extracted + de-RTF'd member", extra={"url": url, "member": member, "bytes": len(raw)})
    return text, member


# ---------------------------------------------------------------------------
# DQ (ADR 0009/0029): blocking uniqueness / non-null / format / parent-resolves;
# WARN cardinality / chapter-block coverage / orphans / V-E share.
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    records: list[icd9cm.Icd9Record],
    nodes: list[icd9cm.Icd9Node],
    edition_years: list[int],
    stats: dict[int, dict[str, Any]],
) -> None:
    table = f"{SCHEMA}.{TABLE}"
    total = len(records)

    dup_keys = icd9cm.find_duplicate_keys(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9cm_code_edition_year_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_keys,
        failing_row_count=len(dup_keys),
        total_row_count=total,
        details={"sample": [list(k) for k in dup_keys[:10]]} if dup_keys else None,
    )

    missing_desc = icd9cm.find_missing_descriptions(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_description_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing_desc,
        failing_row_count=len(missing_desc),
        total_row_count=total,
        details={"sample": [list(k) for k in missing_desc[:10]]} if missing_desc else None,
    )

    bad_format = icd9cm.find_format_violations(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9cm_code_format",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_format,
        failing_row_count=len(bad_format),
        total_row_count=total,
        details={"sample": bad_format[:10]} if bad_format else None,
    )

    dangling = icd9cm.find_dangling_parents(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_parent_referential_integrity",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.FAIL,
        passed=not dangling,
        failing_row_count=len(dangling),
        total_row_count=total,
        details={"sample": dangling[:10]} if dangling else None,
    )

    for year in edition_years:
        n = sum(1 for r in records if r.edition_year == year)
        ok = _CARDINALITY_MIN <= n <= _CARDINALITY_MAX
        ctx.recorder.record(
            table_name=table,
            check_name=f"icd9_cardinality_{year}",
            category=DQCategory.CARDINALITY,
            severity=DQSeverity.WARN,
            passed=ok,
            failing_row_count=0 if ok else 1,
            total_row_count=n,
            details={"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": n},
        )

    # Chapter comes from the static frozen map -> should always resolve; a non-empty
    # result means a category fell outside every ICD-9 chapter range (anomaly).
    unmapped_chapters = icd9cm.find_unmapped_categories(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_chapter_resolved",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not unmapped_chapters,
        failing_row_count=len(unmapped_chapters),
        total_row_count=total,
        details={"unmapped_categories": unmapped_chapters[:30]} if unmapped_chapters else None,
    )

    # Block comes from Appendix E -> categories absent from DC_3D have a null block.
    unmapped_blocks = icd9cm.find_unmapped_blocks(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_block_resolved",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not unmapped_blocks,
        failing_row_count=len(unmapped_blocks),
        total_row_count=total,
        details={"unmapped_blocks": unmapped_blocks[:30]} if unmapped_blocks else None,
    )

    orphans = icd9cm.find_orphan_codes(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_parent_resolved",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not orphans,
        failing_row_count=len(orphans),
        total_row_count=total,
        details={"sample_orphans": orphans[:20]} if orphans else None,
    )

    # V/E share sanity (INFO): both supplementary classifications should be present.
    v_count = sum(1 for r in records if icd9cm.code_class(r.icd9cm_code) == "V")
    e_count = sum(1 for r in records if icd9cm.code_class(r.icd9cm_code) == "E")
    ctx.recorder.record(
        table_name=table,
        check_name="icd9_ve_code_share",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=v_count > 0 and e_count > 0,
        total_row_count=total,
        details={"v_codes": v_count, "e_codes": e_count, "editions": stats},
    )

    failures: list[str] = []
    if dup_keys:
        failures.append(f"duplicate (icd9cm_code, edition_year): {dup_keys[:5]}")
    if missing_desc:
        failures.append(f"null description: {missing_desc[:5]}")
    if bad_format:
        failures.append(f"malformed icd9cm_code: {bad_format[:5]}")
    if dangling:
        failures.append(f"parent not in edition: {dangling[:5]}")
    if failures:
        raise ValueError("ICD-9-CM blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (per-edition replace; full_refresh per edition, ADR 0024)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], edition_years: list[int]
) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=ICD9_SPARK_SCHEMA).sort("edition_year", "icd9cm_code")
    if _table_has_column(spark, full, "edition_year"):
        years_sql = ", ".join(str(y) for y in edition_years)
        spark.sql(f"DELETE FROM {full} WHERE edition_year IN ({years_sql})")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.icd9cm", extra={"rows": len(rows), "editions": edition_years})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ICD-9-CM diagnosis code system (NCHS, frozen through 2015-09-30) with classification "
        f"hierarchy (parent_icd9cm_code, ancestor_codes, chapter/block). One row per code per "
        f"annual edition; PK (icd9cm_code, edition_year). Mirrors codes.icd10cm (ADR 0030/0031). "
        f"Reference table; full_refresh per edition.'"
    )


def _register(spark: SparkSession, catalog: str, edition_years: list[int]) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    cov_start = date(min(edition_years) - 1, 10, 1)  # FY effective Oct 1 of the prior year
    cov_end = date(max(edition_years), 9, 30)
    derived_from = [icd9cm.dtab_zip_url(y) for y in edition_years] + [
        icd9cm.appendix_zip_url(y) for y in edition_years
    ]
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description=(
                "ICD-9-CM (Clinical Modification) diagnosis code system from NCHS, with the "
                "classification hierarchy (parent_icd9cm_code, ancestor_codes, node_level, "
                "chapter/block). One row per code per fiscal-year edition; includes the V and "
                "E supplementary classifications. PK (icd9cm_code, edition_year)."
            ),
            public_health_relevance=(
                "Canonical diagnosis-code standard for U.S. surveillance/clinical data coded "
                "before the 2015-10-01 ICD-10 transition; mirrors codes.icd10cm's hierarchy so "
                "pre/post-2015 data can be rolled up the same way (the GEM crosswalk bridges "
                "the two code sets in a separate table)."
            ),
            spatial_resolution="none",
            spatial_coverage="United States",
            source_provider_code="cdc",
            source_url=icd9cm.SOURCE_LANDING_URL,
            source_documentation_url=icd9cm.SOURCE_LANDING_URL,
            license="public domain (U.S. Government work, 17 U.S.C. 105)",
            dua_required=False,
            dua_reference="No DUA. NCHS ICD-9-CM files are public domain.",
            access_tier="open",
            external_maintainer_name="National Center for Health Statistics (NCHS), CDC",
            is_hosted=True,
            source_data_dictionary_url=icd9cm.readme_url(max(edition_years)),
            temporal_coverage_start=cov_start,
            temporal_coverage_end=cov_end,
            temporal_resolution="annual",
            known_limitations=(
                "ICD-9-CM diagnosis codes only (no Volume 3 procedures, no GEM crosswalk). "
                "Frozen: final edition FY2014, valid through 2015-09-30; no mid-year updates "
                "and no 7th-character concept. is_billable is leaf-of-set (highest-specificity "
                "rule). Chapter/block come from Appendix E; V/E classifications may be sourced "
                "separately if Appendix E does not enumerate them (ADR 0031)."
            ),
            derived_from=derived_from,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="full_refresh",
            materialization_type="table",
            cluster_columns=["edition_year", "icd9cm_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )


def run(
    catalog: str,
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    hierarchy: str = "build",
) -> None:
    editions = sorted(set(edition_years))

    records: list[icd9cm.Icd9Record] = []
    nodes: list[icd9cm.Icd9Node] = []
    source_by_key: dict[tuple[str, int], str] = {}
    stats: dict[int, dict[str, Any]] = {}
    for year in editions:
        dtab_url = icd9cm.dtab_zip_url(year)
        log.info("Downloading DTAB", extra={"edition_year": year, "url": dtab_url})
        dtab_text, dtab_member = _fetch_rtf_text(dtab_url, icd9cm.select_dtab_member)
        edition_records = icd9cm.assemble_records(icd9cm.parse_dtab(dtab_text), year)

        category_map: dict[str, tuple[str, str]] = {}
        appendix_member: str | None = None
        if hierarchy != "skip":
            apx_url = icd9cm.appendix_zip_url(year)
            log.info("Downloading Appendix E", extra={"edition_year": year, "url": apx_url})
            apx_text, appendix_member = _fetch_rtf_text(apx_url, icd9cm.select_appendix_e_member)
            category_map = icd9cm.parse_appendix_e(apx_text)
            # Diagnostic: blocks come from Appendix E, whose real RTF->text shape we
            # confirm here. Logs the first non-empty lines + parse yield so the block
            # parser can be tuned to the actual layout (temporary; remove once stable).
            apx_sample = [ln for ln in apx_text.splitlines() if ln.strip()][:30]
            log.info(
                "Appendix-E sample",
                extra={
                    "edition_year": year,
                    "member": appendix_member,
                    "categories_mapped": len(category_map),
                    "first_lines": apx_sample,
                },
            )

        edition_nodes = icd9cm.build_hierarchy(edition_records, category_map)
        src = dtab_member + (f" + {appendix_member}" if appendix_member else "")
        for n in edition_nodes:
            source_by_key[(n.icd9cm_code, year)] = src
        records.extend(edition_records)
        nodes.extend(edition_nodes)
        stats[year] = {
            "dtab_member": dtab_member,
            "appendix_member": appendix_member,
            "records": len(edition_records),
            "billable": sum(1 for r in edition_records if r.is_billable),
            "unmapped_categories": len(icd9cm.find_unmapped_categories(edition_nodes)),
            "unmapped_blocks": len(icd9cm.find_unmapped_blocks(edition_nodes)),
        }
        log.info("Built edition", extra={"edition_year": year, **stats[year]})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, ...). "
            f"Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(ctx, records, nodes, editions, stats)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "icd9cm_code": n.icd9cm_code,
                "edition_year": n.edition_year,
                "description": n.description,
                "is_billable": n.is_billable,
                "parent_icd9cm_code": n.parent_icd9cm_code,
                "node_level": n.node_level,
                "ancestor_codes": list(n.ancestor_codes),
                "chapter_code": n.chapter_code,
                "chapter_name": n.chapter_name,
                "block_code": n.block_code,
                "block_name": n.block_name,
                "source_file": source_by_key[(n.icd9cm_code, n.edition_year)],
                "ingested_at": now,
            }
            for n in nodes
        ]
        _write_table(ctx.spark, catalog, rows, editions)
        _comment_table(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, editions),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--edition-year",
        type=int,
        nargs="+",
        default=[2012],
        help="ICD-9-CM fiscal-year edition(s). Default: 2012 (latest full RTF release; dir 2011).",
    )
    parser.add_argument(
        "--hierarchy",
        choices=["build", "skip"],
        default="build",
        help="'build' (default) downloads Appendix E for chapter/block; 'skip' leaves them null.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.edition_year,
        args.data_engineers_group,
        args.analysts_group,
        hierarchy=args.hierarchy,
    )


if __name__ == "__main__":
    main()
