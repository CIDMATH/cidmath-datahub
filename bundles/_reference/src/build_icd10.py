"""Build the canonical ``codes.icd10`` reference table (ADR 0014; first table in
the ``codes`` schema).

ICD-10-CM is the U.S. Clinical Modification of ICD-10 diagnosis codes
(CDC/NCHS) -- the public-health diagnosis standard -- *not* WHO ICD-10 or
ICD-10-PCS (procedures). This entrypoint is the thin IO + Spark layer over the
pure logic in ``cidmath_datahub.reference.icd10`` (ADR 0011). For each
fiscal-year edition it:

  1. downloads the CDC NCHS Oct-1 base order file and, when published, overlays
     the Apr-1 mid-year update (update wins per code; ``icd10.overlay_records``),
     so an edition reflects the latest within-year release;
  2. downloads the tabular XML and builds the classification hierarchy
     (``icd10.build_hierarchy``): adjacency (``parent_icd10_code``), materialized
     path (``ancestor_codes``), and depth (``node_level``) from the XML's
     ``chapter -> section -> diag`` nesting, plus the denormalized chapter/block
     labels (ADR 0030). Seventh-character codes fall back to their nearest listed
     ancestor by prefix.

It then runs DQ and writes ``ecdh_model_<env>.codes.icd10`` keyed by
``(icd10_code, edition_year)`` (ADR 0006; ADR 0015: reference table, no Kimball
suffix). ``--midyear-update`` controls the overlay (``auto`` / ``require`` /
``skip``); ``--hierarchy`` (``build`` / ``skip``) controls the tabular-XML
download -- with ``skip``, adjacency degrades to the prefix rule and chapter/block
are null. Pre-FY2025 editions have no mid-year update and are loaded base-only.

Second adopter of the ``run_build`` seam (ADR 0027/0028) after
``build_geography_views.py`` -- the canonical phase order is
``ensure -> [DQ context: work] -> register -> grant``. Editions are
re-pullable, so the table is vintage-reproducible: no SCD2/snapshot needed
(ADR 0007). Each requested edition is fully replaced in place (DELETE the
edition's rows, then append), leaving other loaded editions intact -- the same
per-vintage refresh pattern as the geography vintages (ADR 0024).

Blocking DQ (FAIL, raises): ``(icd10_code, edition_year)`` uniqueness, non-null
``description``, and ICD-10-CM code-format validation. Cardinality (ICD-10-CM
is ~70k+ codes per edition) is a WARN.

Usage:
    build_icd10.py --catalog ecdh_model_dev --edition-year 2026 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # multiple editions in one run (e.g. to backfill U07.1's FY2021 debut):
    build_icd10.py --catalog ecdh_model_dev --edition-year 2026 2021
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
from urllib.error import HTTPError

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import icd10

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "icd10"
PIPELINE_REF = "bundles/_reference/src/build_icd10.py"

# ICD-10-CM has ~70k+ codes per edition (FY2026 ~74k). WARN if a parsed edition
# falls outside a generous sanity band -- a real edition should never be this
# small, and a runaway count signals a parse/layout problem.
_CARDINALITY_MIN = 60_000
_CARDINALITY_MAX = 120_000

# Spark schema: flat code columns + the classification hierarchy (ADR 0030).
# Hierarchy columns are nullable -- chapter/block resolve from the tabular XML,
# and a top-level category has no parent. short_description and instructional
# notes remain out of scope.
ICD10_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("icd10_code", T.StringType(), False),
        T.StructField("edition_year", T.IntegerType(), False),
        T.StructField("description", T.StringType(), False),
        T.StructField("is_billable", T.BooleanType(), False),
        T.StructField("parent_icd10_code", T.StringType(), True),
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
# IO: download + extract a member from a CDC zip (kept out of the pure module
# per ADR 0011; the URL/member knowledge lives in icd10.py). Used for both the
# "Code Descriptions" order file and the "table and index" tabular XML.
# ---------------------------------------------------------------------------


def _fetch_zip_member(
    url: str, selector: Callable[[list[str]], str], *, encoding: str
) -> tuple[str, str]:
    """Download a CDC zip and return ``(member_text, member_name)``.

    External download in a reference build follows the established pattern
    (geography pulls GADM/NHGIS). ``selector`` picks the member from the zip's
    name list; ``encoding`` is ``latin-1`` for the fixed-width order file (so a
    rare non-ASCII byte never aborts the parse) and ``utf-8`` for the tabular XML.
    """
    # CDC filenames contain spaces (e.g. "Code Descriptions"); percent-encode the
    # path so urllib doesn't reject the URL for containing a literal space.
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with tempfile.TemporaryDirectory(prefix="icd10cm_") as tmp:
        zip_path = Path(tmp) / "icd10cm.zip"
        with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC NCHS host
            zip_path.write_bytes(resp.read())
        with zipfile.ZipFile(zip_path) as zf:
            member = selector(zf.namelist())
            raw = zf.read(member)
    log.info("Extracted zip member", extra={"url": url, "member": member, "bytes": len(raw)})
    return raw.decode(encoding), member


def _fetch_optional_zip_member(
    url: str, selector: Callable[[list[str]], str], *, encoding: str, required: bool
) -> tuple[str, str] | None:
    """Fetch a zip member, returning ``None`` when an optional source is absent
    or unusable (unless ``required``).

    Two "no update this edition" signals are tolerated for an optional source and
    skipped with a WARN: a 404 (pre-FY2025 editions have no ``{year}-update/``
    directory), and a zip that exists but carries no usable member -- e.g. a
    code-neutral mid-year update that ships only PDFs (FY2026's Apr-1 update has
    no new codes, so its zip has no order/tabular file). With ``required=True``
    both propagate. Other errors always propagate.
    """
    try:
        return _fetch_zip_member(url, selector, encoding=encoding)
    except HTTPError as exc:
        if exc.code == 404 and not required:
            log.warning("Optional source not found (skipping)", extra={"url": url})
            return None
        raise
    except ValueError as exc:
        if not required:
            log.warning(
                "Optional source has no usable member (skipping)",
                extra={"url": url, "error": str(exc)},
            )
            return None
        raise


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness / non-null / format; WARN cardinality
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    records: list[icd10.Icd10Record],
    edition_years: list[int],
    overlay_stats: dict[int, dict[str, Any]],
) -> None:
    """Record DQ outcomes; raise on any blocking FAIL so the build never writes
    a bad table (the recorder still flushes via run_build's DQ context)."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(records)

    dup_keys = icd10.find_duplicate_keys(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10_code_edition_year_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_keys,
        failing_row_count=len(dup_keys),
        total_row_count=total,
        details={"sample_duplicates": [list(k) for k in dup_keys[:10]]} if dup_keys else None,
    )

    missing_desc = icd10.find_missing_descriptions(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10_description_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing_desc,
        failing_row_count=len(missing_desc),
        total_row_count=total,
        details={"sample_missing": [list(k) for k in missing_desc[:10]]} if missing_desc else None,
    )

    bad_format = icd10.find_format_violations(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10_code_format",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_format,
        failing_row_count=len(bad_format),
        total_row_count=total,
        details={"sample_violations": bad_format[:10]} if bad_format else None,
    )

    # Cardinality is per edition (each fiscal year should independently be ~70k+).
    for year in edition_years:
        n = sum(1 for r in records if r.edition_year == year)
        ok = _CARDINALITY_MIN <= n <= _CARDINALITY_MAX
        ctx.recorder.record(
            table_name=table,
            check_name=f"icd10_cardinality_{year}",
            category=DQCategory.CARDINALITY,
            severity=DQSeverity.WARN,
            passed=ok,
            failing_row_count=0 if ok else 1,
            total_row_count=n,
            details={"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": n},
        )

    # Provenance: record what the mid-year overlay did per edition (INFO; an audit
    # trail in _ops.dq_results, not a pass/fail gate). ADR 0009.
    for year in edition_years:
        stats = overlay_stats.get(year)
        if stats is None:
            continue
        ctx.recorder.record(
            table_name=table,
            check_name=f"midyear_update_overlay_{year}",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.INFO,
            passed=True,
            total_row_count=stats.get("final"),
            details=stats,
        )

    failures: list[str] = []
    if dup_keys:
        failures.append(f"duplicate (icd10_code, edition_year): {dup_keys[:5]}")
    if missing_desc:
        failures.append(f"null description: {missing_desc[:5]}")
    if bad_format:
        failures.append(f"malformed icd10_code: {bad_format[:5]}")
    if failures:
        raise ValueError("ICD-10-CM blocking DQ failed -- " + "; ".join(failures))


def _hierarchy_dq_checks(
    ctx: BuildContext, nodes: list[icd10.Icd10Node], adjacency_mismatches: list[str]
) -> None:
    """Record hierarchy WARN checks (ADR 0030) -- non-blocking by design.

    A category missing from the tabular-XML map yields null chapter/block (e.g. a
    new mid-year category, or the XML having been skipped); an orphan is a 4+ char
    code whose ancestors are all absent (normally impossible); an adjacency
    mismatch is a listed code whose XML parent disagrees with the prefix rule (a
    source anomaly). All degrade gracefully rather than failing the build.
    """
    table = f"{SCHEMA}.{TABLE}"
    total = len(nodes)

    ctx.recorder.record(
        table_name=table,
        check_name="icd10_xml_vs_prefix_adjacency_agree",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not adjacency_mismatches,
        failing_row_count=len(adjacency_mismatches),
        total_row_count=total,
        details={"sample_mismatches": sorted(adjacency_mismatches)[:20]}
        if adjacency_mismatches
        else None,
    )

    unmapped = icd10.find_unmapped_categories(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10_chapter_block_resolved",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not unmapped,
        failing_row_count=len(unmapped),
        total_row_count=total,
        details={"unmapped_categories": unmapped[:20]} if unmapped else None,
    )

    orphans = icd10.find_orphan_codes(nodes)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10_parent_resolved",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not orphans,
        failing_row_count=len(orphans),
        total_row_count=total,
        details={"sample_orphans": orphans[:20]} if orphans else None,
    )


# ---------------------------------------------------------------------------
# Write (per-edition replace; ADR 0024 vintage semantics, full_refresh)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    """True if ``full`` exists and carries ``column`` (drives first-build vs.
    per-edition-replace; avoids importing the geospatial gadm helper)."""
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], edition_years: list[int]
) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=ICD10_SPARK_SCHEMA).sort("edition_year", "icd10_code")
    if _table_has_column(spark, full, "edition_year"):
        # Replace only the editions this run rebuilt; leave other vintages intact.
        years_sql = ", ".join(str(y) for y in edition_years)
        spark.sql(f"DELETE FROM {full} WHERE edition_year IN ({years_sql})")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        # First build: establish the table and its schema.
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.icd10", extra={"rows": len(rows), "editions": edition_years})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ICD-10-CM diagnosis code system (CDC/NCHS) with classification hierarchy "
        f"(parent_icd10_code, ancestor_codes, chapter/block). One row per code per annual "
        f"edition (Oct-1 base + Apr-1 mid-year update overlaid); PK (icd10_code, edition_year). "
        f"Reference table; vintage-reproducible (full_refresh per edition). ADR 0014/0015/0030.'"
    )


def _register(spark: SparkSession, catalog: str, edition_years: list[int]) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    # Editions are effective Oct 1 of the prior calendar year through Sep 30 of
    # the fiscal year (ICD-10-CM is a U.S. federal fiscal-year release).
    cov_start = date(min(edition_years) - 1, 10, 1)
    cov_end = date(max(edition_years), 9, 30)
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description=(
                "ICD-10-CM (Clinical Modification) diagnosis code system from CDC/NCHS, "
                "with the full classification hierarchy: parent_icd10_code, ancestor_codes "
                "(root->parent path), node_level, and denormalized chapter/block labels "
                "(ADR 0030). One row per code per fiscal-year edition, reflecting the latest "
                "within-year release (Oct-1 base with the Apr-1 mid-year update overlaid "
                "where published). is_billable distinguishes valid leaf codes from category "
                "headers. PK (icd10_code, edition_year)."
            ),
            public_health_relevance=(
                "Canonical diagnosis-code standard for U.S. surveillance and clinical data; "
                "lets case/encounter feeds conform diagnosis codes to a shared, versioned "
                "reference (e.g. U07.1 COVID-19, J18.9 pneumonia)."
            ),
            # ICD-10-CM is a non-spatial national code system.
            spatial_resolution="none",
            spatial_coverage="United States",
            source_provider_code="cdc",
            source_url=icd10.SOURCE_FILES_PAGE_URL,
            source_documentation_url=icd10.ORDER_FILE_FORMAT_DOC_URL,
            license="public domain (U.S. Government work, 17 U.S.C. 105)",
            dua_required=False,
            dua_reference="No DUA. CDC/NCHS ICD-10-CM files are public domain.",
            access_tier="open",
            external_maintainer_name="National Center for Health Statistics (NCHS), CDC",
            is_hosted=True,
            source_data_dictionary_url=icd10.ORDER_FILE_FORMAT_DOC_URL,
            temporal_coverage_start=cov_start,
            temporal_coverage_end=cov_end,
            temporal_resolution="annual",
            known_limitations=(
                "ICD-10-CM diagnosis codes only (not ICD-10-PCS procedures or WHO ICD-10). "
                "Each edition is the Oct-1 base with the Apr-1 mid-year update overlaid "
                "where published (update wins per code), so within-year release timing is "
                "collapsed into one edition_year (no as-of-Oct vs as-of-Apr snapshot). "
                "Seventh-character codes attach to their nearest listed ancestor, not a "
                "synthetic stem, and node_level reflects the adjacency tree (ADR 0030). "
                "Instructional notes (excludes1/2, useAdditionalCode, 7th-char definitions) "
                "are out of scope -- a future codes.icd10_note side table."
            ),
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="full_refresh",
            materialization_type="table",
            cluster_columns=["edition_year", "icd10_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )


def run(
    catalog: str,
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    url_template: str = icd10.ORDER_FILE_ZIP_URL_TEMPLATE,
    midyear_update: str = "auto",
    hierarchy: str = "build",
    tabular_url_template: str = icd10.TABULAR_ZIP_URL_TEMPLATE,
) -> None:
    editions = sorted(set(edition_years))

    # Download + assemble every requested edition before the build lifecycle
    # starts. Per edition: the Oct-1 base order file, then (per `midyear_update`)
    # the Apr-1 update overlaid (update wins per code), then (per `hierarchy`) the
    # tabular XML for chapter/block -> build_hierarchy adds adjacency + path
    # (ADR 0030). source_by_key stamps each row's source_file; overlay_stats feeds
    # an INFO DQ record.
    records: list[icd10.Icd10Record] = []
    nodes: list[icd10.Icd10Node] = []
    source_by_key: dict[tuple[str, int], str] = {}
    overlay_stats: dict[int, dict[str, Any]] = {}
    adjacency_mismatches: list[str] = []
    for year in editions:
        base_url = icd10.order_file_zip_url(year, template=url_template)
        log.info("Downloading base order file", extra={"edition_year": year, "url": base_url})
        base_text, base_member = _fetch_zip_member(
            base_url, icd10.select_order_file_member, encoding="latin-1"
        )
        base_recs = icd10.parse_order_file(base_text, year)

        update_recs: list[icd10.Icd10Record] = []
        update_member: str | None = None
        if midyear_update != "skip":
            upd_url = icd10.update_file_zip_url(year)
            log.info("Checking mid-year update", extra={"edition_year": year, "url": upd_url})
            fetched = _fetch_optional_zip_member(
                upd_url,
                icd10.select_order_file_member,
                encoding="latin-1",
                required=(midyear_update == "require"),
            )
            if fetched is not None:
                update_text, update_member = fetched
                update_recs = icd10.parse_order_file(update_text, year)

        merged = icd10.overlay_records(base_recs, update_recs)

        # Hierarchy from the tabular XML (base required unless --hierarchy skip;
        # update tree overlaid when an Apr-1 order-file update was applied). The
        # XML nesting is the source of truth for parent/ancestors; chapter/block
        # come from it too. Seventh-character codes fall back to prefix (ADR 0030).
        category_map: dict[str, icd10.CategoryGroup] = {}
        parent_of: dict[str, str | None] = {}
        if hierarchy != "skip":
            tab_url = icd10.tabular_zip_url(year, template=tabular_url_template)
            log.info("Downloading tabular XML", extra={"edition_year": year, "url": tab_url})
            tab_text, _ = _fetch_zip_member(
                tab_url, icd10.select_tabular_xml_member, encoding="utf-8"
            )
            tree = icd10.parse_tabular_tree(tab_text)
            category_map, parent_of = tree.category_map, tree.parent_of
            if update_member is not None:
                upd_tab = _fetch_optional_zip_member(
                    icd10.update_tabular_zip_url(year),
                    icd10.select_tabular_xml_member,
                    encoding="utf-8",
                    required=False,
                )
                if upd_tab is not None:
                    upd_tree = icd10.parse_tabular_tree(upd_tab[0])
                    category_map = {**category_map, **upd_tree.category_map}
                    parent_of = {**parent_of, **upd_tree.parent_of}

        edition_nodes = icd10.build_hierarchy(merged, category_map, parent_of)
        adjacency_mismatches.extend(icd10.find_adjacency_mismatches(merged, parent_of))

        base_codes = {r.icd10_code for r in base_recs}
        update_codes = {r.icd10_code for r in update_recs}
        for r in merged:
            from_update = update_member is not None and r.icd10_code in update_codes
            source_by_key[(r.icd10_code, year)] = update_member if from_update else base_member
        records.extend(merged)
        nodes.extend(edition_nodes)

        overlay_stats[year] = {
            "base_member": base_member,
            "update_member": update_member,
            "base_codes": len(base_codes),
            "update_codes": len(update_codes),
            "added_by_update": len(update_codes - base_codes),
            "unmapped_categories": len(icd10.find_unmapped_categories(edition_nodes)),
            "final": len(merged),
        }
        log.info("Built edition", extra={"edition_year": year, **overlay_stats[year]})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ...). "
            f"Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(ctx, records, editions, overlay_stats)
        _hierarchy_dq_checks(ctx, nodes, adjacency_mismatches)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "icd10_code": n.icd10_code,
                "edition_year": n.edition_year,
                "description": n.description,
                "is_billable": n.is_billable,
                "parent_icd10_code": n.parent_icd10_code,
                "node_level": n.node_level,
                "ancestor_codes": list(n.ancestor_codes),
                "chapter_code": n.chapter_code,
                "chapter_name": n.chapter_name,
                "block_code": n.block_code,
                "block_name": n.block_name,
                "source_file": source_by_key[(n.icd10_code, n.edition_year)],
                "ingested_at": now,
            }
            for n in nodes
        ]
        _write_table(ctx.spark, catalog, rows, editions)
        _comment_table(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        # Reference data is canonical and pipeline-owned: both engineer and
        # analyst groups get reader-tier only (ADR 0018). Verify the applied
        # grants as a deploy-time access gate.
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
        default=[2026],
        help="ICD-10-CM fiscal-year edition(s) to load (effective Oct 1). Default: 2026.",
    )
    parser.add_argument(
        "--url-template",
        default=icd10.ORDER_FILE_ZIP_URL_TEMPLATE,
        help="Override the base order-file zip URL template (must contain '{year}').",
    )
    parser.add_argument(
        "--midyear-update",
        choices=["auto", "require", "skip"],
        default="auto",
        help=(
            "Apr-1 mid-year update handling: 'auto' overlays it when published and "
            "skips on 404 (default); 'require' fails if it's missing; 'skip' loads "
            "the Oct-1 base only."
        ),
    )
    parser.add_argument(
        "--hierarchy",
        choices=["build", "skip"],
        default="build",
        help=(
            "'build' (default) downloads the tabular XML and sources adjacency + "
            "chapter/block from it; 'skip' downloads no XML, leaves chapter/block "
            "null, and derives adjacency from the code set (prefix rule)."
        ),
    )
    parser.add_argument(
        "--tabular-url-template",
        default=icd10.TABULAR_ZIP_URL_TEMPLATE,
        help="Override the base tabular-XML zip URL template (must contain '{year}').",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.edition_year,
        args.data_engineers_group,
        args.analysts_group,
        url_template=args.url_template,
        midyear_update=args.midyear_update,
        hierarchy=args.hierarchy,
        tabular_url_template=args.tabular_url_template,
    )


if __name__ == "__main__":
    main()
