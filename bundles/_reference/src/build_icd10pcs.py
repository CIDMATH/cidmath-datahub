"""Build the canonical ``codes.icd10pcs`` reference table (ADR 0014).

ICD-10-PCS is the U.S. inpatient **procedure** coding system (CMS) -- the procedure
counterpart to ``codes.icd10cm`` (diagnoses). This entrypoint is the thin IO + Spark layer
over the pure logic in ``cidmath_datahub.reference.icd10pcs`` (ADR 0011). For each fiscal-year
edition it downloads the CMS Oct-1 base order file and, when published, overlays the Apr-1
mid-year update (the New Technology section; update wins per code; ``icd10pcs.overlay_records``),
so an edition reflects the latest within-year release.

It then runs DQ and writes ``ecdh_model_<env>.codes.icd10pcs`` keyed by
``(icd10pcs_code, edition_year)`` (ADR 0006; ADR 0015: reference table, no Kimball suffix).
**Flat**: code + short/long title + a small Section grouping (``section`` / ``section_name`` /
``body_system``). PCS is a 7-axis grammar, not a tree -- there is **no** ADR-0030 hierarchy;
the full axis decomposition / Definitions, the tabular XML, the ICD-9<->10 GEMs, and the
alphabetic index are deferred (separate issues).

Versioned per the ICD-10 model (CM precedent, **not** ADR 0032): each requested ``edition_year``
is replaced in place (``snapshot_replace`` -- DELETE the edition's rows, then append), leaving
other loaded editions intact (ADR 0024). Editions are re-pullable, so the table is
vintage-reproducible. Public domain (U.S. Government work) -- plain HTTPS download, no
credential.

The CMS listing page is JavaScript-rendered and the direct-download slug shifts year to year,
so ``--order-url`` / ``--update-url`` accept the live link (paste it from the CMS ICD-10 page
if the templated default 404s) -- the same operator-override pattern the SNOMED/RxNorm builds
use for their gated URLs.

Blocking DQ (FAIL, raises): ``(icd10pcs_code, edition_year)`` uniqueness; non-null
``long_title``; every **billable** code is a well-formed 7-char PCS code; every code uses only
the PCS charset (no ``I``/``O``); every ``section`` is one of the 17 PCS sections (ADR 0016).
WARN: per-edition billable-code cardinality, section distribution, billable share.

Usage:
    build_icd10pcs.py --catalog ecdh_model_dev --edition-year 2026 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # paste the live CMS link if the templated default has shifted:
    build_icd10pcs.py --catalog ecdh_model_dev --edition-year 2026 \\
        --order-url https://www.cms.gov/files/zip/<live-slug>.zip
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
from cidmath_datahub.reference import icd10pcs

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "icd10pcs"
CURRENT_VIEW = "icd10pcs_current"
PIPELINE_REF = "bundles/_reference/src/build_icd10pcs.py"

# ICD-10-PCS has ~79k valid codes per edition (FY2025 ~79k). WARN if an edition's billable-code
# count falls outside a generous band -- a real edition should never be this small, and a
# runaway count signals a parse/layout problem. (The order file also lists many partial header
# rows; the band is on valid codes only.)
_BILLABLE_CARDINALITY_MIN = 70_000
_BILLABLE_CARDINALITY_MAX = 100_000

# Flat code columns + the small Section grouping (no classification hierarchy). section_name /
# body_system are nullable (an unknown section -> null name; a 1-char header has no body system).
ICD10PCS_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("icd10pcs_code", T.StringType(), False),
        T.StructField("edition_year", T.IntegerType(), False),
        T.StructField("short_title", T.StringType(), True),
        T.StructField("long_title", T.StringType(), False),
        T.StructField("is_billable", T.BooleanType(), False),
        T.StructField("section", T.StringType(), False),
        T.StructField("section_name", T.StringType(), True),
        T.StructField("body_system", T.StringType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: download + extract a member from the CMS order-file zip (kept out of the pure
# module per ADR 0011; the URL/member knowledge lives in icd10pcs.py). Public HTTPS,
# no credential. Mirrors build_icd10cm._fetch_zip_member.
# ---------------------------------------------------------------------------


def _fetch_zip_member(
    url: str, selector: Callable[[list[str]], str], *, encoding: str
) -> tuple[str, str]:
    """Download a CMS zip and return ``(member_text, member_name)`` (public, no auth)."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with tempfile.TemporaryDirectory(prefix="icd10pcs_") as tmp:
        zip_path = Path(tmp) / "icd10pcs.zip"
        with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CMS host
            raw = resp.read()
        # A real release is a zip ("PK" signature). A 200 with non-zip bytes means CMS served
        # an error/HTML page (a shifted slug); fail loudly so --order-url can be supplied.
        if raw[:2] != b"PK":
            preview = raw[:300].decode("utf-8", "replace")
            raise ValueError(
                f"CMS download did not return a zip ({len(raw)} bytes) from {url!r}. The "
                f"direct-download slug likely shifted -- pass the live link via --order-url "
                f"(grab it from {icd10pcs.SOURCE_FILES_PAGE_URL}). Response began: {preview!r}"
            )
        zip_path.write_bytes(raw)
        with zipfile.ZipFile(zip_path) as zf:
            member = selector(zf.namelist())
            data = zf.read(member)
    log.info("Extracted zip member", extra={"url": url, "member": member, "bytes": len(data)})
    return data.decode(encoding), member


def _fetch_optional_zip_member(
    url: str, selector: Callable[[list[str]], str], *, encoding: str, required: bool
) -> tuple[str, str] | None:
    """Fetch a zip member, returning ``None`` when an optional source is absent (unless required).

    A 404 (no mid-year update this edition) and a zip with no usable order-file member (a
    code-neutral update shipping only PDFs) are tolerated with a WARN; with ``required=True``
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
# DQ (ADR 0009): blocking uniqueness / non-null / 7-char / charset / section;
# WARN cardinality + section distribution + billable share
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    records: list[icd10pcs.Icd10pcsRecord],
    edition_years: list[int],
    overlay_stats: dict[int, dict[str, Any]],
) -> None:
    """Record DQ outcomes; raise on any blocking FAIL so the build never writes a bad table."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(records)

    dup_keys = icd10pcs.find_duplicate_keys(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10pcs_code_edition_year_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_keys,
        failing_row_count=len(dup_keys),
        total_row_count=total,
        details={"sample_duplicates": [list(k) for k in dup_keys[:10]]} if dup_keys else None,
    )

    missing_title = icd10pcs.find_missing_titles(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10pcs_long_title_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing_title,
        failing_row_count=len(missing_title),
        total_row_count=total,
        details=(
            {"sample_missing": [list(k) for k in missing_title[:10]]} if missing_title else None
        ),
    )

    bad_billable = icd10pcs.find_invalid_billable_codes(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10pcs_billable_code_is_7char",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_billable,
        failing_row_count=len(bad_billable),
        total_row_count=total,
        details={"sample_violations": bad_billable[:10]} if bad_billable else None,
    )

    bad_charset = icd10pcs.find_charset_violations(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10pcs_code_charset",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_charset,
        failing_row_count=len(bad_charset),
        total_row_count=total,
        details={"sample_violations": bad_charset[:10]} if bad_charset else None,
    )

    bad_sections = icd10pcs.find_bad_sections(records)
    ctx.recorder.record(
        table_name=table,
        check_name="icd10pcs_section_controlled_vocab",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_sections,
        failing_row_count=len(bad_sections),
        total_row_count=total,
        details={
            "allowed": sorted(icd10pcs.PCS_SECTIONS),
            "sample": [list(s) for s in bad_sections[:10]],
        }
        if bad_sections
        else None,
    )

    # --- WARN checks (per edition) ---
    for year in edition_years:
        ed = [r for r in records if r.edition_year == year]
        n_billable = sum(1 for r in ed if r.is_billable)
        ok = _BILLABLE_CARDINALITY_MIN <= n_billable <= _BILLABLE_CARDINALITY_MAX
        ctx.recorder.record(
            table_name=table,
            check_name=f"icd10pcs_billable_cardinality_{year}",
            category=DQCategory.CARDINALITY,
            severity=DQSeverity.WARN,
            passed=ok,
            failing_row_count=0 if ok else 1,
            total_row_count=len(ed),
            details={
                "expected_billable_range": [_BILLABLE_CARDINALITY_MIN, _BILLABLE_CARDINALITY_MAX],
                "actual_billable": n_billable,
                "actual_total_rows": len(ed),
            },
        )
        ctx.recorder.record(
            table_name=table,
            check_name=f"icd10pcs_section_distribution_{year}",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.INFO,
            passed=True,
            total_row_count=len(ed),
            details={
                "distribution": icd10pcs.section_distribution(ed),
                "billable_share": round(icd10pcs.billable_share(ed), 4),
            },
        )

    # Provenance: what the mid-year overlay did per edition (INFO; audit trail, not a gate).
    for year in edition_years:
        stats = overlay_stats.get(year)
        if stats is None:
            continue
        ctx.recorder.record(
            table_name=table,
            check_name=f"icd10pcs_midyear_update_overlay_{year}",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.INFO,
            passed=True,
            total_row_count=stats.get("final"),
            details=stats,
        )

    failures: list[str] = []
    if dup_keys:
        failures.append(f"duplicate (icd10pcs_code, edition_year): {dup_keys[:5]}")
    if missing_title:
        failures.append(f"null long_title: {missing_title[:5]}")
    if bad_billable:
        failures.append(f"billable code not 7-char: {bad_billable[:5]}")
    if bad_charset:
        failures.append(f"code charset violation: {bad_charset[:5]}")
    if bad_sections:
        failures.append(f"section out of vocab: {bad_sections[:5]}")
    if failures:
        raise ValueError("ICD-10-PCS blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace per edition; ADR 0024 vintage semantics)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], edition_years: list[int]
) -> None:
    """snapshot_replace: replace only the editions this run rebuilt; keep other vintages."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=ICD10PCS_SPARK_SCHEMA).sort(
        "edition_year", "icd10pcs_code"
    )
    if _table_has_column(spark, full, "edition_year"):
        years_sql = ", ".join(str(y) for y in edition_years)
        spark.sql(f"DELETE FROM {full} WHERE edition_year IN ({years_sql})")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.icd10pcs", extra={"rows": len(rows), "editions": edition_years})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'ICD-10-PCS inpatient procedure code system (CMS), flat: code + short/long title + "
        f"Section grouping (section, section_name, body_system). One row per code/header per "
        f"fiscal-year edition; PK (icd10pcs_code, edition_year); snapshot_replace. No axis "
        f"decomposition / GEMs (deferred). ADR 0014/0015.'"
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
        f"'codes.icd10pcs restricted to the latest edition_year (the current fiscal-year release).'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008) -- public domain (open)
# ---------------------------------------------------------------------------

_KNOWN_LIMITATIONS = (
    "Code list + Section grouping only: icd10pcs_code, short/long title, is_billable, and "
    "section / section_name / body_system (code characters 1-2). The full 7-axis decomposition "
    "and per-character Definitions (the LOINC-Parts-style richer structure), the tabular XML, "
    "the ICD-9<->ICD-10-PCS GEMs (procedure crosswalk), and the alphabetic index are deferred "
    "(separate issues). PCS is a 7-axis grammar, not a single tree -- there is no chapter/block "
    "hierarchy (contrast codes.icd10cm). Each edition is the Oct-1 base with the Apr-1 mid-year "
    "(New Technology) update overlaid where published (update wins per code), so within-year "
    "release timing is collapsed into one edition_year."
)


def _register(
    spark: SparkSession, catalog: str, edition_years: list[int], *, create_view: bool
) -> None:
    g = f"{catalog}.{SCHEMA}"
    # Editions are effective Oct 1 of the prior calendar year through Sep 30 of the fiscal year.
    cov_start = date(min(edition_years) - 1, 10, 1)
    cov_end = date(max(edition_years), 9, 30)
    common = {
        "subject": SCHEMA,
        "layer": "reference",
        "public_health_relevance": (
            "Canonical inpatient-procedure code standard for U.S. surveillance and clinical "
            "data; lets encounter/procedure feeds conform procedure codes to a shared, "
            "versioned reference (e.g. 0DTJ4ZZ Resection of Appendix, Percutaneous Endoscopic). "
            "The procedure counterpart to codes.icd10cm."
        ),
        "spatial_resolution": "none",
        "spatial_coverage": "United States",
        "source_provider_code": "cms",
        "source_url": icd10pcs.SOURCE_FILES_PAGE_URL,
        "source_documentation_url": icd10pcs.ORDER_FILE_FORMAT_DOC_URL,
        "source_data_dictionary_url": icd10pcs.ORDER_FILE_FORMAT_DOC_URL,
        "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
        "dua_required": False,
        "dua_reference": "No DUA. CMS ICD-10-PCS files are public domain.",
        "access_tier": "open",
        "external_maintainer_name": "Centers for Medicare & Medicaid Services (CMS)",
        "is_hosted": True,
        "temporal_coverage_start": cov_start,
        "temporal_coverage_end": cov_end,
        "temporal_resolution": "annual",
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": [f"CMS ICD-10-PCS order file FY{y}" for y in edition_years],
    }
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TABLE}",
            description=(
                "ICD-10-PCS (Procedure Coding System) inpatient-procedure codes from CMS. Flat: "
                "icd10pcs_code (7-char, no decimal), short/long title, is_billable (valid leaf vs "
                "structural header), and a Section grouping (section character 1 + section_name; "
                "body_system character 2). One row per code per fiscal-year edition, reflecting "
                "the latest within-year release (Oct-1 base with the Apr-1 New Technology update "
                "overlaid where published). PK (icd10pcs_code, edition_year)."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["edition_year", "icd10pcs_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )
    if create_view:
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{CURRENT_VIEW}",
                description="codes.icd10pcs restricted to the latest edition_year.",
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
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    url_template: str = icd10pcs.ORDER_FILE_ZIP_URL_TEMPLATE,
    order_url: str | None = None,
    update_url: str | None = None,
    midyear_update: str = "auto",
    create_view: bool = True,
) -> None:
    editions = sorted(set(edition_years))
    if order_url is not None and len(editions) != 1:
        raise ValueError("--order-url overrides a single edition; pass exactly one --edition-year")

    # Download + assemble every requested edition before the build lifecycle starts. Per
    # edition: the Oct-1 base order file, then (per --midyear-update) the Apr-1 update overlaid
    # (update wins per code). source_by_key stamps each row's source_file; overlay_stats feeds
    # an INFO DQ record.
    records: list[icd10pcs.Icd10pcsRecord] = []
    source_by_key: dict[tuple[str, int], str] = {}
    overlay_stats: dict[int, dict[str, Any]] = {}
    for year in editions:
        base_url = order_url or icd10pcs.order_file_zip_url(year, template=url_template)
        log.info("Downloading base order file", extra={"edition_year": year, "url": base_url})
        base_text, base_member = _fetch_zip_member(
            base_url, icd10pcs.select_order_file_member, encoding=icd10pcs.SOURCE_ENCODING
        )
        base_recs = icd10pcs.parse_order_file(base_text, year)

        update_recs: list[icd10pcs.Icd10pcsRecord] = []
        update_member: str | None = None
        if midyear_update != "skip":
            upd_url = update_url or icd10pcs.update_file_zip_url(year)
            log.info("Checking mid-year update", extra={"edition_year": year, "url": upd_url})
            fetched = _fetch_optional_zip_member(
                upd_url,
                icd10pcs.select_order_file_member,
                encoding=icd10pcs.SOURCE_ENCODING,
                required=(midyear_update == "require"),
            )
            if fetched is not None:
                update_text, update_member = fetched
                update_recs = icd10pcs.parse_order_file(update_text, year)

        merged = icd10pcs.overlay_records(base_recs, update_recs)

        base_codes = {r.icd10pcs_code for r in base_recs}
        update_codes = {r.icd10pcs_code for r in update_recs}
        for r in merged:
            from_update = update_member is not None and r.icd10pcs_code in update_codes
            source_by_key[(r.icd10pcs_code, year)] = update_member if from_update else base_member
        records.extend(merged)

        overlay_stats[year] = {
            "base_member": base_member,
            "update_member": update_member,
            "base_codes": len(base_codes),
            "update_codes": len(update_codes),
            "added_by_update": len(update_codes - base_codes),
            "final": len(merged),
        }
        log.info("Built edition", extra={"edition_year": year, **overlay_stats[year]})

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-10-PCS, "
            f"ICD-9-CM, CVX, NDC, LOINC, SNOMED CT, RxNorm, ...). Owned by the _reference "
            f"bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(ctx, records, editions, overlay_stats)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "icd10pcs_code": r.icd10pcs_code,
                "edition_year": r.edition_year,
                "short_title": r.short_title,
                "long_title": r.long_title,
                "is_billable": r.is_billable,
                "section": r.section,
                "section_name": r.section_name,
                "body_system": r.body_system,
                "source_file": source_by_key[(r.icd10pcs_code, r.edition_year)],
                "ingested_at": now,
            }
            for r in records
        ]
        _write_table(ctx.spark, catalog, rows, editions)
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
        register=lambda spark: _register(spark, catalog, editions, create_view=create_view),
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
        help="ICD-10-PCS fiscal-year edition(s) to load (effective Oct 1). Default: 2026.",
    )
    parser.add_argument(
        "--url-template",
        default=icd10pcs.ORDER_FILE_ZIP_URL_TEMPLATE,
        help="Override the base order-file zip URL template (must contain '{year}').",
    )
    parser.add_argument(
        "--order-url",
        default=None,
        help="Explicit base order-file zip URL for a single edition (takes precedence over "
        "--url-template). Paste the live CMS link if the templated slug has shifted.",
    )
    parser.add_argument(
        "--update-url",
        default=None,
        help="Explicit Apr-1 mid-year update zip URL (overrides the templated update URL).",
    )
    parser.add_argument(
        "--midyear-update",
        choices=["auto", "require", "skip"],
        default="auto",
        help=(
            "Apr-1 mid-year (New Technology) update handling: 'auto' overlays it when published "
            "and skips on 404 (default); 'require' fails if it's missing; 'skip' loads the "
            "Oct-1 base only."
        ),
    )
    parser.add_argument(
        "--no-current-view", action="store_true", help="Skip the codes.icd10pcs_current view."
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
        order_url=args.order_url,
        update_url=args.update_url,
        midyear_update=args.midyear_update,
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
