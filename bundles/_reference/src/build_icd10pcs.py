"""Build the canonical ``codes.icd10pcs`` reference table on the shared builder (ADR 0037/0039).

ICD-10-PCS is the U.S. inpatient **procedure** coding system (CMS) -- the procedure
counterpart to ``codes.icd10cm`` (diagnoses). This entrypoint is the thin IO + Spark layer
over the pure logic in ``cidmath_datahub.reference.icd10pcs`` (ADR 0011). For each fiscal-year
edition it downloads the CMS Oct-1 base order file and, when published, overlays the Apr-1
mid-year update (the New Technology section; update wins per code; ``icd10pcs.overlay_records``),
so an edition reflects the latest within-year release.

**Source-path fold-in (ADR 0037 backport).** Previously model-only + hand-rolled on ``run_build``;
now folded onto the shared ``build_reference`` builder like ``build_ruca.py``. The CMS order-file
zip(s) land verbatim in the source-catalog Volume ``ecdh_<env>.codes_raw._landing`` (ADR 0039),
parse into the 1:1 raw table ``ecdh_<env>.codes_raw.icd10pcs``, and the canonical
``ecdh_model_<env>.codes.icd10pcs`` is promoted from raw. Same schema, same rows -- a build-mechanism
fold-in with data parity; consumers are unaffected. The builder owns the per-edition atomic
``replaceWhere`` write, ``_ops`` registration, and grants.

Keyed by ``(icd10pcs_code, edition_year)`` (ADR 0006; ADR 0015: reference table, no Kimball suffix).
**Flat**: code + short/long title + a small Section grouping (``section`` / ``section_name`` /
``body_system``). PCS is a 7-axis grammar, not a tree -- there is **no** ADR-0030 hierarchy; the
full axis decomposition / Definitions, the tabular XML, the ICD-9<->10 GEMs, and the alphabetic
index are deferred (separate issues).

Versioned per edition (``vintage_column="edition_year"``, ``vintage_snapshot`` -- ADR 0034): each
requested ``edition_year`` is atomically replaced in place via ``replaceWhere``, leaving other
loaded editions intact; editions are immutable and re-pullable (``PER_VINTAGE_IMMUTABLE`` landing
-- fetch-once, skip-if-present). Public domain (U.S. Government work) -- plain HTTPS download, no
credential. The ``codes.icd10pcs_current`` view is dropped (ADR 0034: "current" = ``MAX(edition_year)``
/ the live idiom), matching the RUCA fold.

The CMS listing page is JavaScript-rendered and the direct-download slug shifts year to year, so
``--order-url`` / ``--update-url`` accept the live link (paste it from the CMS ICD-10 page if the
templated default 404s) -- the same operator-override pattern the SNOMED/RxNorm builds use.

Usage:
    build_icd10pcs.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --edition-year 2026 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # paste the live CMS link if the templated default has shifted:
    build_icd10pcs.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --edition-year 2026 \\
        --order-url https://www.cms.gov/files/zip/<live-slug>.zip
"""

from __future__ import annotations

import argparse
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

from pyspark.sql import SparkSession
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
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import icd10pcs

log = get_logger(__name__)

# ADR 0037 backport (wave 1): folded onto build_reference; see module docstring.
SUBJECT = "codes"  # builder derives codes_raw (landing) + codes (canonical)
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "icd10pcs"
PIPELINE_REF = "bundles/_reference/src/build_icd10pcs.py"

# Verbatim landed payload names inside the per-edition Volume dir (dir is already vintage-scoped).
_BASE_ZIP = "base_order_file.zip"
_UPDATE_ZIP = "midyear_update.zip"

# ICD-10-PCS has ~79k valid codes per edition (FY2025 ~79k). WARN if an edition's billable-code
# count falls outside a generous band -- a real edition should never be this small, and a runaway
# count signals a parse/layout problem. (The order file also lists many partial header rows; the
# band is on valid codes only.)
_BILLABLE_CARDINALITY_MIN = 70_000
_BILLABLE_CARDINALITY_MAX = 100_000

# Flat code columns + the small Section grouping (no classification hierarchy). section_name /
# body_system are nullable (an unknown section -> null name; a 1-char header has no body system).
# Raw (codes_raw.icd10pcs) == canonical (codes.icd10pcs): flat, no derived columns.
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

_DDL = (
    "icd10pcs_code STRING, edition_year INT, short_title STRING, long_title STRING, "
    "is_billable BOOLEAN, section STRING, section_name STRING, body_system STRING, "
    "source_file STRING, ingested_at TIMESTAMP"
)

_DESC = (
    "ICD-10-PCS (Procedure Coding System) inpatient-procedure codes from CMS. Flat: icd10pcs_code "
    "(7-char, no decimal), short/long title, is_billable (valid leaf vs structural header), and a "
    "Section grouping (section character 1 + section_name; body_system character 2). One row per "
    "code per fiscal-year edition, reflecting the latest within-year release (Oct-1 base with the "
    "Apr-1 New Technology update overlaid where published). PK (icd10pcs_code, edition_year)."
)
_PHR = (
    "Canonical inpatient-procedure code standard for U.S. surveillance and clinical data; lets "
    "encounter/procedure feeds conform procedure codes to a shared, versioned reference (e.g. "
    "0DTJ4ZZ Resection of Appendix, Percutaneous Endoscopic). The procedure counterpart to "
    "codes.icd10cm."
)
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


# ---------------------------------------------------------------------------
# IO: download + extract a member from the CMS order-file zip (kept out of the pure module per
# ADR 0011; the URL/member knowledge lives in icd10pcs.py). Public HTTPS, no credential.
# ---------------------------------------------------------------------------


def _download_zip(url: str) -> bytes:
    """Download a CMS zip and return its raw bytes (public, no auth); validate the PK signature."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CMS host
        raw = resp.read()
    # A real release is a zip ("PK" signature). A 200 with non-zip bytes means CMS served an
    # error/HTML page (a shifted slug); fail loudly so --order-url can be supplied.
    if raw[:2] != b"PK":
        preview = raw[:300].decode("utf-8", "replace")
        raise ValueError(
            f"CMS download did not return a zip ({len(raw)} bytes) from {url!r}. The "
            f"direct-download slug likely shifted -- pass the live link via --order-url "
            f"(grab it from {icd10pcs.SOURCE_FILES_PAGE_URL}). Response began: {preview!r}"
        )
    log.info("Downloaded ICD-10-PCS zip", extra={"url": url, "bytes": len(raw)})
    return raw


def _download_optional_zip(url: str, *, required: bool) -> bytes | None:
    """Download an optional CMS zip, returning ``None`` when absent (404) unless ``required``.

    A code-neutral update shipping only PDFs (no usable order-file member) is detected downstream
    at parse time; here we only tolerate a 404 (no mid-year update this edition).
    """
    try:
        return _download_zip(url)
    except HTTPError as exc:
        if exc.code == 404 and not required:
            log.warning("Optional mid-year update not found (skipping)", extra={"url": url})
            return None
        raise


def _extract_member(
    zip_bytes: bytes, selector: Callable[[list[str]], str], *, encoding: str
) -> tuple[str, str] | None:
    """Return ``(member_text, member_name)`` for the order-file member, or ``None`` if absent.

    A ``None`` return means the zip has no usable order-file member (a code-neutral update shipping
    only PDFs) -- tolerated for the optional mid-year overlay.
    """
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        try:
            member = selector(zf.namelist())
        except ValueError:
            return None
        data = zf.read(member)
    return data.decode(encoding), member


# ---------------------------------------------------------------------------
# Volume landing hooks (ADR 0039): fetch the CMS zip(s) verbatim, then read + parse into raw.
# fetch_to_volume/read_from_volume close over the run's URL/override config.
# ---------------------------------------------------------------------------


def _make_hooks(
    *, url_template: str, order_url: str | None, update_url: str | None, midyear_update: str
) -> tuple[Callable[[int, str], None], Callable[[BuildContext, int, str], Any]]:
    """Build the ``(fetch_to_volume, read_from_volume)`` pair bound to this run's config."""

    def _fetch(v: int, volume_dir: str) -> None:
        base_url = order_url or icd10pcs.order_file_zip_url(v, template=url_template)
        log.info("Fetching base order file", extra={"edition_year": v, "url": base_url})
        (Path(volume_dir) / _BASE_ZIP).write_bytes(_download_zip(base_url))
        if midyear_update != "skip":
            upd_url = update_url or icd10pcs.update_file_zip_url(v)
            log.info("Checking mid-year update", extra={"edition_year": v, "url": upd_url})
            upd = _download_optional_zip(upd_url, required=(midyear_update == "require"))
            if upd is not None:
                (Path(volume_dir) / _UPDATE_ZIP).write_bytes(upd)

    def _read(ctx: BuildContext, v: int, volume_dir: str) -> Any:
        base_bytes = (Path(volume_dir) / _BASE_ZIP).read_bytes()
        base = _extract_member(
            base_bytes, icd10pcs.select_order_file_member, encoding=icd10pcs.SOURCE_ENCODING
        )
        if base is None:
            raise ValueError(f"ICD-10-PCS base order file for edition {v} has no usable member")
        base_text, base_member = base
        base_recs = icd10pcs.parse_order_file(base_text, v)

        update_recs: list[icd10pcs.Icd10pcsRecord] = []
        update_member: str | None = None
        upd_path = Path(volume_dir) / _UPDATE_ZIP
        if upd_path.exists():
            fetched = _extract_member(
                upd_path.read_bytes(),
                icd10pcs.select_order_file_member,
                encoding=icd10pcs.SOURCE_ENCODING,
            )
            if fetched is not None:
                update_text, update_member = fetched
                update_recs = icd10pcs.parse_order_file(update_text, v)

        merged = icd10pcs.overlay_records(base_recs, update_recs)
        update_codes = {r.icd10pcs_code for r in update_recs}
        base_codes = {r.icd10pcs_code for r in base_recs}
        log.info(
            "Assembled ICD-10-PCS edition",
            extra={
                "edition_year": v,
                "base_member": base_member,
                "update_member": update_member,
                "base_codes": len(base_codes),
                "update_codes": len(update_codes),
                "added_by_update": len(update_codes - base_codes),
                "final": len(merged),
            },
        )

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
                "source_file": (
                    update_member
                    if (update_member is not None and r.icd10pcs_code in update_codes)
                    else base_member
                ),
                "ingested_at": now,
            }
            for r in merged
        ]
        return ctx.spark.createDataFrame(rows, ICD10PCS_SPARK_SCHEMA).sort(
            "edition_year", "icd10pcs_code"
        )

    return _fetch, _read


# ---------------------------------------------------------------------------
# DQ (ADR 0009) in validate_staging: reconstruct records from the staged raw table and reuse the
# pure icd10pcs.py helpers. Blocking (FAIL, raises): (code, edition) uniqueness, non-null
# long_title, billable 7-char, PCS charset, section vocab. WARN/INFO: cardinality, distribution.
# ---------------------------------------------------------------------------


def _reconstruct_records(ctx: BuildContext, staging_fqn: str) -> list[icd10pcs.Icd10pcsRecord]:
    return [
        icd10pcs.Icd10pcsRecord(
            icd10pcs_code=r["icd10pcs_code"],
            edition_year=r["edition_year"],
            short_title=r["short_title"],
            long_title=r["long_title"],
            is_billable=r["is_billable"],
            section=r["section"],
            section_name=r["section_name"],
            body_system=r["body_system"],
        )
        for r in ctx.spark.sql(
            f"SELECT icd10pcs_code, edition_year, short_title, long_title, is_billable, "
            f"section, section_name, body_system FROM {staging_fqn}"
        ).collect()
    ]


def _validate(ctx: BuildContext, staging_fqn: str) -> None:
    record_table = f"{MODEL_SCHEMA}.{TABLE}"
    records = _reconstruct_records(ctx, staging_fqn)
    total = len(records)
    failures: list[str] = []

    # PK uniqueness via TableDQ (non-raising; gather all, raise once at the end).
    dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
    if not dq.unique(
        keys=["icd10pcs_code", "edition_year"],
        check_name="icd10pcs_code_edition_year_uniqueness",
        raise_on_fail=False,
    ):
        failures.append("duplicate (icd10pcs_code, edition_year)")

    missing_title = icd10pcs.find_missing_titles(records)
    _record(ctx, record_table, "icd10pcs_long_title_not_null", DQCategory.NULLABILITY,
            not missing_title, len(missing_title), total,
            {"sample_missing": [list(k) for k in missing_title[:10]]} if missing_title else None)
    if missing_title:
        failures.append(f"null long_title: {missing_title[:5]}")

    bad_billable = icd10pcs.find_invalid_billable_codes(records)
    _record(ctx, record_table, "icd10pcs_billable_code_is_7char", DQCategory.BUSINESS_RULE,
            not bad_billable, len(bad_billable), total,
            {"sample_violations": bad_billable[:10]} if bad_billable else None)
    if bad_billable:
        failures.append(f"billable code not 7-char: {bad_billable[:5]}")

    bad_charset = icd10pcs.find_charset_violations(records)
    _record(ctx, record_table, "icd10pcs_code_charset", DQCategory.BUSINESS_RULE,
            not bad_charset, len(bad_charset), total,
            {"sample_violations": bad_charset[:10]} if bad_charset else None)
    if bad_charset:
        failures.append(f"code charset violation: {bad_charset[:5]}")

    bad_sections = icd10pcs.find_bad_sections(records)
    _record(ctx, record_table, "icd10pcs_section_controlled_vocab", DQCategory.BUSINESS_RULE,
            not bad_sections, len(bad_sections), total,
            {"allowed": sorted(icd10pcs.PCS_SECTIONS), "sample": [list(s) for s in bad_sections[:10]]}
            if bad_sections else None)
    if bad_sections:
        failures.append(f"section out of vocab: {bad_sections[:5]}")

    # --- WARN / INFO per edition ---
    for year in sorted({r.edition_year for r in records}):
        ed = [r for r in records if r.edition_year == year]
        n_billable = sum(1 for r in ed if r.is_billable)
        ok = _BILLABLE_CARDINALITY_MIN <= n_billable <= _BILLABLE_CARDINALITY_MAX
        _record(ctx, record_table, f"icd10pcs_billable_cardinality_{year}", DQCategory.CARDINALITY,
                ok, 0 if ok else 1, len(ed),
                {"expected_billable_range": [_BILLABLE_CARDINALITY_MIN, _BILLABLE_CARDINALITY_MAX],
                 "actual_billable": n_billable, "actual_total_rows": len(ed)},
                severity=DQSeverity.WARN)
        _record(ctx, record_table, f"icd10pcs_section_distribution_{year}", DQCategory.BUSINESS_RULE,
                True, 0, len(ed),
                {"distribution": icd10pcs.section_distribution(ed),
                 "billable_share": round(icd10pcs.billable_share(ed), 4)},
                severity=DQSeverity.INFO)

    if failures:
        raise ValueError("ICD-10-PCS blocking DQ failed -- " + "; ".join(failures))


def _record(
    ctx: BuildContext, table: str, check_name: str, category: DQCategory, passed: bool,
    failing: int, total: int, details: dict[str, Any] | None, *,
    severity: DQSeverity = DQSeverity.FAIL,
) -> None:
    """Thin wrapper over ``ctx.recorder.record`` to keep the check lists readable."""
    ctx.recorder.record(
        table_name=table, check_name=check_name, category=category, severity=severity,
        passed=passed, failing_row_count=failing, total_row_count=total, details=details,
    )


# ---------------------------------------------------------------------------
# Provenance (ADR 0008) -- public domain (open). Cloned per layer by the builder.
# ---------------------------------------------------------------------------


def _base_entry(editions: list[int]) -> registration.DatasetCatalogEntry:
    # Editions are effective Oct 1 of the prior calendar year through Sep 30 of the fiscal year.
    cov_start = date(min(editions) - 1, 10, 1)
    cov_end = date(max(editions), 9, 30)
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        spatial_resolution="none",
        spatial_coverage="United States",
        source_provider_code="cms",
        source_url=icd10pcs.SOURCE_FILES_PAGE_URL,
        source_documentation_url=icd10pcs.ORDER_FILE_FORMAT_DOC_URL,
        source_data_dictionary_url=icd10pcs.ORDER_FILE_FORMAT_DOC_URL,
        license="public domain (U.S. Government work, 17 U.S.C. 105)",
        dua_required=False,
        dua_reference="No DUA. CMS ICD-10-PCS files are public domain.",
        access_tier="open",
        external_maintainer_name="Centers for Medicare & Medicaid Services (CMS)",
        is_hosted=True,
        temporal_coverage_start=cov_start,
        temporal_coverage_end=cov_end,
        temporal_resolution="annual",
        known_limitations=_KNOWN_LIMITATIONS,
        derived_from=[f"CMS ICD-10-PCS order file FY{y}" for y in editions],
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    url_template: str = icd10pcs.ORDER_FILE_ZIP_URL_TEMPLATE,
    order_url: str | None = None,
    update_url: str | None = None,
    midyear_update: str = "auto",
) -> None:
    editions = sorted(set(edition_years))
    if order_url is not None and len(editions) != 1:
        raise ValueError("--order-url overrides a single edition; pass exactly one --edition-year")

    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    fetch, read = _make_hooks(
        url_template=url_template, order_url=order_url, update_url=update_url,
        midyear_update=midyear_update,
    )

    def _ensure_staging(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings for the codes subject (clinical/"
            f"terminology code systems). Engineer-owned; canonicals promote to model codes. ADR 0037.'"
        )
        sp.sql(f"CREATE TABLE IF NOT EXISTS {raw_fqn} ({_DDL}) USING DELTA")

    def _ensure_canonical(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-10-PCS, "
            f"ICD-9-CM, ICD-9 procedures, CVX, NDC, LOINC, SNOMED CT, RxNorm, ...). Owned by "
            f"the _reference bundle. See ADR 0014.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{TABLE} ({_DDL}) USING DELTA"
        )

    def _promote(ctx: BuildContext, v: int) -> Any:
        """Raw is already canonical-shaped (flat); select this edition's rows."""
        return ctx.spark.sql(
            f"SELECT * FROM {raw_fqn} WHERE edition_year = {int(v)}"
        ).sort("edition_year", "icd10pcs_code")

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(editions),
        vintage_column="edition_year",
        raw_landings=[
            RawLanding(
                table=TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=fetch,
                read_from_volume=read,
                description=(
                    "Raw CMS ICD-10-PCS order-file codes, fetched-as-is per fiscal-year edition "
                    "(Oct-1 base with the Apr-1 mid-year update overlaid where published), "
                    "edition-stamped. Volume-landed verbatim, then parsed. Promoted to codes.icd10pcs."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table=TABLE,
                reads=(TABLE,),
                promote=_promote,
                validate_staging=_validate,
                description=_DESC,
                public_health_relevance=_PHR,
                canonical_cluster_columns=["edition_year", "icd10pcs_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=tuple(editions))
    log.info("ICD-10-PCS build complete", extra={"editions": editions})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-catalog", required=True, help="Source catalog for raw (ecdh_<env>).")
    parser.add_argument("--model-catalog", required=True,
                        help="Model catalog for canonical (ecdh_model_<env>).")
    parser.add_argument(
        "--edition-year", type=int, nargs="+", default=[2026],
        help="ICD-10-PCS fiscal-year edition(s) to load (effective Oct 1). Default: 2026.",
    )
    parser.add_argument(
        "--url-template", default=icd10pcs.ORDER_FILE_ZIP_URL_TEMPLATE,
        help="Override the base order-file zip URL template (must contain '{year}').",
    )
    parser.add_argument(
        "--order-url", default=None,
        help="Explicit base order-file zip URL for a single edition (takes precedence over "
        "--url-template). Paste the live CMS link if the templated slug has shifted.",
    )
    parser.add_argument(
        "--update-url", default=None,
        help="Explicit Apr-1 mid-year update zip URL (overrides the templated update URL).",
    )
    parser.add_argument(
        "--midyear-update", choices=["auto", "require", "skip"], default="auto",
        help=(
            "Apr-1 mid-year (New Technology) update handling: 'auto' overlays it when published "
            "and skips on 404 (default); 'require' fails if it's missing; 'skip' loads the "
            "Oct-1 base only."
        ),
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.source_catalog,
        args.model_catalog,
        args.edition_year,
        args.data_engineers_group,
        args.analysts_group,
        url_template=args.url_template,
        order_url=args.order_url,
        update_url=args.update_url,
        midyear_update=args.midyear_update,
    )


if __name__ == "__main__":
    main()
