"""Build the canonical ``codes.icd9_procedures`` reference table on the shared builder (ADR 0037/0039).

ICD-9-CM Volume 3 procedure codes (CMS) -- the ICD-9 procedure counterpart to ``codes.icd10pcs``
and a sibling of ``codes.icd9cm`` (diagnoses). This entrypoint is the thin IO + Spark layer over the
pure logic in ``cidmath_datahub.reference.icd9_procedures`` (ADR 0011). It downloads the CMS
ICD-9-CM Version 32 master-descriptions zip, extracts the procedure (``SG``) long + short title
files, joins them by code, and writes ``(icd9_procedure_code, edition_year)`` rows (ADR 0006;
ADR 0015: reference table, no Kimball suffix).

**Source-path fold-in (ADR 0037 backport).** Previously model-only + hand-rolled on ``run_build``;
now folded onto the shared ``build_reference`` builder like ``build_ruca.py``. The CMS descriptions
zip lands verbatim in the source-catalog Volume ``ecdh_<env>.codes_raw._landing`` (ADR 0039), parses
into the 1:1 raw table ``ecdh_<env>.codes_raw.icd9_procedures``, and the canonical
``ecdh_model_<env>.codes.icd9_procedures`` is promoted from raw. Same schema, same rows -- a
build-mechanism fold-in with data parity; consumers are unaffected. The builder owns the
per-edition atomic ``replaceWhere`` write, ``_ops`` registration, and grants.

**Flat**: code + short/long title + a small chapter grouping (``category`` + ``chapter``). Like
``codes.icd10pcs`` and unlike ``codes.icd9cm``, there is **no** classification hierarchy
(parent/ancestors/level) -- just the flat code list with chapter labels.

ICD-9-CM is **frozen**: Version 32 (effective 2014-10-01) is the final release, valid through
2015-09-30. So this is a single, immutable edition (``edition_year`` defaults to 2015) written with
``vintage_column="edition_year"`` + ``vintage_snapshot`` (atomic ``replaceWhere``; ADR 0034), landed
``PER_VINTAGE_IMMUTABLE`` (fetch-once, skip-if-present). Public domain (CMS) -- plain HTTPS download,
no credential. The ``codes.icd9_procedures_current`` view is dropped (ADR 0034: "current" =
``MAX(edition_year)`` / the live idiom), matching the RUCA fold.

The CMS listing page is JavaScript-rendered and the direct-download slug can shift, so
``--source-url`` accepts the live link (grab it from the CMS ICD-9-CM code-titles page if the
templated default 404s) -- the same operator-override pattern the ICD-10-PCS build uses.

Usage:
    build_icd9_procedures.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --edition-year 2015 --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

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
from cidmath_datahub.reference import icd9_procedures

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "icd9_procedures"
PIPELINE_REF = "bundles/_reference/src/build_icd9_procedures.py"

# Verbatim landed payload name inside the per-edition Volume dir (dir is already vintage-scoped).
_DESCRIPTIONS_ZIP = "master_descriptions.zip"

# ICD-9-CM Volume 3 has ~2,600 procedure codes (V32). WARN outside a generous band -- a real
# edition should never be this small, and a runaway count signals a parse/layout problem.
_CARDINALITY_MIN = 2_000
_CARDINALITY_MAX = 4_500

# Raw (codes_raw.icd9_procedures) == canonical (codes.icd9_procedures): flat, no derived columns.
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

_DDL = (
    "icd9_procedure_code STRING, edition_year INT, short_title STRING, long_title STRING, "
    "is_billable BOOLEAN, category STRING, chapter_code STRING, chapter_name STRING, "
    "source_file STRING, ingested_at TIMESTAMP"
)

_DESC = (
    "ICD-9-CM Volume 3 (procedure) codes from CMS. Flat: icd9_procedure_code (2-digit category + "
    "up to 2 decimals, e.g. 47.01), short/long title, is_billable (leaf code), and a chapter "
    "grouping (category + chapter_code/name). Frozen at Version 32 (final). PK (icd9_procedure_code, "
    "edition_year)."
)
_PHR = (
    "Canonical legacy inpatient-procedure code standard for U.S. data predating the 2015-10-01 "
    "ICD-10 transition; lets historical encounter/procedure feeds conform ICD-9 procedure codes to "
    "a shared reference (e.g. 47.01 Laparoscopic appendectomy). The ICD-9 counterpart to "
    "codes.icd10pcs."
)
_KNOWN_LIMITATIONS = (
    "Code list + chapter grouping only: icd9_procedure_code, short/long title, is_billable "
    "(leaf-of-set), and category + chapter (the 18 fixed Volume-3 chapters). No classification "
    "hierarchy (parent/ancestors/level) -- contrast codes.icd9cm, which carries the ADR-0031 "
    "tree; this table mirrors the flat codes.icd10pcs shape. ICD-9-CM is frozen at Version 32 "
    "(effective 2014-10-01, final), valid for US coding through 2015-09-30; there are no newer "
    "editions and no mid-year updates."
)


# ---------------------------------------------------------------------------
# IO: download the CMS zip + extract the two procedure (SG) members (ADR 0011 keeps the member
# knowledge in the pure module). Public HTTPS, no credential.
# ---------------------------------------------------------------------------


def _download_zip(url: str) -> bytes:
    """Download the CMS master-descriptions zip (public, no auth); validate the PK signature."""
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
    """Return ``(long_text, short_text, long_member_basename)`` for the procedure (SG) files."""
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        long_member = icd9_procedures.select_long_member(names)
        short_member = icd9_procedures.select_short_member(names)
        long_text = zf.read(long_member).decode(icd9_procedures.SOURCE_ENCODING)
        short_text = zf.read(short_member).decode(icd9_procedures.SOURCE_ENCODING)
    log.info("Extracted SG members", extra={"long": long_member, "short": short_member})
    return long_text, short_text, long_member.replace("\\", "/").split("/")[-1]


# ---------------------------------------------------------------------------
# Volume landing hooks (ADR 0039): fetch the CMS zip verbatim, then read + parse into raw.
# ---------------------------------------------------------------------------


def _make_hooks(
    *, source_url: str | None
) -> tuple[Any, Any]:
    """Build the ``(fetch_to_volume, read_from_volume)`` pair bound to this run's config."""

    def _fetch(v: int, volume_dir: str) -> None:
        url = source_url or icd9_procedures.SOURCE_ZIP_URL
        log.info("Fetching ICD-9-CM v32 descriptions zip", extra={"edition_year": v, "url": url})
        (Path(volume_dir) / _DESCRIPTIONS_ZIP).write_bytes(_download_zip(url))

    def _read(ctx: BuildContext, v: int, volume_dir: str) -> Any:
        zip_bytes = (Path(volume_dir) / _DESCRIPTIONS_ZIP).read_bytes()
        long_text, short_text, long_member = _extract_sg_members(zip_bytes)
        long_pairs = icd9_procedures.parse_titles(long_text)
        short_pairs = icd9_procedures.parse_titles(short_text)
        records = icd9_procedures.assemble_records(long_pairs, short_pairs, v)
        log.info("Assembled ICD-9 procedures edition", extra={"edition_year": v, "rows": len(records)})

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
        return ctx.spark.createDataFrame(rows, ICD9_PROCEDURE_SPARK_SCHEMA).sort(
            "edition_year", "icd9_procedure_code"
        )

    return _fetch, _read


# ---------------------------------------------------------------------------
# DQ (ADR 0009) in validate_staging: reconstruct records from the staged raw table and reuse the
# pure icd9_procedures.py helpers. Blocking (FAIL, raises): (code, edition) uniqueness, non-null
# long_title, NN[.NN] format, chapter resolves. WARN/INFO: cardinality, distribution.
# ---------------------------------------------------------------------------


def _reconstruct_records(
    ctx: BuildContext, staging_fqn: str
) -> list[icd9_procedures.Icd9ProcedureRecord]:
    return [
        icd9_procedures.Icd9ProcedureRecord(
            icd9_procedure_code=r["icd9_procedure_code"],
            edition_year=r["edition_year"],
            short_title=r["short_title"],
            long_title=r["long_title"],
            is_billable=r["is_billable"],
            category=r["category"],
            chapter_code=r["chapter_code"],
            chapter_name=r["chapter_name"],
        )
        for r in ctx.spark.sql(
            f"SELECT icd9_procedure_code, edition_year, short_title, long_title, is_billable, "
            f"category, chapter_code, chapter_name FROM {staging_fqn}"
        ).collect()
    ]


def _validate(ctx: BuildContext, staging_fqn: str) -> None:
    record_table = f"{MODEL_SCHEMA}.{TABLE}"
    records = _reconstruct_records(ctx, staging_fqn)
    total = len(records)
    failures: list[str] = []

    dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
    if not dq.unique(
        keys=["icd9_procedure_code", "edition_year"],
        check_name="icd9_procedure_code_edition_year_uniqueness",
        raise_on_fail=False,
    ):
        failures.append("duplicate (icd9_procedure_code, edition_year)")

    missing_title = icd9_procedures.find_missing_long_titles(records)
    _record(ctx, record_table, "icd9_procedure_long_title_not_null", DQCategory.NULLABILITY,
            not missing_title, len(missing_title), total,
            {"sample_missing": [list(k) for k in missing_title[:10]]} if missing_title else None)
    if missing_title:
        failures.append(f"null long_title: {missing_title[:5]}")

    bad_format = icd9_procedures.find_format_violations(records)
    _record(ctx, record_table, "icd9_procedure_code_format", DQCategory.BUSINESS_RULE,
            not bad_format, len(bad_format), total,
            {"sample_violations": bad_format[:10]} if bad_format else None)
    if bad_format:
        failures.append(f"malformed icd9_procedure_code: {bad_format[:5]}")

    bad_chapters = icd9_procedures.find_bad_chapters(records)
    _record(ctx, record_table, "icd9_procedure_chapter_resolved", DQCategory.BUSINESS_RULE,
            not bad_chapters, len(bad_chapters), total,
            {"sample": [list(c) for c in bad_chapters[:10]]} if bad_chapters else None)
    if bad_chapters:
        failures.append(f"unresolved chapter: {bad_chapters[:5]}")

    # --- WARN / INFO per edition ---
    for year in sorted({r.edition_year for r in records}):
        ed = [r for r in records if r.edition_year == year]
        ok = _CARDINALITY_MIN <= len(ed) <= _CARDINALITY_MAX
        _record(ctx, record_table, f"icd9_procedure_cardinality_{year}", DQCategory.CARDINALITY,
                ok, 0 if ok else 1, len(ed),
                {"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": len(ed)},
                severity=DQSeverity.WARN)
        _record(ctx, record_table, f"icd9_procedure_chapter_distribution_{year}",
                DQCategory.BUSINESS_RULE, True, 0, len(ed),
                {"distribution": icd9_procedures.chapter_distribution(ed),
                 "billable_share": round(icd9_procedures.billable_share(ed), 4)},
                severity=DQSeverity.INFO)

    if failures:
        raise ValueError("ICD-9 procedure blocking DQ failed -- " + "; ".join(failures))


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


def _base_entry(edition_year: int) -> registration.DatasetCatalogEntry:
    # Version 32 is effective Oct 1 of the prior calendar year through Sep 30 of the FY.
    cov_start = date(edition_year - 1, 10, 1)
    cov_end = date(edition_year, 9, 30)
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        spatial_resolution="none",
        spatial_coverage="United States",
        source_provider_code="cms",
        source_url=icd9_procedures.SOURCE_LANDING_URL,
        source_documentation_url=icd9_procedures.SOURCE_LANDING_URL,
        source_data_dictionary_url=icd9_procedures.SOURCE_LANDING_URL,
        license="public domain (U.S. Government work, 17 U.S.C. 105)",
        dua_required=False,
        dua_reference="No DUA. CMS ICD-9-CM files are public domain.",
        access_tier="open",
        external_maintainer_name="Centers for Medicare & Medicaid Services (CMS)",
        is_hosted=True,
        temporal_coverage_start=cov_start,
        temporal_coverage_end=cov_end,
        temporal_resolution="annual",
        known_limitations=_KNOWN_LIMITATIONS,
        derived_from=[f"CMS ICD-9-CM Version 32 master descriptions (FY{edition_year})"],
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    edition_year: int = 2015,
    source_url: str | None = None,
) -> None:
    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    fetch, read = _make_hooks(source_url=source_url)

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
        ).sort("edition_year", "icd9_procedure_code")

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(edition_year),
        vintage_column="edition_year",
        raw_landings=[
            RawLanding(
                table=TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=fetch,
                read_from_volume=read,
                description=(
                    "Raw CMS ICD-9-CM Version 32 procedure (SG) master descriptions, fetched-as-is, "
                    "edition-stamped. Volume-landed verbatim, then parsed. Promoted to "
                    "codes.icd9_procedures."
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
                canonical_cluster_columns=["edition_year", "icd9_procedure_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(edition_year,))
    log.info("ICD-9 procedures build complete", extra={"edition": edition_year})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-catalog", required=True, help="Source catalog for raw (ecdh_<env>).")
    parser.add_argument("--model-catalog", required=True,
                        help="Model catalog for canonical (ecdh_model_<env>).")
    parser.add_argument(
        "--edition-year", type=int, default=2015,
        help="Fiscal-year edition to stamp (Version 32 is the final release -> 2015). Default: 2015.",
    )
    parser.add_argument(
        "--source-url", default=None,
        help="Override the CMS Version 32 master-descriptions zip URL (paste the live link if "
        "the templated default has shifted).",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.source_catalog,
        args.model_catalog,
        args.data_engineers_group,
        args.analysts_group,
        edition_year=args.edition_year,
        source_url=args.source_url,
    )


if __name__ == "__main__":
    main()
