"""Build the canonical ``codes.icd10cm`` reference table on the shared builder (ADR 0037/0039/0030).

ICD-10-CM is the U.S. Clinical Modification of ICD-10 diagnosis codes (CDC/NCHS) -- the public-health
diagnosis standard -- *not* WHO ICD-10 or ICD-10-PCS (procedures). This entrypoint is the thin IO +
Spark layer over the pure logic in ``cidmath_datahub.reference.icd10cm`` (ADR 0011). For each
fiscal-year edition it downloads the CDC NCHS Oct-1 base order file and, when published, overlays the
Apr-1 mid-year update (update wins per code), then downloads the tabular XML and builds the
classification hierarchy (adjacency ``parent_icd10cm_code``, materialized path ``ancestor_codes``,
depth ``node_level``, and denormalized chapter/block labels; ADR 0030).

**Source-path fold-in (ADR 0037 backport, wave 4).** Previously model-only + hand-rolled on
``run_build`` with no raw layer, and *mislabeled* ``update_semantics="full_refresh"`` while actually
doing a per-edition DELETE+append. Now folded onto the shared ``build_reference`` builder: the
per-edition source payloads (base order file, optional Apr-1 update, tabular XML, optional update
XML) land verbatim in the *source*-catalog landing Volume ``ecdh_<env>.codes_raw._landing``
(ADR 0039); ``read`` combines them (overlay + ``build_hierarchy``) into the denormalized 1:1 raw
table ``ecdh_<env>.codes_raw.icd10cm``, and the canonical ``ecdh_model_<env>.codes.icd10cm`` is
promoted from raw with per-edition atomic ``replaceWhere`` (``vintage_snapshot`` -- the one real
semantics tidy-up in the epic: ``full_refresh`` -> vintaged, ADR 0034). Same schema, same rows, same
denormalized-hierarchy shape (no processed stage; ADR 0030) -- data parity; consumers unaffected.

Keyed by ``(icd10cm_code, edition_year)``. ``--midyear-update`` (auto/require/skip) controls the
overlay; ``--hierarchy`` (build/skip) controls the tabular-XML download. Editions are immutable +
re-pullable (``PER_VINTAGE_IMMUTABLE`` landing). Public domain (CDC/NCHS). The ``_current`` view was
never created for this table.

Blocking DQ (FAIL, raises): ``(icd10cm_code, edition_year)`` uniqueness, non-null ``description``,
ICD-10-CM code-format. WARN: per-edition cardinality (~70k+), and the hierarchy checks (XML-vs-prefix
adjacency, chapter/block resolution, orphans).

Usage:
    build_icd10cm.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --edition-year 2026 \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
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
from cidmath_datahub.reference import icd10cm

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "icd10cm"
PIPELINE_REF = "bundles/_reference/src/build_icd10cm.py"

# Verbatim landed payload names inside the per-edition Volume dir (dir is already vintage-scoped).
_BASE_ORDER_ZIP = "base_order.zip"
_UPDATE_ORDER_ZIP = "update_order.zip"
_BASE_TABULAR_ZIP = "base_tabular.zip"
_UPDATE_TABULAR_ZIP = "update_tabular.zip"

# ICD-10-CM has ~70k+ codes per edition (FY2026 ~74k). WARN outside a generous sanity band.
_CARDINALITY_MIN = 60_000
_CARDINALITY_MAX = 120_000

# Flat code columns + the classification hierarchy (ADR 0030); raw == canonical.
ICD10_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("icd10cm_code", T.StringType(), False),
        T.StructField("edition_year", T.IntegerType(), False),
        T.StructField("description", T.StringType(), False),
        T.StructField("is_billable", T.BooleanType(), False),
        T.StructField("parent_icd10cm_code", T.StringType(), True),
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

_DDL = (
    "icd10cm_code STRING, edition_year INT, description STRING, is_billable BOOLEAN, "
    "parent_icd10cm_code STRING, node_level INT, ancestor_codes ARRAY<STRING>, chapter_code STRING, "
    "chapter_name STRING, block_code STRING, block_name STRING, source_file STRING, "
    "ingested_at TIMESTAMP"
)

_DESC = (
    "ICD-10-CM (Clinical Modification) diagnosis code system from CDC/NCHS, with the full "
    "classification hierarchy: parent_icd10cm_code, ancestor_codes (root->parent path), node_level, "
    "and denormalized chapter/block labels (ADR 0030). One row per code per fiscal-year edition, "
    "reflecting the latest within-year release (Oct-1 base with the Apr-1 mid-year update overlaid "
    "where published). is_billable distinguishes valid leaf codes from category headers. PK "
    "(icd10cm_code, edition_year)."
)
_PHR = (
    "Canonical diagnosis-code standard for U.S. surveillance and clinical data; lets case/encounter "
    "feeds conform diagnosis codes to a shared, versioned reference (e.g. U07.1 COVID-19, J18.9 "
    "pneumonia)."
)
_KNOWN_LIMITATIONS = (
    "ICD-10-CM diagnosis codes only (not ICD-10-PCS procedures or WHO ICD-10). Each edition is the "
    "Oct-1 base with the Apr-1 mid-year update overlaid where published (update wins per code), so "
    "within-year release timing is collapsed into one edition_year. Seventh-character codes attach "
    "to their nearest listed ancestor, not a synthetic stem, and node_level reflects the adjacency "
    "tree (ADR 0030). Instructional notes (excludes1/2, useAdditionalCode, 7th-char definitions) are "
    "out of scope -- a future codes.icd10_note side table."
)

_COMMON_META: dict[str, Any] = {
    "spatial_resolution": "none",
    "spatial_coverage": "United States",
    "source_provider_code": "cdc",
    "source_url": icd10cm.SOURCE_FILES_PAGE_URL,
    "source_documentation_url": icd10cm.ORDER_FILE_FORMAT_DOC_URL,
    "source_data_dictionary_url": icd10cm.ORDER_FILE_FORMAT_DOC_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. CDC/NCHS ICD-10-CM files are public domain.",
    "access_tier": "open",
    "external_maintainer_name": "National Center for Health Statistics (NCHS), CDC",
    "is_hosted": True,
    "temporal_resolution": "annual",
}


def _base_entry(editions: list[int]) -> registration.DatasetCatalogEntry:
    cov_start = date(min(editions) - 1, 10, 1)
    cov_end = date(max(editions), 9, 30)
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        temporal_coverage_start=cov_start,
        temporal_coverage_end=cov_end,
        derived_from=[f"CDC/NCHS ICD-10-CM order file + tabular XML FY{y}" for y in editions],
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: download a CDC zip; extract a member from landed zip bytes (ADR 0011 keeps the member
# knowledge in icd10cm.py). Public HTTPS, no credential.
# ---------------------------------------------------------------------------


def _download_zip(url: str) -> bytes:
    """Download a CDC zip and return its raw bytes (public, no auth)."""
    # CDC filenames contain spaces; percent-encode the path so urllib accepts the URL.
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC NCHS host
        return resp.read()


def _download_optional_zip(url: str, *, required: bool) -> bytes | None:
    """Download an optional CDC zip, returning ``None`` on 404 (unless ``required``)."""
    try:
        return _download_zip(url)
    except HTTPError as exc:
        if exc.code == 404 and not required:
            log.warning("Optional source not found (skipping)", extra={"url": url})
            return None
        raise


def _extract_member(
    zip_bytes: bytes, selector: Callable[[list[str]], str], *, encoding: str
) -> tuple[str, str] | None:
    """Return ``(member_text, member_name)`` for the selected member, or ``None`` if none matches.

    A ``None`` return (selector raises ValueError -- e.g. a code-neutral update shipping only PDFs)
    is tolerated for optional payloads.
    """
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        try:
            member = selector(zf.namelist())
        except ValueError:
            return None
        raw = zf.read(member)
    return raw.decode(encoding), member


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the run's config + parsed stash).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    url_template: str = icd10cm.ORDER_FILE_ZIP_URL_TEMPLATE,
    midyear_update: str = "auto",
    hierarchy: str = "build",
    tabular_url_template: str = icd10cm.TABULAR_ZIP_URL_TEMPLATE,
) -> None:
    editions = sorted(set(edition_years))
    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    # Per-edition parsed state (records + hierarchy nodes + adjacency mismatches + overlay stats)
    # stashed by read for validate; the tabular parent_of map isn't persisted so validate can't
    # reconstruct the adjacency check from the table.
    stash: dict[int, dict[str, Any]] = {}

    def _fetch(v: int, volume_dir: str) -> None:
        d = Path(volume_dir)
        base_url = icd10cm.order_file_zip_url(v, template=url_template)
        log.info("Fetching base order file", extra={"edition_year": v, "url": base_url})
        (d / _BASE_ORDER_ZIP).write_bytes(_download_zip(base_url))
        update_applied = False
        if midyear_update != "skip":
            upd = _download_optional_zip(
                icd10cm.update_file_zip_url(v), required=(midyear_update == "require")
            )
            if upd is not None:
                (d / _UPDATE_ORDER_ZIP).write_bytes(upd)
                update_applied = True
        if hierarchy != "skip":
            (d / _BASE_TABULAR_ZIP).write_bytes(
                _download_zip(icd10cm.tabular_zip_url(v, template=tabular_url_template))
            )
            if update_applied:
                upd_tab = _download_optional_zip(icd10cm.update_tabular_zip_url(v), required=False)
                if upd_tab is not None:
                    (d / _UPDATE_TABULAR_ZIP).write_bytes(upd_tab)

    def _read(ctx: BuildContext, v: int, volume_dir: str) -> Any:
        d = Path(volume_dir)
        base = _extract_member(
            (d / _BASE_ORDER_ZIP).read_bytes(), icd10cm.select_order_file_member, encoding="latin-1"
        )
        if base is None:
            raise ValueError(f"ICD-10-CM base order file for edition {v} has no usable member")
        base_text, base_member = base
        base_recs = icd10cm.parse_order_file(base_text, v)

        update_recs: list[icd10cm.Icd10Record] = []
        update_member: str | None = None
        if (d / _UPDATE_ORDER_ZIP).exists():
            upd = _extract_member(
                (d / _UPDATE_ORDER_ZIP).read_bytes(),
                icd10cm.select_order_file_member, encoding="latin-1",
            )
            if upd is not None:
                update_text, update_member = upd
                update_recs = icd10cm.parse_order_file(update_text, v)
        merged = icd10cm.overlay_records(base_recs, update_recs)

        category_map: dict[str, icd10cm.CategoryGroup] = {}
        parent_of: dict[str, str | None] = {}
        if (d / _BASE_TABULAR_ZIP).exists():
            tab = _extract_member(
                (d / _BASE_TABULAR_ZIP).read_bytes(),
                icd10cm.select_tabular_xml_member, encoding="utf-8",
            )
            if tab is not None:
                tree = icd10cm.parse_tabular_tree(tab[0])
                category_map, parent_of = tree.category_map, tree.parent_of
                if (d / _UPDATE_TABULAR_ZIP).exists():
                    upd_tab = _extract_member(
                        (d / _UPDATE_TABULAR_ZIP).read_bytes(),
                        icd10cm.select_tabular_xml_member, encoding="utf-8",
                    )
                    if upd_tab is not None:
                        upd_tree = icd10cm.parse_tabular_tree(upd_tab[0])
                        category_map = {**category_map, **upd_tree.category_map}
                        parent_of = {**parent_of, **upd_tree.parent_of}

        nodes = icd10cm.build_hierarchy(merged, category_map, parent_of)
        mismatches = icd10cm.find_adjacency_mismatches(merged, parent_of)
        base_codes = {r.icd10cm_code for r in base_recs}
        update_codes = {r.icd10cm_code for r in update_recs}
        stash[v] = {
            "records": merged, "nodes": nodes, "mismatches": mismatches,
            "stats": {
                "base_member": base_member, "update_member": update_member,
                "base_codes": len(base_codes), "update_codes": len(update_codes),
                "added_by_update": len(update_codes - base_codes),
                "unmapped_categories": len(icd10cm.find_unmapped_categories(nodes)),
                "final": len(merged),
            },
        }
        log.info("Built ICD-10-CM edition", extra={"edition_year": v, **stash[v]["stats"]})

        now = datetime.now(tz=UTC)
        rows = [
            {
                "icd10cm_code": n.icd10cm_code,
                "edition_year": n.edition_year,
                "description": n.description,
                "is_billable": n.is_billable,
                "parent_icd10cm_code": n.parent_icd10cm_code,
                "node_level": n.node_level,
                "ancestor_codes": list(n.ancestor_codes),
                "chapter_code": n.chapter_code,
                "chapter_name": n.chapter_name,
                "block_code": n.block_code,
                "block_name": n.block_name,
                "source_file": (
                    update_member
                    if (update_member is not None and n.icd10cm_code in update_codes)
                    else base_member
                ),
                "ingested_at": now,
            }
            for n in nodes
        ]
        return ctx.spark.createDataFrame(rows, ICD10_SPARK_SCHEMA).sort(
            "edition_year", "icd10cm_code"
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
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, NDC, "
            f"LOINC, SNOMED CT, RxNorm, ...). Owned by the _reference bundle. See ADR 0014.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{TABLE} ({_DDL}) USING DELTA"
        )

    def _promote(ctx: BuildContext, v: int) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {raw_fqn} WHERE edition_year = {int(v)}"
        ).sort("edition_year", "icd10cm_code")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{TABLE}"
        # Aggregate the per-edition stash (typically one edition per run).
        records = [r for e in stash.values() for r in e["records"]]
        nodes = [n for e in stash.values() for n in e["nodes"]]
        mismatches = [m for e in stash.values() for m in e["mismatches"]]
        editions = sorted(stash)
        total = len(records)
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
        if not dq.unique(keys=["icd10cm_code", "edition_year"],
                         check_name="icd10cm_code_edition_year_uniqueness", raise_on_fail=False):
            failures.append("duplicate (icd10cm_code, edition_year)")

        missing_desc = icd10cm.find_missing_descriptions(records)
        _record(ctx, record_table, "icd10_description_not_null", DQCategory.NULLABILITY,
                not missing_desc, len(missing_desc), total,
                {"sample_missing": [list(k) for k in missing_desc[:10]]} if missing_desc else None)
        if missing_desc:
            failures.append(f"null description: {missing_desc[:5]}")

        bad_format = icd10cm.find_format_violations(records)
        _record(ctx, record_table, "icd10cm_code_format", DQCategory.BUSINESS_RULE,
                not bad_format, len(bad_format), total,
                {"sample_violations": bad_format[:10]} if bad_format else None)
        if bad_format:
            failures.append(f"malformed icd10cm_code: {bad_format[:5]}")

        # --- WARN per edition + hierarchy WARNs (non-blocking, ADR 0030) ---
        for year in editions:
            n = sum(1 for r in records if r.edition_year == year)
            ok = _CARDINALITY_MIN <= n <= _CARDINALITY_MAX
            _record(ctx, record_table, f"icd10_cardinality_{year}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, n,
                    {"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": n},
                    severity=DQSeverity.WARN)
            _record(ctx, record_table, f"midyear_update_overlay_{year}", DQCategory.BUSINESS_RULE,
                    True, 0, stash[year]["stats"].get("final"), stash[year]["stats"],
                    severity=DQSeverity.INFO)

        _record(ctx, record_table, "icd10_xml_vs_prefix_adjacency_agree", DQCategory.BUSINESS_RULE,
                not mismatches, len(mismatches), len(nodes),
                {"sample_mismatches": sorted(mismatches)[:20]} if mismatches else None,
                severity=DQSeverity.WARN)
        unmapped = icd10cm.find_unmapped_categories(nodes)
        _record(ctx, record_table, "icd10_chapter_block_resolved", DQCategory.REFERENTIAL,
                not unmapped, len(unmapped), len(nodes),
                {"unmapped_categories": unmapped[:20]} if unmapped else None, severity=DQSeverity.WARN)
        orphans = icd10cm.find_orphan_codes(nodes)
        _record(ctx, record_table, "icd10_parent_resolved", DQCategory.REFERENTIAL,
                not orphans, len(orphans), len(nodes),
                {"sample_orphans": orphans[:20]} if orphans else None, severity=DQSeverity.WARN)

        if failures:
            raise ValueError("ICD-10-CM blocking DQ failed -- " + "; ".join(failures))

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
                fetch_to_volume=_fetch,
                read_from_volume=_read,
                description=(
                    "Raw CDC/NCHS ICD-10-CM per-edition payloads (Oct-1 base order file + optional "
                    "Apr-1 update + tabular XML), fetched-as-is, edition-stamped. Volume-landed "
                    "verbatim, then combined + hierarchy-built. Promoted to codes.icd10cm."
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
                canonical_cluster_columns=["edition_year", "icd10cm_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=tuple(editions))
    log.info("ICD-10-CM build complete", extra={"editions": editions})


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-catalog", required=True, help="Source catalog for raw (ecdh_<env>).")
    parser.add_argument("--model-catalog", required=True,
                        help="Model catalog for canonical (ecdh_model_<env>).")
    parser.add_argument(
        "--edition-year", type=int, nargs="+", default=[2026],
        help="ICD-10-CM fiscal-year edition(s) to load (effective Oct 1). Default: 2026.",
    )
    parser.add_argument(
        "--url-template", default=icd10cm.ORDER_FILE_ZIP_URL_TEMPLATE,
        help="Override the base order-file zip URL template (must contain '{year}').",
    )
    parser.add_argument(
        "--midyear-update", choices=["auto", "require", "skip"], default="auto",
        help="Apr-1 mid-year update handling: 'auto' overlays when published, skips on 404; "
        "'require' fails if missing; 'skip' loads the Oct-1 base only.",
    )
    parser.add_argument(
        "--hierarchy", choices=["build", "skip"], default="build",
        help="'build' (default) downloads the tabular XML and sources adjacency + chapter/block from "
        "it; 'skip' downloads no XML, leaves chapter/block null, derives adjacency by prefix.",
    )
    parser.add_argument(
        "--tabular-url-template", default=icd10cm.TABULAR_ZIP_URL_TEMPLATE,
        help="Override the base tabular-XML zip URL template (must contain '{year}').",
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
        midyear_update=args.midyear_update,
        hierarchy=args.hierarchy,
        tabular_url_template=args.tabular_url_template,
    )


if __name__ == "__main__":
    main()
