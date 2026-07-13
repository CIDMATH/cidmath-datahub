"""Build the canonical ``codes.icd9cm`` reference table on the shared builder (ADR 0037/0039/0031).

ICD-9-CM diagnosis codes (NCHS Tabular List of Diseases, Volume 1, incl. the V and E supplementary
classifications) for U.S. coding before the 2015-10-01 ICD-10 transition. This entrypoint is the thin
IO + Spark layer over the pure logic in ``cidmath_datahub.reference.icd9cm`` (ADR 0011). For each
fiscal-year edition it downloads the ``DTAB`` (tabular list) and ``APPNDX`` (Appendix E) zips, converts
the RTF members to text, parses them, assembles records (``is_billable`` = leaf-of-set) and builds the
hierarchy (prefix-rule adjacency + Appendix-E chapter/block; ADR 0031) -- the same column shape and
semantics as ``codes.icd10cm``.

**Source-path fold-in (ADR 0037 backport, wave 4).** Previously model-only + hand-rolled on
``run_build`` with no raw layer, and *mislabeled* ``update_semantics="full_refresh"`` while actually
doing a per-edition DELETE+append. Now folded onto the shared ``build_reference`` builder: the
per-edition DTAB + Appendix-E zips land verbatim in the *source*-catalog landing Volume
``ecdh_<env>.codes_raw._landing`` (ADR 0039); ``read`` de-RTFs, parses, and builds the hierarchy into
the denormalized 1:1 raw table ``ecdh_<env>.codes_raw.icd9cm``, and the canonical
``ecdh_model_<env>.codes.icd9cm`` is promoted from raw with per-edition atomic ``replaceWhere``
(``vintage_snapshot`` -- the ``full_refresh`` -> vintaged tidy-up, ADR 0034). Same schema, same rows,
same denormalized-hierarchy shape (no processed stage; ADR 0031) -- data parity; consumers unaffected.

Keyed by ``(icd9cm_code, edition_year)``. ICD-9-CM is frozen (final FY2014, valid through 2015-09-30),
so editions are pure annual base releases (no mid-year overlay). ``--hierarchy`` (build/skip) controls
the Appendix-E download; adjacency is always computed from the code set. Editions are immutable +
re-pullable (``PER_VINTAGE_IMMUTABLE`` landing). The NCHS FTP archive's latest full RTF release is
FY2012 (directory ``2011``); available editions run roughly FY1997-FY2012.

Blocking DQ (FAIL, raises): ``(icd9cm_code, edition_year)`` uniqueness, non-null ``description``,
ICD-9-CM code-format, and parent referential integrity. WARN: cardinality (~13k-17k), chapter/block
resolution, orphans, V/E share.

Usage:
    build_icd9cm.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --edition-year 2012 \\
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
from cidmath_datahub.reference import icd9cm

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "icd9cm"
PIPELINE_REF = "bundles/_reference/src/build_icd9cm.py"

# Verbatim landed payload names inside the per-edition Volume dir (dir is already vintage-scoped).
_DTAB_ZIP = "dtab.zip"
_APPENDIX_ZIP = "appendix_e.zip"

# ICD-9-CM diagnosis codes incl. V/E are ~13k-17k per edition. WARN outside a generous band.
_CARDINALITY_MIN = 10_000
_CARDINALITY_MAX = 22_000

# Mirrors codes.icd10cm's shape (ADR 0031): flat columns + hierarchy; raw == canonical.
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

_DDL = (
    "icd9cm_code STRING, edition_year INT, description STRING, is_billable BOOLEAN, "
    "parent_icd9cm_code STRING, node_level INT, ancestor_codes ARRAY<STRING>, chapter_code STRING, "
    "chapter_name STRING, block_code STRING, block_name STRING, source_file STRING, "
    "ingested_at TIMESTAMP"
)

_DESC = (
    "ICD-9-CM (Clinical Modification) diagnosis code system from NCHS, with the classification "
    "hierarchy (parent_icd9cm_code, ancestor_codes, node_level, chapter/block). One row per code per "
    "fiscal-year edition; includes the V and E supplementary classifications. PK (icd9cm_code, "
    "edition_year)."
)
_PHR = (
    "Canonical diagnosis-code standard for U.S. surveillance/clinical data coded before the "
    "2015-10-01 ICD-10 transition; mirrors codes.icd10cm's hierarchy so pre/post-2015 data can be "
    "rolled up the same way (the GEM crosswalk bridges the two code sets in a separate table)."
)
_KNOWN_LIMITATIONS = (
    "ICD-9-CM diagnosis codes only (no Volume 3 procedures, no GEM crosswalk). Frozen: final edition "
    "FY2014, valid through 2015-09-30; no mid-year updates and no 7th-character concept. is_billable "
    "is leaf-of-set (highest-specificity rule). Chapter/block come from Appendix E; V/E "
    "classifications may be sourced separately if Appendix E does not enumerate them (ADR 0031)."
)

_COMMON_META: dict[str, Any] = {
    "spatial_resolution": "none",
    "spatial_coverage": "United States",
    "source_provider_code": "cdc",
    "source_url": icd9cm.SOURCE_LANDING_URL,
    "source_documentation_url": icd9cm.SOURCE_LANDING_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. NCHS ICD-9-CM files are public domain.",
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
        source_data_dictionary_url=icd9cm.readme_url(max(editions)),
        temporal_coverage_start=cov_start,
        temporal_coverage_end=cov_end,
        derived_from=(
            [icd9cm.dtab_zip_url(y) for y in editions]
            + [icd9cm.appendix_zip_url(y) for y in editions]
        ),
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: download a CDC zip; de-RTF a member from landed zip bytes (ADR 0011 keeps member
# knowledge in icd9cm.py; striprtf is lazily imported so the module loads without it).
# ---------------------------------------------------------------------------


def _download_zip(url: str) -> bytes:
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC NCHS host
        return resp.read()


def _rtf_member_text(zip_bytes: bytes, selector: Callable[[list[str]], str]) -> tuple[str, str]:
    """Extract the selected RTF member from zip bytes and return ``(de_rtf_text, member)``."""
    from striprtf.striprtf import rtf_to_text

    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        member = selector(zf.namelist())
        raw = zf.read(member)
    return rtf_to_text(raw.decode("latin-1")), member


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the run's config + parsed stash).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    edition_years: list[int],
    data_engineers_group: str,
    analysts_group: str,
    hierarchy: str = "build",
) -> None:
    editions = sorted(set(edition_years))
    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    stash: dict[int, dict[str, Any]] = {}

    def _fetch(v: int, volume_dir: str) -> None:
        d = Path(volume_dir)
        log.info("Fetching DTAB", extra={"edition_year": v})
        (d / _DTAB_ZIP).write_bytes(_download_zip(icd9cm.dtab_zip_url(v)))
        if hierarchy != "skip":
            log.info("Fetching Appendix E", extra={"edition_year": v})
            (d / _APPENDIX_ZIP).write_bytes(_download_zip(icd9cm.appendix_zip_url(v)))

    def _read(ctx: BuildContext, v: int, volume_dir: str) -> Any:
        d = Path(volume_dir)
        dtab_text, dtab_member = _rtf_member_text(
            (d / _DTAB_ZIP).read_bytes(), icd9cm.select_dtab_member
        )
        records = icd9cm.assemble_records(icd9cm.parse_dtab(dtab_text), v)

        category_map: dict[str, tuple[str, str]] = {}
        appendix_member: str | None = None
        if (d / _APPENDIX_ZIP).exists():
            apx_text, appendix_member = _rtf_member_text(
                (d / _APPENDIX_ZIP).read_bytes(), icd9cm.select_appendix_e_member
            )
            category_map = icd9cm.parse_appendix_e(apx_text)

        nodes = icd9cm.build_hierarchy(records, category_map)
        src = dtab_member + (f" + {appendix_member}" if appendix_member else "")
        stash[v] = {
            "records": records, "nodes": nodes,
            "stats": {
                "dtab_member": dtab_member, "appendix_member": appendix_member,
                "records": len(records),
                "billable": sum(1 for r in records if r.is_billable),
                "unmapped_categories": len(icd9cm.find_unmapped_categories(nodes)),
                "unmapped_blocks": len(icd9cm.find_unmapped_blocks(nodes)),
            },
        }
        log.info("Built ICD-9-CM edition", extra={"edition_year": v, **stash[v]["stats"]})

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
                "source_file": src,
                "ingested_at": now,
            }
            for n in nodes
        ]
        return ctx.spark.createDataFrame(rows, ICD9_SPARK_SCHEMA).sort("edition_year", "icd9cm_code")

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
        ).sort("edition_year", "icd9cm_code")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{TABLE}"
        records = [r for e in stash.values() for r in e["records"]]
        nodes = [n for e in stash.values() for n in e["nodes"]]
        editions_present = sorted(stash)
        stats = {y: stash[y]["stats"] for y in editions_present}
        total = len(records)
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
        if not dq.unique(keys=["icd9cm_code", "edition_year"],
                         check_name="icd9cm_code_edition_year_uniqueness", raise_on_fail=False):
            failures.append("duplicate (icd9cm_code, edition_year)")

        missing_desc = icd9cm.find_missing_descriptions(records)
        _record(ctx, record_table, "icd9_description_not_null", DQCategory.NULLABILITY,
                not missing_desc, len(missing_desc), total,
                {"sample": [list(k) for k in missing_desc[:10]]} if missing_desc else None)
        if missing_desc:
            failures.append(f"null description: {missing_desc[:5]}")

        bad_format = icd9cm.find_format_violations(records)
        _record(ctx, record_table, "icd9cm_code_format", DQCategory.BUSINESS_RULE,
                not bad_format, len(bad_format), total,
                {"sample": bad_format[:10]} if bad_format else None)
        if bad_format:
            failures.append(f"malformed icd9cm_code: {bad_format[:5]}")

        dangling = icd9cm.find_dangling_parents(nodes)
        _record(ctx, record_table, "icd9_parent_referential_integrity", DQCategory.REFERENTIAL,
                not dangling, len(dangling), total, {"sample": dangling[:10]} if dangling else None)
        if dangling:
            failures.append(f"parent not in edition: {dangling[:5]}")

        # --- WARN / INFO ---
        for year in editions_present:
            n = sum(1 for r in records if r.edition_year == year)
            ok = _CARDINALITY_MIN <= n <= _CARDINALITY_MAX
            _record(ctx, record_table, f"icd9_cardinality_{year}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, n,
                    {"expected_range": [_CARDINALITY_MIN, _CARDINALITY_MAX], "actual": n},
                    severity=DQSeverity.WARN)

        unmapped_chapters = icd9cm.find_unmapped_categories(nodes)
        _record(ctx, record_table, "icd9_chapter_resolved", DQCategory.REFERENTIAL,
                not unmapped_chapters, len(unmapped_chapters), total,
                {"unmapped_categories": unmapped_chapters[:30]} if unmapped_chapters else None,
                severity=DQSeverity.WARN)
        unmapped_blocks = icd9cm.find_unmapped_blocks(nodes)
        _record(ctx, record_table, "icd9_block_resolved", DQCategory.REFERENTIAL,
                not unmapped_blocks, len(unmapped_blocks), total,
                {"unmapped_blocks": unmapped_blocks[:30]} if unmapped_blocks else None,
                severity=DQSeverity.WARN)
        orphans = icd9cm.find_orphan_codes(nodes)
        _record(ctx, record_table, "icd9_parent_resolved", DQCategory.REFERENTIAL,
                not orphans, len(orphans), total,
                {"sample_orphans": orphans[:20]} if orphans else None, severity=DQSeverity.WARN)

        v_count = sum(1 for r in records if icd9cm.code_class(r.icd9cm_code) == "V")
        e_count = sum(1 for r in records if icd9cm.code_class(r.icd9cm_code) == "E")
        _record(ctx, record_table, "icd9_ve_code_share", DQCategory.CARDINALITY,
                v_count > 0 and e_count > 0, 0, total,
                {"v_codes": v_count, "e_codes": e_count, "editions": stats},
                severity=DQSeverity.WARN)

        if failures:
            raise ValueError("ICD-9-CM blocking DQ failed -- " + "; ".join(failures))

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
                    "Raw NCHS ICD-9-CM per-edition payloads (DTAB tabular list + Appendix-E RTF "
                    "zips), fetched-as-is, edition-stamped. Volume-landed verbatim, then de-RTF'd, "
                    "parsed, and hierarchy-built. Promoted to codes.icd9cm."
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
                canonical_cluster_columns=["edition_year", "icd9cm_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=tuple(editions))
    log.info("ICD-9-CM build complete", extra={"editions": editions})


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
        "--edition-year", type=int, nargs="+", default=[2012],
        help="ICD-9-CM fiscal-year edition(s). Default: 2012 (latest full RTF release; dir 2011).",
    )
    parser.add_argument(
        "--hierarchy", choices=["build", "skip"], default="build",
        help="'build' (default) downloads Appendix E for chapter/block; 'skip' leaves them null.",
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
        hierarchy=args.hierarchy,
    )


if __name__ == "__main__":
    main()
