"""Build the canonical ``codes.ndc_product`` + ``codes.ndc_package`` tables (ADR 0014, ADR 0032).

The FDA NDC Directory (finished drugs) is the canonical national drug code set
that drug/pharmacy data (claims, dispensing, vaccine NDCs) conform to. This
entrypoint is the thin IO + Spark layer over the pure logic in
``cidmath_datahub.reference.ndc`` (ADR 0011). Two **flat** grains (no hierarchy):
``codes.ndc_product`` (product-level) and ``codes.ndc_package`` (package-level,
the grain pharmacy claims carry), FK-linked by ``product_id``.

History model (ADR 0032). The directory is revised in place (FDA publishes only
the current list), so we preserve history ourselves with two paired mechanisms:

  1. **Raw immutable Volume snapshot.** Each run writes the fetched ``ndctext.zip``
     verbatim to a date-stamped file on a UC Volume
     (``/Volumes/<catalog>/codes/<volume>/ndctext_<YYYY-MM-DD>.zip``) and never
     overwrites an existing date. A same-day re-run reads the existing file, so the
     tables always reflect the immutable snapshot of record.
  2. **In-table revision tracking via ``snapshot_replace``.** Both tables are keyed
     by ``snapshot_date``; each run DELETEs only its own ``snapshot_date`` rows and
     appends, leaving prior snapshots intact (geography per-vintage replace, ADR
     0024). Pulled quarterly; the source's marketing dates already encode
     real-world validity, so periodic snapshots suffice (no SCD2; see ADR 0032).

Keys (confirmed against the FDA file-definition pages): ``ndc_product`` is keyed
by ``(product_id, snapshot_date)`` -- FDA's ``ProductID`` (NDCproductcode + SPL
documentID) is the stable join/dedup key because ``PRODUCTNDC`` is not unique per
snapshot; ``ndc_package`` is keyed by ``(product_id, ndc_package_code,
snapshot_date)`` and FK-links to ``ndc_product`` by ``product_id``.

Thin entrypoint over the ``run_build`` seam (ADR 0027). Blocking DQ (FAIL, raises):
PK uniqueness on both tables (after collapsing the FDA files' occasional duplicate
key-rows); non-null required fields; ``*_ndc`` normalization (package -> 11 digits,
product -> 9 digits). The package -> product FK is **informational** (ADR 0014) -- the
FDA files have real referential gaps -- so it is a WARN, not a build blocker. WARN
also: cardinality, collapsed-duplicate counts, marketing-date order, DEA-schedule
vocab, freshness.

Usage:
    build_ndc.py --catalog ecdh_model_dev \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import io
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
from cidmath_datahub.reference import ndc

log = get_logger(__name__)

SCHEMA = "codes"
PRODUCT_TABLE = "ndc_product"
PACKAGE_TABLE = "ndc_package"
PRODUCT_VIEW = "ndc_product_current"
PACKAGE_VIEW = "ndc_package_current"
PIPELINE_REF = "bundles/_reference/src/build_ndc.py"

#: Managed Volume (codes schema) for the immutable raw ndctext.zip snapshots (ADR 0032).
DEFAULT_VOLUME = "ndc_raw"

PRODUCT_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), False),
        T.StructField("product_ndc", T.StringType(), False),
        T.StructField("product_ndc_normalized", T.StringType(), True),
        T.StructField("product_type_name", T.StringType(), True),
        T.StructField("proprietary_name", T.StringType(), True),
        T.StructField("proprietary_name_suffix", T.StringType(), True),
        T.StructField("nonproprietary_name", T.StringType(), True),
        T.StructField("dosage_form_name", T.StringType(), True),
        T.StructField("route_name", T.StringType(), True),
        T.StructField("start_marketing_date", T.DateType(), True),
        T.StructField("end_marketing_date", T.DateType(), True),
        T.StructField("marketing_category_name", T.StringType(), True),
        T.StructField("application_number", T.StringType(), True),
        T.StructField("labeler_name", T.StringType(), False),
        T.StructField("substance_name", T.StringType(), True),
        T.StructField("active_numerator_strength", T.StringType(), True),
        T.StructField("active_ingred_unit", T.StringType(), True),
        T.StructField("pharm_classes", T.StringType(), True),
        T.StructField("dea_schedule", T.StringType(), True),
        T.StructField("ndc_exclude_flag", T.StringType(), True),
        T.StructField("listing_record_certified_through", T.DateType(), True),
        T.StructField("snapshot_date", T.DateType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)

PACKAGE_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), False),
        T.StructField("product_ndc", T.StringType(), False),
        T.StructField("ndc_package_code", T.StringType(), False),
        T.StructField("package_ndc_11", T.StringType(), True),
        T.StructField("package_description", T.StringType(), True),
        T.StructField("start_marketing_date", T.DateType(), True),
        T.StructField("end_marketing_date", T.DateType(), True),
        T.StructField("ndc_exclude_flag", T.StringType(), True),
        T.StructField("sample_package", T.BooleanType(), False),
        T.StructField("snapshot_date", T.DateType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: fetch the zip + the immutable Volume snapshot (kept out of the pure module)
# ---------------------------------------------------------------------------


def _fetch_zip(url: str) -> bytes:
    """Download ``ndctext.zip`` and return the raw bytes (written verbatim to the Volume)."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted FDA host
        raw = resp.read()
    log.info("Fetched ndctext.zip", extra={"url": url, "bytes": len(raw)})
    return raw


def _select_member(names: list[str], basename: str) -> str:
    """Pick a member from the zip by basename, case-insensitively."""
    matches = [n for n in names if n.split("/")[-1].lower() == basename.lower()]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one {basename} in zip; found {matches or 'none'}")
    return matches[0]


def _extract_text(zip_bytes: bytes, basename: str) -> str:
    """Extract a member's text from the zip bytes, decoded per the source encoding."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = _select_member(zf.namelist(), basename)
        raw = zf.read(member)
    return raw.decode(ndc.SOURCE_ENCODING)


def _volume_dir(catalog: str, volume: str) -> str:
    return f"/Volumes/{catalog}/{SCHEMA}/{volume}"


def _snapshot_path(catalog: str, volume: str, snapshot_date: date) -> str:
    return f"{_volume_dir(catalog, volume)}/ndctext_{snapshot_date.isoformat()}.zip"


def _persist_snapshot(path: str, raw: bytes) -> tuple[bytes, bool]:
    """Write the raw zip to ``path`` unless it already exists (immutability; ADR 0032).

    Returns ``(snapshot_bytes, wrote_new_file)``. A same-day re-run reads the
    existing file so the tables reflect the immutable snapshot of record.
    """
    p = Path(path)
    if p.exists():
        log.warning("Raw snapshot already exists; not overwriting", extra={"path": path})
        return p.read_bytes(), False
    p.write_bytes(raw)
    log.info("Wrote immutable raw snapshot", extra={"path": path, "bytes": len(raw)})
    return raw, True


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness / non-null / normalization / FK; WARN rest
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    products: list[ndc.NdcProduct],
    packages: list[ndc.NdcPackage],
    snapshot_date: date,
    *,
    wrote_new_file: bool,
    product_dups: int = 0,
    package_dups: int = 0,
) -> None:
    """Record DQ; raise on any blocking FAIL so a bad table never writes."""
    p_table, k_table = f"{SCHEMA}.{PRODUCT_TABLE}", f"{SCHEMA}.{PACKAGE_TABLE}"
    n_prod, n_pkg = len(products), len(packages)

    dup_prod = ndc.find_duplicate_product_keys(products)
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_product_id_snapshot_date_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_prod,
        failing_row_count=len(dup_prod),
        total_row_count=n_prod,
        details={"sample": dup_prod[:10]} if dup_prod else None,
    )

    dup_pkg = ndc.find_duplicate_package_keys(packages)
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_key_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_pkg,
        failing_row_count=len(dup_pkg),
        total_row_count=n_pkg,
        details={"sample": [list(k) for k in dup_pkg[:10]]} if dup_pkg else None,
    )

    miss_prod = ndc.find_missing_product_fields(products)
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_product_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not miss_prod,
        failing_row_count=len(miss_prod),
        total_row_count=n_prod,
        details={"sample": [list(m) for m in miss_prod[:10]]} if miss_prod else None,
    )

    miss_pkg = ndc.find_missing_package_fields(packages)
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not miss_pkg,
        failing_row_count=len(miss_pkg),
        total_row_count=n_pkg,
        details={"sample": [list(m) for m in miss_pkg[:10]]} if miss_pkg else None,
    )

    bad_prod_ndc = ndc.find_bad_product_ndc(products)
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_product_ndc_normalizes_to_9_digits",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_prod_ndc,
        failing_row_count=len(bad_prod_ndc),
        total_row_count=n_prod,
        details={"sample": bad_prod_ndc[:10]} if bad_prod_ndc else None,
    )

    bad_pkg_ndc = ndc.find_bad_package_ndc(packages)
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_ndc_normalizes_to_11_digits",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_pkg_ndc,
        failing_row_count=len(bad_pkg_ndc),
        total_row_count=n_pkg,
        details={"sample": bad_pkg_ndc[:10]} if bad_pkg_ndc else None,
    )

    # --- WARN checks ---
    # Package -> product FK is INFORMATIONAL (ADR 0014): the FDA files have real
    # referential gaps (a package can reference a product excluded from product.txt
    # via NDC_EXCLUDE_FLAG asymmetry or daily-update timing), so orphans are recorded
    # and kept, not treated as a build-blocking failure.
    product_ids = {p.product_id for p in products}
    orphans = ndc.find_package_orphans(packages, product_ids)
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_product_fk",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not orphans,
        failing_row_count=len(orphans),
        total_row_count=n_pkg,
        details={"orphan_count": len(orphans), "sample": [list(o) for o in orphans[:10]]}
        if orphans
        else None,
    )

    # Source rows collapsed by dedupe (the table keys require it; FDA data quirk).
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_product_duplicate_rows_collapsed",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.WARN,
        passed=product_dups == 0,
        failing_row_count=product_dups,
        total_row_count=n_prod,
        details={"collapsed": product_dups} if product_dups else None,
    )
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_duplicate_rows_collapsed",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.WARN,
        passed=package_dups == 0,
        failing_row_count=package_dups,
        total_row_count=n_pkg,
        details={"collapsed": package_dups} if package_dups else None,
    )

    prod_card_ok = n_prod >= ndc.PRODUCT_CARDINALITY_MIN
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_product_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=prod_card_ok,
        failing_row_count=0 if prod_card_ok else n_prod,
        total_row_count=n_prod,
        details={"expected_min": ndc.PRODUCT_CARDINALITY_MIN, "actual": n_prod},
    )
    pkg_card_ok = n_pkg >= ndc.PACKAGE_CARDINALITY_MIN
    ctx.recorder.record(
        table_name=k_table,
        check_name="ndc_package_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=pkg_card_ok,
        failing_row_count=0 if pkg_card_ok else n_pkg,
        total_row_count=n_pkg,
        details={"expected_min": ndc.PACKAGE_CARDINALITY_MIN, "actual": n_pkg},
    )

    for table, recs, n in ((p_table, products, n_prod), (k_table, packages, n_pkg)):
        bad_order = ndc.find_bad_marketing_date_order(recs)
        ctx.recorder.record(
            table_name=table,
            check_name="ndc_end_marketing_date_after_start",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.WARN,
            passed=not bad_order,
            failing_row_count=len(bad_order),
            total_row_count=n,
            details={"sample": bad_order[:10]} if bad_order else None,
        )

    bad_dea = ndc.find_bad_dea_schedule(products)
    ctx.recorder.record(
        table_name=p_table,
        check_name="ndc_dea_schedule_controlled_vocab",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not bad_dea,
        failing_row_count=len(bad_dea),
        total_row_count=n_prod,
        details={
            "allowed": sorted(ndc.DEA_SCHEDULE_VALUES),
            "sample": [list(b) for b in bad_dea[:10]],
        }
        if bad_dea
        else None,
    )

    for table, n in ((p_table, n_prod), (k_table, n_pkg)):
        ctx.recorder.record(
            table_name=table,
            check_name="ndc_snapshot_freshness",
            category=DQCategory.FRESHNESS,
            severity=DQSeverity.WARN,
            passed=n > 0,
            failing_row_count=0 if n > 0 else 1,
            total_row_count=n,
            details={
                "snapshot_date": snapshot_date.isoformat(),
                "wrote_new_volume_file": wrote_new_file,
            },
        )

    failures: list[str] = []
    if dup_prod:
        failures.append(f"duplicate product_id: {dup_prod[:5]}")
    if dup_pkg:
        failures.append(f"duplicate package key: {dup_pkg[:5]}")
    if miss_prod:
        failures.append(f"null product field: {miss_prod[:5]}")
    if miss_pkg:
        failures.append(f"null package field: {miss_pkg[:5]}")
    if bad_prod_ndc:
        failures.append(f"bad product_ndc: {bad_prod_ndc[:5]}")
    if bad_pkg_ndc:
        failures.append(f"bad package ndc: {bad_pkg_ndc[:5]}")
    if failures:
        raise ValueError("NDC blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace by snapshot_date; ADR 0032 / ADR 0024)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession,
    full: str,
    schema: T.StructType,
    rows: list[dict[str, Any]],
    snapshot_date: date,
    sort_cols: list[str],
) -> None:
    """snapshot_replace: replace only this run's ``snapshot_date`` rows; keep priors."""
    df = spark.createDataFrame(rows, schema=schema).sort(*sort_cols)
    if _table_has_column(spark, full, "snapshot_date"):
        spark.sql(f"DELETE FROM {full} WHERE snapshot_date = DATE'{snapshot_date.isoformat()}'")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info(
        "Wrote table",
        extra={"table": full, "rows": len(rows), "snapshot": snapshot_date.isoformat()},
    )


def _create_current_view(spark: SparkSession, catalog: str, table: str, view: str) -> None:
    full = f"{catalog}.{SCHEMA}.{table}"
    view_full = f"{catalog}.{SCHEMA}.{view}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view_full} AS "
        f"SELECT * FROM {full} WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view_full} IS "
        f"'{table} restricted to the latest snapshot_date (the current NDC Directory). ADR 0032.'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008)
# ---------------------------------------------------------------------------

_KNOWN_LIMITATIONS = (
    "Finished marketed drugs only (the FDA NDC Directory excludes animal drugs, blood "
    "products, APIs / drugs not in final marketed form, and kit-only / inner-layer "
    "products); the main product/package files also omit NDC_EXCLUDE_FLAG (E/U/I/D) rows. "
    "Revision-tracked by quarterly snapshot (snapshot_date) with the raw ndctext.zip "
    "preserved verbatim on the codes.ndc_raw Volume; 'current' is the latest snapshot_date. "
    "A newly-launched NDC may be up to a quarter stale in-table; the source's marketing "
    "dates carry real-world validity. The FDA files contain occasional duplicate package "
    "listings (same product_id + package code) which are collapsed to one row per key, and "
    "some packages reference a product absent from product.txt -- the package->product FK is "
    "informational (ADR 0014), so such rows are kept. PHARM_CLASSES / SUBSTANCENAME are "
    "multi-valued raw text (semicolon-delimited; not exploded). The NDC switches to a "
    "12-digit format in 2033 (out of scope here)."
)


def _register(
    spark: SparkSession, catalog: str, snapshot_date: date, *, create_views: bool
) -> None:
    g = f"{catalog}.{SCHEMA}"
    derived = [ndc.SOURCE_TEXT_ZIP_URL]
    common = {
        "subject": SCHEMA,
        "layer": "reference",
        "public_health_relevance": (
            "Canonical national drug code standard for U.S. drug/pharmacy data; lets claims, "
            "dispensing, and vaccine-NDC feeds conform to a shared, revision-tracked reference "
            "(labeler, dosage form, route, substance, marketing dates, DEA schedule)."
        ),
        "spatial_resolution": "none",
        "spatial_coverage": "United States",
        "source_provider_code": "fda",
        "source_url": ndc.SOURCE_LANDING_URL,
        "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
        "dua_required": False,
        "dua_reference": "No DUA. FDA NDC Directory is public domain.",
        "access_tier": "open",
        "external_maintainer_name": "U.S. Food and Drug Administration (CDER)",
        "is_hosted": True,
        "temporal_coverage_start": snapshot_date,
        "temporal_coverage_end": snapshot_date,
        "temporal_resolution": "quarterly",
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": derived,
    }

    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{PRODUCT_TABLE}",
            description=(
                "FDA NDC Directory product-level reference (finished drugs): one row per "
                "product listing per snapshot. Carries the verbatim PRODUCTNDC plus the "
                "9-digit (5-4) normalized product NDC, labeler, dosage form, route, substance, "
                "marketing dates, and DEA schedule. PK (product_id, snapshot_date); product_id "
                "(NDCproductcode + SPL doc id) is the stable key since PRODUCTNDC is not unique."
            ),
            source_documentation_url=ndc.PRODUCT_DEFINITIONS_URL,
            source_data_dictionary_url=ndc.PRODUCT_DEFINITIONS_URL,
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{PRODUCT_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["snapshot_date", "product_id"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{PACKAGE_TABLE}",
            description=(
                "FDA NDC Directory package-level reference (finished drugs): one row per "
                "package listing per snapshot -- the grain pharmacy claims carry. Carries the "
                "verbatim NDCPACKAGECODE plus the 11-digit (5-4-2) normalized package NDC, "
                "package description, and marketing dates. PK (product_id, ndc_package_code, "
                "snapshot_date); FK product_id -> ndc_product (same snapshot)."
            ),
            source_documentation_url=ndc.PACKAGE_DEFINITIONS_URL,
            source_data_dictionary_url=ndc.PACKAGE_DEFINITIONS_URL,
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{PACKAGE_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["snapshot_date", "product_id"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    if create_views:
        for table, view in ((PRODUCT_TABLE, PRODUCT_VIEW), (PACKAGE_TABLE, PACKAGE_VIEW)):
            registration.register_dataset(
                spark,
                catalog,
                registration.DatasetCatalogEntry(
                    full_table_name=f"{g}.{view}",
                    description=f"{table} restricted to the latest snapshot_date (ADR 0032).",
                    source_documentation_url=ndc.SOURCE_LANDING_URL,
                    source_data_dictionary_url=ndc.SOURCE_LANDING_URL,
                    **{**common, "is_hosted": False},
                ),
                registration.DatasetEngineeringEntry(
                    full_table_name=f"{g}.{view}",
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
    snapshot_date: date | None = None,
    volume: str = DEFAULT_VOLUME,
    source_url: str = ndc.SOURCE_TEXT_ZIP_URL,
    create_views: bool = True,
) -> None:
    snap = snapshot_date or datetime.now(tz=UTC).date()
    raw = _fetch_zip(source_url)
    snapshot_path = _snapshot_path(catalog, volume, snap)

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, "
            f"NDC, ...). Owned by the _reference bundle. See ADR 0014.'"
        )
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {catalog}.{SCHEMA}.{volume} "
            f"COMMENT 'Immutable date-stamped raw FDA ndctext.zip snapshots (ADR 0032).'"
        )

    def _work(ctx: BuildContext) -> None:
        snapshot_bytes, wrote_new_file = _persist_snapshot(snapshot_path, raw)
        products = ndc.parse_product_file(_extract_text(snapshot_bytes, ndc.PRODUCT_MEMBER))
        packages = ndc.parse_package_file(_extract_text(snapshot_bytes, ndc.PACKAGE_MEMBER))
        # Collapse the FDA files' occasional duplicate key-rows so the PKs hold; the
        # counts are recorded as WARNs (the package->product FK stays informational).
        products, product_dups = ndc.dedupe_products(products)
        packages, package_dups = ndc.dedupe_packages(packages)
        _dq_checks(
            ctx,
            products,
            packages,
            snap,
            wrote_new_file=wrote_new_file,
            product_dups=product_dups,
            package_dups=package_dups,
        )

        now = datetime.now(tz=UTC)
        product_rows = [
            {
                "product_id": p.product_id,
                "product_ndc": p.product_ndc,
                "product_ndc_normalized": p.product_ndc_normalized,
                "product_type_name": p.product_type_name,
                "proprietary_name": p.proprietary_name,
                "proprietary_name_suffix": p.proprietary_name_suffix,
                "nonproprietary_name": p.nonproprietary_name,
                "dosage_form_name": p.dosage_form_name,
                "route_name": p.route_name,
                "start_marketing_date": p.start_marketing_date,
                "end_marketing_date": p.end_marketing_date,
                "marketing_category_name": p.marketing_category_name,
                "application_number": p.application_number,
                "labeler_name": p.labeler_name,
                "substance_name": p.substance_name,
                "active_numerator_strength": p.active_numerator_strength,
                "active_ingred_unit": p.active_ingred_unit,
                "pharm_classes": p.pharm_classes,
                "dea_schedule": p.dea_schedule,
                "ndc_exclude_flag": p.ndc_exclude_flag,
                "listing_record_certified_through": p.listing_record_certified_through,
                "snapshot_date": snap,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for p in products
        ]
        package_rows = [
            {
                "product_id": p.product_id,
                "product_ndc": p.product_ndc,
                "ndc_package_code": p.ndc_package_code,
                "package_ndc_11": p.package_ndc_11,
                "package_description": p.package_description,
                "start_marketing_date": p.start_marketing_date,
                "end_marketing_date": p.end_marketing_date,
                "ndc_exclude_flag": p.ndc_exclude_flag,
                "sample_package": p.sample_package,
                "snapshot_date": snap,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for p in packages
        ]

        g = f"{catalog}.{SCHEMA}"
        _write_table(
            ctx.spark,
            f"{g}.{PRODUCT_TABLE}",
            PRODUCT_SPARK_SCHEMA,
            product_rows,
            snap,
            ["product_id"],
        )
        _write_table(
            ctx.spark,
            f"{g}.{PACKAGE_TABLE}",
            PACKAGE_SPARK_SCHEMA,
            package_rows,
            snap,
            ["product_id", "ndc_package_code"],
        )
        if create_views:
            _create_current_view(ctx.spark, catalog, PRODUCT_TABLE, PRODUCT_VIEW)
            _create_current_view(ctx.spark, catalog, PACKAGE_TABLE, PACKAGE_VIEW)

    def _grant(spark: SparkSession) -> None:
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)
        # READ VOLUME is volume-scoped (not covered by schema SELECT) -- ADR 0032.
        grants.grant_volume_reader(spark, catalog, SCHEMA, volume, data_engineers_group)
        grants.grant_volume_reader(spark, catalog, SCHEMA, volume, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, snap, create_views=create_views),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=None,
        help="Snapshot date (YYYY-MM-DD) for this run. Default: today (UTC).",
    )
    parser.add_argument(
        "--volume",
        default=DEFAULT_VOLUME,
        help=f"Managed Volume (codes schema) for raw snapshots. Default: {DEFAULT_VOLUME}.",
    )
    parser.add_argument(
        "--source-url",
        default=ndc.SOURCE_TEXT_ZIP_URL,
        help="Override the ndctext.zip download URL.",
    )
    parser.add_argument(
        "--no-current-views",
        action="store_true",
        help="Skip creating/refreshing the *_current views.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.data_engineers_group,
        args.analysts_group,
        snapshot_date=args.snapshot_date,
        volume=args.volume,
        source_url=args.source_url,
        create_views=not args.no_current_views,
    )


if __name__ == "__main__":
    main()
