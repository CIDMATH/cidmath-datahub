"""Build the canonical ``codes.ndc_product`` + ``codes.ndc_package`` tables on the shared builder.

The FDA NDC Directory (finished drugs) is the canonical national drug code set that drug/pharmacy
data (claims, dispensing, vaccine NDCs) conform to. This entrypoint is the thin IO + Spark layer
over the pure logic in ``cidmath_datahub.reference.ndc`` (ADR 0011). Two **flat** grains (no
hierarchy): ``codes.ndc_product`` (product-level) and ``codes.ndc_package`` (package-level, the grain
pharmacy claims carry), FK-linked by ``product_id``.

**Source-path fold-in (ADR 0037 backport, wave 2).** Previously model-only + hand-rolled on
``run_build`` with the immutable raw ``ndctext.zip`` snapshots on a Volume in the *model* catalog
(``codes.ndc_raw``). Now folded onto the shared ``build_reference`` builder: the one ``ndctext.zip``
lands verbatim in the *source*-catalog landing Volume ``ecdh_<env>.codes_raw._landing`` (ADR 0039)
and backs **both** raw landings (``codes_raw.ndc_product`` + ``codes_raw.ndc_package``) via a shared
``volume_key`` -- fetched once, parsed into each 1:1 raw table -- and the canonicals
``ecdh_model_<env>.codes.ndc_{product,package}`` are promoted from raw. Same schemas, same rows -- a
build-mechanism fold-in with data parity; consumers unaffected.

History model (ADR 0032) -- preserved. The directory is revised in place (FDA publishes only the
current list), so both tables are keyed by ``snapshot_date`` and each run replaces only its own
snapshot (``vintage_column="snapshot_date"``). The raw payload is landed **``PER_VINTAGE_IMMUTABLE``
keyed by the snapshot_date** -- the Volume dir (``.../ndc/vintage=<YYYY-MM-DD>``), immutability
("never overwrite an existing date"), and the ``--snapshot-date`` reproduce-a-past-date behavior all
fall out of the builder's per-vintage fetch-once/skip-if-present logic. The ``*_current`` views are
dropped (ADR 0034: "current" = ``MAX(snapshot_date)`` / the live idiom), matching the RUCA fold.

Blocking DQ (FAIL): PK uniqueness on both tables (after collapsing the FDA files' occasional
duplicate key-rows); non-null required fields; ``*_ndc`` normalization (package -> 11 digits,
product -> 9). The package -> product FK is **informational** (ADR 0014, WARN). WARN also:
cardinality, collapsed-duplicate counts, marketing-date order, DEA-schedule vocab, freshness.

Usage:
    build_ndc.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev \\
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
from cidmath_datahub.reference import ndc

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
PRODUCT_TABLE = "ndc_product"
PACKAGE_TABLE = "ndc_package"
PIPELINE_REF = "bundles/_reference/src/build_ndc.py"

# Both raw tables are backed by ONE ndctext.zip payload; a shared volume_key makes the builder fetch
# it once (the second landing's fetch is skipped by the completion marker) and read it twice.
_VOLUME_KEY = "ndc_directory"
_SNAPSHOT_ZIP = "ndctext.zip"

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

_PRODUCT_DDL = (
    "product_id STRING, product_ndc STRING, product_ndc_normalized STRING, product_type_name STRING, "
    "proprietary_name STRING, proprietary_name_suffix STRING, nonproprietary_name STRING, "
    "dosage_form_name STRING, route_name STRING, start_marketing_date DATE, end_marketing_date DATE, "
    "marketing_category_name STRING, application_number STRING, labeler_name STRING, "
    "substance_name STRING, active_numerator_strength STRING, active_ingred_unit STRING, "
    "pharm_classes STRING, dea_schedule STRING, ndc_exclude_flag STRING, "
    "listing_record_certified_through DATE, snapshot_date DATE, source_file STRING, "
    "loaded_at TIMESTAMP"
)
_PACKAGE_DDL = (
    "product_id STRING, product_ndc STRING, ndc_package_code STRING, package_ndc_11 STRING, "
    "package_description STRING, start_marketing_date DATE, end_marketing_date DATE, "
    "ndc_exclude_flag STRING, sample_package BOOLEAN, snapshot_date DATE, source_file STRING, "
    "loaded_at TIMESTAMP"
)

_PRODUCT_DESC = (
    "FDA NDC Directory product-level reference (finished drugs): one row per product listing per "
    "snapshot. Carries the verbatim PRODUCTNDC plus the 9-digit (5-4) normalized product NDC, "
    "labeler, dosage form, route, substance, marketing dates, and DEA schedule. PK (product_id, "
    "snapshot_date); product_id (NDCproductcode + SPL doc id) is the stable key since PRODUCTNDC is "
    "not unique."
)
_PACKAGE_DESC = (
    "FDA NDC Directory package-level reference (finished drugs): one row per package listing per "
    "snapshot -- the grain pharmacy claims carry. Carries the verbatim NDCPACKAGECODE plus the "
    "11-digit (5-4-2) normalized package NDC, package description, and marketing dates. PK "
    "(product_id, ndc_package_code, snapshot_date); FK product_id -> ndc_product (same snapshot)."
)
_PHR = (
    "Canonical national drug code standard for U.S. drug/pharmacy data; lets claims, dispensing, and "
    "vaccine-NDC feeds conform to a shared, revision-tracked reference (labeler, dosage form, route, "
    "substance, marketing dates, DEA schedule)."
)
_KNOWN_LIMITATIONS = (
    "Finished marketed drugs only (the FDA NDC Directory excludes animal drugs, blood products, "
    "APIs / drugs not in final marketed form, and kit-only / inner-layer products); the main "
    "product/package files also omit NDC_EXCLUDE_FLAG (E/U/I/D) rows. Revision-tracked by quarterly "
    "snapshot (snapshot_date) with the raw ndctext.zip preserved verbatim on the codes_raw._landing "
    "Volume; 'current' is the latest snapshot_date. A newly-launched NDC may be up to a quarter "
    "stale in-table; the source's marketing dates carry real-world validity. The FDA package file "
    "occasionally re-lists the same package code under one SPL with a newer start marketing date; "
    "such duplicates are collapsed to the most recent listing per (product_id, package code) "
    "(earlier listings remain in the raw Volume snapshot). Some packages reference a product absent "
    "from product.txt -- the package->product FK is informational (ADR 0014), so such rows are kept. "
    "PHARM_CLASSES / SUBSTANCENAME are multi-valued raw text (semicolon-delimited; not exploded). "
    "The NDC switches to a 12-digit format in 2033 (out of scope here)."
)

_COMMON_META: dict[str, Any] = {
    "spatial_resolution": "none",
    "spatial_coverage": "United States",
    "source_provider_code": "fda",
    "source_url": ndc.SOURCE_LANDING_URL,
    "source_documentation_url": ndc.SOURCE_LANDING_URL,
    "source_data_dictionary_url": ndc.SOURCE_LANDING_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. FDA NDC Directory is public domain.",
    "access_tier": "open",
    "external_maintainer_name": "U.S. Food and Drug Administration (CDER)",
    "is_hosted": True,
    "temporal_resolution": "quarterly",
}


def _base_entry(snapshot_date: date) -> registration.DatasetCatalogEntry:
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_PRODUCT_DESC,  # per-output description overrides this for each table
        public_health_relevance=_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        temporal_coverage_start=snapshot_date,
        temporal_coverage_end=snapshot_date,
        derived_from=[ndc.SOURCE_TEXT_ZIP_URL],
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: fetch the zip + extract a member (kept out of the pure module per ADR 0011).
# ---------------------------------------------------------------------------


def _download_zip(url: str) -> bytes:
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


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the run's snapshot + source url + stashes).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    snapshot_date: date | None = None,
    source_url: str = ndc.SOURCE_TEXT_ZIP_URL,
) -> None:
    snap = snapshot_date or datetime.now(tz=UTC).date()
    product_raw = f"{source_catalog}.{RAW_SCHEMA}.{PRODUCT_TABLE}"
    package_raw = f"{source_catalog}.{RAW_SCHEMA}.{PACKAGE_TABLE}"
    # Parsed + deduped records stashed by read for validate (the dedupe counts and the FK need the
    # in-memory records; the *_current cross-table FK needs both product + package present).
    stash: dict[str, Any] = {}

    def _fetch(v: date, volume_dir: str) -> None:
        # One shared payload backs both landings (shared volume_key): the builder runs this once and
        # skips the second landing's fetch via the completion marker. Immutable per snapshot_date.
        (Path(volume_dir) / _SNAPSHOT_ZIP).write_bytes(_download_zip(source_url))

    def _read_product(ctx: BuildContext, v: date, volume_dir: str) -> Any:
        snapshot_path = str(Path(volume_dir) / _SNAPSHOT_ZIP)
        products = ndc.parse_product_file(
            _extract_text(Path(snapshot_path).read_bytes(), ndc.PRODUCT_MEMBER)
        )
        products, product_dups = ndc.dedupe_products(products)
        stash["products"] = products
        stash["product_dups"] = product_dups
        log.info("Parsed NDC products", extra={"snapshot_date": v.isoformat(), "rows": len(products)})
        now = datetime.now(tz=UTC)
        rows = [
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
                "snapshot_date": v,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for p in products
        ]
        return ctx.spark.createDataFrame(rows, PRODUCT_SPARK_SCHEMA).sort("product_id")

    def _read_package(ctx: BuildContext, v: date, volume_dir: str) -> Any:
        snapshot_path = str(Path(volume_dir) / _SNAPSHOT_ZIP)
        packages = ndc.parse_package_file(
            _extract_text(Path(snapshot_path).read_bytes(), ndc.PACKAGE_MEMBER)
        )
        packages, package_dups = ndc.dedupe_packages(packages)
        stash["packages"] = packages
        stash["package_dups"] = package_dups
        log.info("Parsed NDC packages", extra={"snapshot_date": v.isoformat(), "rows": len(packages)})
        now = datetime.now(tz=UTC)
        rows = [
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
                "snapshot_date": v,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for p in packages
        ]
        return ctx.spark.createDataFrame(rows, PACKAGE_SPARK_SCHEMA).sort(
            "product_id", "ndc_package_code"
        )

    def _ensure_staging(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings for the codes subject (clinical/"
            f"terminology code systems). Engineer-owned; canonicals promote to model codes. ADR 0037.'"
        )
        sp.sql(f"CREATE TABLE IF NOT EXISTS {product_raw} ({_PRODUCT_DDL}) USING DELTA")
        sp.sql(f"CREATE TABLE IF NOT EXISTS {package_raw} ({_PACKAGE_DDL}) USING DELTA")

    def _ensure_canonical(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, NDC, "
            f"LOINC, SNOMED CT, RxNorm, ...). Owned by the _reference bundle. See ADR 0014.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{PRODUCT_TABLE} "
            f"({_PRODUCT_DDL}) USING DELTA"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{PACKAGE_TABLE} "
            f"({_PACKAGE_DDL}) USING DELTA"
        )

    def _promote_product(ctx: BuildContext, v: date) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {product_raw} WHERE snapshot_date = DATE'{v.isoformat()}'"
        ).sort("product_id")

    def _promote_package(ctx: BuildContext, v: date) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {package_raw} WHERE snapshot_date = DATE'{v.isoformat()}'"
        ).sort("product_id", "ndc_package_code")

    def _validate_product(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{PRODUCT_TABLE}"
        where = f"snapshot_date = DATE'{snap.isoformat()}'"
        products = stash["products"]
        n = len(products)
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["product_id", "snapshot_date"],
                         check_name="ndc_product_id_snapshot_date_uniqueness", raise_on_fail=False):
            failures.append("duplicate (product_id, snapshot_date)")

        miss = ndc.find_missing_product_fields(products)
        _record(ctx, record_table, "ndc_product_required_fields_not_null", DQCategory.NULLABILITY,
                not miss, len(miss), n, {"sample": [list(m) for m in miss[:10]]} if miss else None)
        if miss:
            failures.append(f"null product field: {miss[:5]}")

        bad_ndc = ndc.find_bad_product_ndc(products)
        _record(ctx, record_table, "ndc_product_ndc_normalizes_to_9_digits", DQCategory.BUSINESS_RULE,
                not bad_ndc, len(bad_ndc), n, {"sample": bad_ndc[:10]} if bad_ndc else None)
        if bad_ndc:
            failures.append(f"bad product_ndc: {bad_ndc[:5]}")

        # --- WARN / INFO ---
        dups = stash.get("product_dups", 0)
        _record(ctx, record_table, "ndc_product_duplicate_rows_collapsed", DQCategory.UNIQUENESS,
                dups == 0, dups, n, {"collapsed": dups} if dups else None, severity=DQSeverity.WARN)
        card_ok = n >= ndc.PRODUCT_CARDINALITY_MIN
        _record(ctx, record_table, "ndc_product_cardinality", DQCategory.CARDINALITY,
                card_ok, 0 if card_ok else n, n,
                {"expected_min": ndc.PRODUCT_CARDINALITY_MIN, "actual": n}, severity=DQSeverity.WARN)
        bad_order = ndc.find_bad_marketing_date_order(products)
        _record(ctx, record_table, "ndc_end_marketing_date_after_start", DQCategory.BUSINESS_RULE,
                not bad_order, len(bad_order), n,
                {"sample": bad_order[:10]} if bad_order else None, severity=DQSeverity.WARN)
        bad_dea = ndc.find_bad_dea_schedule(products)
        _record(ctx, record_table, "ndc_dea_schedule_controlled_vocab", DQCategory.BUSINESS_RULE,
                not bad_dea, len(bad_dea), n,
                {"allowed": sorted(ndc.DEA_SCHEDULE_VALUES),
                 "sample": [list(b) for b in bad_dea[:10]]} if bad_dea else None,
                severity=DQSeverity.WARN)
        _record(ctx, record_table, "ndc_snapshot_freshness", DQCategory.FRESHNESS,
                n > 0, 0 if n > 0 else 1, n, {"snapshot_date": snap.isoformat()},
                severity=DQSeverity.WARN)

        if failures:
            raise ValueError("NDC product blocking DQ failed -- " + "; ".join(failures))

    def _validate_package(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{PACKAGE_TABLE}"
        where = f"snapshot_date = DATE'{snap.isoformat()}'"
        packages = stash["packages"]
        products = stash.get("products", [])
        n = len(packages)
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["product_id", "ndc_package_code", "snapshot_date"],
                         check_name="ndc_package_key_uniqueness", raise_on_fail=False):
            failures.append("duplicate (product_id, ndc_package_code, snapshot_date)")

        miss = ndc.find_missing_package_fields(packages)
        _record(ctx, record_table, "ndc_package_required_fields_not_null", DQCategory.NULLABILITY,
                not miss, len(miss), n, {"sample": [list(m) for m in miss[:10]]} if miss else None)
        if miss:
            failures.append(f"null package field: {miss[:5]}")

        bad_ndc = ndc.find_bad_package_ndc(packages)
        _record(ctx, record_table, "ndc_package_ndc_normalizes_to_11_digits", DQCategory.BUSINESS_RULE,
                not bad_ndc, len(bad_ndc), n, {"sample": bad_ndc[:10]} if bad_ndc else None)
        if bad_ndc:
            failures.append(f"bad package ndc: {bad_ndc[:5]}")

        # --- WARN / INFO ---
        # Package -> product FK is INFORMATIONAL (ADR 0014): the FDA files have real referential
        # gaps, so orphans are recorded and kept, not treated as a build-blocking failure.
        product_ids = {p.product_id for p in products}
        orphans = ndc.find_package_orphans(packages, product_ids)
        _record(ctx, record_table, "ndc_package_product_fk", DQCategory.REFERENTIAL,
                not orphans, len(orphans), n,
                {"orphan_count": len(orphans), "sample": [list(o) for o in orphans[:10]]}
                if orphans else None, severity=DQSeverity.WARN)
        dups = stash.get("package_dups", 0)
        _record(ctx, record_table, "ndc_package_duplicate_rows_collapsed", DQCategory.UNIQUENESS,
                dups == 0, dups, n, {"collapsed": dups} if dups else None, severity=DQSeverity.WARN)
        card_ok = n >= ndc.PACKAGE_CARDINALITY_MIN
        _record(ctx, record_table, "ndc_package_cardinality", DQCategory.CARDINALITY,
                card_ok, 0 if card_ok else n, n,
                {"expected_min": ndc.PACKAGE_CARDINALITY_MIN, "actual": n}, severity=DQSeverity.WARN)
        bad_order = ndc.find_bad_marketing_date_order(packages)
        _record(ctx, record_table, "ndc_end_marketing_date_after_start", DQCategory.BUSINESS_RULE,
                not bad_order, len(bad_order), n,
                {"sample": bad_order[:10]} if bad_order else None, severity=DQSeverity.WARN)
        _record(ctx, record_table, "ndc_snapshot_freshness", DQCategory.FRESHNESS,
                n > 0, 0 if n > 0 else 1, n, {"snapshot_date": snap.isoformat()},
                severity=DQSeverity.WARN)

        if failures:
            raise ValueError("NDC package blocking DQ failed -- " + "; ".join(failures))

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(snap),
        vintage_column="snapshot_date",
        raw_landings=[
            RawLanding(
                table=PRODUCT_TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=_VOLUME_KEY,
                fetch_to_volume=_fetch,
                read_from_volume=_read_product,
                description=(
                    "Raw FDA NDC Directory product.txt (finished drugs), fetched-as-is per "
                    "snapshot_date (immutable). Volume-landed verbatim, then parsed. Promoted to "
                    "codes.ndc_product."
                ),
            ),
            RawLanding(
                table=PACKAGE_TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=_VOLUME_KEY,
                fetch_to_volume=_fetch,
                read_from_volume=_read_package,
                description=(
                    "Raw FDA NDC Directory package.txt (finished drugs), fetched-as-is per "
                    "snapshot_date (immutable). Shares the ndctext.zip landing with ndc_product. "
                    "Promoted to codes.ndc_package."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table=PRODUCT_TABLE,
                reads=(PRODUCT_TABLE,),
                promote=_promote_product,
                validate_staging=_validate_product,
                description=_PRODUCT_DESC,
                public_health_relevance=_PHR,
                canonical_cluster_columns=["snapshot_date", "product_id"],
            ),
            CanonicalOutput(
                canonical_table=PACKAGE_TABLE,
                reads=(PACKAGE_TABLE,),
                promote=_promote_package,
                validate_staging=_validate_package,
                description=_PACKAGE_DESC,
                public_health_relevance=_PHR,
                canonical_cluster_columns=["snapshot_date", "product_id"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(snap,))
    log.info("NDC build complete", extra={"snapshot_date": snap.isoformat()})


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
        "--snapshot-date", type=date.fromisoformat, default=None,
        help="Snapshot date (YYYY-MM-DD) for this run. Default: today (UTC).",
    )
    parser.add_argument(
        "--source-url", default=ndc.SOURCE_TEXT_ZIP_URL, help="Override the ndctext.zip download URL.",
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.source_catalog,
        args.model_catalog,
        args.data_engineers_group,
        args.analysts_group,
        snapshot_date=args.snapshot_date,
        source_url=args.source_url,
    )


if __name__ == "__main__":
    main()
