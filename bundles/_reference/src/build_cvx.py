"""Build the canonical ``codes.cvx`` reference table on the shared builder (ADR 0037/0039/0032).

CVX is the CDC IIS "Vaccine Administered" code set -- the canonical vaccine codes that vaccine
surveillance / clinical feeds conform to. This entrypoint is the thin IO + Spark layer over the
pure logic in ``cidmath_datahub.reference.cvx`` (ADR 0011). It is **flat** -- one row per CVX code,
no classification hierarchy.

**Source-path fold-in (ADR 0037 backport, wave 2).** Previously model-only + hand-rolled on
``run_build``, with the immutable raw XML snapshots on a Volume in the *model* catalog
(``codes.cvx_raw``). Now folded onto the shared ``build_reference`` builder: the CVX XML-new lands
verbatim in the *source*-catalog landing Volume ``ecdh_<env>.codes_raw._landing`` (ADR 0039), parses
into the 1:1 raw table ``ecdh_<env>.codes_raw.cvx``, and the canonical ``ecdh_model_<env>.codes.cvx``
is promoted from raw. Same schema, same rows -- a build-mechanism fold-in with data parity;
consumers unaffected. The builder owns the per-snapshot atomic ``replaceWhere``, ``_ops``
registration, and grants.

History model (ADR 0032) -- preserved. CVX is revised in place and CDC publishes only the *current*
list, so we snapshot it: ``codes.cvx`` is keyed ``(cvx_code, snapshot_date)`` and each run replaces
only its own ``snapshot_date`` (``vintage_column="snapshot_date"``), leaving prior snapshots intact.
The raw payload is landed **``PER_VINTAGE_IMMUTABLE`` keyed by the snapshot_date** -- so the Volume
dir (``.../cvx/vintage=<YYYY-MM-DD>``), the immutability ("never overwrite an existing date"), and
the ``--snapshot-date`` reproduce-a-past-date behavior all fall out of the builder's per-vintage
fetch-once/skip-if-present logic, with the dir key == the write predicate == the snapshot_date.
("Current" is ``MAX(snapshot_date)`` -- the ADR 0034 live idiom; the ``codes.cvx_current`` view is
dropped, matching the RUCA fold.)

Blocking DQ (FAIL, raises): ``(cvx_code, snapshot_date)`` uniqueness, non-null required fields, and
``vaccine_status`` in the controlled vocabulary. WARN/INFO: cardinality, ``cvx_last_updated``
parses / not-future, and snapshot freshness.

Usage:
    build_cvx.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # reproduce a specific snapshot date from an already-landed Volume file:
    build_cvx.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --snapshot-date 2026-06-15
"""

from __future__ import annotations

import argparse
import urllib.parse
import urllib.request
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
from cidmath_datahub.reference import cvx

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "cvx"
PIPELINE_REF = "bundles/_reference/src/build_cvx.py"

# Verbatim landed payload name inside the per-snapshot Volume dir (dir is already date-scoped).
_SNAPSHOT_XML = "cvx_xml_new.xml"

# Raw (codes_raw.cvx) == canonical (codes.cvx): flat, PK (cvx_code, snapshot_date). ADR 0032.
CVX_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("cvx_code", T.StringType(), False),
        T.StructField("snapshot_date", T.DateType(), False),
        T.StructField("short_description", T.StringType(), False),
        T.StructField("full_vaccine_name", T.StringType(), False),
        T.StructField("vaccine_status", T.StringType(), False),
        T.StructField("cvx_last_updated", T.DateType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)

_DDL = (
    "cvx_code STRING, snapshot_date DATE, short_description STRING, full_vaccine_name STRING, "
    "vaccine_status STRING, cvx_last_updated DATE, source_file STRING, loaded_at TIMESTAMP"
)

_DESC = (
    "CDC IIS CVX (Vaccine Administered) code set: canonical vaccine codes with short and full "
    "vaccine names, a controlled vaccine_status (active / inactive / pending / non_us / "
    "never_active), and the source 'Last Updated' date. Flat (no hierarchy). One row per code per "
    "snapshot; PK (cvx_code, snapshot_date)."
)
_PHR = (
    "Canonical vaccine-code standard for U.S. immunization data; lets vaccine surveillance / "
    "clinical feeds conform administered-vaccine codes to a shared, revision-tracked reference."
)
_KNOWN_LIMITATIONS = (
    "Flat CVX code set (no vaccine-group mapping -- that arrives via a separate CDSi job; no "
    "MVX/CPT/NDC). Revision-tracked by snapshot: each run keeps a new snapshot_date, with the raw "
    "XML-new preserved verbatim on the codes_raw._landing Volume; 'current' is the latest "
    "snapshot_date. The XML-new payload has no vaccine/non-vaccine indicator, so no such flag is "
    "synthesized: every code is loaded as published, including Non-US and Never-Active entries and "
    "administrative codes (e.g. 998 'no vaccine administered'), distinguishable by cvx_code and "
    "description."
)

_COMMON_META: dict[str, Any] = {
    "spatial_resolution": "none",
    "spatial_coverage": "United States",
    "source_provider_code": "cdc",
    "source_url": cvx.SOURCE_XML_NEW_URL,
    "source_documentation_url": cvx.SOURCE_LANDING_URL,
    "source_data_dictionary_url": cvx.SOURCE_DATA_DICTIONARY_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. CDC IIS CVX code set is public domain.",
    "access_tier": "open",
    "external_maintainer_name": "CDC Immunization Information Systems (IIS)",
    "is_hosted": True,
    "temporal_resolution": "annual",
}


def _base_entry(snapshot_date: date) -> registration.DatasetCatalogEntry:
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        temporal_coverage_start=snapshot_date,
        temporal_coverage_end=snapshot_date,
        derived_from=[cvx.SOURCE_XML_NEW_URL],
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: fetch the XML-new payload (kept out of the pure module per ADR 0011).
# ---------------------------------------------------------------------------


def _download_xml(url: str) -> bytes:
    """Download the CVX XML-new payload and return the raw bytes (verbatim)."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC IIS host
        raw = resp.read()
    log.info("Fetched CVX XML-new", extra={"url": url, "bytes": len(raw)})
    return raw


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the run's snapshot + source url).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    snapshot_date: date | None = None,
    source_url: str = cvx.SOURCE_XML_NEW_URL,
) -> None:
    snap = snapshot_date or datetime.now(tz=UTC).date()
    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    # Parsed records (with parse-time-only *_raw fields) stashed by read for validate to DQ, since
    # those fields are not persisted to the table and so cannot be reconstructed from staging.
    parsed: dict[date, list[cvx.CvxRecord]] = {}

    def _fetch(v: date, volume_dir: str) -> None:
        # v is the snapshot_date; the dir is already vintage=<snapshot_date>-scoped, and the builder
        # skips this fetch if the date's payload is already landed (immutability; ADR 0032).
        (Path(volume_dir) / _SNAPSHOT_XML).write_bytes(_download_xml(source_url))

    def _read(ctx: BuildContext, v: date, volume_dir: str) -> Any:
        snapshot_path = str(Path(volume_dir) / _SNAPSHOT_XML)
        records = cvx.parse_cvx_xml(Path(snapshot_path).read_bytes().decode(cvx.SOURCE_ENCODING))
        parsed[v] = records
        log.info("Parsed CVX", extra={"snapshot_date": v.isoformat(), "rows": len(records)})
        now = datetime.now(tz=UTC)
        rows = [
            {
                "cvx_code": r.cvx_code,
                "snapshot_date": v,
                "short_description": r.short_description,
                "full_vaccine_name": r.full_vaccine_name,
                "vaccine_status": r.vaccine_status,
                "cvx_last_updated": r.cvx_last_updated,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for r in records
        ]
        return ctx.spark.createDataFrame(rows, CVX_SPARK_SCHEMA).sort("cvx_code")

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

    def _promote(ctx: BuildContext, v: date) -> Any:
        """Raw is already canonical-shaped (flat); select this snapshot's rows."""
        return ctx.spark.sql(
            f"SELECT * FROM {raw_fqn} WHERE snapshot_date = DATE'{v.isoformat()}'"
        ).sort("cvx_code")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        # DQ this run's freshly-parsed records (they carry the parse-time-only *_raw fields the
        # unparseable-date check needs). PK uniqueness via make_staging_dq scoped to this snapshot.
        record_table = f"{MODEL_SCHEMA}.{TABLE}"
        where = f"snapshot_date = DATE'{snap.isoformat()}'"
        records = parsed[snap]
        total = len(records)
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(
            keys=["cvx_code", "snapshot_date"],
            check_name="cvx_code_snapshot_date_uniqueness",
            raise_on_fail=False,
        ):
            failures.append("duplicate (cvx_code, snapshot_date)")

        missing = cvx.find_missing_required(records)
        _record(ctx, record_table, "cvx_required_fields_not_null", DQCategory.NULLABILITY,
                not missing, len(missing), total,
                {"sample_missing": [list(m) for m in missing[:10]]} if missing else None)
        if missing:
            failures.append(f"null required field: {missing[:5]}")

        bad_status = cvx.find_status_violations(records)
        _record(ctx, record_table, "cvx_vaccine_status_controlled_vocab", DQCategory.BUSINESS_RULE,
                not bad_status, len(bad_status), total,
                {"allowed": sorted(cvx.VACCINE_STATUS_VALUES),
                 "sample_violations": [list(b) for b in bad_status[:10]]} if bad_status else None)
        if bad_status:
            failures.append(f"vaccine_status out of vocab: {bad_status[:5]}")

        # --- WARN / INFO ---
        card_ok = cvx.CARDINALITY_MIN <= total <= cvx.CARDINALITY_MAX
        _record(ctx, record_table, "cvx_cardinality", DQCategory.CARDINALITY,
                card_ok, 0 if card_ok else total, total,
                {"expected_range": [cvx.CARDINALITY_MIN, cvx.CARDINALITY_MAX], "actual": total},
                severity=DQSeverity.WARN)

        unparseable = cvx.find_unparseable_last_updated(records)
        _record(ctx, record_table, "cvx_last_updated_parses", DQCategory.BUSINESS_RULE,
                not unparseable, len(unparseable), total,
                {"sample": [list(u) for u in unparseable[:10]]} if unparseable else None,
                severity=DQSeverity.WARN)

        future = cvx.find_future_last_updated(records, snap)
        _record(ctx, record_table, "cvx_last_updated_not_future", DQCategory.BUSINESS_RULE,
                not future, len(future), total,
                {"as_of": snap.isoformat(), "sample": [[c, d.isoformat()] for c, d in future[:10]]}
                if future else None, severity=DQSeverity.WARN)

        _record(ctx, record_table, "cvx_snapshot_freshness", DQCategory.FRESHNESS,
                total > 0, 0 if total > 0 else 1, total,
                {"snapshot_date": snap.isoformat()}, severity=DQSeverity.WARN)

        if failures:
            raise ValueError("CVX blocking DQ failed -- " + "; ".join(failures))

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
                table=TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch,
                read_from_volume=_read,
                description=(
                    "Raw CDC IIS CVX XML-new, fetched-as-is per snapshot_date (immutable; never "
                    "overwritten). Volume-landed verbatim, then parsed. Promoted to codes.cvx."
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
                canonical_cluster_columns=["snapshot_date", "cvx_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(snap,))
    log.info("CVX build complete", extra={"snapshot_date": snap.isoformat()})


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
        "--source-url", default=cvx.SOURCE_XML_NEW_URL, help="Override the CVX XML-new download URL.",
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
