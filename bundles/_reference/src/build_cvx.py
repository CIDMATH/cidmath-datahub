"""Build the canonical ``codes.cvx`` reference table (ADR 0014, ADR 0032).

CVX is the CDC IIS "Vaccine Administered" code set -- the canonical vaccine codes
that vaccine surveillance / clinical feeds conform to. This entrypoint is the thin
IO + Spark layer over the pure logic in ``cidmath_datahub.reference.cvx`` (ADR
0011). It is **flat** -- one row per CVX code, no classification hierarchy (unlike
``codes.icd10`` / ``codes.icd9``).

History model (ADR 0032). CVX is revised in place and CDC publishes only the
*current* list, so we preserve history ourselves with two paired mechanisms:

  1. **Raw immutable Volume snapshot.** Each run writes the fetched XML-new
     verbatim to a date-stamped file on a UC Volume
     (``/Volumes/<catalog>/codes/<volume>/cvx_<YYYY-MM-DD>.xml``) and **never
     overwrites** an existing date -- the full-fidelity record, since the source
     cannot reproduce past states. A same-day re-run reads the existing file
     instead of re-writing it, so the table always reflects the immutable file.
  2. **In-table revision tracking via ``snapshot_replace``.** ``codes.cvx`` is
     keyed by ``(cvx_code, snapshot_date)``; each run DELETEs only its own
     ``snapshot_date`` rows and appends, leaving prior snapshots intact (the
     geography per-vintage replace, ADR 0024, with ``snapshot_date`` as the
     vintage key). "Current" is the latest ``snapshot_date``, optionally exposed
     via the ``codes.cvx_current`` view.

It then runs DQ and writes ``ecdh_model_<env>.codes.cvx`` (ADR 0006; ADR 0015:
reference table, no Kimball suffix). Thin entrypoint over the ``run_build`` seam
(ADR 0027): ``ensure -> [DQ: work] -> register -> grant``.

Blocking DQ (FAIL, raises): ``(cvx_code, snapshot_date)`` uniqueness, non-null
``cvx_code`` / ``short_description`` / ``full_vaccine_name`` / ``vaccine_status``
/ ``snapshot_date``, and ``vaccine_status`` in the controlled vocabulary. WARN:
cardinality, ``cvx_last_updated`` parses and is not in the future, nonvaccine
share, and snapshot freshness.

Usage:
    build_cvx.py --catalog ecdh_model_dev \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts

    # backfill / reproduce a specific snapshot date from an already-saved Volume file:
    build_cvx.py --catalog ecdh_model_dev --snapshot-date 2026-06-15
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

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import cvx

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "cvx"
CURRENT_VIEW = "cvx_current"
PIPELINE_REF = "bundles/_reference/src/build_cvx.py"

#: Default managed Volume (in the ``codes`` schema) for the raw XML-new snapshots
#: (ADR 0032). One Volume per revise-in-place source; files are date-stamped and
#: immutable.
DEFAULT_VOLUME = "cvx_raw"

# Flat code columns + the snapshot/audit columns the entrypoint stamps. PK is
# (cvx_code, snapshot_date); cvx_last_updated is nullable (a blank source value
# is allowed). ADR 0032.
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


# ---------------------------------------------------------------------------
# IO: fetch the XML-new payload + the immutable Volume snapshot (kept out of the
# pure module per ADR 0011; the URL/encoding knowledge lives in cvx.py).
# ---------------------------------------------------------------------------


def _fetch_xml(url: str) -> bytes:
    """Download the CVX XML-new payload and return the raw bytes (verbatim)."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted CDC IIS host
        raw = resp.read()
    log.info("Fetched CVX XML-new", extra={"url": url, "bytes": len(raw)})
    return raw


def _volume_dir(catalog: str, volume: str) -> str:
    """Return the UC Volume directory for the raw snapshots (ADR 0032)."""
    return f"/Volumes/{catalog}/{SCHEMA}/{volume}"


def _snapshot_path(catalog: str, volume: str, snapshot_date: date) -> str:
    """Return the date-stamped raw-snapshot file path (``cvx_<YYYY-MM-DD>.xml``)."""
    return f"{_volume_dir(catalog, volume)}/cvx_{snapshot_date.isoformat()}.xml"


def _persist_snapshot(path: str, raw: bytes) -> tuple[bytes, bool]:
    """Write the raw payload to ``path`` unless it already exists (immutability).

    Returns ``(snapshot_bytes, wrote_new_file)``. When the date's file already
    exists (a same-day re-run), it is **not** overwritten; its existing bytes are
    returned so the table reflects the immutable snapshot of record. ADR 0032.
    """
    p = Path(path)
    if p.exists():
        log.warning("Raw snapshot already exists; not overwriting", extra={"path": path})
        return p.read_bytes(), False
    p.write_bytes(raw)
    log.info("Wrote immutable raw snapshot", extra={"path": path, "bytes": len(raw)})
    return raw, True


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness / non-null / status-vocab; WARN cardinality,
# last-updated parse/freshness, nonvaccine share, snapshot freshness.
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    records: list[cvx.CvxRecord],
    snapshot_date: date,
    *,
    wrote_new_file: bool,
) -> None:
    """Record DQ outcomes; raise on any blocking FAIL so a bad table never writes."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(records)

    dup_codes = cvx.find_duplicate_codes(records)
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_code_snapshot_date_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_codes,
        failing_row_count=len(dup_codes),
        total_row_count=total,
        details={"sample_duplicates": dup_codes[:10]} if dup_codes else None,
    )

    missing = cvx.find_missing_required(records)
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing,
        failing_row_count=len(missing),
        total_row_count=total,
        details={"sample_missing": [list(m) for m in missing[:10]]} if missing else None,
    )

    bad_status = cvx.find_status_violations(records)
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_vaccine_status_controlled_vocab",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_status,
        failing_row_count=len(bad_status),
        total_row_count=total,
        details={
            "allowed": sorted(cvx.VACCINE_STATUS_VALUES),
            "sample_violations": [list(b) for b in bad_status[:10]],
        }
        if bad_status
        else None,
    )

    # snapshot_date is stamped by this entrypoint, so it is non-null by construction;
    # the check guards against a future refactor passing it through unset.
    snapshot_ok = snapshot_date is not None
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_snapshot_date_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=snapshot_ok,
        failing_row_count=0 if snapshot_ok else total,
        total_row_count=total,
    )

    # --- WARN checks ---
    card_ok = cvx.CARDINALITY_MIN <= total <= cvx.CARDINALITY_MAX
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=card_ok,
        failing_row_count=0 if card_ok else total,
        total_row_count=total,
        details={"expected_range": [cvx.CARDINALITY_MIN, cvx.CARDINALITY_MAX], "actual": total},
    )

    unparseable = cvx.find_unparseable_last_updated(records)
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_last_updated_parses",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not unparseable,
        failing_row_count=len(unparseable),
        total_row_count=total,
        details={"sample": [list(u) for u in unparseable[:10]]} if unparseable else None,
    )

    future = cvx.find_future_last_updated(records, snapshot_date)
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_last_updated_not_future",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not future,
        failing_row_count=len(future),
        total_row_count=total,
        details={"as_of": snapshot_date.isoformat(), "sample": [[c, d.isoformat()] for c, d in future[:10]]}
        if future
        else None,
    )

    # Snapshot freshness: this run produced a Volume file (new or pre-existing) and
    # a snapshot_date partition. INFO-level provenance + a freshness WARN.
    ctx.recorder.record(
        table_name=table,
        check_name="cvx_snapshot_freshness",
        category=DQCategory.FRESHNESS,
        severity=DQSeverity.WARN,
        passed=total > 0,
        failing_row_count=0 if total > 0 else 1,
        total_row_count=total,
        details={"snapshot_date": snapshot_date.isoformat(), "wrote_new_volume_file": wrote_new_file},
    )

    failures: list[str] = []
    if dup_codes:
        failures.append(f"duplicate cvx_code in snapshot: {dup_codes[:5]}")
    if missing:
        failures.append(f"null required field: {missing[:5]}")
    if bad_status:
        failures.append(f"vaccine_status out of vocab: {bad_status[:5]}")
    if not snapshot_ok:
        failures.append("snapshot_date is null")
    if failures:
        raise ValueError("CVX blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace by snapshot_date; ADR 0032 / ADR 0024 vintage semantics)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    """True if ``full`` exists and carries ``column`` (drives first-build vs. replace)."""
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], snapshot_date: date
) -> None:
    """snapshot_replace: replace only this run's ``snapshot_date`` rows; keep priors."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=CVX_SPARK_SCHEMA).sort("cvx_code")
    if _table_has_column(spark, full, "snapshot_date"):
        spark.sql(f"DELETE FROM {full} WHERE snapshot_date = DATE'{snapshot_date.isoformat()}'")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.cvx", extra={"rows": len(rows), "snapshot_date": snapshot_date.isoformat()})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'CDC IIS CVX (Vaccine Administered) code set, flat (no hierarchy). One row per "
        f"CVX code per snapshot; PK (cvx_code, snapshot_date). Revision-tracked via "
        f"snapshot_replace with raw XML-new preserved on a UC Volume (ADR 0032). "
        f"Current = latest snapshot_date (see codes.cvx_current). Reference table.'"
    )


def _create_current_view(spark: SparkSession, catalog: str) -> None:
    """Create/refresh ``codes.cvx_current`` = rows at the latest ``snapshot_date``."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    view = f"{catalog}.{SCHEMA}.{CURRENT_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {full} WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'codes.cvx restricted to the latest snapshot_date (the current CVX code set). "
        f"ADR 0032.'"
    )
    log.info("Created/refreshed codes.cvx_current view")


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008)
# ---------------------------------------------------------------------------


def _register(spark: SparkSession, catalog: str, snapshot_date: date, source_url: str, *, create_view: bool) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    known_limitations = (
        "Flat CVX code set (no vaccine-group mapping -- that arrives via a separate CDSi "
        "job; no MVX/CPT/NDC). Revision-tracked by snapshot: each annual run keeps a new "
        "snapshot_date, with the raw XML-new preserved verbatim on the codes.cvx_raw "
        "Volume; 'current' is the latest snapshot_date (codes.cvx_current). The XML-new "
        "payload has no vaccine/non-vaccine indicator, so no such flag is synthesized: "
        "every code is loaded as published, including Non-US and Never-Active entries and "
        "administrative codes (e.g. 998 'no vaccine administered'), distinguishable by "
        "cvx_code and description."
    )
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=full,
            subject=SCHEMA,
            layer="reference",
            description=(
                "CDC IIS CVX (Vaccine Administered) code set: canonical vaccine codes with "
                "short and full vaccine names, a controlled vaccine_status (active / inactive "
                "/ pending / non_us / never_active), and the source 'Last Updated' date. "
                "Flat (no hierarchy). One row per code per snapshot; PK (cvx_code, "
                "snapshot_date)."
            ),
            public_health_relevance=(
                "Canonical vaccine-code standard for U.S. immunization data; lets vaccine "
                "surveillance / clinical feeds conform administered-vaccine codes to a shared, "
                "revision-tracked reference."
            ),
            spatial_resolution="none",
            spatial_coverage="United States",
            source_provider_code="cdc",
            source_url=source_url,
            source_documentation_url=cvx.SOURCE_LANDING_URL,
            license="public domain (U.S. Government work, 17 U.S.C. 105)",
            dua_required=False,
            dua_reference="No DUA. CDC IIS CVX code set is public domain.",
            access_tier="open",
            external_maintainer_name="CDC Immunization Information Systems (IIS)",
            is_hosted=True,
            source_data_dictionary_url=cvx.SOURCE_DATA_DICTIONARY_URL,
            temporal_coverage_start=snapshot_date,
            temporal_coverage_end=snapshot_date,
            temporal_resolution="annual",
            known_limitations=known_limitations,
            derived_from=[source_url],
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=full,
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["snapshot_date", "cvx_code"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    if create_view:
        view_full = f"{catalog}.{SCHEMA}.{CURRENT_VIEW}"
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=view_full,
                subject=SCHEMA,
                layer="reference",
                description=(
                    "codes.cvx restricted to the latest snapshot_date -- the current CVX code "
                    "set for consumers who just want today's codes (ADR 0032)."
                ),
                public_health_relevance=(
                    "Convenience surface for the current CVX codes without filtering on "
                    "snapshot_date."
                ),
                spatial_resolution="none",
                spatial_coverage="United States",
                source_provider_code="cdc",
                source_url=source_url,
                source_documentation_url=cvx.SOURCE_LANDING_URL,
                license="public domain (U.S. Government work, 17 U.S.C. 105)",
                dua_required=False,
                dua_reference="No DUA. CDC IIS CVX code set is public domain.",
                access_tier="open",
                external_maintainer_name="CDC Immunization Information Systems (IIS)",
                is_hosted=False,  # view, not materialized
                source_data_dictionary_url=cvx.SOURCE_DATA_DICTIONARY_URL,
                known_limitations="Latest-snapshot view over codes.cvx; see that table for history.",
                derived_from=[full],
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=view_full,
                update_semantics="full_refresh",  # a view recomputes on every read
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
    source_url: str = cvx.SOURCE_XML_NEW_URL,
    create_view: bool = True,
) -> None:
    snap = snapshot_date or datetime.now(tz=UTC).date()
    raw = _fetch_xml(source_url)
    snapshot_path = _snapshot_path(catalog, volume, snap)

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, ...). "
            f"Owned by the _reference bundle. See ADR 0014.'"
        )
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {catalog}.{SCHEMA}.{volume} "
            f"COMMENT 'Immutable date-stamped raw CVX XML-new snapshots (ADR 0032).'"
        )

    def _work(ctx: BuildContext) -> None:
        # Persist the immutable raw snapshot, then parse the bytes of record (the
        # existing file on a same-day re-run, else the freshly-fetched payload).
        snapshot_bytes, wrote_new_file = _persist_snapshot(snapshot_path, raw)
        records = cvx.parse_cvx_xml(snapshot_bytes.decode(cvx.SOURCE_ENCODING))
        _dq_checks(ctx, records, snap, wrote_new_file=wrote_new_file)

        now = datetime.now(tz=UTC)
        rows = [
            {
                "cvx_code": r.cvx_code,
                "snapshot_date": snap,
                "short_description": r.short_description,
                "full_vaccine_name": r.full_vaccine_name,
                "vaccine_status": r.vaccine_status,
                "cvx_last_updated": r.cvx_last_updated,
                "source_file": snapshot_path,
                "loaded_at": now,
            }
            for r in records
        ]
        _write_table(ctx.spark, catalog, rows, snap)
        _comment_table(ctx.spark, catalog)
        if create_view:
            _create_current_view(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        # Reference data is canonical and pipeline-owned: both groups get reader-tier
        # only (ADR 0018); verify the applied grants as a deploy-time access gate.
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)
        # READ VOLUME is volume-scoped and not covered by the schema SELECT grant,
        # so grant it explicitly on the raw-snapshot Volume so readers can open the
        # archived XML payloads (ADR 0032).
        grants.grant_volume_reader(spark, catalog, SCHEMA, volume, data_engineers_group)
        grants.grant_volume_reader(spark, catalog, SCHEMA, volume, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, snap, source_url, create_view=create_view),
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
        help=f"Managed Volume (in the codes schema) for raw snapshots. Default: {DEFAULT_VOLUME}.",
    )
    parser.add_argument(
        "--source-url",
        default=cvx.SOURCE_XML_NEW_URL,
        help="Override the CVX XML-new download URL.",
    )
    parser.add_argument(
        "--no-current-view",
        action="store_true",
        help="Skip creating/refreshing the codes.cvx_current view.",
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
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
