"""Build the canonical ``codes.rxnorm`` reference table (ADR 0014).

NLM RxNorm normalized drug concepts (``SAB=RXNORM`` RxCUIs + term types) for conforming
drug/medication data to canonical RxCUIs. This entrypoint is the thin IO + Spark layer over
the pure logic in ``cidmath_datahub.reference.rxnorm`` (ADR 0011). **Flat** for v1 -- one
row per concept; the ``RXNREL`` relationship graph and ``RXNSAT`` attributes (incl. the
RxNorm<->NDC crosswalk) are deferred (they reuse the same downloaded RRF).

Versioning model (ICD-10 per-version, NOT ADR 0032). RxNorm ships monthly and the NLM
archives every release, so it is re-pullable: ``codes.rxnorm`` is keyed by ``rxnorm_version``
(the release identifier, e.g. ``"04072025"``) with ``snapshot_replace`` (ADR 0024).

Access is the authenticated NLM/UTS download of the full RRF release (UMLS API key from the
**shared** ``umls_secret_scope`` -- the same scope/key the SNOMED build uses; ADR 0012; no
creds in code). The RxNorm vocabulary (``SAB=RXNORM``) is non-proprietary, so the table
registers ``access_tier="open"`` (lighter than SNOMED) -- only the download is UMLS-gated.

The UTS download here mirrors ``build_snomed.py`` rather than sharing a helper: SNOMED is the
first user and is in review, so this is the second use; a common helper can be extracted at
the rule-of-three (noted in the PR).

Thin entrypoint over the ``run_build`` seam (ADR 0027). Blocking DQ (FAIL, raises):
``(rxcui, rxnorm_version)`` uniqueness; non-null ``rxcui`` / ``name`` / ``tty`` /
``rxnorm_version``; ``tty`` in the RxNorm term-type vocabulary; and (when an expected MD5 is
supplied) the download checksum matches. WARN: cardinality, TTY distribution.

Usage:
    build_rxnorm.py --catalog ecdh_model_dev --rxnorm-version 04072025 \\
        --release-url <NLM RxNorm_full_<MMDDYYYY>.zip url> --umls-secret-scope ecdh-dev-umls \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import rxnorm

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "rxnorm"
CURRENT_VIEW = "rxnorm_current"
PIPELINE_REF = "bundles/_reference/src/build_rxnorm.py"

#: UTS Release API -- lists each release (with its downloadUrl) for a release type, so
#: the exact NLM URL is resolved at runtime rather than hardcoded.
RELEASES_URL = "https://uts-ws.nlm.nih.gov/releases"
RELEASE_TYPE = "rxnorm-full-monthly-release"

RXNORM_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("rxcui", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("tty", T.StringType(), False),
        T.StructField("rxnorm_version", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# IO: shared UMLS secret + UTS-authenticated download (kept out of the pure module)
# ---------------------------------------------------------------------------


def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - depends on runtime flavor
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope=scope, key=key)


def _resolve_release_url(version: str) -> str:
    """Resolve the NLM download URL for an RxNorm ``version`` via the UTS Release API.

    ``GET uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-full-monthly-release`` lists each
    release with its ``downloadUrl``; ``version`` is matched leniently (its digits must appear
    in the release's version/filename/url, e.g. ``"04072025"`` matches
    ``RxNorm_full_04072025.zip``). ``"current"`` (or empty) takes the current release. No API
    key needed for the listing.
    """
    current = version.strip().lower() in ("", "current")
    params = {"releaseType": RELEASE_TYPE}
    if current:
        params["current"] = "true"
    url = f"{RELEASES_URL}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted NLM/UTS host
        releases = json.loads(resp.read())
    if not releases:
        raise ValueError(f"UTS Release API returned no releases for {RELEASE_TYPE!r}")
    if current:
        chosen = releases[0]
    else:
        want = "".join(c for c in version if c.isdigit())
        chosen = next(
            (
                r
                for r in releases
                if want
                in "".join(
                    c
                    for c in f"{r.get('releaseVersion', '')}{r.get('fileName', '')}{r.get('downloadUrl', '')}"
                    if c.isdigit()
                )
            ),
            None,
        )
        if chosen is None:
            available = [r.get("releaseVersion") for r in releases[:10]]
            raise ValueError(
                f"No {RELEASE_TYPE} release matching version {version!r}; available: {available}"
            )
    download_url = chosen["downloadUrl"]
    log.info("Resolved release URL", extra={"version": version, "download_url": download_url})
    return download_url


def _uts_download(release_url: str, api_key: str) -> bytes:
    """Download an NLM release via the UTS download proxy (UMLS API key auth).

    ``GET uts-ws.nlm.nih.gov/download?url=<file>&apiKey=<key>`` validates the key and
    streams the file. (Same mechanism as build_snomed.py -- extract a shared helper at
    the rule-of-three.)
    """
    query = urllib.parse.urlencode({"url": release_url, "apiKey": api_key})
    url = f"{rxnorm.UTS_DOWNLOAD_URL}?{query}"
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted NLM/UTS host
        raw = resp.read()
        content_type = resp.headers.get("Content-Type", "")
    # A real release is a zip ("PK" signature). A 200 with non-zip bytes means the UTS
    # proxy returned an error/HTML page (wrong --release-url or unauthorized key).
    if raw[:2] != b"PK":
        preview = raw[:500].decode("utf-8", "replace")
        raise ValueError(
            f"UTS download did not return a zip ({len(raw)} bytes, Content-Type={content_type!r}). "
            f"Check that --release-url is a valid current NLM release URL and the UMLS key is "
            f"authorized. Response began: {preview!r}"
        )
    log.info("Downloaded RxNorm RRF release", extra={"release_url": release_url, "bytes": len(raw)})
    return raw


def _extract_conso(zip_bytes: bytes) -> tuple[str, str]:
    """Extract the full-release ``RXNCONSO.RRF`` text from the zip. Returns ``(text, member)``.

    Member selection (full file vs the prescribe/ subset) lives in the pure module
    (:func:`rxnorm.select_conso_member`) so it stays testable (ADR 0011).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = rxnorm.select_conso_member(zf.namelist())
        raw = zf.read(member)
    return raw.decode(rxnorm.SOURCE_ENCODING), member.replace("\\", "/").split("/")[-1]


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness/non-null/tty/checksum; WARN cardinality/tty dist
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    concepts: list[rxnorm.RxnormConcept],
    version: str,
    *,
    checksum_ok: bool | None,
    computed_md5: str,
    expected_md5: str,
) -> None:
    """Record DQ; raise on any blocking FAIL so a bad table never writes."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(concepts)

    if checksum_ok is not None:
        ctx.recorder.record(
            table_name=table,
            check_name="rxnorm_download_checksum_matches",
            category=DQCategory.SCHEMA,
            severity=DQSeverity.FAIL,
            passed=checksum_ok,
            failing_row_count=0 if checksum_ok else 1,
            total_row_count=total,
            details=None if checksum_ok else {"computed": computed_md5, "expected": expected_md5},
        )
    else:
        ctx.recorder.record(
            table_name=table,
            check_name="rxnorm_download_checksum_provided",
            category=DQCategory.SCHEMA,
            severity=DQSeverity.WARN,
            passed=False,
            total_row_count=total,
            details={"note": "no --expected-md5 supplied; download integrity not verified"},
        )

    dup = rxnorm.find_duplicate_rxcui(concepts)
    ctx.recorder.record(
        table_name=table,
        check_name="rxnorm_rxcui_version_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup,
        failing_row_count=len(dup),
        total_row_count=total,
        details={"sample": dup[:10]} if dup else None,
    )

    miss = rxnorm.find_missing_fields(concepts)
    ctx.recorder.record(
        table_name=table,
        check_name="rxnorm_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not miss,
        failing_row_count=len(miss),
        total_row_count=total,
        details={"sample": [list(m) for m in miss[:10]]} if miss else None,
    )

    bad_tty = rxnorm.find_bad_tty(concepts)
    ctx.recorder.record(
        table_name=table,
        check_name="rxnorm_tty_controlled_vocab",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_tty,
        failing_row_count=len(bad_tty),
        total_row_count=total,
        details={"allowed": sorted(rxnorm.RXNORM_TTY_VALUES), "sample": [list(b) for b in bad_tty[:10]]}
        if bad_tty
        else None,
    )

    # --- WARN checks ---
    card_ok = total >= rxnorm.CARDINALITY_MIN
    ctx.recorder.record(
        table_name=table,
        check_name="rxnorm_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=card_ok,
        failing_row_count=0 if card_ok else total,
        total_row_count=total,
        details={"expected_min": rxnorm.CARDINALITY_MIN, "actual": total},
    )

    ctx.recorder.record(
        table_name=table,
        check_name="rxnorm_tty_distribution",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.INFO,
        passed=True,
        total_row_count=total,
        details={"distribution": rxnorm.tty_distribution(concepts)},
    )

    failures: list[str] = []
    if checksum_ok is False:
        failures.append(f"download MD5 mismatch (got {computed_md5}, want {expected_md5})")
    if dup:
        failures.append(f"duplicate rxcui: {dup[:5]}")
    if miss:
        failures.append(f"null required field: {miss[:5]}")
    if bad_tty:
        failures.append(f"tty out of vocab: {bad_tty[:5]}")
    if failures:
        raise ValueError("RxNorm blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace by rxnorm_version; ADR 0024)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(spark: SparkSession, catalog: str, rows: list[dict[str, Any]], version: str) -> None:
    """snapshot_replace: replace only this run's ``rxnorm_version`` rows; keep priors."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=RXNORM_SPARK_SCHEMA).sort("rxcui")
    if _table_has_column(spark, full, "rxnorm_version"):
        spark.sql(f"DELETE FROM {full} WHERE rxnorm_version = '{version}'")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.rxnorm", extra={"rows": len(rows), "rxnorm_version": version})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'NLM RxNorm normalized drug concepts (SAB=RXNORM, flat: rxcui + name + tty) from the "
        f"RRF release. One row per concept per release; PK (rxcui, rxnorm_version); "
        f"snapshot_replace. Relationships/attributes/NDC crosswalk deferred. ADR 0014.'"
    )


def _create_current_view(spark: SparkSession, catalog: str) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    view = f"{catalog}.{SCHEMA}.{CURRENT_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {full} WHERE rxnorm_version = (SELECT MAX(rxnorm_version) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'codes.rxnorm restricted to the latest rxnorm_version (the current RxNorm release).'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008) -- non-proprietary (open)
# ---------------------------------------------------------------------------

_KNOWN_LIMITATIONS = (
    "SAB=RXNORM, SUPPRESS=N concepts only: one row per RxCUI with its normalized name + TTY. "
    "Relationships (RXNREL ingredient->drug graph), attributes (RXNSAT, incl. the RxNorm<->NDC "
    "crosswalk to codes.ndc), source-vocabulary atoms (SAB != RXNORM), and the synonym/atom "
    "grain are deferred (separate issues, reusing the same RRF). The RxNorm vocabulary is "
    "non-proprietary; only the UTS download is UMLS-account-gated."
)


def _register(spark: SparkSession, catalog: str, version: str, *, create_view: bool) -> None:
    g = f"{catalog}.{SCHEMA}"
    common = {
        "subject": SCHEMA,
        "layer": "reference",
        "public_health_relevance": (
            "Canonical normalized drug terminology for conforming medication data to RxCUIs "
            "(e.g. 161 Acetaminophen); the basis for the deferred RxNorm<->NDC crosswalk."
        ),
        "spatial_resolution": "none",
        "spatial_coverage": "United States",
        "source_provider_code": "nlm",
        "source_url": rxnorm.SOURCE_LANDING_URL,
        "source_documentation_url": rxnorm.SOURCE_DOCUMENTATION_URL,
        "source_data_dictionary_url": rxnorm.SOURCE_DATA_DICTIONARY_URL,
        "license": "public domain (NLM RxNorm; SAB=RXNORM is non-proprietary)",
        "dua_required": False,
        "dua_reference": "No DUA for SAB=RXNORM data. The UTS download requires a UMLS account.",
        "access_tier": "open",
        "external_maintainer_name": "National Library of Medicine (NLM)",
        "is_hosted": True,
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": [f"NLM RxNorm full RRF release {version}"],
    }
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TABLE}",
            description=(
                "NLM RxNorm normalized drug concepts (SAB=RXNORM, SUPPRESS=N) from RXNCONSO.RRF: "
                "rxcui, normalized name, and term type (TTY: IN / SCD / SBD / BN / ...). Flat -- "
                "one row per RxCUI. PK (rxcui, rxnorm_version)."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["rxnorm_version", "rxcui"],
            pipeline_reference=PIPELINE_REF,
        ),
    )
    if create_view:
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{CURRENT_VIEW}",
                description="codes.rxnorm restricted to the latest rxnorm_version.",
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
    data_engineers_group: str,
    analysts_group: str,
    rxnorm_version: str,
    release_url: str | None = None,
    umls_secret_scope: str = "",
    umls_secret_key: str = "umls_api_key",
    expected_md5: str | None = None,
    create_view: bool = True,
) -> None:
    if not umls_secret_scope:
        raise ValueError("--umls-secret-scope is required to pull the RxNorm RRF release")

    # Resolve the exact NLM download URL from the Release API unless one is given.
    resolved_url = release_url or _resolve_release_url(rxnorm_version)

    api_key = _get_secret(umls_secret_scope, umls_secret_key)
    zip_bytes = _uts_download(resolved_url, api_key)
    computed_md5 = hashlib.md5(zip_bytes).hexdigest()  # nosec B324 - integrity, not security
    checksum_ok = (computed_md5.lower() == expected_md5.lower()) if expected_md5 else None

    conso_text, conso_member = _extract_conso(zip_bytes)
    atoms = rxnorm.parse_rxnconso(conso_text)
    concepts = rxnorm.reduce_to_concepts(atoms)

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, "
            f"NDC, LOINC, SNOMED CT, RxNorm, ...). Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(
            ctx,
            concepts,
            rxnorm_version,
            checksum_ok=checksum_ok,
            computed_md5=computed_md5,
            expected_md5=expected_md5 or "",
        )
        now = datetime.now(tz=UTC)
        rows = [
            {
                "rxcui": c.rxcui,
                "name": c.name,
                "tty": c.tty,
                "rxnorm_version": rxnorm_version,
                "source_file": conso_member,
                "loaded_at": now,
            }
            for c in concepts
        ]
        _write_table(ctx.spark, catalog, rows, rxnorm_version)
        _comment_table(ctx.spark, catalog)
        if create_view:
            _create_current_view(ctx.spark, catalog)

    def _grant(spark: SparkSession) -> None:
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.verify_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=_ensure,
        work=_work,
        register=lambda spark: _register(spark, catalog, rxnorm_version, create_view=create_view),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--rxnorm-version",
        required=True,
        help="Release identifier, e.g. 04072025 (MMDDYYYY). Used to resolve the download URL "
        "via the UTS Release API and to stamp the table.",
    )
    parser.add_argument(
        "--release-url",
        default=None,
        help="Override the NLM RxNorm full RRF zip URL. If omitted, it's resolved from "
        "--rxnorm-version via the UTS Release API.",
    )
    parser.add_argument("--umls-secret-scope", required=True, help="Shared UMLS secret scope.")
    parser.add_argument("--umls-secret-key", default="umls_api_key")
    parser.add_argument(
        "--expected-md5", default=None, help="Published release MD5 to verify (blocking if set)."
    )
    parser.add_argument(
        "--no-current-view", action="store_true", help="Skip the codes.rxnorm_current view."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.data_engineers_group,
        args.analysts_group,
        args.rxnorm_version,
        args.release_url,
        args.umls_secret_scope,
        umls_secret_key=args.umls_secret_key,
        expected_md5=args.expected_md5,
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
