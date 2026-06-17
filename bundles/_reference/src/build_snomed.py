"""Build the canonical ``codes.snomed`` reference table (ADR 0014).

SNOMED CT US Edition concepts (concept + FSN + preferred term + semantic tag) for
conforming clinical data to canonical SNOMED concept IDs. This entrypoint is the thin
IO + Spark layer over the pure logic in ``cidmath_datahub.reference.snomed`` (ADR 0011).
**Flat** for v1 -- the IS-A relationship graph (a polyhierarchy/DAG of millions of
edges) is a separate major effort and is not attempted here.

Versioning model (ICD-10 per-version, NOT ADR 0032). The US Edition ships semi-annually
and the NLM archives every release, so it is re-pullable: ``codes.snomed`` is keyed by
``snomed_version`` (the release effective date, e.g. ``"20260301"``) with
``snapshot_replace`` -- replace this release's rows, retain others (ADR 0024). Uses the
RF2 **Snapshot** view (current state), not Full or Delta.

Access is the authenticated NLM/UTS download (UMLS API key from the shared
``umls_secret_scope`` -- the same scope/key the RxNorm build uses; ADR 0012; no creds in
code). SNOMED CT is licensed (UMLS Metathesaurus License incl. the SNOMED CT affiliate
license; free for US use), so the table registers ``access_tier="restricted"`` /
``dua_required=True``; no external redistribution.

Thin entrypoint over the ``run_build`` seam (ADR 0027). Blocking DQ (FAIL, raises):
``(concept_id, snomed_version)`` uniqueness; non-null ``concept_id`` / ``snomed_version``;
``concept_id`` is a valid SCTID (Verhoeff check digit); active concepts have a non-null
FSN and exactly one active FSN + one preferred synonym; and (when an expected MD5 is
supplied) the download checksum matches. WARN: cardinality, inactive share, semantic-tag
distribution, preferred-term coverage.

Usage:
    build_snomed.py --catalog ecdh_model_dev --snomed-version 20260301 \\
        --release-url <NLM US Edition RF2 zip url> --umls-secret-scope ecdh-dev-umls \\
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
from cidmath_datahub.reference import snomed

log = get_logger(__name__)

SCHEMA = "codes"
TABLE = "snomed"
CURRENT_VIEW = "snomed_current"
PIPELINE_REF = "bundles/_reference/src/build_snomed.py"

#: UTS Release API -- lists each release (with its downloadUrl) for a release type, so
#: the exact NLM URL is resolved at runtime rather than hardcoded.
RELEASES_URL = "https://uts-ws.nlm.nih.gov/releases"
RELEASE_TYPE = "snomed-ct-us-edition"

SNOMED_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("concept_id", T.StringType(), False),
        T.StructField("fsn", T.StringType(), True),
        T.StructField("preferred_term", T.StringType(), True),
        T.StructField("semantic_tag", T.StringType(), True),
        T.StructField("active", T.BooleanType(), False),
        T.StructField("module_id", T.StringType(), True),
        T.StructField("effective_time", T.StringType(), True),
        T.StructField("snomed_version", T.StringType(), False),
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
    """Resolve the NLM download URL for a SNOMED US Edition ``version`` via the Release API.

    ``GET uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-us-edition`` lists each release
    with its ``downloadUrl``; ``version`` is matched leniently (its digits must appear in the
    release's version/filename/url, so ``"20260301"`` matches ``releaseVersion`` ``"2026-03-01"``
    / file ``...20260301T120000Z.zip``). ``"current"`` (or empty) takes the current release.
    No API key needed for the listing.
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

    The UTS mechanism is ``GET uts-ws.nlm.nih.gov/download?url=<file>&apiKey=<key>``;
    it validates the key and streams the file. (Shared mechanism with the RxNorm RRF
    download -- a candidate to factor into a common helper later.)
    """
    query = urllib.parse.urlencode({"url": release_url, "apiKey": api_key})
    url = f"{snomed.UTS_DOWNLOAD_URL}?{query}"
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted NLM/UTS host
        raw = resp.read()
        content_type = resp.headers.get("Content-Type", "")
    # A real release is a zip (starts with the "PK" local-file signature). A 200 with
    # non-zip bytes means the UTS proxy returned an error/HTML page -- almost always a
    # wrong --release-url or an unauthorized key -- so fail with the response, not a
    # cryptic BadZipFile later.
    if raw[:2] != b"PK":
        preview = raw[:500].decode("utf-8", "replace")
        raise ValueError(
            f"UTS download did not return a zip ({len(raw)} bytes, Content-Type={content_type!r}). "
            f"Check that --release-url is a valid current NLM release URL and the UMLS key is "
            f"authorized. Response began: {preview!r}"
        )
    log.info("Downloaded SNOMED RF2 release", extra={"release_url": release_url, "bytes": len(raw)})
    return raw


def _extract_member(zip_bytes: bytes, pattern) -> str:
    """Extract the single Snapshot member matching ``pattern`` (by basename), as UTF-8."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        matches = [n for n in zf.namelist() if pattern.search(n.split("/")[-1])]
        if len(matches) != 1:
            raise ValueError(
                f"Expected exactly one member matching {pattern.pattern}; found {matches}"
            )
        raw = zf.read(matches[0])
    return raw.decode(snomed.SOURCE_ENCODING)


def _member_name(zip_bytes: bytes, pattern) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return next(n.split("/")[-1] for n in zf.namelist() if pattern.search(n.split("/")[-1]))


# ---------------------------------------------------------------------------
# DQ (ADR 0009): blocking uniqueness/non-null/SCTID/FSN-preferred/checksum; WARN rest
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    rows: list[snomed.SnomedConcept],
    concepts: list[snomed.SnomedConceptRow],
    descriptions: list[snomed.SnomedDescription],
    preferred_ids: set[str],
    version: str,
    *,
    checksum_ok: bool | None,
    computed_md5: str,
    expected_md5: str,
) -> None:
    """Record DQ; raise on any blocking FAIL so a bad table never writes."""
    table = f"{SCHEMA}.{TABLE}"
    total = len(rows)
    n_active = sum(1 for r in rows if r.active)

    if checksum_ok is not None:
        ctx.recorder.record(
            table_name=table,
            check_name="snomed_download_checksum_matches",
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
            check_name="snomed_download_checksum_provided",
            category=DQCategory.SCHEMA,
            severity=DQSeverity.WARN,
            passed=False,
            total_row_count=total,
            details={"note": "no --expected-md5 supplied; download integrity not verified"},
        )

    dup = snomed.find_duplicate_concept_ids(rows)
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_concept_id_version_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup,
        failing_row_count=len(dup),
        total_row_count=total,
        details={"sample": dup[:10]} if dup else None,
    )

    bad_sctid = snomed.find_invalid_sctids(rows)
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_concept_id_valid_sctid",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_sctid,
        failing_row_count=len(bad_sctid),
        total_row_count=total,
        details={"sample": bad_sctid[:10]} if bad_sctid else None,
    )

    missing_fsn = snomed.find_active_missing_fsn(rows)
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_active_fsn_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not missing_fsn,
        failing_row_count=len(missing_fsn),
        total_row_count=n_active,
        details={"sample": missing_fsn[:10]} if missing_fsn else None,
    )

    bad_fsn_count = snomed.find_active_fsn_count_anomalies(concepts, descriptions)
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_active_exactly_one_fsn",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_fsn_count,
        failing_row_count=len(bad_fsn_count),
        total_row_count=n_active,
        details={"sample": bad_fsn_count[:10]} if bad_fsn_count else None,
    )

    bad_pref_count = snomed.find_active_preferred_count_anomalies(
        concepts, descriptions, preferred_ids
    )
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_active_exactly_one_preferred",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_pref_count,
        failing_row_count=len(bad_pref_count),
        total_row_count=n_active,
        details={"sample": bad_pref_count[:10]} if bad_pref_count else None,
    )

    # --- WARN checks ---
    card_ok = n_active >= snomed.ACTIVE_CONCEPT_MIN
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_active_concept_cardinality",
        category=DQCategory.CARDINALITY,
        severity=DQSeverity.WARN,
        passed=card_ok,
        failing_row_count=0 if card_ok else n_active,
        total_row_count=n_active,
        details={"expected_min": snomed.ACTIVE_CONCEPT_MIN, "active": n_active, "total": total},
    )

    missing_pref = snomed.find_active_missing_preferred(rows)
    ctx.recorder.record(
        table_name=table,
        check_name="snomed_active_preferred_coverage",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.WARN,
        passed=not missing_pref,
        failing_row_count=len(missing_pref),
        total_row_count=n_active,
        details={"sample": missing_pref[:10]} if missing_pref else None,
    )

    ctx.recorder.record(
        table_name=table,
        check_name="snomed_semantic_tag_distribution",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.INFO,
        passed=True,
        total_row_count=n_active,
        details={
            "inactive_concepts": snomed.inactive_count(rows),
            "semantic_tags": snomed.semantic_tag_distribution(rows),
        },
    )

    failures: list[str] = []
    if checksum_ok is False:
        failures.append(f"download MD5 mismatch (got {computed_md5}, want {expected_md5})")
    if dup:
        failures.append(f"duplicate concept_id: {dup[:5]}")
    if bad_sctid:
        failures.append(f"invalid SCTID: {bad_sctid[:5]}")
    if missing_fsn:
        failures.append(f"active concept missing FSN: {missing_fsn[:5]}")
    if bad_fsn_count:
        failures.append(f"active concept not exactly one FSN: {bad_fsn_count[:5]}")
    if bad_pref_count:
        failures.append(f"active concept not exactly one preferred: {bad_pref_count[:5]}")
    if failures:
        raise ValueError("SNOMED blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace by snomed_version; ADR 0024)
# ---------------------------------------------------------------------------


def _table_has_column(spark: SparkSession, full: str, column: str) -> bool:
    if not spark.catalog.tableExists(full):
        return False
    return column in {f.name for f in spark.table(full).schema.fields}


def _write_table(
    spark: SparkSession, catalog: str, rows: list[dict[str, Any]], version: str
) -> None:
    """snapshot_replace: replace only this run's ``snomed_version`` rows; keep priors."""
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    df = spark.createDataFrame(rows, schema=SNOMED_SPARK_SCHEMA).sort("concept_id")
    if _table_has_column(spark, full, "snomed_version"):
        spark.sql(f"DELETE FROM {full} WHERE snomed_version = '{version}'")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote codes.snomed", extra={"rows": len(rows), "snomed_version": version})


def _comment_table(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"COMMENT ON TABLE {catalog}.{SCHEMA}.{TABLE} IS "
        f"'SNOMED CT US Edition concepts (flat: concept + FSN + preferred term + semantic "
        f"tag) from the RF2 Snapshot. One row per concept per release; PK (concept_id, "
        f"snomed_version); snapshot_replace. IS-A hierarchy deferred. Licensed (UMLS/SNOMED), "
        f"restricted. ADR 0014.'"
    )


def _create_current_view(spark: SparkSession, catalog: str) -> None:
    full = f"{catalog}.{SCHEMA}.{TABLE}"
    view = f"{catalog}.{SCHEMA}.{CURRENT_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {full} WHERE snomed_version = (SELECT MAX(snomed_version) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'codes.snomed restricted to the latest snomed_version (the current US Edition).'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008) -- restricted (UMLS/SNOMED licensed)
# ---------------------------------------------------------------------------

_LICENSE = (
    "SNOMED CT US Edition via the NLM under the UMLS Metathesaurus License (includes the "
    "SNOMED CT affiliate license). Free for US use; SNOMED International attribution "
    "required. See https://www.nlm.nih.gov/healthit/snomedct/."
)
_DUA_REFERENCE = (
    "UMLS Metathesaurus License + SNOMED CT affiliate license (US Edition via NLM). "
    "Internal warehouse/conformance use only; no external redistribution / Delta-share."
)
_KNOWN_LIMITATIONS = (
    "Concept grain only: one row per concept with FSN + preferred term + semantic tag. "
    "The IS-A relationship graph (polyhierarchy/DAG), full descriptions/synonyms, reference "
    "sets, and the SNOMED<->ICD-10-CM / other maps are deferred (separate issues). Built "
    "from the RF2 Snapshot (current state), not Full (no history). Licensed (UMLS/SNOMED "
    "affiliate): internal conformance use only -- no external redistribution / Delta-share."
)


def _register(spark: SparkSession, catalog: str, version: str, *, create_view: bool) -> None:
    g = f"{catalog}.{SCHEMA}"
    common = {
        "subject": SCHEMA,
        "layer": "reference",
        "public_health_relevance": (
            "Canonical clinical terminology for conforming diagnosis/finding/procedure data "
            "to shared SNOMED concept IDs (e.g. 73211009 Diabetes mellitus (disorder))."
        ),
        "spatial_resolution": "none",
        "spatial_coverage": "United States",
        "source_provider_code": "nlm",
        "source_url": snomed.SOURCE_LANDING_URL,
        "source_documentation_url": snomed.SOURCE_DOCUMENTATION_URL,
        "source_data_dictionary_url": snomed.SOURCE_DATA_DICTIONARY_URL,
        "license": _LICENSE,
        "dua_required": True,
        "dua_reference": _DUA_REFERENCE,
        "access_tier": "restricted",
        "external_maintainer_name": "SNOMED International (US Edition via NLM)",
        "is_hosted": True,
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": [f"SNOMED CT US Edition RF2 Snapshot {version}"],
    }
    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{TABLE}",
            description=(
                "SNOMED CT US Edition concepts (flat) from the RF2 Snapshot: concept_id, "
                "Fully Specified Name, preferred term, and the FSN semantic tag (disorder / "
                "procedure / finding / ...), plus active / module_id / effective_time. PK "
                "(concept_id, snomed_version). IS-A hierarchy deferred."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["snomed_version", "concept_id"],
            pipeline_reference=PIPELINE_REF,
        ),
    )
    if create_view:
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{CURRENT_VIEW}",
                description="codes.snomed restricted to the latest snomed_version.",
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
    snomed_version: str,
    release_url: str | None = None,
    umls_secret_scope: str = "",
    umls_secret_key: str = "umls_api_key",
    expected_md5: str | None = None,
    create_view: bool = True,
) -> None:
    if not umls_secret_scope:
        raise ValueError("--umls-secret-scope is required to pull the licensed SNOMED release")

    # Resolve the exact NLM download URL from the Release API unless one is given.
    resolved_url = release_url or _resolve_release_url(snomed_version)

    api_key = _get_secret(umls_secret_scope, umls_secret_key)
    zip_bytes = _uts_download(resolved_url, api_key)
    computed_md5 = hashlib.md5(zip_bytes).hexdigest()  # nosec B324 - integrity, not security
    checksum_ok = (computed_md5.lower() == expected_md5.lower()) if expected_md5 else None

    concept_member = _member_name(zip_bytes, snomed.CONCEPT_MEMBER_RE)
    concepts = snomed.parse_concepts(_extract_member(zip_bytes, snomed.CONCEPT_MEMBER_RE))
    descriptions = snomed.parse_descriptions(
        _extract_member(zip_bytes, snomed.DESCRIPTION_MEMBER_RE)
    )
    preferred_ids = snomed.parse_preferred_description_ids(
        _extract_member(zip_bytes, snomed.LANGUAGE_MEMBER_RE)
    )
    rows_obj = snomed.assemble_concepts(concepts, descriptions, preferred_ids)

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, "
            f"NDC, LOINC, SNOMED CT, ...). Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        _dq_checks(
            ctx,
            rows_obj,
            concepts,
            descriptions,
            preferred_ids,
            snomed_version,
            checksum_ok=checksum_ok,
            computed_md5=computed_md5,
            expected_md5=expected_md5 or "",
        )
        now = datetime.now(tz=UTC)
        rows = [
            {
                "concept_id": r.concept_id,
                "fsn": r.fsn,
                "preferred_term": r.preferred_term,
                "semantic_tag": r.semantic_tag,
                "active": r.active,
                "module_id": r.module_id,
                "effective_time": r.effective_time,
                "snomed_version": snomed_version,
                "source_file": concept_member,
                "loaded_at": now,
            }
            for r in rows_obj
        ]
        _write_table(ctx.spark, catalog, rows, snomed_version)
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
        register=lambda spark: _register(spark, catalog, snomed_version, create_view=create_view),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--snomed-version",
        required=True,
        help="Release effective date, e.g. 20260301. Used to resolve the download URL via the "
        "UTS Release API and to stamp the table.",
    )
    parser.add_argument(
        "--release-url",
        default=None,
        help="Override the NLM US Edition RF2 zip URL. If omitted, it's resolved from "
        "--snomed-version via the UTS Release API.",
    )
    parser.add_argument("--umls-secret-scope", required=True, help="Shared UMLS secret scope.")
    parser.add_argument("--umls-secret-key", default="umls_api_key")
    parser.add_argument(
        "--expected-md5", default=None, help="Published release MD5 to verify (blocking if set)."
    )
    parser.add_argument(
        "--no-current-view", action="store_true", help="Skip the codes.snomed_current view."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.data_engineers_group,
        args.analysts_group,
        args.snomed_version,
        args.release_url,
        args.umls_secret_scope,
        umls_secret_key=args.umls_secret_key,
        expected_md5=args.expected_md5,
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
