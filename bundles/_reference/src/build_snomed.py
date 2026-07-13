"""Build the canonical ``codes.snomed`` reference table on the shared builder (ADR 0037/0039).

SNOMED CT US Edition concepts (concept + FSN + preferred term + semantic tag) for conforming
clinical data to canonical SNOMED concept IDs. This entrypoint is the thin IO + Spark layer over the
pure logic in ``cidmath_datahub.reference.snomed`` (ADR 0011). **Flat** for v1 -- the IS-A
relationship graph (a polyhierarchy/DAG of millions of edges) is a separate major effort.

**Source-path fold-in (ADR 0037 backport, wave 3).** Previously model-only + hand-rolled on
``run_build`` with no raw layer. Now folded onto the shared ``build_reference`` builder: the
authenticated RF2 release zip lands verbatim in the *source*-catalog landing Volume
``ecdh_<env>.codes_raw._landing`` (ADR 0039), parses into the 1:1 raw table ``ecdh_<env>.codes_raw
.snomed``, and the canonical ``ecdh_model_<env>.codes.snomed`` is promoted from raw. Same schema,
same rows -- a build-mechanism fold-in with data parity; consumers unaffected.

Versioning model (per-version, NOT ADR 0032). The US Edition ships semi-annually and the NLM
archives every release, so it is re-pullable: ``codes.snomed`` is keyed by ``snomed_version`` (the
release effective date, e.g. ``"20260301"``) -- ``vintage_column="snomed_version"`` with per-version
atomic ``replaceWhere`` (ADR 0034; the builder's string-vintage support, ADR 0037 wave 2). Uses the
RF2 **Snapshot** view (current state), not Full or Delta. Landed ``PER_VINTAGE_IMMUTABLE`` (a release
is immutable). The ``codes.snomed_current`` view is dropped (ADR 0034: "current" = ``MAX``).

Access is the authenticated NLM/UTS download (UMLS API key from the shared ``umls_secret_scope`` --
the same scope/key the RxNorm build uses; ADR 0012; no creds in code). The raw landing Volume is
engineer-only. SNOMED CT is licensed (UMLS Metathesaurus License incl. the SNOMED CT affiliate
license; free for US use), so the table registers ``access_tier="restricted"`` / ``dua_required=True``;
no external redistribution. When ``--expected-md5`` is supplied it is verified at fetch (mismatch
raises before the payload is cached).

Blocking DQ (FAIL, raises): ``(concept_id, snomed_version)`` uniqueness; ``concept_id`` a valid SCTID
(Verhoeff); active concepts have a non-null FSN and exactly one active FSN + one preferred synonym;
and (when supplied) the download checksum. WARN: cardinality, inactive share, semantic-tag
distribution, preferred-term coverage, unrecognized tags.

Usage:
    build_snomed.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --snomed-version 20260301 --umls-secret-scope ecdh-dev-umls \\
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
from cidmath_datahub.reference import snomed

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "snomed"
PIPELINE_REF = "bundles/_reference/src/build_snomed.py"

_RELEASE_ZIP = "snomed_rf2.zip"

#: UTS Release API -- lists each release (with its downloadUrl) for a release type.
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

_DDL = (
    "concept_id STRING, fsn STRING, preferred_term STRING, semantic_tag STRING, active BOOLEAN, "
    "module_id STRING, effective_time STRING, snomed_version STRING, source_file STRING, "
    "loaded_at TIMESTAMP"
)

_DESC = (
    "SNOMED CT US Edition concepts (flat) from the RF2 Snapshot: concept_id, Fully Specified Name, "
    "preferred term, and the FSN semantic tag (disorder / procedure / finding / ...), plus active / "
    "module_id / effective_time. PK (concept_id, snomed_version). IS-A hierarchy deferred."
)
_PHR = (
    "Canonical clinical terminology for conforming diagnosis/finding/procedure data to shared "
    "SNOMED concept IDs (e.g. 73211009 Diabetes mellitus (disorder))."
)
_LICENSE = (
    "SNOMED CT US Edition via the NLM under the UMLS Metathesaurus License (includes the SNOMED CT "
    "affiliate license). Free for US use; SNOMED International attribution required. See "
    "https://www.nlm.nih.gov/healthit/snomedct/."
)
_DUA_REFERENCE = (
    "UMLS Metathesaurus License + SNOMED CT affiliate license (US Edition via NLM). Internal "
    "warehouse/conformance use only; no external redistribution / Delta-share."
)
_KNOWN_LIMITATIONS = (
    "Concept grain only: one row per concept with FSN + preferred term + semantic tag. The IS-A "
    "relationship graph (polyhierarchy/DAG), full descriptions/synonyms, reference sets, and the "
    "SNOMED<->ICD-10-CM / other maps are deferred (separate issues). Built from the RF2 Snapshot "
    "(current state), not Full (no history). Licensed (UMLS/SNOMED affiliate): internal conformance "
    "use only -- no external redistribution / Delta-share."
)

_COMMON_META: dict[str, Any] = {
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
}


def _base_entry(version: str) -> registration.DatasetCatalogEntry:
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        derived_from=[f"SNOMED CT US Edition RF2 Snapshot {version}"],
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: shared UMLS secret + UTS-authenticated download (kept out of the pure module, ADR 0011).
# ---------------------------------------------------------------------------


def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - depends on runtime flavor
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope=scope, key=key)


def _resolve_release_url(version: str) -> str:
    """Resolve the NLM download URL for a SNOMED US Edition ``version`` via the Release API."""
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
    """Download an NLM release via the UTS download proxy (UMLS API key auth)."""
    query = urllib.parse.urlencode({"url": release_url, "apiKey": api_key})
    url = f"{snomed.UTS_DOWNLOAD_URL}?{query}"
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted NLM/UTS host
        raw = resp.read()
        content_type = resp.headers.get("Content-Type", "")
    if raw[:2] != b"PK":
        preview = raw[:500].decode("utf-8", "replace")
        raise ValueError(
            f"UTS download did not return a zip ({len(raw)} bytes, Content-Type={content_type!r}). "
            f"Check the release URL is a valid NLM release and the UMLS key is authorized. "
            f"Response began: {preview!r}"
        )
    log.info("Downloaded SNOMED RF2 release", extra={"release_url": release_url, "bytes": len(raw)})
    return raw


def _extract_member(zip_bytes: bytes, pattern: Any) -> str:
    """Extract the single Snapshot member matching ``pattern`` (by basename), as text."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        matches = [n for n in zf.namelist() if pattern.search(n.split("/")[-1])]
        if len(matches) != 1:
            raise ValueError(
                f"Expected exactly one member matching {pattern.pattern}; found {matches}"
            )
        raw = zf.read(matches[0])
    return raw.decode(snomed.SOURCE_ENCODING)


def _member_name(zip_bytes: bytes, pattern: Any) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return next(n.split("/")[-1] for n in zf.namelist() if pattern.search(n.split("/")[-1]))


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the version, auth, and parsed stash).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    snomed_version: str,
    release_url: str | None = None,
    umls_secret_scope: str = "",
    umls_secret_key: str = "umls_api_key",
    expected_md5: str | None = None,
) -> None:
    if not umls_secret_scope:
        raise ValueError("--umls-secret-scope is required to pull the licensed SNOMED release")

    api_key = _get_secret(umls_secret_scope, umls_secret_key)
    raw_fqn = f"{source_catalog}.{RAW_SCHEMA}.{TABLE}"
    stash: dict[str, Any] = {}

    def _fetch(v: str, volume_dir: str) -> None:
        resolved_url = release_url or _resolve_release_url(v)
        zip_bytes = _uts_download(resolved_url, api_key)
        if expected_md5:
            computed = hashlib.md5(zip_bytes).hexdigest()  # nosec B324 - integrity, not security
            if computed.lower() != expected_md5.lower():
                raise ValueError(
                    f"SNOMED {v} download MD5 mismatch (got {computed}, want {expected_md5})"
                )
        (Path(volume_dir) / _RELEASE_ZIP).write_bytes(zip_bytes)

    def _read(ctx: BuildContext, v: str, volume_dir: str) -> Any:
        zip_bytes = (Path(volume_dir) / _RELEASE_ZIP).read_bytes()
        concept_member = _member_name(zip_bytes, snomed.CONCEPT_MEMBER_RE)
        concepts = snomed.parse_concepts(_extract_member(zip_bytes, snomed.CONCEPT_MEMBER_RE))
        descriptions = snomed.parse_descriptions(
            _extract_member(zip_bytes, snomed.DESCRIPTION_MEMBER_RE)
        )
        preferred_ids = snomed.parse_preferred_description_ids(
            _extract_member(zip_bytes, snomed.LANGUAGE_MEMBER_RE)
        )
        rows_obj = snomed.assemble_concepts(concepts, descriptions, preferred_ids)
        stash["rows_obj"] = rows_obj
        stash["concepts"] = concepts
        stash["descriptions"] = descriptions
        stash["preferred_ids"] = preferred_ids
        log.info("Assembled SNOMED concepts", extra={"version": v, "rows": len(rows_obj)})
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
                "snomed_version": v,
                "source_file": concept_member,
                "loaded_at": now,
            }
            for r in rows_obj
        ]
        return ctx.spark.createDataFrame(rows, SNOMED_SPARK_SCHEMA).sort("concept_id")

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

    def _promote(ctx: BuildContext, v: str) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {raw_fqn} WHERE snomed_version = '{v}'"
        ).sort("concept_id")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{TABLE}"
        where = f"snomed_version = '{snomed_version}'"
        rows_obj = stash["rows_obj"]
        concepts = stash["concepts"]
        descriptions = stash["descriptions"]
        preferred_ids = stash["preferred_ids"]
        total = len(rows_obj)
        n_active = sum(1 for r in rows_obj if r.active)
        failures: list[str] = []

        if expected_md5:
            _record(ctx, record_table, "snomed_download_checksum_matches", DQCategory.SCHEMA,
                    True, 0, total, {"expected": expected_md5})
        else:
            _record(ctx, record_table, "snomed_download_checksum_provided", DQCategory.SCHEMA,
                    False, 0, total,
                    {"note": "no --expected-md5 supplied; download integrity not verified"},
                    severity=DQSeverity.WARN)

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["concept_id", "snomed_version"],
                         check_name="snomed_concept_id_version_uniqueness", raise_on_fail=False):
            failures.append("duplicate (concept_id, snomed_version)")

        bad_sctid = snomed.find_invalid_sctids(rows_obj)
        _record(ctx, record_table, "snomed_concept_id_valid_sctid", DQCategory.BUSINESS_RULE,
                not bad_sctid, len(bad_sctid), total, {"sample": bad_sctid[:10]} if bad_sctid else None)
        if bad_sctid:
            failures.append(f"invalid SCTID: {bad_sctid[:5]}")

        missing_fsn = snomed.find_active_missing_fsn(rows_obj)
        _record(ctx, record_table, "snomed_active_fsn_not_null", DQCategory.NULLABILITY,
                not missing_fsn, len(missing_fsn), n_active,
                {"sample": missing_fsn[:10]} if missing_fsn else None)
        if missing_fsn:
            failures.append(f"active concept missing FSN: {missing_fsn[:5]}")

        bad_fsn_count = snomed.find_active_fsn_count_anomalies(concepts, descriptions)
        _record(ctx, record_table, "snomed_active_exactly_one_fsn", DQCategory.BUSINESS_RULE,
                not bad_fsn_count, len(bad_fsn_count), n_active,
                {"sample": bad_fsn_count[:10]} if bad_fsn_count else None)
        if bad_fsn_count:
            failures.append(f"active concept not exactly one FSN: {bad_fsn_count[:5]}")

        bad_pref_count = snomed.find_active_preferred_count_anomalies(
            concepts, descriptions, preferred_ids
        )
        _record(ctx, record_table, "snomed_active_exactly_one_preferred", DQCategory.BUSINESS_RULE,
                not bad_pref_count, len(bad_pref_count), n_active,
                {"sample": bad_pref_count[:10]} if bad_pref_count else None)
        if bad_pref_count:
            failures.append(f"active concept not exactly one preferred: {bad_pref_count[:5]}")

        # --- WARN / INFO ---
        card_ok = n_active >= snomed.ACTIVE_CONCEPT_MIN
        _record(ctx, record_table, "snomed_active_concept_cardinality", DQCategory.CARDINALITY,
                card_ok, 0 if card_ok else n_active, n_active,
                {"expected_min": snomed.ACTIVE_CONCEPT_MIN, "active": n_active, "total": total},
                severity=DQSeverity.WARN)
        missing_pref = snomed.find_active_missing_preferred(rows_obj)
        _record(ctx, record_table, "snomed_active_preferred_coverage", DQCategory.NULLABILITY,
                not missing_pref, len(missing_pref), n_active,
                {"sample": missing_pref[:10]} if missing_pref else None, severity=DQSeverity.WARN)
        _record(ctx, record_table, "snomed_semantic_tag_distribution", DQCategory.BUSINESS_RULE,
                True, 0, n_active,
                {"inactive_concepts": snomed.inactive_count(rows_obj),
                 "semantic_tags": snomed.semantic_tag_distribution(rows_obj)},
                severity=DQSeverity.INFO)
        unrecognized = snomed.find_active_unrecognized_tags(rows_obj)
        unrecognized_tags = sorted({tag for _, tag in unrecognized})
        _record(ctx, record_table, "snomed_active_tags_in_published_set", DQCategory.BUSINESS_RULE,
                not unrecognized, len(unrecognized), n_active,
                {"unrecognized_tags": unrecognized_tags[:25]} if unrecognized else None,
                severity=DQSeverity.WARN)

        if failures:
            raise ValueError("SNOMED blocking DQ failed -- " + "; ".join(failures))

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(snomed_version),
        vintage_column="snomed_version",
        raw_landings=[
            RawLanding(
                table=TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch,
                read_from_volume=_read,
                description=(
                    "Raw SNOMED CT US Edition RF2 Snapshot release, fetched-as-is per snomed_version "
                    "(immutable). Volume-landed verbatim, then parsed (concept + description + "
                    "language members). Promoted to codes.snomed."
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
                canonical_cluster_columns=["snomed_version", "concept_id"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(snomed_version,))
    log.info("SNOMED build complete", extra={"snomed_version": snomed_version})


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
        "--snomed-version", required=True,
        help="Release effective date, e.g. 20260301. Resolves the download URL via the UTS Release "
        "API and stamps the table.",
    )
    parser.add_argument(
        "--release-url", default=None,
        help="Override the NLM US Edition RF2 zip URL. If omitted, it's resolved from "
        "--snomed-version via the UTS Release API.",
    )
    parser.add_argument("--umls-secret-scope", required=True, help="Shared UMLS secret scope.")
    parser.add_argument("--umls-secret-key", default="umls_api_key")
    parser.add_argument(
        "--expected-md5", default=None, help="Published release MD5 to verify (blocking if set)."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.source_catalog,
        args.model_catalog,
        args.data_engineers_group,
        args.analysts_group,
        args.snomed_version,
        args.release_url,
        args.umls_secret_scope,
        umls_secret_key=args.umls_secret_key,
        expected_md5=args.expected_md5,
    )


if __name__ == "__main__":
    main()
