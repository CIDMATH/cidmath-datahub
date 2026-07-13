"""Build the canonical ``codes.rxnorm`` reference table on the shared builder (ADR 0037/0039).

NLM RxNorm normalized drug concepts (``SAB=RXNORM`` RxCUIs + term types) for conforming
drug/medication data to canonical RxCUIs. This entrypoint is the thin IO + Spark layer over the pure
logic in ``cidmath_datahub.reference.rxnorm`` (ADR 0011). **Flat** for v1 -- one row per concept; the
``RXNREL`` relationship graph and ``RXNSAT`` attributes (incl. the RxNorm<->NDC crosswalk) are
deferred (they reuse the same downloaded RRF).

**Source-path fold-in (ADR 0037 backport, wave 3).** Previously model-only + hand-rolled on
``run_build`` with no raw layer. Now folded onto the shared ``build_reference`` builder: the
authenticated RxNorm RRF release zip lands verbatim in the *source*-catalog landing Volume
``ecdh_<env>.codes_raw._landing`` (ADR 0039), parses into the 1:1 raw table ``ecdh_<env>.codes_raw
.rxnorm``, and the canonical ``ecdh_model_<env>.codes.rxnorm`` is promoted from raw. Same schema,
same rows -- a build-mechanism fold-in with data parity; consumers unaffected.

Versioning model (per-version, NOT ADR 0032). RxNorm ships monthly and the NLM archives every
release, so it is re-pullable: ``codes.rxnorm`` is keyed by ``rxnorm_version`` (the release
identifier, e.g. ``"04072025"``) -- ``vintage_column="rxnorm_version"`` with per-version atomic
``replaceWhere`` (ADR 0034; the builder's string-vintage support, ADR 0037 wave 2). Landed
``PER_VINTAGE_IMMUTABLE`` (a release is immutable; fetch-once, skip-if-present). The
``codes.rxnorm_current`` view is dropped (ADR 0034: "current" = ``MAX(rxnorm_version)``).

Access is the authenticated NLM/UTS download of the full RRF release (UMLS API key from the shared
``umls_secret_scope`` -- the same scope/key the SNOMED build uses; ADR 0012; no creds in code). The
raw landing Volume is engineer-only. The RxNorm vocabulary (``SAB=RXNORM``) is non-proprietary, so
the table registers ``access_tier="open"`` -- only the download is UMLS-gated. When ``--expected-md5``
is supplied it is verified at fetch (mismatch raises before the payload is cached).

Blocking DQ (FAIL, raises): ``(rxcui, rxnorm_version)`` uniqueness; non-null ``rxcui`` / ``name`` /
``tty`` / ``rxnorm_version``; and (when an expected MD5 is supplied) the download checksum. WARN:
cardinality, TTY recognition, TTY distribution.

Usage:
    build_rxnorm.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev \\
        --rxnorm-version 04072025 --umls-secret-scope ecdh-dev-umls \\
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
from cidmath_datahub.reference import rxnorm

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
TABLE = "rxnorm"
PIPELINE_REF = "bundles/_reference/src/build_rxnorm.py"

_RELEASE_ZIP = "rxnorm_full.zip"

#: UTS Release API -- lists each release (with its downloadUrl) for a release type, so the exact
#: NLM URL is resolved at runtime rather than hardcoded.
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

_DDL = (
    "rxcui STRING, name STRING, tty STRING, rxnorm_version STRING, source_file STRING, "
    "loaded_at TIMESTAMP"
)

_DESC = (
    "NLM RxNorm normalized drug concepts (SAB=RXNORM, SUPPRESS=N) from RXNCONSO.RRF: rxcui, "
    "normalized name, and term type (TTY: IN / SCD / SBD / BN / ...). Flat -- one row per RxCUI. "
    "PK (rxcui, rxnorm_version)."
)
_PHR = (
    "Canonical normalized drug terminology for conforming medication data to RxCUIs (e.g. 161 "
    "Acetaminophen); the basis for the deferred RxNorm<->NDC crosswalk."
)
_KNOWN_LIMITATIONS = (
    "SAB=RXNORM, SUPPRESS=N concepts only: one row per RxCUI with its normalized name + TTY. "
    "Relationships (RXNREL ingredient->drug graph), attributes (RXNSAT, incl. the RxNorm<->NDC "
    "crosswalk to codes.ndc), source-vocabulary atoms (SAB != RXNORM), and the synonym/atom grain "
    "are deferred (separate issues, reusing the same RRF). The RxNorm vocabulary is non-proprietary; "
    "only the UTS download is UMLS-account-gated."
)

_COMMON_META: dict[str, Any] = {
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
}


def _base_entry(version: str) -> registration.DatasetCatalogEntry:
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_DESC,
        public_health_relevance=_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        derived_from=[f"NLM RxNorm full RRF release {version}"],
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
    """Resolve the NLM download URL for an RxNorm ``version`` via the UTS Release API (no key)."""
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
    url = f"{rxnorm.UTS_DOWNLOAD_URL}?{query}"
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted NLM/UTS host
        raw = resp.read()
        content_type = resp.headers.get("Content-Type", "")
    # A real release is a zip ("PK"). A 200 with non-zip bytes means the UTS proxy returned an
    # error/HTML page (wrong release-url or unauthorized key).
    if raw[:2] != b"PK":
        preview = raw[:500].decode("utf-8", "replace")
        raise ValueError(
            f"UTS download did not return a zip ({len(raw)} bytes, Content-Type={content_type!r}). "
            f"Check the release URL is a valid NLM release and the UMLS key is authorized. "
            f"Response began: {preview!r}"
        )
    log.info("Downloaded RxNorm RRF release", extra={"release_url": release_url, "bytes": len(raw)})
    return raw


def _extract_conso(zip_bytes: bytes) -> tuple[str, str]:
    """Extract the full-release ``RXNCONSO.RRF`` text from the zip. Returns ``(text, member)``."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = rxnorm.select_conso_member(zf.namelist())
        raw = zf.read(member)
    return raw.decode(rxnorm.SOURCE_ENCODING), member.replace("\\", "/").split("/")[-1]


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the version, auth, and parsed stash).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    rxnorm_version: str,
    release_url: str | None = None,
    umls_secret_scope: str = "",
    umls_secret_key: str = "umls_api_key",
    expected_md5: str | None = None,
) -> None:
    if not umls_secret_scope:
        raise ValueError("--umls-secret-scope is required to pull the RxNorm RRF release")

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
                    f"RxNorm {v} download MD5 mismatch (got {computed}, want {expected_md5})"
                )
        (Path(volume_dir) / _RELEASE_ZIP).write_bytes(zip_bytes)

    def _read(ctx: BuildContext, v: str, volume_dir: str) -> Any:
        zip_bytes = (Path(volume_dir) / _RELEASE_ZIP).read_bytes()
        conso_text, conso_member = _extract_conso(zip_bytes)
        atoms = rxnorm.parse_rxnconso(conso_text)
        concepts = rxnorm.reduce_to_concepts(atoms)
        stash["concepts"] = concepts
        log.info("Parsed RxNorm concepts", extra={"version": v, "rows": len(concepts)})
        now = datetime.now(tz=UTC)
        rows = [
            {
                "rxcui": c.rxcui,
                "name": c.name,
                "tty": c.tty,
                "rxnorm_version": v,
                "source_file": conso_member,
                "loaded_at": now,
            }
            for c in concepts
        ]
        return ctx.spark.createDataFrame(rows, RXNORM_SPARK_SCHEMA).sort("rxcui")

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
        return ctx.spark.sql(f"SELECT * FROM {raw_fqn} WHERE rxnorm_version = '{v}'").sort("rxcui")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{TABLE}"
        where = f"rxnorm_version = '{rxnorm_version}'"
        concepts = stash["concepts"]
        total = len(concepts)
        failures: list[str] = []

        # Download checksum: verified at fetch when --expected-md5 was supplied (mismatch already
        # raised); otherwise surfaced as a WARN that integrity was not verified.
        if expected_md5:
            _record(ctx, record_table, "rxnorm_download_checksum_matches", DQCategory.SCHEMA,
                    True, 0, total, {"expected": expected_md5})
        else:
            _record(ctx, record_table, "rxnorm_download_checksum_provided", DQCategory.SCHEMA,
                    False, 0, total,
                    {"note": "no --expected-md5 supplied; download integrity not verified"},
                    severity=DQSeverity.WARN)

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["rxcui", "rxnorm_version"],
                         check_name="rxnorm_rxcui_version_uniqueness", raise_on_fail=False):
            failures.append("duplicate (rxcui, rxnorm_version)")

        miss = rxnorm.find_missing_fields(concepts)
        _record(ctx, record_table, "rxnorm_required_fields_not_null", DQCategory.NULLABILITY,
                not miss, len(miss), total,
                {"sample": [list(m) for m in miss[:10]]} if miss else None)
        if miss:
            failures.append(f"null required field: {miss[:5]}")

        # TTY recognition is a WARN, not a gate (the RxNorm term-type set can grow).
        bad_tty = rxnorm.find_bad_tty(concepts)
        _record(ctx, record_table, "rxnorm_tty_recognized", DQCategory.BUSINESS_RULE,
                not bad_tty, len(bad_tty), total,
                {"allowed": sorted(rxnorm.RXNORM_TTY_VALUES),
                 "sample": [list(b) for b in bad_tty[:10]]} if bad_tty else None,
                severity=DQSeverity.WARN)

        # --- WARN / INFO ---
        card_ok = total >= rxnorm.CARDINALITY_MIN
        _record(ctx, record_table, "rxnorm_cardinality", DQCategory.CARDINALITY,
                card_ok, 0 if card_ok else total, total,
                {"expected_min": rxnorm.CARDINALITY_MIN, "actual": total}, severity=DQSeverity.WARN)
        _record(ctx, record_table, "rxnorm_tty_distribution", DQCategory.BUSINESS_RULE,
                True, 0, total, {"distribution": rxnorm.tty_distribution(concepts)},
                severity=DQSeverity.INFO)

        if failures:
            raise ValueError("RxNorm blocking DQ failed -- " + "; ".join(failures))

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(rxnorm_version),
        vintage_column="rxnorm_version",
        raw_landings=[
            RawLanding(
                table=TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=_fetch,
                read_from_volume=_read,
                description=(
                    "Raw NLM RxNorm full RRF release (RXNCONSO.RRF), fetched-as-is per "
                    "rxnorm_version (immutable). Volume-landed verbatim, then parsed. Promoted to "
                    "codes.rxnorm."
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
                canonical_cluster_columns=["rxnorm_version", "rxcui"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(rxnorm_version,))
    log.info("RxNorm build complete", extra={"rxnorm_version": rxnorm_version})


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
        "--rxnorm-version", required=True,
        help="Release identifier, e.g. 04072025 (MMDDYYYY). Resolves the download URL via the UTS "
        "Release API and stamps the table.",
    )
    parser.add_argument(
        "--release-url", default=None,
        help="Override the NLM RxNorm full RRF zip URL. If omitted, it's resolved from "
        "--rxnorm-version via the UTS Release API.",
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
        args.rxnorm_version,
        args.release_url,
        args.umls_secret_scope,
        umls_secret_key=args.umls_secret_key,
        expected_md5=args.expected_md5,
    )


if __name__ == "__main__":
    main()
