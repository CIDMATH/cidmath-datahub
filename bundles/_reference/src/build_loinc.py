"""Build the canonical ``codes.loinc`` + ``codes.loinc_map_to`` tables on the shared builder.

LOINC is the canonical code set for lab tests, measurements, and clinical observations. This
entrypoint is the thin IO + Spark layer over the pure logic in ``cidmath_datahub.reference.loinc``
(ADR 0011). Two **flat** grains (the multi-axial hierarchy is deferred):

* ``codes.loinc`` -- the core term table (``LoincTableCore.csv``).
* ``codes.loinc_map_to`` -- the deprecated->replacement map (``MapTo.csv``).

**Source-path fold-in (ADR 0037 backport, wave 3).** Previously model-only + hand-rolled on
``run_build`` with no raw layer. Now folded onto the shared ``build_reference`` builder: the
authenticated LOINC release zip lands verbatim in the *source*-catalog landing Volume
``ecdh_<env>.codes_raw._landing`` (ADR 0039) and backs **both** raw landings
(``codes_raw.loinc`` + ``codes_raw.loinc_map_to``) via a shared ``volume_key`` -- fetched once,
parsed into each 1:1 raw table -- and the canonicals ``ecdh_model_<env>.codes.loinc{,_map_to}`` are
promoted from raw. Same schemas, same rows -- a build-mechanism fold-in with data parity; consumers
unaffected. The builder owns the per-version atomic ``replaceWhere``, ``_ops`` registration, grants.

Versioning model (per-version, NOT ADR 0032). LOINC ships discrete versions and the Download API
serves every past release, so it is vintage-reproducible: both tables are keyed by ``loinc_version``
(the release string, e.g. ``"2.82"``) -- ``vintage_column="loinc_version"`` with per-version atomic
``replaceWhere`` (ADR 0034; the builder's string-vintage support, ADR 0037 wave 2). Landed
``PER_VINTAGE_IMMUTABLE`` (a release is immutable; fetch-once, skip-if-present). The
``codes.loinc_current`` view is dropped (ADR 0034: "current" = ``MAX(loinc_version)``).

Access is the authenticated LOINC Download API (HTTP Basic, username/password from a Databricks
secret scope; ADR 0012). Credentials never appear in code; the raw landing Volume is engineer-only.
LOINC is licensed (Regenstrief): both tables register ``access_tier="restricted"`` /
``dua_required=True``. The download's MD5 is verified against the API's ``downloadMD5Hash`` at fetch
time (a corrupt download raises before the payload is marked complete, so it is never cached).

Blocking DQ (FAIL, raises): download MD5 == the API hash; ``codes.loinc`` PK uniqueness, non-null
``loinc_num``/``long_common_name``/``status``, ``status`` in vocab; ``codes.loinc_map_to`` PK
uniqueness + non-null keys. WARN: row count == ``numberOfLoincs``, name-axis coverage, status
distribution, the map target FK, and that a mapped code is retired.

Usage:
    build_loinc.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --loinc-version 2.82 \\
        --loinc-secret-scope ecdh-dev-loinc \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime
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
from cidmath_datahub.reference import loinc

log = get_logger(__name__)

SUBJECT = "codes"
RAW_SCHEMA = "codes_raw"
MODEL_SCHEMA = "codes"
CORE_TABLE = "loinc"
MAP_TABLE = "loinc_map_to"
PIPELINE_REF = "bundles/_reference/src/build_loinc.py"

# Both raw tables share ONE release-zip payload (shared volume_key -> fetched once, read twice).
_VOLUME_KEY = "loinc_release"
_RELEASE_ZIP = "loinc_release.zip"
_META_JSON = "release_meta.json"

CORE_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("loinc_num", T.StringType(), False),
        T.StructField("component", T.StringType(), True),
        T.StructField("property", T.StringType(), True),
        T.StructField("time_aspct", T.StringType(), True),
        T.StructField("system", T.StringType(), True),
        T.StructField("scale_typ", T.StringType(), True),
        T.StructField("method_typ", T.StringType(), True),
        T.StructField("loinc_class", T.StringType(), True),
        T.StructField("classtype", T.StringType(), True),
        T.StructField("long_common_name", T.StringType(), False),
        T.StructField("shortname", T.StringType(), True),
        T.StructField("external_copyright_notice", T.StringType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("version_first_released", T.StringType(), True),
        T.StructField("version_last_changed", T.StringType(), True),
        T.StructField("loinc_version", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)

MAP_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("loinc_num", T.StringType(), False),
        T.StructField("map_to_loinc_num", T.StringType(), False),
        T.StructField("comment", T.StringType(), True),
        T.StructField("loinc_version", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("loaded_at", T.TimestampType(), False),
    ]
)

_CORE_DDL = (
    "loinc_num STRING, component STRING, property STRING, time_aspct STRING, system STRING, "
    "scale_typ STRING, method_typ STRING, loinc_class STRING, classtype STRING, "
    "long_common_name STRING, shortname STRING, external_copyright_notice STRING, status STRING, "
    "version_first_released STRING, version_last_changed STRING, loinc_version STRING, "
    "source_file STRING, loaded_at TIMESTAMP"
)
_MAP_DDL = (
    "loinc_num STRING, map_to_loinc_num STRING, comment STRING, loinc_version STRING, "
    "source_file STRING, loaded_at TIMESTAMP"
)

_CORE_DESC = (
    "LOINC core term table (lab tests, measurements, clinical observations) from LoincTableCore.csv: "
    "loinc_num, the six name axes (component/property/time/system/scale/method), CLASS/CLASSTYPE, "
    "long common name, status, and version metadata. Flat (multi-axial hierarchy deferred). PK "
    "(loinc_num, loinc_version)."
)
_MAP_DESC = (
    "LOINC deprecated->replacement map (MapTo.csv): when LOINC retires a term it publishes the "
    "successor here, so retired codes can be remapped to current ones. A deprecated code may map "
    "to several replacements, so PK (loinc_num, map_to_loinc_num, loinc_version); FK loinc_num and "
    "map_to_loinc_num -> codes.loinc (same version)."
)
_CORE_PHR = (
    "Canonical standard for lab/observation data; lets results feeds conform to shared, versioned "
    "LOINC codes (e.g. 2160-0 Creatinine [Mass/volume] in Serum or Plasma)."
)
_MAP_PHR = (
    "Lets conformance remap retired LOINC codes in historical lab data to their current replacement "
    "instead of dropping them."
)
_LICENSE = (
    "LOINC license (Regenstrief Institute): free with registration; attribution required, "
    "redistribution restricted. See LoincLicense_*.txt in the release and https://loinc.org/license/."
)
_DUA_REFERENCE = (
    "LOINC license (https://loinc.org/license/) + the LoincLicense_*.txt file in the release zip. "
    "Internal conformance use only; no external redistribution / Delta-share without a license check."
)
_KNOWN_LIMITATIONS = (
    "Core term table (LoincTableCore.csv) + MapTo only; the multi-axial hierarchy, Part ontology, "
    "answer lists, LOINC Groups, translations, panels/forms, and the full Loinc.csv columns are "
    "deferred (separate issues). Versioned + archived (re-pullable per loinc_version via the "
    "Download API; not ADR 0032). Licensed (Regenstrief): internal conformance use only -- no "
    "external redistribution / Delta-share without a license check. Some terms carry third-party "
    "copyright (external_copyright_notice); the restricted access tier covers them."
)

_COMMON_META: dict[str, Any] = {
    "spatial_resolution": "none",
    "spatial_coverage": "Global",
    "source_provider_code": "loinc",
    "source_url": loinc.SOURCE_LANDING_URL,
    "source_documentation_url": loinc.SOURCE_DOCUMENTATION_URL,
    "source_data_dictionary_url": loinc.SOURCE_DATA_DICTIONARY_URL,
    "license": _LICENSE,
    "dua_required": True,
    "dua_reference": _DUA_REFERENCE,
    "access_tier": "restricted",
    "external_maintainer_name": "Regenstrief Institute, Inc.",
    "is_hosted": True,
}


def _base_entry(version: str) -> registration.DatasetCatalogEntry:
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=_CORE_DESC,  # per-output description overrides this for each table
        public_health_relevance=_CORE_PHR,
        known_limitations=_KNOWN_LIMITATIONS,
        derived_from=[f"{loinc.API_BASE_URL}/Loinc/Download?version={version}"],
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: secret-scoped HTTP Basic auth + LOINC Download API (kept out of the pure module, ADR 0011).
# ---------------------------------------------------------------------------


def _get_secret(scope: str, key: str) -> str:
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - depends on runtime flavor
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(SparkSession.builder.getOrCreate())
    return dbutils.secrets.get(scope=scope, key=key)


def _basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    return f"Basic {token}"


def _http_get(url: str, *, auth_header: str | None = None, accept: str | None = None) -> bytes:
    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header
    if accept:
        headers["Accept"] = accept
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:  # nosec B310 - trusted LOINC host
        return resp.read()


def _extract_text(zip_bytes: bytes, basename: str) -> str:
    """Extract a member's text from the zip by basename (case-insensitive), per source encoding."""
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        matches = [n for n in zf.namelist() if n.split("/")[-1].lower() == basename.lower()]
        if not matches:
            raise ValueError(f"{basename} not found in LOINC release zip; members={zf.namelist()}")
        raw = zf.read(matches[0])
    return raw.decode(loinc.SOURCE_ENCODING)


# ---------------------------------------------------------------------------
# Orchestration (fetch/read/promote/validate close over the version, auth, and parsed stash).
# ---------------------------------------------------------------------------


def run(
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    loinc_version: str,
    loinc_secret_scope: str,
    loinc_username_key: str = "loinc_username",
    loinc_password_key: str = "loinc_password",
    api_base: str = loinc.API_BASE_URL,
) -> None:
    if not loinc_secret_scope:
        raise ValueError("--loinc-secret-scope is required to pull the licensed LOINC release")

    auth_header = _basic_auth_header(
        _get_secret(loinc_secret_scope, loinc_username_key),
        _get_secret(loinc_secret_scope, loinc_password_key),
    )
    core_raw = f"{source_catalog}.{RAW_SCHEMA}.{CORE_TABLE}"
    map_raw = f"{source_catalog}.{RAW_SCHEMA}.{MAP_TABLE}"
    # Parsed records (with parse-time-only *_raw fields) + release meta stashed by read for validate.
    stash: dict[str, Any] = {}

    def _fetch(v: str, volume_dir: str) -> None:
        # Authenticated: GET metadata (downloadUrl + MD5 + count), download the release, verify the
        # MD5, then land the zip + a small meta.json. Raising on MD5 mismatch means the completion
        # marker is not written, so a corrupt download is retried next run (never cached).
        meta_url = f"{api_base}/Loinc?version={urllib.parse.quote(v)}"
        meta = json.loads(_http_get(meta_url, auth_header=auth_header, accept="application/json"))
        download_url = meta.get("downloadUrl") or (
            f"{api_base}/Loinc/Download?version={urllib.parse.quote(v)}"
        )
        zip_bytes = _http_get(download_url, auth_header=auth_header)
        expected_md5 = str(meta.get("downloadMD5Hash") or "")
        computed_md5 = hashlib.md5(zip_bytes).hexdigest()  # nosec B324 - integrity, not security
        if not (expected_md5 and computed_md5.lower() == expected_md5.lower()):
            raise ValueError(
                f"LOINC {v} download MD5 mismatch (got {computed_md5}, want {expected_md5!r})"
            )
        (Path(volume_dir) / _RELEASE_ZIP).write_bytes(zip_bytes)
        (Path(volume_dir) / _META_JSON).write_text(
            json.dumps(
                {
                    "downloadMD5Hash": expected_md5,
                    "numberOfLoincs": meta.get("numberOfLoincs"),
                    "releaseDate": meta.get("releaseDate"),
                },
            ),
            encoding="utf-8",
        )
        log.info("Fetched LOINC release", extra={"version": v, "release_date": meta.get("releaseDate")})

    def _read_meta(volume_dir: str) -> dict[str, Any]:
        return json.loads((Path(volume_dir) / _META_JSON).read_text(encoding="utf-8"))

    def _source_file(v: str, meta: dict[str, Any]) -> str:
        return f"LOINC {v} ({meta.get('releaseDate', 'release')})"

    def _read_core(ctx: BuildContext, v: str, volume_dir: str) -> Any:
        zip_bytes = (Path(volume_dir) / _RELEASE_ZIP).read_bytes()
        meta = _read_meta(volume_dir)
        terms = loinc.parse_loinc_core(_extract_text(zip_bytes, loinc.CORE_MEMBER))
        stash["terms"] = terms
        stash["meta"] = meta
        log.info("Parsed LOINC core", extra={"version": v, "rows": len(terms)})
        source_file = _source_file(v, meta)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "loinc_num": t.loinc_num,
                "component": t.component,
                "property": t.property,
                "time_aspct": t.time_aspct,
                "system": t.system,
                "scale_typ": t.scale_typ,
                "method_typ": t.method_typ,
                "loinc_class": t.loinc_class,
                "classtype": t.classtype,
                "long_common_name": t.long_common_name,
                "shortname": t.shortname,
                "external_copyright_notice": t.external_copyright_notice,
                "status": t.status,
                "version_first_released": t.version_first_released,
                "version_last_changed": t.version_last_changed,
                "loinc_version": v,
                "source_file": source_file,
                "loaded_at": now,
            }
            for t in terms
        ]
        return ctx.spark.createDataFrame(rows, CORE_SPARK_SCHEMA).sort("loinc_num")

    def _read_map(ctx: BuildContext, v: str, volume_dir: str) -> Any:
        zip_bytes = (Path(volume_dir) / _RELEASE_ZIP).read_bytes()
        meta = _read_meta(volume_dir)
        maps = loinc.parse_map_to(_extract_text(zip_bytes, loinc.MAP_TO_MEMBER))
        stash["maps"] = maps
        log.info("Parsed LOINC MapTo", extra={"version": v, "rows": len(maps)})
        source_file = _source_file(v, meta)
        now = datetime.now(tz=UTC)
        rows = [
            {
                "loinc_num": m.loinc_num,
                "map_to_loinc_num": m.map_to_loinc_num,
                "comment": m.comment,
                "loinc_version": v,
                "source_file": source_file,
                "loaded_at": now,
            }
            for m in maps
        ]
        return ctx.spark.createDataFrame(rows, MAP_SPARK_SCHEMA).sort("loinc_num")

    def _ensure_staging(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings for the codes subject (clinical/"
            f"terminology code systems). Engineer-owned; canonicals promote to model codes. ADR 0037.'"
        )
        sp.sql(f"CREATE TABLE IF NOT EXISTS {core_raw} ({_CORE_DDL}) USING DELTA")
        sp.sql(f"CREATE TABLE IF NOT EXISTS {map_raw} ({_MAP_DDL}) USING DELTA")

    def _ensure_canonical(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, NDC, "
            f"LOINC, SNOMED CT, RxNorm, ...). Owned by the _reference bundle. See ADR 0014.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{CORE_TABLE} "
            f"({_CORE_DDL}) USING DELTA"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{MAP_TABLE} "
            f"({_MAP_DDL}) USING DELTA"
        )

    def _promote_core(ctx: BuildContext, v: str) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {core_raw} WHERE loinc_version = '{v}'"
        ).sort("loinc_num")

    def _promote_map(ctx: BuildContext, v: str) -> Any:
        return ctx.spark.sql(
            f"SELECT * FROM {map_raw} WHERE loinc_version = '{v}'"
        ).sort("loinc_num")

    def _validate_core(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{CORE_TABLE}"
        where = f"loinc_version = '{loinc_version}'"
        terms = stash["terms"]
        meta = stash.get("meta", {})
        n = len(terms)
        failures: list[str] = []

        # MD5 was verified at fetch (a mismatch raises before the payload is cached); record the pass.
        _record(ctx, record_table, "loinc_download_md5_matches", DQCategory.SCHEMA,
                True, 0, n, {"downloadMD5Hash": meta.get("downloadMD5Hash")})

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["loinc_num", "loinc_version"],
                         check_name="loinc_num_version_uniqueness", raise_on_fail=False):
            failures.append("duplicate (loinc_num, loinc_version)")

        miss = loinc.find_missing_term_fields(terms)
        _record(ctx, record_table, "loinc_required_fields_not_null", DQCategory.NULLABILITY,
                not miss, len(miss), n, {"sample": [list(m) for m in miss[:10]]} if miss else None)
        if miss:
            failures.append(f"null core field: {miss[:5]}")

        bad_status = loinc.find_status_violations(terms)
        _record(ctx, record_table, "loinc_status_controlled_vocab", DQCategory.BUSINESS_RULE,
                not bad_status, len(bad_status), n,
                {"allowed": sorted(loinc.LOINC_STATUS_VALUES),
                 "sample": [list(b) for b in bad_status[:10]]} if bad_status else None)
        if bad_status:
            failures.append(f"status out of vocab: {bad_status[:5]}")

        # --- WARN / INFO ---
        number_of_loincs = meta.get("numberOfLoincs")
        if number_of_loincs is not None:
            count_ok = n == number_of_loincs
            _record(ctx, record_table, "loinc_row_count_matches_api", DQCategory.CARDINALITY,
                    count_ok, 0 if count_ok else abs(n - number_of_loincs), n,
                    {"api_number_of_loincs": number_of_loincs, "parsed_rows": n},
                    severity=DQSeverity.WARN)
        bad_axes = loinc.find_missing_name_axes(terms)
        _record(ctx, record_table, "loinc_name_axes_populated", DQCategory.NULLABILITY,
                not bad_axes, len(bad_axes), n, {"sample": bad_axes[:10]} if bad_axes else None,
                severity=DQSeverity.WARN)
        _record(ctx, record_table, "loinc_status_distribution", DQCategory.BUSINESS_RULE,
                True, 0, n, {"distribution": loinc.status_distribution(terms)},
                severity=DQSeverity.INFO)

        if failures:
            raise ValueError("LOINC core blocking DQ failed -- " + "; ".join(failures))

    def _validate_map(ctx: BuildContext, staging_fqn: str) -> None:
        record_table = f"{MODEL_SCHEMA}.{MAP_TABLE}"
        where = f"loinc_version = '{loinc_version}'"
        maps = stash["maps"]
        terms = stash.get("terms", [])
        n = len(maps)
        failures: list[str] = []

        # A deprecated LOINC can map to several replacements, so the PK is the (deprecated,
        # replacement) pair per version -- (loinc_num, map_to_loinc_num, loinc_version), matching
        # loinc.find_duplicate_map_keys -- not loinc_num alone.
        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table, where=where)
        if not dq.unique(keys=["loinc_num", "map_to_loinc_num", "loinc_version"],
                         check_name="loinc_map_to_key_uniqueness", raise_on_fail=False):
            failures.append("duplicate (loinc_num, map_to_loinc_num, loinc_version)")

        miss = loinc.find_missing_map_fields(maps)
        _record(ctx, record_table, "loinc_map_to_required_fields_not_null", DQCategory.NULLABILITY,
                not miss, len(miss), n, {"sample": [list(m) for m in miss[:10]]} if miss else None)
        if miss:
            failures.append(f"null map field: {miss[:5]}")

        # --- WARN ---
        # MapTo -> core FK is INFORMATIONAL (ADR 0014): a replacement can point at a code absent from
        # this release's core file (chained deprecations), so orphans are recorded and kept.
        term_nums = {t.loinc_num for t in terms}
        orphans = loinc.find_map_target_orphans(maps, term_nums)
        _record(ctx, record_table, "loinc_map_to_target_fk", DQCategory.REFERENTIAL,
                not orphans, len(orphans), n,
                {"orphan_count": len(orphans), "sample": [list(o) for o in orphans[:10]]}
                if orphans else None, severity=DQSeverity.WARN)
        status_by = {t.loinc_num: t.status for t in terms}
        not_retired = loinc.find_map_source_not_retired(maps, status_by)
        _record(ctx, record_table, "loinc_map_to_source_is_retired", DQCategory.BUSINESS_RULE,
                not not_retired, len(not_retired), n,
                {"sample": [list(x) for x in not_retired[:10]]} if not_retired else None,
                severity=DQSeverity.WARN)

        if failures:
            raise ValueError("LOINC map blocking DQ failed -- " + "; ".join(failures))

    def _landing(table: str, read_fn: Any, desc: str) -> RawLanding:
        return RawLanding(
            table=table,
            landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
            volume_key=_VOLUME_KEY,
            fetch_to_volume=_fetch,
            read_from_volume=read_fn,
            description=desc,
        )

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(loinc_version),
        vintage_column="loinc_version",
        raw_landings=[
            _landing(CORE_TABLE, _read_core,
                     "Raw LOINC LoincTableCore.csv, fetched-as-is per loinc_version (immutable). "
                     "Shares the release-zip landing with loinc_map_to. Promoted to codes.loinc."),
            _landing(MAP_TABLE, _read_map,
                     "Raw LOINC MapTo.csv (deprecated->replacement), fetched-as-is per loinc_version. "
                     "Shares the release-zip landing with loinc. Promoted to codes.loinc_map_to."),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table=CORE_TABLE, reads=(CORE_TABLE,), promote=_promote_core,
                validate_staging=_validate_core, description=_CORE_DESC,
                public_health_relevance=_CORE_PHR,
                canonical_cluster_columns=["loinc_version", "loinc_num"],
            ),
            CanonicalOutput(
                canonical_table=MAP_TABLE, reads=(MAP_TABLE,), promote=_promote_map,
                validate_staging=_validate_map, description=_MAP_DESC,
                public_health_relevance=_MAP_PHR,
                canonical_cluster_columns=["loinc_version", "loinc_num"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=(loinc_version,))
    log.info("LOINC build complete", extra={"loinc_version": loinc_version})


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
        "--loinc-version", default="2.82",
        help="LOINC release to load (explicit + reproducible). Default: 2.82.",
    )
    parser.add_argument("--loinc-secret-scope", required=True, help="Secret scope with LOINC creds.")
    parser.add_argument("--loinc-username-key", default="loinc_username")
    parser.add_argument("--loinc-password-key", default="loinc_password")
    parser.add_argument("--api-base", default=loinc.API_BASE_URL)
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.source_catalog,
        args.model_catalog,
        args.data_engineers_group,
        args.analysts_group,
        args.loinc_version,
        args.loinc_secret_scope,
        loinc_username_key=args.loinc_username_key,
        loinc_password_key=args.loinc_password_key,
        api_base=args.api_base,
    )


if __name__ == "__main__":
    main()
