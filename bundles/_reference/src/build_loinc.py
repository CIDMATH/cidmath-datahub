"""Build the canonical ``codes.loinc`` + ``codes.loinc_map_to`` tables (ADR 0014).

LOINC is the canonical code set for lab tests, measurements, and clinical
observations. This entrypoint is the thin IO + Spark layer over the pure logic in
``cidmath_datahub.reference.loinc`` (ADR 0011). Two **flat** grains (the multi-axial
hierarchy is deferred):

* ``codes.loinc`` -- the core term table (``LoincTableCore.csv``).
* ``codes.loinc_map_to`` -- the deprecated->replacement map (``MapTo.csv``), so
  retired codes are remapped to their successor rather than dropped.

Versioning model (ICD-10 per-version, NOT ADR 0032). LOINC ships discrete versions
and the Download API serves every past release, so it is vintage-reproducible: both
tables are keyed by ``loinc_version`` (the release string, e.g. ``"2.82"``) with
``snapshot_replace`` -- replace this version's rows, retain others (the geography
per-vintage / ICD-10 per-edition pattern, ADR 0024). No history-snapshot machinery.

Access is the authenticated LOINC Download API (HTTP Basic, username/password from a
Databricks secret scope -- the same secret-scoped pattern as IPUMS NHGIS; ADR 0012;
no new ADR). Credentials never appear in code. LOINC is licensed (Regenstrief): free
but attribution-required and redistribution-restricted, so both tables register
``access_tier="restricted"`` / ``dua_required=True`` (the NHGIS licensed-source
pattern).

Thin entrypoint over the ``run_build`` seam (ADR 0027). Blocking DQ (FAIL, raises):
download MD5 == the API's ``downloadMD5Hash``; ``codes.loinc`` PK uniqueness,
non-null ``loinc_num``/``long_common_name``/``status``, ``status`` in the controlled
vocab; ``codes.loinc_map_to`` PK uniqueness, non-null keys, and the
``map_to_loinc_num`` -> ``codes.loinc`` FK (same version). WARN: row count ==
``numberOfLoincs``, name-axis coverage, status distribution, and that a mapped
(deprecated) code is actually retired in the core table.

Usage:
    build_loinc.py --catalog ecdh_model_dev --loinc-version 2.82 \\
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
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import loinc

log = get_logger(__name__)

SCHEMA = "codes"
CORE_TABLE = "loinc"
MAP_TABLE = "loinc_map_to"
CORE_VIEW = "loinc_current"
PIPELINE_REF = "bundles/_reference/src/build_loinc.py"

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


# ---------------------------------------------------------------------------
# IO: secret-scoped HTTP Basic auth + LOINC Download API (kept out of the pure
# module per ADR 0011; mirrors build_crosswalk.py's _get_secret -> auth -> fetch)
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
    """GET ``url`` and return the raw body (auth/accept headers optional)."""
    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header
    if accept:
        headers["Accept"] = accept
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:  # nosec B310 - trusted LOINC host
        return resp.read()


def _release_metadata(api_base: str, version: str, auth_header: str) -> dict[str, Any]:
    """``GET /Loinc?version=<v>`` -> release metadata (numberOfLoincs, MD5, downloadUrl)."""
    url = f"{api_base}/Loinc?version={urllib.parse.quote(version)}"
    meta = json.loads(_http_get(url, auth_header=auth_header, accept="application/json"))
    log.info(
        "LOINC release metadata",
        extra={
            "version": meta.get("version"),
            "release_date": meta.get("releaseDate"),
            "number_of_loincs": meta.get("numberOfLoincs"),
        },
    )
    return meta


def _download_release(api_base: str, version: str, meta: dict[str, Any], auth_header: str) -> bytes:
    """Download the release zip via the authenticated Download API.

    Both the metadata's ``downloadUrl`` and the explicit ``GET /Loinc/Download?version=<v>``
    endpoint require the same HTTP Basic auth as the metadata call (a request without it
    returns 401), so the auth header is sent on the download too. ``downloadUrl`` is
    preferred when present; otherwise the explicit endpoint is used.
    """
    download_url = meta.get("downloadUrl") or (
        f"{api_base}/Loinc/Download?version={urllib.parse.quote(version)}"
    )
    return _http_get(download_url, auth_header=auth_header)


def _extract_text(zip_bytes: bytes, basename: str) -> str:
    """Extract a member's text from the zip by basename (case-insensitive), utf-8-sig."""
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        matches = [n for n in zf.namelist() if n.split("/")[-1].lower() == basename.lower()]
        if not matches:
            raise ValueError(f"{basename} not found in LOINC release zip; members={zf.namelist()}")
        raw = zf.read(matches[0])
    return raw.decode(loinc.SOURCE_ENCODING)


# ---------------------------------------------------------------------------
# DQ (ADR 0009): MD5 + blocking uniqueness/non-null/status/FK; WARN rest
# ---------------------------------------------------------------------------


def _dq_checks(
    ctx: BuildContext,
    terms: list[loinc.LoincTerm],
    maps: list[loinc.LoincMapTo],
    version: str,
    *,
    md5_ok: bool,
    computed_md5: str,
    expected_md5: str,
    number_of_loincs: int | None,
) -> None:
    """Record DQ; raise on any blocking FAIL so a bad table never writes."""
    c_table, m_table = f"{SCHEMA}.{CORE_TABLE}", f"{SCHEMA}.{MAP_TABLE}"
    n_terms, n_maps = len(terms), len(maps)

    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_download_md5_matches",
        category=DQCategory.SCHEMA,
        severity=DQSeverity.FAIL,
        passed=md5_ok,
        failing_row_count=0 if md5_ok else 1,
        total_row_count=n_terms,
        details=None if md5_ok else {"computed": computed_md5, "expected": expected_md5},
    )

    dup = loinc.find_duplicate_loinc_nums(terms)
    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_num_version_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup,
        failing_row_count=len(dup),
        total_row_count=n_terms,
        details={"sample": dup[:10]} if dup else None,
    )

    miss = loinc.find_missing_term_fields(terms)
    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not miss,
        failing_row_count=len(miss),
        total_row_count=n_terms,
        details={"sample": [list(m) for m in miss[:10]]} if miss else None,
    )

    bad_status = loinc.find_status_violations(terms)
    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_status_controlled_vocab",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.FAIL,
        passed=not bad_status,
        failing_row_count=len(bad_status),
        total_row_count=n_terms,
        details={
            "allowed": sorted(loinc.LOINC_STATUS_VALUES),
            "sample": [list(b) for b in bad_status[:10]],
        }
        if bad_status
        else None,
    )

    dup_map = loinc.find_duplicate_map_keys(maps)
    ctx.recorder.record(
        table_name=m_table,
        check_name="loinc_map_to_key_uniqueness",
        category=DQCategory.UNIQUENESS,
        severity=DQSeverity.FAIL,
        passed=not dup_map,
        failing_row_count=len(dup_map),
        total_row_count=n_maps,
        details={"sample": [list(k) for k in dup_map[:10]]} if dup_map else None,
    )

    miss_map = loinc.find_missing_map_fields(maps)
    ctx.recorder.record(
        table_name=m_table,
        check_name="loinc_map_to_required_fields_not_null",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.FAIL,
        passed=not miss_map,
        failing_row_count=len(miss_map),
        total_row_count=n_maps,
        details={"sample": [list(m) for m in miss_map[:10]]} if miss_map else None,
    )

    # --- WARN checks ---
    # MapTo -> core FK is INFORMATIONAL (ADR 0014): a replacement can point at a code
    # absent from this release's core file (chained/multi-step deprecations), so
    # orphans are recorded and the rows kept, not treated as build-blocking.
    term_nums = {t.loinc_num for t in terms}
    orphans = loinc.find_map_target_orphans(maps, term_nums)
    ctx.recorder.record(
        table_name=m_table,
        check_name="loinc_map_to_target_fk",
        category=DQCategory.REFERENTIAL,
        severity=DQSeverity.WARN,
        passed=not orphans,
        failing_row_count=len(orphans),
        total_row_count=n_maps,
        details={"orphan_count": len(orphans), "sample": [list(o) for o in orphans[:10]]}
        if orphans
        else None,
    )
    if number_of_loincs is not None:
        count_ok = n_terms == number_of_loincs
        ctx.recorder.record(
            table_name=c_table,
            check_name="loinc_row_count_matches_api",
            category=DQCategory.CARDINALITY,
            severity=DQSeverity.WARN,
            passed=count_ok,
            failing_row_count=0 if count_ok else abs(n_terms - number_of_loincs),
            total_row_count=n_terms,
            details={"api_number_of_loincs": number_of_loincs, "parsed_rows": n_terms},
        )

    bad_axes = loinc.find_missing_name_axes(terms)
    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_name_axes_populated",
        category=DQCategory.NULLABILITY,
        severity=DQSeverity.WARN,
        passed=not bad_axes,
        failing_row_count=len(bad_axes),
        total_row_count=n_terms,
        details={"sample": bad_axes[:10]} if bad_axes else None,
    )

    status_by = {t.loinc_num: t.status for t in terms}
    not_retired = loinc.find_map_source_not_retired(maps, status_by)
    ctx.recorder.record(
        table_name=m_table,
        check_name="loinc_map_to_source_is_retired",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.WARN,
        passed=not not_retired,
        failing_row_count=len(not_retired),
        total_row_count=n_maps,
        details={"sample": [list(x) for x in not_retired[:10]]} if not_retired else None,
    )

    ctx.recorder.record(
        table_name=c_table,
        check_name="loinc_status_distribution",
        category=DQCategory.BUSINESS_RULE,
        severity=DQSeverity.INFO,
        passed=True,
        total_row_count=n_terms,
        details={"distribution": loinc.status_distribution(terms)},
    )

    failures: list[str] = []
    if not md5_ok:
        failures.append(f"download MD5 mismatch (got {computed_md5}, want {expected_md5})")
    if dup:
        failures.append(f"duplicate loinc_num: {dup[:5]}")
    if miss:
        failures.append(f"null core field: {miss[:5]}")
    if bad_status:
        failures.append(f"status out of vocab: {bad_status[:5]}")
    if dup_map:
        failures.append(f"duplicate map key: {dup_map[:5]}")
    if miss_map:
        failures.append(f"null map field: {miss_map[:5]}")
    if failures:
        raise ValueError("LOINC blocking DQ failed -- " + "; ".join(failures))


# ---------------------------------------------------------------------------
# Write (snapshot_replace by loinc_version; ADR 0024)
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
    version: str,
    sort_cols: list[str],
) -> None:
    """snapshot_replace: replace only this run's ``loinc_version`` rows; keep priors."""
    df = spark.createDataFrame(rows, schema=schema).sort(*sort_cols)
    if _table_has_column(spark, full, "loinc_version"):
        spark.sql(f"DELETE FROM {full} WHERE loinc_version = '{version}'")
        df.write.option("mergeSchema", "true").mode("append").saveAsTable(full)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    log.info("Wrote table", extra={"table": full, "rows": len(rows), "loinc_version": version})


def _create_current_view(spark: SparkSession, catalog: str) -> None:
    full = f"{catalog}.{SCHEMA}.{CORE_TABLE}"
    view = f"{catalog}.{SCHEMA}.{CORE_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {full} WHERE loinc_version = "
        f"(SELECT MAX(loinc_version) FROM {full})"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'codes.loinc restricted to the latest loinc_version (the current LOINC release).'"
    )


# ---------------------------------------------------------------------------
# Register (_ops metadata, ADR 0008) -- licensed/restricted (NHGIS-style)
# ---------------------------------------------------------------------------

_LICENSE = (
    "LOINC license (Regenstrief Institute): free with registration; attribution "
    "required, redistribution restricted. See LoincLicense_*.txt in the release and "
    "https://loinc.org/license/."
)
_DUA_REFERENCE = (
    "LOINC license (https://loinc.org/license/) + the LoincLicense_*.txt file in the "
    "release zip. Internal conformance use only; no external redistribution / "
    "Delta-share without a license check."
)
_KNOWN_LIMITATIONS = (
    "Core term table (LoincTableCore.csv) + MapTo only; the multi-axial hierarchy, Part "
    "ontology, answer lists, LOINC Groups, translations, panels/forms, and the full "
    "Loinc.csv columns are deferred (separate issues). Versioned + archived (re-pullable "
    "per loinc_version via the Download API; snapshot_replace, not ADR 0032). Licensed "
    "(Regenstrief): internal conformance use only -- no external redistribution / "
    "Delta-share without a license check. Some terms carry third-party copyright "
    "(external_copyright_notice); the restricted access tier covers them."
)


def _register(spark: SparkSession, catalog: str, version: str, *, create_view: bool) -> None:
    g = f"{catalog}.{SCHEMA}"
    derived = [f"{loinc.API_BASE_URL}/Loinc/Download?version={version}"]
    common = {
        "subject": SCHEMA,
        "layer": "reference",
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
        "known_limitations": _KNOWN_LIMITATIONS,
        "derived_from": derived,
    }

    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{CORE_TABLE}",
            description=(
                "LOINC core term table (lab tests, measurements, clinical observations) from "
                "LoincTableCore.csv: loinc_num, the six name axes (component/property/time/"
                "system/scale/method), CLASS/CLASSTYPE, long common name, status, and version "
                "metadata. Flat (multi-axial hierarchy deferred). PK (loinc_num, loinc_version)."
            ),
            public_health_relevance=(
                "Canonical standard for lab/observation data; lets results feeds conform to "
                "shared, versioned LOINC codes (e.g. 2160-0 Creatinine [Mass/volume] in Serum "
                "or Plasma)."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{CORE_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["loinc_version", "loinc_num"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    registration.register_dataset(
        spark,
        catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{MAP_TABLE}",
            description=(
                "LOINC deprecated->replacement map (MapTo.csv): when LOINC retires a term it "
                "publishes the successor here, so retired codes can be remapped to current ones. "
                "PK (loinc_num, loinc_version); FK loinc_num and map_to_loinc_num -> codes.loinc "
                "(same version)."
            ),
            public_health_relevance=(
                "Lets conformance remap retired LOINC codes in historical lab data to their "
                "current replacement instead of dropping them."
            ),
            **common,
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{MAP_TABLE}",
            update_semantics="snapshot_replace",
            materialization_type="table",
            cluster_columns=["loinc_version", "loinc_num"],
            pipeline_reference=PIPELINE_REF,
        ),
    )

    if create_view:
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=f"{g}.{CORE_VIEW}",
                description="codes.loinc restricted to the latest loinc_version.",
                public_health_relevance="Convenience surface for the current LOINC release.",
                **{**common, "is_hosted": False},
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=f"{g}.{CORE_VIEW}",
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
    loinc_version: str,
    loinc_secret_scope: str,
    loinc_username_key: str = "loinc_username",
    loinc_password_key: str = "loinc_password",
    api_base: str = loinc.API_BASE_URL,
    create_view: bool = True,
) -> None:
    if not loinc_secret_scope:
        raise ValueError("--loinc-secret-scope is required to pull the licensed LOINC release")

    username = _get_secret(loinc_secret_scope, loinc_username_key)
    password = _get_secret(loinc_secret_scope, loinc_password_key)
    auth_header = _basic_auth_header(username, password)

    meta = _release_metadata(api_base, loinc_version, auth_header)
    zip_bytes = _download_release(api_base, loinc_version, meta, auth_header)
    expected_md5 = str(meta.get("downloadMD5Hash") or "")
    computed_md5 = hashlib.md5(zip_bytes).hexdigest()  # nosec B324 - integrity, not security
    md5_ok = bool(expected_md5) and computed_md5.lower() == expected_md5.lower()
    number_of_loincs = meta.get("numberOfLoincs")
    source_file = f"LOINC {loinc_version} ({meta.get('releaseDate', 'release')})"

    def _ensure(spark: SparkSession) -> None:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA} "
            f"COMMENT 'Canonical clinical/terminology code systems (ICD-10-CM, ICD-9-CM, CVX, "
            f"NDC, LOINC, ...). Owned by the _reference bundle. See ADR 0014.'"
        )

    def _work(ctx: BuildContext) -> None:
        # MD5 is verified inside the DQ context so the result is recorded; a corrupt
        # download raises before any parse/write.
        if md5_ok:
            terms = loinc.parse_loinc_core(_extract_text(zip_bytes, loinc.CORE_MEMBER))
            maps = loinc.parse_map_to(_extract_text(zip_bytes, loinc.MAP_TO_MEMBER))
        else:
            terms, maps = [], []
        _dq_checks(
            ctx,
            terms,
            maps,
            loinc_version,
            md5_ok=md5_ok,
            computed_md5=computed_md5,
            expected_md5=expected_md5,
            number_of_loincs=number_of_loincs,
        )

        now = datetime.now(tz=UTC)
        core_rows = [
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
                "loinc_version": loinc_version,
                "source_file": source_file,
                "loaded_at": now,
            }
            for t in terms
        ]
        map_rows = [
            {
                "loinc_num": m.loinc_num,
                "map_to_loinc_num": m.map_to_loinc_num,
                "comment": m.comment,
                "loinc_version": loinc_version,
                "source_file": source_file,
                "loaded_at": now,
            }
            for m in maps
        ]

        g = f"{catalog}.{SCHEMA}"
        _write_table(
            ctx.spark,
            f"{g}.{CORE_TABLE}",
            CORE_SPARK_SCHEMA,
            core_rows,
            loinc_version,
            ["loinc_num"],
        )
        _write_table(
            ctx.spark, f"{g}.{MAP_TABLE}", MAP_SPARK_SCHEMA, map_rows, loinc_version, ["loinc_num"]
        )
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
        register=lambda spark: _register(spark, catalog, loinc_version, create_view=create_view),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument(
        "--loinc-version",
        default="2.82",
        help="LOINC release to load (explicit + reproducible). Default: 2.82.",
    )
    parser.add_argument(
        "--loinc-secret-scope", required=True, help="Secret scope with LOINC creds."
    )
    parser.add_argument("--loinc-username-key", default="loinc_username")
    parser.add_argument("--loinc-password-key", default="loinc_password")
    parser.add_argument("--api-base", default=loinc.API_BASE_URL)
    parser.add_argument(
        "--no-current-view", action="store_true", help="Skip the codes.loinc_current view."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(
        args.catalog,
        args.data_engineers_group,
        args.analysts_group,
        args.loinc_version,
        args.loinc_secret_scope,
        loinc_username_key=args.loinc_username_key,
        loinc_password_key=args.loinc_password_key,
        api_base=args.api_base,
        create_view=not args.no_current_view,
    )


if __name__ == "__main__":
    main()
