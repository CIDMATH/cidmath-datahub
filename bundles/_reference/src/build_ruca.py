"""Build the USDA ERS RUCA geography reference on the shared builder (ADR 0038 Delta 6).

RUCA (Rural-Urban Commuting Area) codes are a sub-county rural/urban classification keyed to
census tracts and (from 2010 on) ZIP codes. This entrypoint was reconciled to the source->model
conventions on 2026-06-22 (ADR 0038 "Reconciliation"); Delta 6 — folding the hand-rolled
orchestration onto the shared ``build_reference`` builder (ADR 0036/0037/0039) — was deferred until
the builder existed. It now does (proven across geography US + international and ``time``), so this
build is the fold-in: **same tables, same rows, now built through ``build_reference``**, and it
gains the ADR 0039 Volume landing (the ERS files land verbatim in the landing Volume before parse).

Two vintaged specs run in one entrypoint (the builder takes one vintage set per call, and the two
tables have different vintage coverage):
  - **tract** (1990/2000/2010/2020) -> ``geography.us_ruca_tract``, PK ``(geoid, vintage)``;
    promote adds derived ``state_geoid``/``county_geoid`` (``substring(geoid, ...)``).
  - **zip** (2010/2020 only) -> ``geography.us_ruca_zip``, PK ``(zip_code, vintage)``; plus the
    ``us_ruca_zcta`` approximate ZIP->ZCTA bridge **view**, rebuilt post-promote.

Raw lands 1:1 in ``ecdh_<env>.geography_raw.us_ruca_{tract,zip}`` (Volume-backed,
``PER_VINTAGE_IMMUTABLE`` — decennial + immutable). Simple tier: no ``_processed`` stage; each
output's ``promote`` reads its raw landing directly. ``vintage_snapshot`` + atomic Delta
``replaceWhere`` (ADR 0034). Provenance is USDA ERS public domain (no per-landing override needed).

Pure logic (parsers, normalizers, validators, DQ helpers) stays in
``cidmath_datahub.reference.ruca`` (ADR 0011) and is reused unchanged.

Usage:
    build_ruca.py --source-catalog ecdh_dev --model-catalog ecdh_model_dev --level both \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse
import urllib.parse
import urllib.request
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
from cidmath_datahub.reference import ruca

log = get_logger(__name__)

SUBJECT = "geography"  # builder derives geography_raw (landings) + geography (canonical)
RAW_SCHEMA = "geography_raw"
MODEL_SCHEMA = "geography"
PROCESSED_SCHEMA = "geography_processed"  # US geography levels' processed tables (FK oracle, same catalog)
TRACT_TABLE = "us_ruca_tract"
ZIP_TABLE = "us_ruca_zip"
ZCTA_VIEW = "us_ruca_zcta"
ZCTA_TABLE = "us_zcta"
US_TRACT_TABLE = "us_tract"
PIPELINE_REF = "bundles/_reference/src/build_ruca.py"
RUCA_VOLUME_KEY_TRACT = "usda_ruca_tract"
RUCA_VOLUME_KEY_ZIP = "usda_ruca_zip"

# Per-vintage ERS download URLs (public HTTPS; the ?v= cache-buster shifts on re-post — update here
# if a default 404s). CSV where ERS publishes one; 2010 tract is XLSX; 2000/1990 are legacy .xls.
TRACT_URLS: dict[int, str] = {
    2020: "https://www.ers.usda.gov/media/5443/2020-rural-urban-commuting-area-codes-census-tracts.csv?v=68522",
    2010: "https://www.ers.usda.gov/media/5438/2010-rural-urban-commuting-area-codes-revised-732019.xlsx?v=66488",
    2000: "https://www.ers.usda.gov/media/5437/2000-rural-urban-commuting-area-codes.xls?v=85378",
    1990: "https://www.ers.usda.gov/media/5436/1990-rural-urban-commuting-area-codes.xls?v=76004",
}
ZIP_URLS: dict[int, str] = {
    2020: "https://www.ers.usda.gov/media/5444/2020-rural-urban-commuting-area-codes-zip-codes.csv?v=79637",
    2010: "https://www.ers.usda.gov/media/5440/2010-rural-urban-commuting-area-codes-zip-code-file.csv?v=19921",
}

TRACT_VINTAGES = (1990, 2000, 2010, 2020)
ZIP_VINTAGES = (2010, 2020)

# Generous per-vintage cardinality bands (WARN only): a count far outside signals a parse problem.
_TRACT_CARDINALITY_MIN, _TRACT_CARDINALITY_MAX = 50_000, 100_000
_ZIP_CARDINALITY_MIN, _ZIP_CARDINALITY_MAX = 30_000, 60_000

# Raw tract = source-fidelity (1:1), no derived parents (added at promote).
RAW_TRACT_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("geoid", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state", T.StringType(), True),
        T.StructField("county", T.StringType(), True),
        T.StructField("primary_ruca", T.IntegerType(), False),
        T.StructField("secondary_ruca", T.StringType(), False),
        T.StructField("population", T.LongType(), True),
        T.StructField("land_area_sqmi", T.DoubleType(), True),
        T.StructField("population_density", T.DoubleType(), True),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)

# ZIP raw == ZIP canonical (no derived columns); one schema in both catalogs.
ZIP_SPARK_SCHEMA = T.StructType(
    [
        T.StructField("zip_code", T.StringType(), False),
        T.StructField("vintage", T.IntegerType(), False),
        T.StructField("state", T.StringType(), True),
        T.StructField("zip_code_type", T.StringType(), True),
        T.StructField("po_name", T.StringType(), True),
        T.StructField("primary_ruca", T.IntegerType(), False),
        T.StructField("secondary_ruca", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)

# DDL (kept in lockstep with the schemas). Canonical tract = raw + derived state_geoid/county_geoid.
_RAW_TRACT_DDL = (
    "geoid STRING, vintage INT, state STRING, county STRING, primary_ruca INT, "
    "secondary_ruca STRING, population BIGINT, land_area_sqmi DOUBLE, population_density DOUBLE, "
    "source_file STRING, ingested_at TIMESTAMP"
)
_CANON_TRACT_DDL = (
    "geoid STRING, vintage INT, state_geoid STRING, county_geoid STRING, state STRING, "
    "county STRING, primary_ruca INT, secondary_ruca STRING, population BIGINT, "
    "land_area_sqmi DOUBLE, population_density DOUBLE, source_file STRING, ingested_at TIMESTAMP"
)
_ZIP_DDL = (
    "zip_code STRING, vintage INT, state STRING, zip_code_type STRING, po_name STRING, "
    "primary_ruca INT, secondary_ruca STRING, source_file STRING, ingested_at TIMESTAMP"
)

_TRACT_DESC = (
    "USDA ERS Rural-Urban Commuting Area (RUCA) codes by census tract. Attribute extension of "
    "geography.us_tract keyed (geoid, vintage): primary_ruca (1-10, 99), secondary_ruca (verbatim), "
    "plus source population, land_area_sqmi, population_density, state/county labels, and derived "
    "state_geoid/county_geoid."
)
_TRACT_PHR = (
    "Sub-county rural/urban classification: lets tract-coded surveillance/population data be made "
    "rural-aware (urban core vs commuting area vs rural) at a finer grain than county-level schemes."
)
_ZIP_DESC = (
    "USDA ERS Rural-Urban Commuting Area (RUCA) codes by ZIP code (>= 2010). Keyed (zip_code, "
    "vintage): primary_ruca (1-10, 99), secondary_ruca (verbatim), plus state, zip_code_type, "
    "po_name. ZIP is not a census GEOID."
)
_ZIP_PHR = (
    "ZIP-grain rural/urban classification for data captured by ZIP rather than tract (claims, "
    "registries), approximating the tract RUCA scheme."
)
_TRACT_KNOWN_LIMITATIONS = (
    "Primary + secondary RUCA codes (stored verbatim) and the source-provided population / land "
    "area / population density only; no derived rural-urban flag (combine the two code levels "
    "downstream). Vintages (1990/2000/2010/2020) are NOT comparable across decades. Code-definition "
    "labels live as constants in reference/ruca.py; a us_ruca_code_definitions lookup is deferred."
)
_ZIP_KNOWN_LIMITATIONS = (
    "Primary + secondary RUCA codes (verbatim) + ZIP labels (state, zip_code_type, po_name) only. "
    "ZIP codes are USPS routes, not census GEOIDs; ZCTA is the Census areal approximation, so "
    "zip_code joins approximately to us_zcta.geoid on (zip_code = geoid, vintage) — see the "
    "us_ruca_zcta view. The match is not 1:1. ZIP files exist only from the 2010 vintage on."
)

# Provenance shared by every RUCA dataset entry (raw + canonical + view). Already USDA ERS public
# domain, so this base entry covers all landings — no per-landing catalog_overrides needed.
_COMMON_META: dict[str, Any] = {
    "spatial_coverage": "United States",
    "source_provider_code": "usda_ers",
    "source_url": ruca.SOURCE_PRODUCT_URL,
    "source_documentation_url": ruca.SOURCE_DOC_URL,
    "source_data_dictionary_url": ruca.SOURCE_DOC_URL,
    "license": "public domain (U.S. Government work, 17 U.S.C. 105)",
    "dua_required": False,
    "dua_reference": "No DUA. USDA ERS RUCA files are public domain.",
    "access_tier": "open",
    "external_maintainer_name": "USDA Economic Research Service (ERS)",
    "is_hosted": True,
    "temporal_resolution": "decennial",
}


def _base_entry(*, spatial_resolution: str, description: str, phr: str, known_limitations: str
                ) -> registration.DatasetCatalogEntry:
    """Shared provenance entry the builder clones per layer/table (ADR 0008)."""
    return registration.DatasetCatalogEntry(
        full_table_name="(set per layer by the builder)",
        subject=SUBJECT,
        layer="reference",
        description=description,
        public_health_relevance=phr,
        spatial_resolution=spatial_resolution,
        known_limitations=known_limitations,
        **_COMMON_META,
    )


# ---------------------------------------------------------------------------
# IO: download an ERS file + read CSV / XLSX / .xls into row dicts (ADR 0011; lazy pandas/xlrd).
# ---------------------------------------------------------------------------


def _download(url: str) -> tuple[bytes, str]:
    """Download an ERS file; return ``(raw_bytes, filename)``. Filename feeds ``source_file``."""
    parts = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(parts._replace(path=urllib.parse.quote(parts.path)))
    with urllib.request.urlopen(safe_url) as resp:  # nosec B310 - trusted ERS host
        raw = resp.read()
    filename = Path(urllib.parse.unquote(parts.path)).name
    log.info("Downloaded RUCA file", extra={"url": url, "file": filename, "bytes": len(raw)})
    return raw, filename


def _norm_header(name: Any) -> str:
    """Lower-case, drop surrounding quotes, keep [a-z0-9] (mirrors ruca._normalize_header)."""
    import re

    return re.sub(r"[^a-z0-9]", "", str(name).strip().strip("'\"").lower())


# Older/compact ERS files (2010 ZIP, and some older tract vintages) quote their headers and name the
# codes RUCA1 / RUCA2 rather than PrimaryRUCA / SecondaryRUCA. ERS convention: RUCA1 = primary code,
# RUCA2 = secondary code. Applied at the READ layer only (ruca.py stays unchanged) and only fires on
# an exact ruca1/ruca2 header, so the 2020 "Primary RUCA Code 2020" files are untouched. VERIFY the
# mapping in dev (spot-check a few ids -> primary/secondary against the ERS published codes).
_RUCA_HEADER_ALIASES = {"ruca1": "Primary RUCA Code", "ruca2": "Secondary RUCA Code"}


def _detect_header_row(frame: Any) -> int | None:
    """Index of the first row that looks like a RUCA header (a geo column + a RUCA column).

    ERS files vary by vintage: the older .xls carry a leading errata/notes row (the 12/9/2025 1990
    errata banner is literally row 0 now) or bury the data behind a notes sheet, so the header is
    *detected*, not assumed to be row 0.
    """
    for i in range(min(len(frame), 30)):
        cells = [_norm_header(c) for c in frame.iloc[i].tolist()]
        has_geo = any(("tract" in c or "zipcode" in c or "statecounty" in c) for c in cells)
        has_ruca = any("ruca" in c for c in cells)
        if has_geo and has_ruca:
            return i
    return None


def _header_score(header: list[str]) -> int:
    """How many RUCA logical columns a header row resolves (to pick the data sheet in a workbook)."""
    norm = {_norm_header(h) for h in header}
    return sum(1 for t in ("tract", "zipcode", "primaryruca", "secondaryruca", "state")
               if any(t in n for n in norm))


def _read_rows(raw: bytes, filename: str, *, aliases: dict[str, str] | None = None
               ) -> list[dict[str, Any]]:
    """Read a downloaded CSV / XLSX / .xls into header -> cell dicts (all text, blanks -> "").

    Detects the real header row (skipping errata/notes preamble rows and picking the best sheet in a
    multi-sheet workbook) instead of assuming row 0; strips quote qualifiers; and applies ``aliases``
    (normalized-header -> replacement) so vintage-variant names reach the parser (e.g. the compact
    ZIP ruca1/ruca2). All cells read as text so leading zeros in GEOIDs/ZIPs survive.
    """
    import io

    import pandas as pd

    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        # Latin-1 / Windows-1252 (place names carry high bytes); read header-less to detect it.
        frames = {"csv": pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False,
                                     header=None, encoding="latin-1")}
    elif suffix in (".xlsx", ".xls"):
        # openpyxl for .xlsx, xlrd for legacy .xls; scan every sheet for the data sheet.
        frames = pd.read_excel(io.BytesIO(raw), sheet_name=None, dtype=str,
                               keep_default_na=False, header=None)
    else:
        raise ValueError(f"Unsupported RUCA file type {suffix!r} for {filename!r}")

    best: tuple[int, list[dict[str, Any]]] | None = None
    for frame in frames.values():
        hdr = _detect_header_row(frame)
        if hdr is None:
            continue
        header = [str(c).strip().strip("'\"") for c in frame.iloc[hdr].tolist()]
        if aliases:
            header = [aliases.get(_norm_header(c), c) for c in header]
        rows = [
            dict(zip(header, [str(v) for v in rec]))
            for rec in frame.iloc[hdr + 1:].itertuples(index=False)
        ]
        score = _header_score(header)
        if best is None or score > best[0]:
            best = (score, rows)
    if best is None:
        raise ValueError(f"Could not locate a RUCA header row in {filename!r}")
    return best[1]


def _find_payload(volume_dir: Path) -> Path:
    """The single landed ERS file under a landing Volume dir (excluding the fetch-complete marker)."""
    files = [p for p in volume_dir.iterdir() if p.is_file() and not p.name.startswith("_FETCH")]
    if not files:
        raise FileNotFoundError(f"No landed RUCA payload under {volume_dir}")
    return files[0]


def _raw_tract_row(r: ruca.RucaTractRecord, source_file: str, now: datetime) -> dict[str, Any]:
    """Source-fidelity raw tract row (no derived parents; those are added at promote)."""
    return {
        "geoid": r.geoid,
        "vintage": r.vintage,
        "state": r.state,
        "county": r.county,
        "primary_ruca": r.primary_ruca,
        "secondary_ruca": r.secondary_ruca,
        "population": r.population,
        "land_area_sqmi": r.land_area_sqmi,
        "population_density": r.population_density,
        "source_file": source_file,
        "ingested_at": now,
    }


def _raw_zip_row(r: ruca.RucaZipRecord, source_file: str, now: datetime) -> dict[str, Any]:
    return {
        "zip_code": r.zip_code,
        "vintage": r.vintage,
        "state": r.state,
        "zip_code_type": r.zip_code_type,
        "po_name": r.po_name,
        "primary_ruca": r.primary_ruca,
        "secondary_ruca": r.secondary_ruca,
        "source_file": source_file,
        "ingested_at": now,
    }


# --- Volume landing hooks (ADR 0039): fetch the ERS file verbatim, then read+parse into raw -------
def _fetch_tract(v: int, volume_dir: str) -> None:
    raw, filename = _download(TRACT_URLS[int(v)])
    (Path(volume_dir) / filename).write_bytes(raw)


def _read_tract(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    path = _find_payload(Path(volume_dir))
    records = ruca.parse_tract_rows(
        _read_rows(path.read_bytes(), path.name, aliases=_RUCA_HEADER_ALIASES), int(v)
    )
    now = datetime.now(tz=UTC)
    rows = [_raw_tract_row(r, path.name, now) for r in records]
    log.info("Parsed RUCA tracts", extra={"vintage": int(v), "rows": len(rows)})
    return ctx.spark.createDataFrame(rows, RAW_TRACT_SPARK_SCHEMA)


def _fetch_zip(v: int, volume_dir: str) -> None:
    raw, filename = _download(ZIP_URLS[int(v)])
    (Path(volume_dir) / filename).write_bytes(raw)


def _read_zip(ctx: BuildContext, v: int, volume_dir: str) -> Any:
    path = _find_payload(Path(volume_dir))
    records = ruca.parse_zip_rows(
        _read_rows(path.read_bytes(), path.name, aliases=_RUCA_HEADER_ALIASES), int(v)
    )
    now = datetime.now(tz=UTC)
    rows = [_raw_zip_row(r, path.name, now) for r in records]
    log.info("Parsed RUCA ZIPs", extra={"vintage": int(v), "rows": len(rows)})
    return ctx.spark.createDataFrame(rows, ZIP_SPARK_SCHEMA)


# ---------------------------------------------------------------------------
# ZIP -> ZCTA bridge view (the builder makes tables, not views; rebuilt post-promote).
# ---------------------------------------------------------------------------


def _create_zcta_view(spark: SparkSession, model_catalog: str) -> bool:
    """Materialize us_ruca_zip x us_zcta on (zip_code = geoid, vintage). Skip+WARN if us_zcta absent."""
    g = f"{model_catalog}.{MODEL_SCHEMA}"
    zcta_full, zip_full = f"{g}.{ZCTA_TABLE}", f"{g}.{ZIP_TABLE}"
    if not (spark.catalog.tableExists(zcta_full) and spark.catalog.tableExists(zip_full)):
        log.warning(
            "Skipping us_ruca_zcta view -- us_zcta or us_ruca_zip missing (build geography first)",
            extra={"zcta_table": zcta_full, "zip_table": zip_full},
        )
        return False
    view = f"{g}.{ZCTA_VIEW}"
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT r.*, z.gisjoin AS zcta_gisjoin, "
        f"z.centroid_geo_lon AS zcta_centroid_geo_lon, "
        f"z.centroid_geo_lat AS zcta_centroid_geo_lat, "
        f"z.area_land_sqm AS zcta_area_land_sqm "
        f"FROM {zip_full} r JOIN {zcta_full} z ON r.zip_code = z.geoid AND r.vintage = z.vintage"
    )
    spark.sql(
        f"COMMENT ON VIEW {view} IS "
        f"'us_ruca_zip joined to us_zcta on (zip_code = geoid, vintage) -- the approximate "
        f"ZIP->ZCTA bridge (ZCTA = Census ZIP approximation), with ZCTA geometry attached. INNER "
        f"join: ZIPs without a matching ZCTA (point/PO-box/newer) are omitted. Not exact. ADR 0038.'"
    )
    log.info("Created us_ruca_zcta bridge view", extra={"view": view})
    return True


def _register_zcta_view(spark: SparkSession, model_catalog: str) -> None:
    """Register the ZIP->ZCTA bridge view in _ops (layer=reference, full_refresh view)."""
    g = f"{model_catalog}.{MODEL_SCHEMA}"
    registration.register_dataset(
        spark, model_catalog,
        registration.DatasetCatalogEntry(
            full_table_name=f"{g}.{ZCTA_VIEW}",
            subject=SUBJECT, layer="reference",
            description=(
                "us_ruca_zip joined to us_zcta on (zip_code = geoid, vintage) -- the approximate "
                "ZIP->ZCTA bridge with ZCTA geometry attached. INNER join; ZIPs without a matching "
                "ZCTA are omitted. Not exact identity."
            ),
            public_health_relevance=(
                "ZCTA-keyed RUCA classification for joining to census-geography (ZCTA) data without "
                "hand-writing the approximate ZIP->ZCTA bridge."
            ),
            spatial_resolution="us_zcta",
            known_limitations="Approximate ZIP->ZCTA match (not 1:1); point/PO-box and newer ZIPs absent.",
            derived_from=[f"{g}.{ZIP_TABLE}", f"{g}.{ZCTA_TABLE}"],
            **{**_COMMON_META, "is_hosted": False},
        ),
        registration.DatasetEngineeringEntry(
            full_table_name=f"{g}.{ZCTA_VIEW}",
            update_semantics="full_refresh", materialization_type="view",
            cluster_columns=None, pipeline_reference=PIPELINE_REF,
        ),
    )


# ---------------------------------------------------------------------------
# Tract spec
# ---------------------------------------------------------------------------


def _build_tract(*, source_catalog: str, model_catalog: str, data_engineers_group: str,
                 analysts_group: str, vintages: tuple[int, ...], spark: SparkSession) -> None:
    raw_tract = f"{source_catalog}.{RAW_SCHEMA}.{TRACT_TABLE}"
    processed_us_tract = f"{source_catalog}.{PROCESSED_SCHEMA}.{US_TRACT_TABLE}"

    def _ensure_staging(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings that augment the geography subject "
            f"(e.g. USDA ERS RUCA). Engineer-owned; canonicals promote to model geography. ADR 0037.'"
        )
        sp.sql(f"CREATE TABLE IF NOT EXISTS {raw_tract} ({_RAW_TRACT_DDL}) USING DELTA")

    def _ensure_canonical(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical US geography reference (states, counties, tracts, ZCTAs, HHS "
            f"regions, RUCA codes, boundaries). Owned by the _reference bundle. ADR 0020.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{TRACT_TABLE} "
            f"({_CANON_TRACT_DDL}) USING DELTA"
        )

    def _promote(ctx: BuildContext, v: int) -> Any:
        """Promote raw tract -> canonical, deriving state_geoid/county_geoid from the GEOID."""
        return ctx.spark.sql(
            f"SELECT geoid, vintage, substring(geoid, 1, 2) AS state_geoid, "
            f"substring(geoid, 1, 5) AS county_geoid, state, county, primary_ruca, secondary_ruca, "
            f"population, land_area_sqmi, population_density, source_file, ingested_at "
            f"FROM {raw_tract} WHERE vintage = {int(v)}"
        ).sort("geoid")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        sp = ctx.spark
        record_table = f"{MODEL_SCHEMA}.{TRACT_TABLE}"
        failures: list[str] = []

        # PK uniqueness via TableDQ (record non-raising; collect all checks, raise once at the end).
        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
        if not dq.unique(keys=["geoid", "vintage"],
                         check_name="us_ruca_tract_geoid_vintage_uniqueness", raise_on_fail=False):
            failures.append("duplicate (geoid, vintage)")

        # Reconstruct records from the staged raw table -> reuse ruca.py helpers (exact parity).
        records = [
            ruca.RucaTractRecord(
                geoid=r["geoid"], vintage=r["vintage"],
                state_geoid=(r["geoid"][:2] if r["geoid"] else ""),
                county_geoid=(r["geoid"][:5] if r["geoid"] else ""),
                state=r["state"], county=r["county"],
                primary_ruca=r["primary_ruca"], secondary_ruca=r["secondary_ruca"],
                population=r["population"], land_area_sqmi=r["land_area_sqmi"],
                population_density=r["population_density"],
            )
            for r in sp.sql(
                f"SELECT geoid, vintage, state, county, primary_ruca, secondary_ruca, population, "
                f"land_area_sqmi, population_density FROM {staging_fqn}"
            ).collect()
        ]
        n = len(records)

        bad_geoid = ruca.find_bad_tract_geoids(records)
        _record(ctx, record_table, "us_ruca_tract_geoid_is_11_digit", DQCategory.BUSINESS_RULE,
                not bad_geoid, len(bad_geoid), n, {"sample": bad_geoid[:10]} if bad_geoid else None)
        if bad_geoid:
            failures.append(f"tract geoid not 11-digit: {bad_geoid[:5]}")

        bad_pri = ruca.find_invalid_primary_codes(records)
        _record(ctx, record_table, "us_ruca_tract_primary_code_valid", DQCategory.BUSINESS_RULE,
                not bad_pri, len(bad_pri), n,
                {"allowed": sorted(ruca.PRIMARY_RUCA_CODES), "sample": [list(s) for s in bad_pri[:10]]}
                if bad_pri else None)
        if bad_pri:
            failures.append(f"tract primary out of vocab: {bad_pri[:5]}")

        bad_sec = ruca.find_invalid_secondary_codes(records)
        _record(ctx, record_table, "us_ruca_tract_secondary_code_valid", DQCategory.BUSINESS_RULE,
                not bad_sec, len(bad_sec), n,
                {"sample": [list(s) for s in bad_sec[:10]]} if bad_sec else None)
        if bad_sec:
            failures.append(f"tract secondary out of vocab: {bad_sec[:5]}")

        for vintage in sorted({r.vintage for r in records}):
            tract_v = [r for r in records if r.vintage == vintage]
            ok = _TRACT_CARDINALITY_MIN <= len(tract_v) <= _TRACT_CARDINALITY_MAX
            _record(ctx, record_table, f"us_ruca_tract_cardinality_{vintage}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, len(tract_v),
                    {"expected_range": [_TRACT_CARDINALITY_MIN, _TRACT_CARDINALITY_MAX],
                     "actual": len(tract_v)}, severity=DQSeverity.WARN)
            n_pop = sum(1 for r in tract_v if r.population is not None)
            _record(ctx, record_table, f"us_ruca_tract_population_present_{vintage}",
                    DQCategory.NULLABILITY, n_pop == len(tract_v), len(tract_v) - n_pop, len(tract_v),
                    {"with_population": n_pop}, severity=DQSeverity.WARN)
            _record(ctx, record_table, f"us_ruca_tract_primary_distribution_{vintage}",
                    DQCategory.BUSINESS_RULE, True, 0, len(tract_v),
                    {"distribution": ruca.primary_distribution(tract_v)}, severity=DQSeverity.INFO)

        # FK to us_tract (WARN, not blocking): RUCA augments geography. Validate against the
        # SAME-source-catalog processed table (ADR 0037 decision 1 — builds never read the model
        # catalog mid-build). Scoped to vintages us_tract actually covers (2010/2020); pre-2010 RUCA
        # vintages have no us_tract parent by design, so they are excluded from the denominator.
        if sp.catalog.tableExists(processed_us_tract):
            orphans = sp.sql(
                f"SELECT count(*) AS c FROM {staging_fqn} r "
                f"LEFT ANTI JOIN {processed_us_tract} p ON r.geoid = p.geoid AND r.vintage = p.vintage "
                f"WHERE r.vintage IN (SELECT DISTINCT vintage FROM {processed_us_tract})"
            ).collect()[0]["c"]
            scoped_total = sp.sql(
                f"SELECT count(*) AS c FROM {staging_fqn} "
                f"WHERE vintage IN (SELECT DISTINCT vintage FROM {processed_us_tract})"
            ).collect()[0]["c"]
            _record(ctx, record_table, "us_ruca_tract_us_tract_fk_integrity", DQCategory.REFERENTIAL,
                    orphans == 0, orphans, scoped_total,
                    {"orphan_geoids": orphans, "scope": "vintages present in geography_processed.us_tract"},
                    severity=DQSeverity.WARN)
        else:
            log.warning("us_tract FK check skipped (parent not built yet)", extra={"parent": processed_us_tract})

        if failures:
            raise ValueError("RUCA tract blocking DQ failed -- " + "; ".join(failures))

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(spatial_resolution="us_tract", description=_TRACT_DESC,
                                       phr=_TRACT_PHR, known_limitations=_TRACT_KNOWN_LIMITATIONS),
        raw_landings=[
            RawLanding(
                table=TRACT_TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=RUCA_VOLUME_KEY_TRACT,
                fetch_to_volume=_fetch_tract,
                read_from_volume=_read_tract,
                description=(
                    "Raw USDA ERS RUCA census-tract codes, fetched-as-is (1:1 with source rows), "
                    "vintage-stamped. Volume-landed verbatim, then parsed. Promoted to us_ruca_tract."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table=TRACT_TABLE,
                reads=(TRACT_TABLE,),
                promote=_promote,
                validate_staging=_validate,
                description=_TRACT_DESC,
                public_health_relevance=_TRACT_PHR,
                canonical_cluster_columns=["vintage", "geoid"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=tuple(vintages), spark=spark)
    log.info("RUCA tract build complete", extra={"vintages": vintages})


# ---------------------------------------------------------------------------
# ZIP spec (+ ZCTA bridge view post-promote)
# ---------------------------------------------------------------------------


def _build_zip(*, source_catalog: str, model_catalog: str, data_engineers_group: str,
               analysts_group: str, vintages: tuple[int, ...], spark: SparkSession) -> None:
    raw_zip = f"{source_catalog}.{RAW_SCHEMA}.{ZIP_TABLE}"
    processed_us_zcta = f"{source_catalog}.{PROCESSED_SCHEMA}.{ZCTA_TABLE}"

    def _ensure_staging(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {source_catalog}.{RAW_SCHEMA} "
            f"COMMENT 'Raw, fetched-as-is source landings that augment the geography subject "
            f"(e.g. USDA ERS RUCA). Engineer-owned; canonicals promote to model geography. ADR 0037.'"
        )
        sp.sql(f"CREATE TABLE IF NOT EXISTS {raw_zip} ({_ZIP_DDL}) USING DELTA")

    def _ensure_canonical(sp: SparkSession) -> None:
        sp.sql(
            f"CREATE SCHEMA IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA} "
            f"COMMENT 'Canonical US geography reference (states, counties, tracts, ZCTAs, HHS "
            f"regions, RUCA codes, boundaries). Owned by the _reference bundle. ADR 0020.'"
        )
        sp.sql(
            f"CREATE TABLE IF NOT EXISTS {model_catalog}.{MODEL_SCHEMA}.{ZIP_TABLE} "
            f"({_ZIP_DDL}) USING DELTA"
        )

    def _promote(ctx: BuildContext, v: int) -> Any:
        """Promote raw ZIP -> canonical; ZIP raw and canonical share one schema."""
        return ctx.spark.sql(f"SELECT * FROM {raw_zip} WHERE vintage = {int(v)}").sort("zip_code")

    def _validate(ctx: BuildContext, staging_fqn: str) -> None:
        sp = ctx.spark
        record_table = f"{MODEL_SCHEMA}.{ZIP_TABLE}"
        failures: list[str] = []

        dq = make_staging_dq(ctx, staging_fqn, record_table=record_table)
        if not dq.unique(keys=["zip_code", "vintage"],
                         check_name="us_ruca_zip_zip_code_vintage_uniqueness", raise_on_fail=False):
            failures.append("duplicate (zip_code, vintage)")

        records = [
            ruca.RucaZipRecord(
                zip_code=r["zip_code"], vintage=r["vintage"], state=r["state"],
                zip_code_type=r["zip_code_type"], po_name=r["po_name"],
                primary_ruca=r["primary_ruca"], secondary_ruca=r["secondary_ruca"],
            )
            for r in sp.sql(
                f"SELECT zip_code, vintage, state, zip_code_type, po_name, primary_ruca, "
                f"secondary_ruca FROM {staging_fqn}"
            ).collect()
        ]
        n = len(records)

        bad_zip = ruca.find_bad_zip_codes(records)
        _record(ctx, record_table, "us_ruca_zip_zip_code_is_5_digit", DQCategory.BUSINESS_RULE,
                not bad_zip, len(bad_zip), n, {"sample": bad_zip[:10]} if bad_zip else None)
        if bad_zip:
            failures.append(f"zip code not 5-digit: {bad_zip[:5]}")

        bad_pri = ruca.find_invalid_primary_codes(records)
        _record(ctx, record_table, "us_ruca_zip_primary_code_valid", DQCategory.BUSINESS_RULE,
                not bad_pri, len(bad_pri), n,
                {"sample": [list(s) for s in bad_pri[:10]]} if bad_pri else None)
        if bad_pri:
            failures.append(f"zip primary out of vocab: {bad_pri[:5]}")

        bad_sec = ruca.find_invalid_secondary_codes(records)
        _record(ctx, record_table, "us_ruca_zip_secondary_code_valid", DQCategory.BUSINESS_RULE,
                not bad_sec, len(bad_sec), n,
                {"sample": [list(s) for s in bad_sec[:10]]} if bad_sec else None)
        if bad_sec:
            failures.append(f"zip secondary out of vocab: {bad_sec[:5]}")

        for vintage in sorted({r.vintage for r in records}):
            zip_v = [r for r in records if r.vintage == vintage]
            ok = _ZIP_CARDINALITY_MIN <= len(zip_v) <= _ZIP_CARDINALITY_MAX
            _record(ctx, record_table, f"us_ruca_zip_cardinality_{vintage}", DQCategory.CARDINALITY,
                    ok, 0 if ok else 1, len(zip_v),
                    {"expected_range": [_ZIP_CARDINALITY_MIN, _ZIP_CARDINALITY_MAX],
                     "actual": len(zip_v)}, severity=DQSeverity.WARN)

        # Approximate ZIP->ZCTA match rate (INFO, never blocking — the match is intentionally not
        # 1:1). Validate against the SAME-source-catalog processed us_zcta (ADR 0037 decision 1).
        if sp.catalog.tableExists(processed_us_zcta):
            matched = sp.sql(
                f"SELECT count(*) AS c FROM {staging_fqn} r "
                f"LEFT SEMI JOIN {processed_us_zcta} z ON r.zip_code = z.geoid AND r.vintage = z.vintage"
            ).collect()[0]["c"]
            rate = (matched / n * 100) if n else 0.0
            _record(ctx, record_table, "us_ruca_zip_us_zcta_match_rate", DQCategory.REFERENTIAL,
                    True, n - matched, n,
                    {"matched": matched, "match_rate_pct": round(rate, 2),
                     "note": "approximate ZIP->ZCTA; point/PO-box + newer ZIPs have no ZCTA"},
                    severity=DQSeverity.INFO)
        else:
            log.warning("ZIP->ZCTA match-rate skipped (us_zcta not built yet)",
                        extra={"parent": processed_us_zcta})

        if failures:
            raise ValueError("RUCA zip blocking DQ failed -- " + "; ".join(failures))

    spec = ReferenceBuildSpec(
        subject=SUBJECT,
        source_catalog=source_catalog,
        model_catalog=model_catalog,
        pipeline_reference=PIPELINE_REF,
        reader_groups=(data_engineers_group, analysts_group),
        engineer_group=data_engineers_group,
        base_catalog_entry=_base_entry(spatial_resolution="zip_code", description=_ZIP_DESC,
                                       phr=_ZIP_PHR, known_limitations=_ZIP_KNOWN_LIMITATIONS),
        raw_landings=[
            RawLanding(
                table=ZIP_TABLE,
                landing_retention=LandingRetention.PER_VINTAGE_IMMUTABLE,
                volume_key=RUCA_VOLUME_KEY_ZIP,
                fetch_to_volume=_fetch_zip,
                read_from_volume=_read_zip,
                description=(
                    "Raw USDA ERS RUCA ZIP-code codes (>= 2010), fetched-as-is (1:1 with source "
                    "rows), vintage-stamped. Volume-landed verbatim, then parsed. Promoted to us_ruca_zip."
                ),
            ),
        ],
        outputs=[
            CanonicalOutput(
                canonical_table=ZIP_TABLE,
                reads=(ZIP_TABLE,),
                promote=_promote,
                validate_staging=_validate,
                description=_ZIP_DESC,
                public_health_relevance=_ZIP_PHR,
                canonical_cluster_columns=["vintage", "zip_code"],
            ),
        ],
        ensure_staging=_ensure_staging,
        ensure_canonical=_ensure_canonical,
        update_semantics="vintage_snapshot",
    )
    build_reference(spec, vintages=tuple(vintages), spark=spark)
    log.info("RUCA zip build complete", extra={"vintages": vintages})

    # ZIP->ZCTA bridge view (post-promote; the builder makes tables, not views).
    if _create_zcta_view(spark, model_catalog):
        _register_zcta_view(spark, model_catalog)


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


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def build_ruca_layered(
    *,
    source_catalog: str,
    model_catalog: str,
    data_engineers_group: str,
    analysts_group: str,
    level: str = "both",
    vintages: tuple[int, ...] | None = None,
) -> None:
    """Build RUCA tract and/or zip on the shared builder (one Spark session for both specs)."""
    spark = SparkSession.builder.getOrCreate()
    tract_vintages = (
        tuple(v for v in vintages if v in TRACT_VINTAGES) if vintages else TRACT_VINTAGES
    )
    zip_vintages = tuple(v for v in vintages if v in ZIP_VINTAGES) if vintages else ZIP_VINTAGES

    if level in ("tract", "both") and tract_vintages:
        _build_tract(source_catalog=source_catalog, model_catalog=model_catalog,
                     data_engineers_group=data_engineers_group, analysts_group=analysts_group,
                     vintages=tract_vintages, spark=spark)
    if level in ("zip", "both") and zip_vintages:
        _build_zip(source_catalog=source_catalog, model_catalog=model_catalog,
                   data_engineers_group=data_engineers_group, analysts_group=analysts_group,
                   vintages=zip_vintages, spark=spark)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-catalog", required=True, help="Source catalog for raw (ecdh_<env>).")
    parser.add_argument("--model-catalog", required=True, help="Model catalog for canonical (ecdh_model_<env>).")
    parser.add_argument("--level", choices=["tract", "zip", "both"], default="both",
                        help="Which RUCA table(s) to build. Default: both.")
    parser.add_argument("--vintages", default=None,
                        help="Comma-separated subset (e.g. 2020). Default: full per-level set "
                             "(tract 1990/2000/2010/2020, zip 2010/2020).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    vintages = tuple(int(x) for x in args.vintages.split(",")) if args.vintages else None
    build_ruca_layered(
        source_catalog=args.source_catalog,
        model_catalog=args.model_catalog,
        data_engineers_group=args.data_engineers_group,
        analysts_group=args.analysts_group,
        level=args.level,
        vintages=vintages,
    )
    log.info("RUCA reference build complete", extra={"model_catalog": args.model_catalog, "level": args.level})


if __name__ == "__main__":
    main()
