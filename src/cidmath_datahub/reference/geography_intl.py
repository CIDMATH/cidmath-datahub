"""International geography reference logic (ADR 0022).

Pure, unit-testable helpers for the global ``geography.country`` /
``geography.country_subdivision`` / ``geography.subnational`` tables. Sibling
to ``geography.py`` (which owns the US-specific tables). No Spark, no IO,
no geospatial dependencies here — the bundle entrypoints
(``bundles/_reference/src/build_geography_country.py`` etc.) handle the
GADM download, the live WHO / UN M49 region fetches, and the writes; this
module owns the deterministic parts that can be tested without a workspace
or network access.

Canonical identifiers:
  - country PK is ISO 3166-1 alpha-3 (``"USA"``, ``"BRA"``). Alpha-2 and
    numeric stored as alternate keys.
  - subdivision PK is ISO 3166-2 (``"US-GA"``, ``"BR-SP"``).
  - subnational PK is GADM ``GID_N`` (``"USA.10.121_1"``).

Edge cases (ADR 0022): GADM uses X-prefixed codes for non-ISO territories
(``"XKO"`` Kosovo, ``"XNC"`` Northern Cyprus, etc.); these are excluded
from ``geography.country`` / ``geography.boundary`` because they have no
ISO surveillance key. Territories with ISO codes but no WHO membership
(Taiwan, Palestine, Vatican, dependent territories) get ``None`` for
``who_region``; this is consistent with how the WHO GHO API treats them
(simply absent from the COUNTRY dimension).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

# WHO region codes — short forms used by GHO API ``ParentCode`` (not the
# org-chart suffixed forms AFRO/AMRO/EMRO/EURO/SEARO/WPRO).
WHO_REGION_CODES: frozenset[str] = frozenset({"AFR", "AMR", "EMR", "EUR", "SEAR", "WPR"})

# UN M49 top-level region names (English, as published).
UN_REGION_NAMES: frozenset[str] = frozenset(
    {"Africa", "Americas", "Asia", "Europe", "Oceania", "Antarctica"}
)

# GADM ADM0 entries that are NOT in ISO 3166-1. GID_0 values start with X
# for these GADM-coined codes (gadm.org/download_country.html). We skip
# these in geography.country and geography.boundary — they have no
# canonical ISO key, so they can't FK from any surveillance source that
# uses ISO. If a non-ISO territory becomes important to model in its own
# right, add a separate subnational entry (ADR 0022 slice 3c).
GADM_NON_ISO_GID0: frozenset[str] = frozenset({"XKO", "XNC", "XAD", "XCA", "XCL", "XPI", "XSP"})


def normalize_alpha3(value: str) -> str:
    """Return a validated upper-case 3-letter ISO 3166-1 alpha-3 code.

    Raises ValueError for malformed input. Does not check membership against
    the ISO list (that's the caller's job via pycountry).
    """
    if not isinstance(value, str):
        raise ValueError(f"alpha3 must be a string, got {type(value).__name__}")
    s = value.strip().upper()
    if len(s) != 3 or not s.isalpha():
        raise ValueError(f"alpha3 {value!r} is not three letters")
    return s


def normalize_alpha2(value: str) -> str:
    """Return a validated upper-case 2-letter ISO 3166-1 alpha-2 code."""
    if not isinstance(value, str):
        raise ValueError(f"alpha2 must be a string, got {type(value).__name__}")
    s = value.strip().upper()
    if len(s) != 2 or not s.isalpha():
        raise ValueError(f"alpha2 {value!r} is not two letters")
    return s


def normalize_numeric(value: str | int) -> str:
    """Return a validated zero-padded 3-digit ISO 3166-1 numeric code.

    Accepts ``4``, ``"4"``, ``"004"`` — all become ``"004"`` (Afghanistan).
    Leading zeros are significant; storing as int loses them, so the column
    type stays STRING (ADR 0022 storage guardrail).
    """
    if isinstance(value, int):
        s = str(value)
    elif isinstance(value, str):
        s = value.strip()
    else:
        raise ValueError(f"numeric code must be str or int, got {type(value).__name__}")
    if not s.isdigit() or len(s) > 3:
        raise ValueError(f"numeric code {value!r} is not 1-3 digits")
    return s.zfill(3)


def is_iso_gid0(gid0: str) -> bool:
    """Return True if a GADM GID_0 value is a real ISO 3166-1 alpha-3.

    GADM uses GID_0 as the alpha-3 *when available* (gadm.org/metadata.html)
    and coins X-prefixed codes for non-ISO territories. This excludes the
    GADM-coined codes from anything ISO-keyed.
    """
    if not isinstance(gid0, str) or len(gid0) != 3:
        return False
    if gid0 in GADM_NON_ISO_GID0:
        return False
    # Belt-and-suspenders for any future X-prefixed code GADM might add.
    if gid0.startswith("X"):
        return False
    return True


def assemble_country_row(
    *,
    alpha2: str,
    alpha3: str,
    numeric: str | int,
    name: str,
    official_name: str | None,
    who_region: str | None,
    un_region: str | None,
    un_subregion: str | None,
    is_un_member: bool,
    is_sovereign: bool,
    iso_3166_3_predecessor: str | None,
    centroid_geo_lon: float | None,
    centroid_geo_lat: float | None,
    source_file: str,
) -> dict[str, Any]:
    """Assemble one ``geography.country`` row from validated inputs.

    Centralizes the normalization + nullability rules so the build script
    can call this once per pycountry record and get a dict that matches
    the Spark schema exactly. Raises ValueError for malformed identifiers
    or out-of-vocabulary region codes.
    """
    a3 = normalize_alpha3(alpha3)
    a2 = normalize_alpha2(alpha2)
    num = normalize_numeric(numeric)

    if who_region is not None and who_region not in WHO_REGION_CODES:
        raise ValueError(
            f"who_region {who_region!r} for {a3} not in vocabulary {sorted(WHO_REGION_CODES)}"
        )
    if un_region is not None and un_region not in UN_REGION_NAMES:
        raise ValueError(
            f"un_region {un_region!r} for {a3} not in vocabulary {sorted(UN_REGION_NAMES)}"
        )

    if (centroid_geo_lon is None) != (centroid_geo_lat is None):
        raise ValueError(
            f"centroid lon/lat must both be set or both None (got "
            f"lon={centroid_geo_lon}, lat={centroid_geo_lat})"
        )

    return {
        "country_alpha3": a3,
        "country_alpha2": a2,
        "country_numeric": num,
        "country_name": name,
        "country_official_name": official_name,
        "who_region": who_region,
        "un_region": un_region,
        "un_subregion": un_subregion,
        "is_un_member": bool(is_un_member),
        "is_sovereign": bool(is_sovereign),
        "iso_3166_3_predecessor": iso_3166_3_predecessor,
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "source_file": source_file,
    }


def check_join_coverage(
    iso_alpha3_list: Iterable[str],
    gadm_alpha3_set: set[str],
) -> tuple[int, int, list[str]]:
    """Return ``(matched, total, sample_missing)`` for ISO→GADM join coverage.

    Used by the build's DQ: a coverage drop usually means GADM dropped a
    territory (or we have a stale GADM release). ``sample_missing`` returns
    up to ten alpha-3 codes that had no GADM polygon, for inclusion in
    the ``_ops.dq_results`` details payload.
    """
    iso_list = list(iso_alpha3_list)
    missing = sorted(a for a in iso_list if a not in gadm_alpha3_set)
    matched = len(iso_list) - len(missing)
    return matched, len(iso_list), missing[:10]
