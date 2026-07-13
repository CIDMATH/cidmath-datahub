"""Rural-Urban Commuting Area (RUCA) codes (USDA ERS; ADR 0020, ADR 0038).

Pure, unit-testable logic for the ``geography`` schema's two RUCA tables: a census-tract
table (``geography.us_ruca_tract``) and a ZIP-code table (``geography.us_ruca_zip``). RUCA
is the USDA Economic Research Service's sub-county rural/urban classification -- a tract is
assigned a **primary** code (whole number ``1``-``10``, plus ``99`` for water/zero-population
tracts) encoding its urban-core class and largest commuting flow, and a **secondary** code
(e.g. ``1.0`` .. ``10.6``, plus ``99``) subdividing it by the second-largest flow. Both levels are
stored verbatim; combining them flexibly is the point of the scheme, so we derive no single
rural/urban flag (ADR 0038).

The RUCA coding scheme is **versioned**: the code values, the secondary set, and the descriptions
differ by version, keyed to the vintage (1990 -> v1.11, 2000 -> v2.0, 2010/2020 -> v3.x current).
The controlled vocabularies + descriptions are therefore version-keyed (see the ``*_BY_VERSION``
registries and ``RUCA_VERSION_BY_VINTAGE``), and validation is version-aware -- a 1990 row is checked
against v1.11, a 2020 row against v3.x. The version-specific descriptions are also surfaced as
queryable data by the ``geography.us_ruca_code_definitions`` lookup (built from :func:`code_definitions`).

This module is the single source of truth for RUCA code sets, identifier normalization, code
validation, source-header resolution, and the pure DQ helpers. It holds **no Spark and no IO**
(ADR 0011); the entrypoint ``bundles/_reference/src/build_ruca.py`` does the public HTTPS
download, reads the CSV/XLSX/legacy ``.xls`` file into row dicts, and writes the Delta tables.

How RUCA differs from the entity geography tables (so it is not mis-modeled):

* **Tract grain reuses the census GEOID.** ``us_ruca_tract`` keys on ``(geoid, vintage)`` -- the
  same 11-digit tract GEOID + decennial ``vintage`` as ``geography.us_tract`` -- so it is an
  attribute extension that joins ``USING (geoid, vintage)``. ``state_geoid``/``county_geoid`` are
  derived from the GEOID exactly as in :mod:`cidmath_datahub.reference.geography`.
* **ZIP grain is not a census GEOID, but joins to ZCTA approximately.** ``us_ruca_zip`` keys on
  ``(zip_code, vintage)``. ZIP codes are USPS routes, not census units -- hence a descriptively
  named key, not ``geoid``. However ZCTA is the Census Bureau's areal approximation of ZIP codes,
  so the 5-digit ``zip_code`` is treated as an **approximate** foreign key to
  ``geography.us_zcta.geoid`` (join on ``(zip_code = geoid, vintage)``). The match is not 1:1 --
  point / PO-box ZIPs and newer ZIPs have no ZCTA, and ZCTA boundaries lag ZIP changes -- so the
  join is for approximate geographic enrichment, not exact identity (ADR 0038).
* **Vintages are not comparable across decades.** Tract boundaries and the urban-core methodology
  change each decade, so ``vintage`` is part of the key and nothing assumes a tract persists across
  decades. ZIP files exist only from 2010 on.
* **Headers drift across vintages/geographies.** The 2020 ZIP file is
  ``ZIPCode,State,ZIPCodeType,POName,PrimaryRUCA,SecondaryRUCA``; tract headers carry the year
  (``Primary RUCA Code 2020``). Columns are resolved by normalized-name match, not position.

License: public domain (U.S. Government work); plain HTTPS download, no credential.

Sources:
    * Product page: https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes
    * Documentation (code definitions):
      https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/documentation
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from cidmath_datahub.reference.geography import (
    COUNTY_GEOID_WIDTH,
    STATE_GEOID_WIDTH,
    normalize_geoid,
)

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: Human-facing landing + documentation pages (recorded in registration provenance).
SOURCE_PRODUCT_URL = "https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes"
SOURCE_DOC_URL = (
    "https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/documentation"
)

#: Tract GEOID is 11 digits; ZIP code is 5 digits (zero-padded, kept as strings).
TRACT_GEOID_WIDTH = 11
ZIP_CODE_WIDTH = 5

# ---------------------------------------------------------------------------
# Versioned RUCA code vocabularies (single-sourced here; ADR 0006/0016/0038).
#
# RUCA's coding scheme is **versioned**, and the code values, secondary sets, AND descriptions
# differ by version -- this is not just wording (ADR 0038). Each decennial vintage is coded to a
# specific RUCA version, so the controlled vocabulary + descriptions must be keyed to the version:
#
#   vintage 1990 -> v1_11  (UW WWAMI RHRC, https://depts.washington.edu/uwruca/ruca1/ruca-codes11.php)
#   vintage 2000 -> v2_0   (UW WWAMI RHRC, https://depts.washington.edu/uwruca/ruca-codes.php)
#   vintage 2010 -> v3_x   (USDA ERS "Primary/Secondary RUCA codes, 2010" tables -- current)
#   vintage 2020 -> v3_x   (USDA ERS "Primary/Secondary RUCA codes, 2020" tables -- current)
#
# The primary codes are 1-10 (+ ``99``) in every version, but the *terminology* differs (v1.11's
# "urban core / large town / small town" vs the modern "metropolitan / micropolitan"). The
# **secondary** sets genuinely differ: v1.11 has a unique ``2.2`` and lacks ``4.2``/``5.2``/``6.1``/
# ``10.6``; v2.0 carries the full ``x.2``/``6.1``/``10.6`` secondaries the modern ERS product drops.
# So a 1990/2000 row must be validated + labelled against *its* version's vocabulary, not the modern
# set. Descriptions live as version-keyed constants here and are surfaced as queryable data by the
# ``geography.us_ruca_code_definitions`` lookup the build materializes from them.
#
# The ``99`` (zero-population / no-data) code is a modern (v3_x) convention. Whether the legacy
# 1990/2000 ERS media ``.xls`` files use ``99`` or a different missing indicator is not yet confirmed
# from the source files; ``99`` is accepted in every version's set so a zero-population tract does not
# fail the blocking vocab DQ, and a genuinely different sentinel in the older files would surface as
# an out-of-vocab row for a human to add. VERIFY against the 1990/2000 files in dev (ADR 0038).
# ---------------------------------------------------------------------------

#: Ordered RUCA code-scheme versions (oldest -> current).
RUCA_VERSIONS: tuple[str, ...] = ("v1_11", "v2_0", "v3_x")

#: The current (modern ERS 2010/2020) version -- the default when no vintage/version is given.
DEFAULT_RUCA_VERSION = "v3_x"

#: Decennial RUCA vintage -> code-scheme version (ADR 0038).
RUCA_VERSION_BY_VINTAGE: dict[int, str] = {1990: "v1_11", 2000: "v2_0", 2010: "v3_x", 2020: "v3_x"}

#: UW/ERS code-definition source page per version (recorded in the lookup's provenance). The
#: 1990/2000 code semantics are UW WWAMI RHRC-authored (the original RUCA authors); 2010/2020 are ERS.
CODE_DEFINITION_SOURCE_URL_BY_VERSION: dict[str, str] = {
    "v1_11": "https://depts.washington.edu/uwruca/ruca1/ruca-codes11.php",
    "v2_0": "https://depts.washington.edu/uwruca/ruca-codes.php",
    "v3_x": SOURCE_DOC_URL,
}

# --- v3_x (modern ERS, vintages 2010/2020) -- the current vocabulary, unchanged ---------------

#: v3_x primary codes 1-10 + ``99``, code -> description (ERS "Primary RUCA codes" table).
PRIMARY_RUCA_DESCRIPTIONS_V3_X: dict[int, str] = {
    1: "Metropolitan core: primary flow within an urban area of 50,000+ (metro UA)",
    2: "Metropolitan high commuting: primary flow 30%+ to a metro UA",
    3: "Metropolitan low commuting: primary flow 10%-30% to a metro UA",
    4: "Micropolitan core: primary flow within an urban area of 10,000-49,999 (micro UA)",
    5: "Micropolitan high commuting: primary flow 30%+ to a micro UA",
    6: "Micropolitan low commuting: primary flow 10%-30% to a micro UA",
    7: "Small town core: primary flow within an urban area of <=9,999 (small town UA)",
    8: "Small town high commuting: primary flow 30%+ to a small town UA",
    9: "Small town low commuting: primary flow 10%-30% to a small town UA",
    10: "Rural area: primary flow to a tract outside an urban area",
    99: "Not coded: water/zero-population tract (zero population and zero land area)",
}

#: v3_x secondary codes (21 published + ``99``), code -> description (ERS "Secondary RUCA codes").
SECONDARY_RUCA_DESCRIPTIONS_V3_X: dict[str, str] = {
    "1.0": "No additional code",
    "1.1": "Secondary flow 30%-50% to a larger UA",
    "2.0": "No additional code",
    "2.1": "Secondary flow 30%-50% to a larger UA",
    "3.0": "No additional code",
    "4.0": "No additional code",
    "4.1": "Secondary flow 30%-50% to a metro UA",
    "5.0": "No additional code",
    "5.1": "Secondary flow 30%-50% to a metro UA",
    "6.0": "No additional code",
    "7.0": "No additional code",
    "7.1": "Secondary flow 30%-50% to a metro UA",
    "7.2": "Secondary flow 30%-50% to a micro UA",
    "8.0": "No additional code",
    "8.1": "Secondary flow 30%-50% to a metro UA",
    "8.2": "Secondary flow 30%-50% to a micro UA",
    "9.0": "No additional code",
    "10.0": "No additional code",
    "10.1": "Secondary flow 30%-50% to a metro UA",
    "10.2": "Secondary flow 30%-50% to a micro UA",
    "10.3": "Secondary flow 30%-50% to a small town UA",
    "99": "Not coded: water/zero-population tract",
}

# --- v2_0 (vintage 2000; UW WWAMI RHRC "Code Definitions: Version 2.0") -------------------------
# Modern-style terminology (Metropolitan area / Urban Cluster (UC) / small town / rural areas), but
# a wider secondary set than v3_x: v2.0 keeps the ``x.2`` "10%-29%" secondaries and ``6.1``/``10.6``
# that the modern ERS product dropped. UA=Urbanized Area, UC=Urban Cluster; "large UC" = 10,000-49,999
# ("micropolitan"), "small UC" = 2,500-9,999.

#: v2_0 primary codes, code -> description (UW v2.0).
PRIMARY_RUCA_DESCRIPTIONS_V2_0: dict[int, str] = {
    1: "Metropolitan area core: primary flow within an Urbanized Area (UA)",
    2: "Metropolitan area high commuting: primary flow 30% or more to a UA",
    3: "Metropolitan area low commuting: primary flow 10% to 30% to a UA",
    4: "Micropolitan area core: primary flow within an Urban Cluster (UC) of 10,000-49,999 (large UC)",
    5: "Micropolitan high commuting: primary flow 30% or more to a large UC",
    6: "Micropolitan low commuting: primary flow 10% to 30% to a large UC",
    7: "Small town core: primary flow within an Urban Cluster of 2,500-9,999 (small UC)",
    8: "Small town high commuting: primary flow 30% or more to a small UC",
    9: "Small town low commuting: primary flow 10% through 29% to a small UC",
    10: "Rural areas: primary flow to a tract outside a UA or UC (including self)",
    99: "Not coded: zero-population/no-data tract (verify sentinel against the 2000 source file)",
}

#: v2_0 secondary codes, code -> description (UW v2.0).
SECONDARY_RUCA_DESCRIPTIONS_V2_0: dict[str, str] = {
    "1.0": "No additional code",
    "1.1": "Secondary flow 30% through 49% to a larger UA",
    "2.0": "No additional code",
    "2.1": "Secondary flow 30% through 49% to a larger UA",
    "3.0": "No additional code",
    "4.0": "No additional code",
    "4.1": "Secondary flow 30% through 49% to a UA",
    "4.2": "Secondary flow 10% through 29% to a UA",
    "5.0": "No additional code",
    "5.1": "Secondary flow 30% through 49% to a UA",
    "5.2": "Secondary flow 10% through 29% to a UA",
    "6.0": "No additional code",
    "6.1": "Secondary flow 10% through 29% to a UA",
    "7.0": "No additional code",
    "7.1": "Secondary flow 30% through 49% to a UA",
    "7.2": "Secondary flow 30% through 49% to a large UC",
    "7.3": "Secondary flow 10% through 29% to a UA",
    "7.4": "Secondary flow 10% through 29% to a large UC",
    "8.0": "No additional code",
    "8.1": "Secondary flow 30% through 49% to a UA",
    "8.2": "Secondary flow 30% through 49% to a large UC",
    "8.3": "Secondary flow 10% through 29% to a UA",
    "8.4": "Secondary flow 10% through 29% to a large UC",
    "9.0": "No additional code",
    "9.1": "Secondary flow 10% through 29% to a UA",
    "9.2": "Secondary flow 10% through 29% to a large UC",
    "10.0": "No additional code",
    "10.1": "Secondary flow 30% through 49% to a UA",
    "10.2": "Secondary flow 30% through 49% to a large UC",
    "10.3": "Secondary flow 30% through 49% to a small UC",
    "10.4": "Secondary flow 10% through 29% to a UA",
    "10.5": "Secondary flow 10% through 29% to a large UC",
    "10.6": "Secondary flow 10% through 29% to a small UC",
    "99": "Not coded: zero-population/no-data tract (verify sentinel against the 2000 source file)",
}

# --- v1_11 (vintage 1990; UW WWAMI RHRC "RUCA Version 1.1 Code Definitions") --------------------
# Older terminology: "urban core / large town / small town / isolated small rural" and "Urban Place"
# rather than "Urban Cluster". Distinct secondary set: unique ``2.2`` (combined flows), and it lacks
# ``4.2``/``5.2``/``6.1``/``10.6``. Thresholds are also stated differently (5-30% vs 10-29%). ``x.0``
# is UW's "otherwise" (no qualifying secondary flow); stored as "No additional code" for consistency.

#: v1_11 primary codes, code -> description (UW v1.11).
PRIMARY_RUCA_DESCRIPTIONS_V1_11: dict[int, str] = {
    1: "Urban core Census tract: primary flow within a Census Bureau Urbanized Area (metro >= 50,000)",
    2: "Census tract strongly tied to urban core: primary flow to an Urbanized Area (>30%)",
    3: "Census tract weakly tied to urban core: primary flow to an Urbanized Area but 5-30%",
    4: "Large town Census tract: primary flow within a large Urban Place (10,000-49,999 & >30%)",
    5: "Census tract strongly tied to large town: primary flow to a large Urban Place (>30%)",
    6: "Census tract weakly tied to large town: primary flow to a large Urban Place (5-30%)",
    7: "Small town Census tract: primary flow within a small Urban Place (>= 2,500 & <10,000 & >30%)",
    8: "Census tract strongly tied to small town: primary flow to a small Urban Place (>30%)",
    9: "Census tract weakly tied to small town: primary flow to a small Urban Place (5-30%)",
    10: "Isolated small rural Census tract: no primary flow over 5% to any Urbanized Area or Urban Place",
    99: "Not coded: zero-population/no-data tract (verify sentinel against the 1990 source file)",
}

#: v1_11 secondary codes, code -> description (UW v1.11).
SECONDARY_RUCA_DESCRIPTIONS_V1_11: dict[str, str] = {
    "1.0": "No additional code",
    "1.1": "Secondary flow (30-50%) to a larger urbanized area",
    "2.0": "No additional code",
    "2.1": "Secondary flow (30-50%) to a larger urbanized area",
    "2.2": "Combined flows to urbanized areas of >30% and greater than the primary flow",
    "3.0": "No additional code",
    "4.0": "No additional code",
    "4.1": "Secondary flow (30-50%) to an urbanized area",
    "5.0": "No additional code",
    "5.1": "Secondary flow (30-50%) to an urbanized area",
    "6.0": "No additional code",
    "7.0": "No additional code",
    "7.1": "Secondary flow (30-50%) to an urbanized area",
    "7.2": "Secondary flow (30-50%) to a large urban place",
    "7.3": "Secondary flow (5-30%) to an urbanized area",
    "7.4": "Secondary flow (5-30%) to a large urban place",
    "8.0": "No additional code",
    "8.1": "Secondary flow (30-50%) to an urbanized area",
    "8.2": "Secondary flow (30-50%) to a large urban place",
    "8.3": "Secondary flow (5-30%) to an urbanized area",
    "8.4": "Secondary flow (5-30%) to a large urban place",
    "9.0": "No additional code",
    "9.1": "Secondary flow (5-30%) to an urbanized area",
    "9.2": "Secondary flow (5-30%) to a large urban place",
    "10.0": "No additional code",
    "10.1": "Secondary flow (30-50%) to an urbanized area",
    "10.2": "Secondary flow (30-50%) to a large urban place",
    "10.3": "Secondary flow (30-50%) to a small urban place",
    "10.4": "Secondary flow (5-30%) to an urbanized area",
    "10.5": "Secondary flow (5-30%) to a large urban place",
    "99": "Not coded: zero-population/no-data tract (verify sentinel against the 1990 source file)",
}

# --- Version registries (the version-aware source of truth) ------------------------------------

#: version -> {primary code -> description}.
PRIMARY_RUCA_DESCRIPTIONS_BY_VERSION: dict[str, dict[int, str]] = {
    "v1_11": PRIMARY_RUCA_DESCRIPTIONS_V1_11,
    "v2_0": PRIMARY_RUCA_DESCRIPTIONS_V2_0,
    "v3_x": PRIMARY_RUCA_DESCRIPTIONS_V3_X,
}

#: version -> {secondary code -> description}.
SECONDARY_RUCA_DESCRIPTIONS_BY_VERSION: dict[str, dict[str, str]] = {
    "v1_11": SECONDARY_RUCA_DESCRIPTIONS_V1_11,
    "v2_0": SECONDARY_RUCA_DESCRIPTIONS_V2_0,
    "v3_x": SECONDARY_RUCA_DESCRIPTIONS_V3_X,
}

#: version -> valid primary codes (controlled vocab; ADR 0016).
PRIMARY_RUCA_CODES_BY_VERSION: dict[str, frozenset[int]] = {
    v: frozenset(d) for v, d in PRIMARY_RUCA_DESCRIPTIONS_BY_VERSION.items()
}

#: version -> valid secondary codes (controlled vocab; ADR 0016).
SECONDARY_RUCA_CODES_BY_VERSION: dict[str, frozenset[str]] = {
    v: frozenset(d) for v, d in SECONDARY_RUCA_DESCRIPTIONS_BY_VERSION.items()
}

# Backward-compatible aliases: the **current** (v3_x) vocabulary. Version-aware callers should use
# the ``*_BY_VERSION`` registries (or pass ``version=``); these name the default/modern set only.
PRIMARY_RUCA_DESCRIPTIONS: dict[int, str] = PRIMARY_RUCA_DESCRIPTIONS_V3_X
SECONDARY_RUCA_DESCRIPTIONS: dict[str, str] = SECONDARY_RUCA_DESCRIPTIONS_V3_X
PRIMARY_RUCA_CODES: frozenset[int] = PRIMARY_RUCA_CODES_BY_VERSION[DEFAULT_RUCA_VERSION]
SECONDARY_RUCA_CODES: frozenset[str] = SECONDARY_RUCA_CODES_BY_VERSION[DEFAULT_RUCA_VERSION]


def version_for_vintage(vintage: int) -> str:
    """Return the RUCA code-scheme version for a decennial ``vintage`` (see RUCA_VERSION_BY_VINTAGE).

    Unknown vintages fall back to the current version (``v3_x``) so validators never crash on an
    unexpected vintage; the real vintages (1990/2000/2010/2020) are all mapped explicitly.

    Examples:
        >>> version_for_vintage(1990)
        'v1_11'
        >>> version_for_vintage(2020)
        'v3_x'
    """
    return RUCA_VERSION_BY_VINTAGE.get(vintage, DEFAULT_RUCA_VERSION)


@dataclass(frozen=True)
class RucaCodeDefinition:
    """One ``geography.us_ruca_code_definitions`` row: a version-specific code label (ADR 0038).

    Makes the version-specific primary/secondary descriptions live as queryable data (keyed by
    ``(ruca_version, code_level, code)``) rather than only as in-code constants, so a consumer can
    join a human-readable description per (vintage -> version, code). ``code`` is stored as a string
    for both levels (``"1"``/``"10"``/``"99"`` for primary, ``"1.0"``/``"10.3"`` for secondary).

    Attributes:
        ruca_version: RUCA code-scheme version ("v1_11", "v2_0", "v3_x").
        code_level: "primary" or "secondary".
        code: The code as a string.
        description: The version-specific published description.
    """

    ruca_version: str
    code_level: str
    code: str
    description: str


def code_definitions() -> list[RucaCodeDefinition]:
    """All version-specific RUCA code definitions (primary + secondary, every version).

    The single source for the ``geography.us_ruca_code_definitions`` lookup: a flat projection of
    the ``*_BY_VERSION`` description registries. Returned in a stable order (version, then primary
    before secondary, then insertion order of the code).
    """
    out: list[RucaCodeDefinition] = []
    for version in RUCA_VERSIONS:
        for primary_code, desc in PRIMARY_RUCA_DESCRIPTIONS_BY_VERSION[version].items():
            out.append(RucaCodeDefinition(version, "primary", str(primary_code), desc))
        for secondary_code, desc in SECONDARY_RUCA_DESCRIPTIONS_BY_VERSION[version].items():
            out.append(RucaCodeDefinition(version, "secondary", secondary_code, desc))
    return out


# ---------------------------------------------------------------------------
# Identifier + code normalization
# ---------------------------------------------------------------------------


def normalize_tract_geoid(value: str | int) -> str:
    """Return a zero-padded 11-digit census-tract GEOID (see geography.normalize_geoid)."""
    return normalize_geoid(value, TRACT_GEOID_WIDTH)


def normalize_zip_code(value: str | int) -> str:
    """Return a zero-padded 5-digit ZIP code.

    ZIP codes carry significant leading zeros (``00601`` is a Puerto Rico ZIP, not ``601``),
    so they are stored as strings; reuses the geography numeric-GEOID guard.

    Examples:
        >>> normalize_zip_code(601)
        '00601'
        >>> normalize_zip_code("30322")
        '30322'
    """
    return normalize_geoid(value, ZIP_CODE_WIDTH)


def normalize_primary_code(value: str | int | float) -> int:
    """Normalize a raw primary RUCA value to an integer (does not validate the result).

    Tolerates ints, strings, and float-like values a spreadsheet reader may produce
    (``1`` / ``"1"`` / ``1.0`` / ``"10"``). Pass the output to :func:`validate_primary_code`.

    Args:
        value: The raw primary code.

    Returns:
        The integer primary code.

    Raises:
        ValueError: If ``value`` is blank or non-numeric.

    Examples:
        >>> normalize_primary_code("10")
        10
        >>> normalize_primary_code(1.0)
        1
    """
    s = str(value).strip()
    if not s:
        raise ValueError("empty primary RUCA code")
    return int(float(s)) if "." in s else int(s)


def normalize_secondary_code(value: str | int | float) -> str:
    """Normalize a raw secondary RUCA value to its canonical dotted string.

    Secondary codes are ``primary.secondary`` (e.g. ``1.0``, ``10.3``) plus the special ``99``.
    Sources vary: the ZIP file writes ``10`` for ``10.0`` and a reader may deliver a float
    (``1.1``). This standardizes to the canonical dotted form **without** rounding (``1.0`` and
    ``1.1`` stay distinct). ``99`` is preserved as-is. Pass the output to
    :func:`validate_secondary_code`.

    Examples:
        >>> normalize_secondary_code("10")
        '10.0'
        >>> normalize_secondary_code(1.1)
        '1.1'
        >>> normalize_secondary_code("99")
        '99'
        >>> normalize_secondary_code(" 7.2 ")
        '7.2'
    """
    s = str(value).strip()
    if not s:
        raise ValueError("empty secondary RUCA code")
    if s == "99":
        return "99"
    if "." in s:
        whole, _, frac = s.partition(".")
        return f"{int(whole)}.{frac}"
    return f"{int(s)}.0"


def validate_primary_code(code: int, version: str = DEFAULT_RUCA_VERSION) -> bool:
    """Return True if ``code`` is a valid primary RUCA code for ``version`` (1-10 or 99).

    Primary codes are 1-10 (+ ``99``) in every version, but the parameter keeps the primary and
    secondary validators symmetric and future-proofs a version whose primary set diverges. Use
    :func:`version_for_vintage` to resolve the version from a vintage.

    Examples:
        >>> validate_primary_code(1)
        True
        >>> validate_primary_code(11)
        False
        >>> validate_primary_code(2, "v1_11")
        True
    """
    return code in PRIMARY_RUCA_CODES_BY_VERSION[version]


def validate_secondary_code(code: str, version: str = DEFAULT_RUCA_VERSION) -> bool:
    """Return True if ``code`` is a published secondary RUCA code for ``version``.

    Secondary sets differ by version (e.g. ``2.2`` is valid only in v1.11; ``10.6`` only in v2.0).
    Expects already-normalized input (see :func:`normalize_secondary_code`). Use
    :func:`version_for_vintage` to resolve the version from a vintage.

    Examples:
        >>> validate_secondary_code("10.3")
        True
        >>> validate_secondary_code("10.9")
        False
        >>> validate_secondary_code("2.2", "v1_11")
        True
        >>> validate_secondary_code("2.2", "v3_x")
        False
    """
    return code in SECONDARY_RUCA_CODES_BY_VERSION[version]


# ---------------------------------------------------------------------------
# Source-header resolution (headers drift across vintages/geographies -- ADR 0038)
# ---------------------------------------------------------------------------


def _normalize_header(name: str) -> str:
    """Lower-case a header and strip everything but ``[a-z0-9]`` for tolerant matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def resolve_column(
    headers: Iterable[str], *, equals: Iterable[str] = (), contains: Iterable[str] = ()
) -> str | None:
    """Return the actual header matching a logical field, or ``None`` if absent.

    Resolution is by normalized name (case/space/punctuation-insensitive): an exact match in
    ``equals`` wins; otherwise the first header containing any token in ``contains`` matches.
    Lets one parser read the year-suffixed tract headers (``Primary RUCA Code 2020``) and the
    compact ZIP headers (``PrimaryRUCA``) without per-vintage position constants.

    Args:
        headers: The source file's actual column names.
        equals: Normalized names that match exactly (highest priority).
        contains: Normalized tokens; a header containing any of them matches.

    Returns:
        The original (un-normalized) header name, or ``None``.
    """
    norm = {h: _normalize_header(h) for h in headers}
    equals_set = {_normalize_header(e) for e in equals}
    for original, n in norm.items():
        if n in equals_set:
            return original
    contains_set = [_normalize_header(c) for c in contains]
    for original, n in norm.items():
        if any(tok in n for tok in contains_set):
            return original
    return None


# Logical field -> (equals, contains) match spec, applied via :func:`resolve_column`. The
# ``contains`` tokens are ordered so the most specific column wins; e.g. "density" is excluded
# from population by checking the density column separately, and the tract id is the header that
# contains "tract" (the 5-digit state-county FIPS column does not).
_TRACT_FIELD_MATCHERS: dict[str, dict[str, tuple[str, ...]]] = {
    "geoid": {
        "equals": ("statecountytractfipscode", "censustract", "tractfips"),
        "contains": ("tract",),
    },
    "state": {"equals": ("state", "selectstate"), "contains": ()},
    "county": {"equals": ("county", "selectcounty"), "contains": ()},
    "primary_ruca": {"equals": ("primaryruca",), "contains": ("primaryruca",)},
    "secondary_ruca": {"equals": ("secondaryruca",), "contains": ("secondaryruca",)},
}

_ZIP_FIELD_MATCHERS: dict[str, dict[str, tuple[str, ...]]] = {
    "zip_code": {"equals": ("zipcode",), "contains": ("zipcode",)},
    "state": {"equals": ("state",), "contains": ()},
    "zip_code_type": {"equals": ("zipcodetype", "ziptype"), "contains": ("zipcodetype", "ziptype")},
    "po_name": {"equals": ("poname",), "contains": ("poname",)},
    "primary_ruca": {"equals": ("primaryruca",), "contains": ("primaryruca",)},
    "secondary_ruca": {"equals": ("secondaryruca",), "contains": ("secondaryruca",)},
}


def _resolve_population_columns(headers: Iterable[str]) -> dict[str, str | None]:
    """Resolve the three tract numeric columns, disambiguating population vs density.

    Both "Tract Population" and "Population Density" contain "population", so density is matched
    first (by "density") and excluded from the population match.
    """
    headers = list(headers)
    density = resolve_column(headers, equals=("populationdensity",), contains=("density",))
    land_area = resolve_column(headers, equals=("landarea",), contains=("landarea",))
    population = None
    for h in headers:
        n = _normalize_header(h)
        if "population" in n and "density" not in n:
            population = h
            break
    return {"population": population, "land_area_sqmi": land_area, "population_density": density}


# ---------------------------------------------------------------------------
# Records (mirror the table shape minus the audit columns the entrypoint stamps)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RucaTractRecord:
    """One ``geography.us_ruca_tract`` row for a given RUCA vintage.

    Attributes:
        geoid: 11-digit census-tract GEOID (PK component; joins to ``us_tract.geoid``).
        vintage: Decennial RUCA vintage (1990/2000/2010/2020); PK component.
        state_geoid: Derived 2-digit state GEOID (``geoid[:2]``).
        county_geoid: Derived 5-digit county GEOID (``geoid[:5]``).
        state: Source state abbreviation (label; may be None for older vintages).
        county: Source county name (label; may be None).
        primary_ruca: Primary RUCA code (1-10 or 99).
        secondary_ruca: Secondary RUCA code (canonical dotted string, or "99").
        population: Tract population for the vintage (None if absent).
        land_area_sqmi: Land area in square miles (None if absent).
        population_density: Population per square mile (None if absent).
    """

    geoid: str
    vintage: int
    state_geoid: str
    county_geoid: str
    state: str | None
    county: str | None
    primary_ruca: int
    secondary_ruca: str
    population: int | None
    land_area_sqmi: float | None
    population_density: float | None


@dataclass(frozen=True)
class RucaZipRecord:
    """One ``geography.us_ruca_zip`` row for a given RUCA vintage.

    Attributes:
        zip_code: 5-digit ZIP code (PK component); not a census GEOID.
        vintage: RUCA vintage (>= 2010; ZIP files began in 2010); PK component.
        state: Source state abbreviation (label; may be None).
        zip_code_type: ESRI ZIP type, e.g. "ZIP Code Area" / point (label; may be None).
        po_name: Post-office / place name (label; may be None).
        primary_ruca: Primary RUCA code (1-10 or 99).
        secondary_ruca: Secondary RUCA code (canonical dotted string, or "99").
    """

    zip_code: str
    vintage: int
    state: str | None
    zip_code_type: str | None
    po_name: str | None
    primary_ruca: int
    secondary_ruca: str


# ---------------------------------------------------------------------------
# Cell coercion
# ---------------------------------------------------------------------------

_NULLISH = frozenset({"", "na", "n/a", "null", "none", "."})


def _clean(value: Any) -> str:
    """String-ify a cell and strip whitespace (a reader may hand us ints/floats/None)."""
    if value is None:
        return ""
    return str(value).strip()


def _to_int(value: Any) -> int | None:
    """Parse an integer cell, tolerating thousands separators and blanks (-> None)."""
    s = _clean(value).replace(",", "")
    if s.lower() in _NULLISH:
        return None
    return int(float(s)) if "." in s else int(s)


def _to_float(value: Any) -> float | None:
    """Parse a float cell, tolerating thousands separators and blanks (-> None)."""
    s = _clean(value).replace(",", "")
    if s.lower() in _NULLISH:
        return None
    return float(s)


def _label(value: Any) -> str | None:
    """Return a stripped label or None for blank/missing."""
    s = _clean(value)
    return s or None


def _is_nullish(value: str) -> bool:
    """True for blank or sentinel-null id tokens (``""``, ``N/A``, ``NA``, ...).

    ERS files carry trailing footer/notes rows and unassigned rows whose identifier is a sentinel
    rather than a code; these are not data and are skipped during parse.
    """
    return value.lower() in _NULLISH


def _pad_numeric_id(value: str, width: int) -> str:
    """Zero-pad a numeric identifier to ``width``; otherwise return it cleaned, unchanged.

    Unlike :func:`normalize_geoid`, this never raises -- a malformed (non-numeric / over-long)
    identifier is passed through so the batch DQ check (:func:`find_bad_tract_geoids` /
    :func:`find_bad_zip_codes`) records and fails on it, rather than the parser aborting mid-file
    (ADR 0009). Callers skip sentinel-null ids first via :func:`_is_nullish`.
    """
    s = value.strip()
    return s.zfill(width) if (s.isdigit() and len(s) <= width) else s


# ---------------------------------------------------------------------------
# Parsing (row dicts in -> records; the entrypoint reads CSV/XLSX into row dicts)
# ---------------------------------------------------------------------------


def parse_tract_rows(rows: Iterable[dict[str, Any]], vintage: int) -> list[RucaTractRecord]:
    """Parse RUCA census-tract rows into :class:`RucaTractRecord` (ADR 0011).

    Columns are resolved by header name (alias-tolerant) from the first row's keys, so the same
    code reads any vintage's layout. Identifiers are normalized; ``state_geoid``/``county_geoid``
    are derived from the GEOID. Codes are normalized but **not** validated here -- validation runs
    over the batch as DQ so violations are recorded, not silently dropped (ADR 0009). Fully blank
    rows (no GEOID) are skipped.

    Args:
        rows: Source rows as header -> cell dicts.
        vintage: The RUCA vintage these rows represent (e.g. 2020).

    Returns:
        Records in source order.
    """
    rows = list(rows)
    if not rows:
        return []
    headers = list(rows[0].keys())
    cols = {
        field: resolve_column(headers, equals=spec["equals"], contains=spec["contains"])
        for field, spec in _TRACT_FIELD_MATCHERS.items()
    }
    cols.update(_resolve_population_columns(headers))
    if cols["geoid"] is None or cols["primary_ruca"] is None or cols["secondary_ruca"] is None:
        raise ValueError(
            f"RUCA tract file missing required column(s); resolved {cols} from headers {headers}"
        )

    records: list[RucaTractRecord] = []
    for row in rows:
        raw_geoid = _clean(row.get(cols["geoid"]))
        if _is_nullish(raw_geoid):
            continue  # blank trailing / footer / unassigned row (e.g. "N/A")
        geoid = _pad_numeric_id(raw_geoid, TRACT_GEOID_WIDTH)
        records.append(
            RucaTractRecord(
                geoid=geoid,
                vintage=vintage,
                state_geoid=geoid[:STATE_GEOID_WIDTH],
                county_geoid=geoid[:COUNTY_GEOID_WIDTH],
                state=_label(row.get(cols["state"])) if cols["state"] else None,
                county=_label(row.get(cols["county"])) if cols["county"] else None,
                primary_ruca=normalize_primary_code(row[cols["primary_ruca"]]),
                secondary_ruca=normalize_secondary_code(row[cols["secondary_ruca"]]),
                population=_to_int(row.get(cols["population"])) if cols["population"] else None,
                land_area_sqmi=(
                    _to_float(row.get(cols["land_area_sqmi"])) if cols["land_area_sqmi"] else None
                ),
                population_density=(
                    _to_float(row.get(cols["population_density"]))
                    if cols["population_density"]
                    else None
                ),
            )
        )
    return records


def parse_zip_rows(rows: Iterable[dict[str, Any]], vintage: int) -> list[RucaZipRecord]:
    """Parse RUCA ZIP-code rows into :class:`RucaZipRecord` (ADR 0011).

    Mirrors :func:`parse_tract_rows`: alias-tolerant header resolution, normalize-not-validate,
    skip blank rows. ZIP files exist only from vintage 2010 on.

    Args:
        rows: Source rows as header -> cell dicts.
        vintage: The RUCA vintage these rows represent (>= 2010).

    Returns:
        Records in source order.
    """
    rows = list(rows)
    if not rows:
        return []
    headers = list(rows[0].keys())
    cols = {
        field: resolve_column(headers, equals=spec["equals"], contains=spec["contains"])
        for field, spec in _ZIP_FIELD_MATCHERS.items()
    }
    if cols["zip_code"] is None or cols["primary_ruca"] is None or cols["secondary_ruca"] is None:
        raise ValueError(
            f"RUCA ZIP file missing required column(s); resolved {cols} from headers {headers}"
        )

    records: list[RucaZipRecord] = []
    for row in rows:
        raw_zip = _clean(row.get(cols["zip_code"]))
        if _is_nullish(raw_zip):
            continue  # blank trailing / footer row
        records.append(
            RucaZipRecord(
                zip_code=_pad_numeric_id(raw_zip, ZIP_CODE_WIDTH),
                vintage=vintage,
                state=_label(row.get(cols["state"])) if cols["state"] else None,
                zip_code_type=(
                    _label(row.get(cols["zip_code_type"])) if cols["zip_code_type"] else None
                ),
                po_name=_label(row.get(cols["po_name"])) if cols["po_name"] else None,
                primary_ruca=normalize_primary_code(row[cols["primary_ruca"]]),
                secondary_ruca=normalize_secondary_code(row[cols["secondary_ruca"]]),
            )
        )
    return records


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_tract_keys(records: list[RucaTractRecord]) -> list[tuple[str, int]]:
    """Return ``(geoid, vintage)`` keys appearing more than once (blocking PK)."""
    return _duplicates((r.geoid, r.vintage) for r in records)


def find_duplicate_zip_keys(records: list[RucaZipRecord]) -> list[tuple[str, int]]:
    """Return ``(zip_code, vintage)`` keys appearing more than once (blocking PK)."""
    return _duplicates((r.zip_code, r.vintage) for r in records)


def _duplicates(keys: Iterable[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    seen: dict[tuple[Any, ...], int] = {}
    for key in keys:
        seen[key] = seen.get(key, 0) + 1
    return [key for key, count in seen.items() if count > 1]


def find_bad_tract_geoids(records: list[RucaTractRecord]) -> list[str]:
    """Return tract GEOIDs that are not exactly 11 digits (blocking format)."""
    return [
        r.geoid for r in records if not (len(r.geoid) == TRACT_GEOID_WIDTH and r.geoid.isdigit())
    ]


def find_bad_zip_codes(records: list[RucaZipRecord]) -> list[str]:
    """Return ZIP codes that are not exactly 5 digits (blocking format)."""
    return [
        r.zip_code
        for r in records
        if not (len(r.zip_code) == ZIP_CODE_WIDTH and r.zip_code.isdigit())
    ]


def find_invalid_primary_codes(records: Iterable[Any]) -> list[tuple[str, int]]:
    """Return ``(id, primary_ruca)`` for records whose primary code is out of vocab (blocking).

    Each record is validated against **its own vintage's** RUCA version (ADR 0038): a 1990 row is
    checked against v1.11, a 2020 row against v3_x. Works for either record type: the id is the
    GEOID (tract) or ZIP code, whichever the record carries.
    """
    out: list[tuple[str, int]] = []
    for r in records:
        if not validate_primary_code(r.primary_ruca, version_for_vintage(r.vintage)):
            out.append((_record_id(r), r.primary_ruca))
    return out


def find_invalid_secondary_codes(records: Iterable[Any]) -> list[tuple[str, str]]:
    """Return ``(id, secondary_ruca)`` for records with an out-of-vocab secondary (blocking).

    Each record is validated against **its own vintage's** RUCA version (ADR 0038), so v1.11's
    ``2.2`` and v2.0's ``10.6`` pass for 1990/2000 rows but fail for a modern (v3_x) vintage.
    """
    out: list[tuple[str, str]] = []
    for r in records:
        if not validate_secondary_code(r.secondary_ruca, version_for_vintage(r.vintage)):
            out.append((_record_id(r), r.secondary_ruca))
    return out


def _record_id(record: Any) -> str:
    """The natural id of a RUCA record: tract GEOID or ZIP code."""
    return getattr(record, "geoid", None) or record.zip_code


def primary_distribution(records: Iterable[Any]) -> dict[int, int]:
    """``{primary_ruca: count}`` over records (backs the distribution WARN).

    (Version-aware vocabulary added under ADR 0038; see the ``*_BY_VERSION`` registries above.)
    """
    dist: dict[int, int] = {}
    for r in records:
        dist[r.primary_ruca] = dist.get(r.primary_ruca, 0) + 1
    return dist
