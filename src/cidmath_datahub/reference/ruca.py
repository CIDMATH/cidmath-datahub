"""Rural-Urban Commuting Area (RUCA) codes (USDA ERS; ADR 0020, ADR 0038).

Pure, unit-testable logic for the ``geography`` schema's two RUCA tables: a census-tract
table (``geography.us_ruca_tract``) and a ZIP-code table (``geography.us_ruca_zip``). RUCA
is the USDA Economic Research Service's sub-county rural/urban classification -- a tract is
assigned a **primary** code (whole number ``1``-``10``, plus ``99`` for water/zero-population
tracts) encoding its urban-core class and largest commuting flow, and a **secondary** code
(``1.0`` .. ``10.3``, plus ``99``) subdividing it by the second-largest flow. Both levels are
stored verbatim; combining them flexibly is the point of the scheme, so we derive no single
rural/urban flag (ADR 0038).

This module is the single source of truth for RUCA code sets, identifier normalization, code
validation, source-header resolution, and the pure DQ helpers. It holds **no Spark and no IO**
(ADR 0011); the entrypoint ``bundles/_reference/src/build_ruca.py`` does the public HTTPS
download, reads the CSV/XLSX/legacy ``.xls`` file into row dicts, and writes the Delta tables.

How RUCA differs from the entity geography tables (so it is not mis-modeled):

* **Tract grain reuses the census GEOID.** ``us_ruca_tract`` keys on ``(geoid, vintage)`` -- the
  same 11-digit tract GEOID + decennial ``vintage`` as ``geography.us_tract`` -- so it is an
  attribute extension that joins ``USING (geoid, vintage)``. ``state_geoid``/``county_geoid`` are
  derived from the GEOID exactly as in :mod:`cidmath_datahub.reference.geography`.
* **ZIP grain is not a census GEOID.** ``us_ruca_zip`` keys on ``(zip_code, vintage)``. ZIP codes
  are USPS routes, do not nest in census geography, and do **not** join to ``us_zcta`` -- hence a
  descriptively named key, not ``geoid``.
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
# Canonical RUCA code sets (single-sourced here; ADR 0006/0016) -- from the ERS
# documentation "Primary RUCA codes, 2020" and "Secondary RUCA codes, 2020" tables.
# ---------------------------------------------------------------------------

#: The 10 primary RUCA codes + ``99`` (water/zero-population), code -> description.
PRIMARY_RUCA_DESCRIPTIONS: dict[int, str] = {
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

#: Valid primary codes (controlled vocab; ADR 0016).
PRIMARY_RUCA_CODES: frozenset[int] = frozenset(PRIMARY_RUCA_DESCRIPTIONS)

#: The 21 published secondary codes + ``99``, code -> description (canonical dotted form).
SECONDARY_RUCA_DESCRIPTIONS: dict[str, str] = {
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

#: Valid secondary codes (controlled vocab; ADR 0016).
SECONDARY_RUCA_CODES: frozenset[str] = frozenset(SECONDARY_RUCA_DESCRIPTIONS)

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


def validate_primary_code(code: int) -> bool:
    """Return True if ``code`` is one of the valid primary RUCA codes (1-10 or 99).

    Examples:
        >>> validate_primary_code(1)
        True
        >>> validate_primary_code(11)
        False
    """
    return code in PRIMARY_RUCA_CODES


def validate_secondary_code(code: str) -> bool:
    """Return True if ``code`` is one of the published secondary RUCA codes.

    Expects already-normalized input (see :func:`normalize_secondary_code`).

    Examples:
        >>> validate_secondary_code("10.3")
        True
        >>> validate_secondary_code("10.9")
        False
    """
    return code in SECONDARY_RUCA_CODES


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
        if not raw_geoid:
            continue  # blank trailing row
        geoid = normalize_tract_geoid(raw_geoid)
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
        if not raw_zip:
            continue
        records.append(
            RucaZipRecord(
                zip_code=normalize_zip_code(raw_zip),
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

    Works for either record type: the id is the GEOID (tract) or ZIP code, whichever the record
    carries.
    """
    out: list[tuple[str, int]] = []
    for r in records:
        if not validate_primary_code(r.primary_ruca):
            out.append((_record_id(r), r.primary_ruca))
    return out


def find_invalid_secondary_codes(records: Iterable[Any]) -> list[tuple[str, str]]:
    """Return ``(id, secondary_ruca)`` for records with an out-of-vocab secondary (blocking)."""
    out: list[tuple[str, str]] = []
    for r in records:
        if not validate_secondary_code(r.secondary_ruca):
            out.append((_record_id(r), r.secondary_ruca))
    return out


def _record_id(record: Any) -> str:
    """The natural id of a RUCA record: tract GEOID or ZIP code."""
    return getattr(record, "geoid", None) or record.zip_code


def primary_distribution(records: Iterable[Any]) -> dict[int, int]:
    """``{primary_ruca: count}`` over records (backs the distribution WARN)."""
    dist: dict[int, int] = {}
    for r in records:
        dist[r.primary_ruca] = dist.get(r.primary_ruca, 0) + 1
    return dist
