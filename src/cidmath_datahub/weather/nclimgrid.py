"""Pure parsing for NOAA nClimGrid-Daily area-average CSVs (ADR 0025).

Raw-layer logic only: parse the headerless area-average files into faithful
long-form rows, preserving the NCEI region codes exactly as published. The
NCEI->FIPS conformance to the geography reference is a *processed*-layer concern
(a later slice) and deliberately does not live here — raw stays faithful to the
source.

File shape (verified against real 1975/2024/2026 files): one CSV per
``variable x region-type x month``, named ``<var>-<YYYYMM>-<rrr>-<status>.csv``
(``var`` in prcp/tavg/tmax/tmin; ``rrr`` in cty/ste/...; ``status`` scaled or
prelim). Headerless; each row is exactly 37 comma-separated fields:

    region_type, region_code, region_name, year, month, VARIABLE, d1, d2, ... d31

i.e. 6 metadata fields + 31 fixed daily-value columns. Months shorter than 31
days pad the trailing day columns with the missing-value sentinel ``-999.99``;
genuinely missing observations use the same sentinel. State-file values are
space-padded, county-file values are not — both parse fine after a strip.
"""

from __future__ import annotations

import calendar
import csv
import re
from collections.abc import Iterable
from datetime import date
from typing import Any

# nClimGrid missing-value marker (also pads day-columns of short months). Real
# temperatures/precip never approach -999, so a <= -999 test is a safe sentinel.
SENTINEL = -999.99

_METADATA_COLUMNS = 6
_DAY_COLUMNS = 31
_ROW_FIELDS = _METADATA_COLUMNS + _DAY_COLUMNS  # 37, fixed

VARIABLES: frozenset[str] = frozenset({"prcp", "tavg", "tmax", "tmin"})
# Region types this bundle ingests (ADR 0025 v1). cen/div/hc1/nca/reg/wfo later.
REGION_TYPES: frozenset[str] = frozenset({"cty", "ste"})

_FILENAME_RE = re.compile(
    r"^(?P<variable>prcp|tavg|tmax|tmin)-(?P<year>\d{4})(?P<month>\d{2})-"
    r"(?P<region_type>[a-z0-9]{2,3})-(?P<status>scaled|prelim)\.csv$"
)


def parse_average_filename(name: str) -> dict[str, Any] | None:
    """Parse an nClimGrid averages filename into its parts, or ``None``.

    ``"tavg-202401-cty-scaled.csv"`` ->
    ``{variable: 'tavg', year: 2024, month: 1, region_type: 'cty', status: 'scaled'}``.
    Returns ``None`` for anything that doesn't match (e.g. the ``ncdd-*-version.txt``
    files), so a directory listing can be filtered with it.
    """
    if not isinstance(name, str):
        return None
    m = _FILENAME_RE.match(name.strip())
    if m is None:
        return None
    return {
        "variable": m["variable"],
        "year": int(m["year"]),
        "month": int(m["month"]),
        "region_type": m["region_type"],
        "status": m["status"],
    }


_CSV_LINK_RE = re.compile(r'href="([^"?/]+\.csv)"', re.IGNORECASE)


def extract_csv_links(html: str) -> list[str]:
    """Extract ``.csv`` filenames from an Apache-style directory autoindex page.

    The nClimGrid ``access/averages/<year>/`` folders are served as autoindex
    HTML; the ingest lists the page and discovers files rather than guessing
    filenames (handles the scaled-vs-prelim suffix without assuming which).
    Returns de-duplicated basenames, order-preserving; query strings / parent
    links are ignored.
    """
    if not isinstance(html, str):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _CSV_LINK_RE.finditer(html):
        name = m.group(1)
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _to_value(raw: str) -> float | None:
    """Parse one daily value; the ``-999.99`` sentinel (any <= -999) -> ``None``."""
    v = float(raw.strip())
    return None if v <= -999.0 else v


def parse_average_csv(lines: Iterable[str], *, source_file: str) -> list[dict[str, Any]]:
    """Parse one headerless nClimGrid area-average CSV into long-form raw rows.

    Emits one row per *real* calendar day (day <= days-in-month for the row's
    year/month), with the sentinel mapped to ``None`` so genuine missingness is
    explicit and the padding days of short months are dropped. The NCEI
    ``region_code`` is preserved verbatim (conformance to FIPS/geography happens
    in the processed layer). ``variable`` is lower-cased to the controlled set.

    Raises ``ValueError`` on a row whose field count isn't the expected 37 — a
    source format change fails loudly here rather than silently mis-parsing.
    """
    out: list[dict[str, Any]] = []
    for fields in csv.reader(lines):
        if not fields or (len(fields) == 1 and not fields[0].strip()):
            continue  # skip blank lines
        if len(fields) != _ROW_FIELDS:
            raise ValueError(
                f"{source_file}: expected {_ROW_FIELDS} fields, got {len(fields)}: {fields[:8]}"
            )
        region_type = fields[0].strip()
        region_code = fields[1].strip()
        region_name = fields[2].strip()
        year = int(fields[3])
        month = int(fields[4])
        variable = fields[5].strip().lower()
        daily = fields[_METADATA_COLUMNS:]
        ndays = calendar.monthrange(year, month)[1]
        for day in range(1, ndays + 1):
            out.append(
                {
                    "region_type": region_type,
                    "region_code": region_code,
                    "region_name": region_name,
                    "variable": variable,
                    "obs_date": date(year, month, day),
                    "value": _to_value(daily[day - 1]),
                    "source_file": source_file,
                }
            )
    return out


# ---------------------------------------------------------------------------
# Conformance to the geography reference (ADR 0025 processed layer)
# ---------------------------------------------------------------------------

# Variable -> physical unit (nClimGrid v1 user guide): precipitation in mm,
# temperatures in degrees Celsius.
UNITS: dict[str, str] = {"prcp": "mm", "tavg": "degC", "tmax": "degC", "tmin": "degC"}

# nClimGrid region type -> conformed geography level (FK target).
GEO_LEVEL_BY_REGION_TYPE: dict[str, str] = {"cty": "us_county", "ste": "us_state"}


def parse_ncei_fips_crosswalk(lines: Iterable[str]) -> dict[str, str]:
    """Parse NOAA's ``us-state-codes_ncei-to-fips.csv`` into ``{NCEI_code: FIPS_code}``.

    Keys on the **numeric** ``NCEI_code`` -> ``FIPS_code`` columns only. The
    file's ``state_name`` column has a known upstream bug (Illinois/Indiana
    swapped), so names are deliberately ignored; the numeric codes are
    internally correct. Both codes are normalized to zero-padded 2-char strings
    (leading zeros are significant — e.g. Alabama ``01``). Expects a header:
    ``state_name, postal_code, NCEI_code, FIPS_code``.
    """
    out: dict[str, str] = {}
    for row in csv.DictReader(lines):
        ncei = (row.get("NCEI_code") or "").strip()
        fips = (row.get("FIPS_code") or "").strip()
        if ncei and fips:
            out[ncei.zfill(2)] = fips.zfill(2)
    return out


# NCEI files a few regions under a different state than their FIPS state, so the
# state cross-reference alone mis-conforms them. Data-derived fixup (keyed on the
# raw NCEI region_code, verified against the published region_name), NOT a guess:
#   - "18511" -> region_name "DC: District of Columbia": filed under Maryland
#     (NCEI state 18 -> FIPS 24), but DC is its own FIPS state; geoid is 11001.
# A new entry is added only when the blocking geoid-FK DQ surfaces an absent geoid
# and the data's region_name identifies the true entity.
_NCEI_COUNTY_FIPS_OVERRIDES: dict[str, str] = {
    "18511": "11001",  # DC: District of Columbia
}


def conform_region(region_type: str, region_code: str, ncei_to_fips: dict[str, str]) -> str | None:
    """Translate an nClimGrid NCEI region code to its FIPS ``geoid``, or ``None``.

    - ``ste``: geoid = FIPS state code (NCEI 2-digit mapped via ``ncei_to_fips``).
    - ``cty``: geoid = FIPS state (2) + the 3-digit FIPS **county** suffix. Only
      the state prefix is NCEI-coded; the county suffix is already the FIPS
      county code (verified against the real data — e.g. ``02001`` Apache AZ ->
      ``04`` + ``001`` = ``04001``).

    Returns ``None`` when the NCEI state prefix isn't in the map or the county
    suffix is malformed; the build's blocking DQ surfaces any such row.
    """
    if not isinstance(region_code, str):
        return None
    code = region_code.strip()
    if region_type == "cty" and code in _NCEI_COUNTY_FIPS_OVERRIDES:
        return _NCEI_COUNTY_FIPS_OVERRIDES[code]
    fips_state = ncei_to_fips.get(code[:2])
    if fips_state is None:
        return None
    if region_type == "ste":
        return fips_state
    if region_type == "cty":
        suffix = code[2:5]
        if len(suffix) != 3 or not suffix.isdigit():
            return None
        return fips_state + suffix
    return None
