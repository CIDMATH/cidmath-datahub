"""Geography reference logic (ADR 0020, class: authoritative slow-changing).

Pure, unit-testable helpers for the ``geography`` schema: GEOID normalization
and parent derivation, the static HHS-region grouping of states, and crosswalk
weight validation. There is no Spark, IO, or geospatial dependency here -- the
bundle entrypoint (``bundles/_reference/src/build_geography.py``) pulls
boundaries and crosswalks from IPUMS NHGIS and writes Delta tables; this module
owns the deterministic parts that can be tested without a workspace or API key.

GEOIDs are always strings. Leading zeros are significant (Alabama is ``"01"``,
not ``1``), so these helpers never let a GEOID become an integer (ADR 0020
storage guardrails).

HHS regions are the ten fixed U.S. Department of Health and Human Services
regions -- a static federal grouping of states, not a census geography -- so we
build them in code rather than ingest them (ADR 0020).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

# GEOID widths by level (number of digits). Census GEOIDs are fixed-width and
# zero-padded (ADR 0020).
STATE_GEOID_WIDTH = 2
COUNTY_GEOID_WIDTH = 5
TRACT_GEOID_WIDTH = 11
ZCTA_GEOID_WIDTH = 5
BG_GEOID_WIDTH = 12


def normalize_geoid(value: str | int, width: int) -> str:
    """Return a zero-padded, validated GEOID string of exactly ``width`` digits.

    Sources sometimes deliver FIPS codes as integers, dropping significant
    leading zeros (state ``1`` instead of ``"01"``). This restores them and
    enforces that the result is purely numeric and no longer than ``width``.

    Args:
        value: The raw code, as a string or integer.
        width: Required number of digits (2 for state, 5 for county).

    Returns:
        The zero-padded GEOID as a string.

    Raises:
        ValueError: If ``value`` is non-numeric or longer than ``width`` digits.

    Examples:
        >>> normalize_geoid(1, 2)
        '01'
        >>> normalize_geoid("01001", 5)
        '01001'
    """
    s = str(value).strip()
    if not s.isdigit():
        raise ValueError(f"GEOID {value!r} is not purely numeric")
    if len(s) > width:
        raise ValueError(f"GEOID {value!r} has more than {width} digits")
    return s.zfill(width)


def validate_state_geoid(value: str | int) -> str:
    """Return a normalized 2-digit state GEOID (see :func:`normalize_geoid`)."""
    return normalize_geoid(value, STATE_GEOID_WIDTH)


def validate_county_geoid(value: str | int) -> str:
    """Return a normalized 5-digit county GEOID (see :func:`normalize_geoid`)."""
    return normalize_geoid(value, COUNTY_GEOID_WIDTH)


def validate_tract_geoid(value: str | int) -> str:
    """Return a normalized 11-digit census tract GEOID (see :func:`normalize_geoid`)."""
    return normalize_geoid(value, TRACT_GEOID_WIDTH)


def validate_zcta_geoid(value: str | int) -> str:
    """Return a normalized 5-digit ZCTA GEOID (see :func:`normalize_geoid`)."""
    return normalize_geoid(value, ZCTA_GEOID_WIDTH)


def validate_bg_geoid(value: str | int) -> str:
    """Return a normalized 12-digit block group GEOID (see :func:`normalize_geoid`)."""
    return normalize_geoid(value, BG_GEOID_WIDTH)


def state_geoid_of_county(county_geoid: str | int) -> str:
    """Return the 2-digit state GEOID that a 5-digit county GEOID belongs to.

    A county GEOID is its state GEOID followed by the 3-digit county code, so the
    parent state is the first two digits. This is how the derived
    ``county.state_geoid`` foreign key is produced (ADR 0020).

    Examples:
        >>> state_geoid_of_county("13121")  # Fulton County, GA
        '13'
    """
    return validate_county_geoid(county_geoid)[:STATE_GEOID_WIDTH]


def state_geoid_of_tract(tract_geoid: str | int) -> str:
    """Return the 2-digit state GEOID for an 11-digit tract GEOID (its first 2 digits)."""
    return validate_tract_geoid(tract_geoid)[:STATE_GEOID_WIDTH]


def county_geoid_of_tract(tract_geoid: str | int) -> str:
    """Return the 5-digit county GEOID for an 11-digit tract GEOID (its first 5 digits)."""
    return validate_tract_geoid(tract_geoid)[:COUNTY_GEOID_WIDTH]


def gisjoin_to_geoid(gisjoin: str, level: str) -> str:
    """Convert an NHGIS GISJOIN key to the standard Census GEOID for a level.

    NHGIS GISJOIN adds a ``"G"`` prefix and a separator digit after each FIPS
    component so identifiers stay text and sort correctly. For current vintages
    the separators are ``"0"`` (the trailing digit differentiates historical
    areas)::

        state   G + SS + 0                          -> 2-digit GEOID
        county  G + SS + 0 + CCC + 0                -> 5-digit GEOID
        tract   G + SS + 0 + CCC + 0 + TTTTTT       -> 11-digit GEOID
        zcta    G + ZZZZZ                           -> 5-digit GEOID (non-nested)
        bg      G + SS + 0 + CCC + 0 + TTTTTT + B   -> 12-digit GEOID

    The GEOID is recovered by position (separators dropped), so this is correct
    even when the differentiator digit is non-zero. State/county/tract are
    verified against IPUMS NHGIS GISJOIN documentation; ZCTA and bg are
    confirmed on first download by the length/digit check.

    Args:
        gisjoin: The NHGIS GISJOIN key (must start with ``"G"``).
        level: ``"state"``, ``"county"``, ``"tract"``, ``"zcta"``, or ``"bg"``.

    Returns:
        The zero-padded Census GEOID (2-digit state, 5-digit county, 11-digit
        tract, 5-digit ZCTA, 12-digit bg).

    Raises:
        ValueError: If the GISJOIN is malformed for the level, or the level is
            not a supported level.
    """
    g = gisjoin.strip().upper()
    if not g.startswith("G"):
        raise ValueError(f"GISJOIN {gisjoin!r} does not start with 'G'")
    body = g[1:]
    if level == "state":
        if len(body) != 3 or not body.isdigit():
            raise ValueError(f"state GISJOIN {gisjoin!r} is malformed")
        return body[0:2]
    if level == "county":
        if len(body) != 7 or not body.isdigit():
            raise ValueError(f"county GISJOIN {gisjoin!r} is malformed")
        return body[0:2] + body[3:6]  # SS [sep] CCC [sep]
    if level == "tract":
        if len(body) != 13 or not body.isdigit():
            raise ValueError(f"tract GISJOIN {gisjoin!r} is malformed")
        return body[0:2] + body[3:6] + body[7:13]  # SS [sep] CCC [sep] TTTTTT
    if level == "zcta":
        if len(body) != 5 or not body.isdigit():
            raise ValueError(f"zcta GISJOIN {gisjoin!r} is malformed")
        return body  # G + 5-digit ZCTA (non-nested); confirmed on first extract
    if level == "bg":
        if len(body) != 14 or not body.isdigit():
            raise ValueError(f"bg GISJOIN {gisjoin!r} is malformed")
        return body[0:2] + body[3:6] + body[7:13] + body[13:14]  # SS [sep] CCC [sep] TTTTTT B
    raise ValueError(
        f"unsupported level {level!r} (expected 'state', 'county', 'tract', 'zcta', or 'bg')"
    )


# --- HHS regions -----------------------------------------------------------

# Region number -> conventional name (the HHS regional-office HQ city). HHS
# regions have no official names beyond "Region N"; the HQ city is the common
# shorthand and is more informative in a lookup table.
HHS_REGION_HQ: dict[int, str] = {
    1: "Boston",
    2: "New York",
    3: "Philadelphia",
    4: "Atlanta",
    5: "Chicago",
    6: "Dallas",
    7: "Kansas City",
    8: "Denver",
    9: "San Francisco",
    10: "Seattle",
}

# Region number -> member state/territory USPS codes. The fixed federal grouping
# (HHS regional offices). Territories that exist in U.S. census geography (AS,
# GU, MP) are included for completeness; slice 1 covers the 50 states + DC. The
# freely associated states (FM, MH, PW) are HHS Region 9 administratively but are
# not U.S. census geography, so they are intentionally omitted here.
_HHS_REGION_STATES: dict[int, tuple[str, ...]] = {
    1: ("CT", "ME", "MA", "NH", "RI", "VT"),
    2: ("NJ", "NY", "PR", "VI"),
    3: ("DC", "DE", "MD", "PA", "VA", "WV"),
    4: ("AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN"),
    5: ("IL", "IN", "MI", "MN", "OH", "WI"),
    6: ("AR", "LA", "NM", "OK", "TX"),
    7: ("IA", "KS", "MO", "NE"),
    8: ("CO", "MT", "ND", "SD", "UT", "WY"),
    9: ("AS", "AZ", "CA", "GU", "HI", "MP", "NV"),
    10: ("AK", "ID", "OR", "WA"),
}

# Inverted lookup: USPS code -> region number.
_STATE_TO_HHS_REGION: dict[str, int] = {
    usps: region for region, members in _HHS_REGION_STATES.items() for usps in members
}


def hhs_region_for_state(stusps: str) -> int:
    """Return the HHS region number (1-10) for a USPS state/territory code.

    Args:
        stusps: Two-letter USPS code, e.g. ``"GA"`` (case-insensitive).

    Raises:
        ValueError: If the code is not assigned to an HHS region.
    """
    key = stusps.strip().upper()
    try:
        return _STATE_TO_HHS_REGION[key]
    except KeyError:
        raise ValueError(f"No HHS region for state {stusps!r}") from None


def hhs_region_name(region: int) -> str:
    """Return the conventional (HQ-city) name for an HHS region number.

    Raises:
        ValueError: If ``region`` is not in 1-10.
    """
    try:
        return HHS_REGION_HQ[region]
    except KeyError:
        raise ValueError(f"{region!r} is not a valid HHS region (expected 1-10)") from None


def generate_hhs_regions() -> list[dict[str, Any]]:
    """Return the ten rows for ``geography.us_hhs_region``, ordered by region number.

    Each row carries the region number, its HQ-city name, and the sorted member
    USPS codes. The authoritative state->region membership is materialized as the
    ``hhs_region`` column on ``geography.us_state``; ``member_states`` here is a
    convenience for QA and a standalone lookup.
    """
    return [
        {
            "hhs_region": region,
            "name": HHS_REGION_HQ[region],
            "member_states": sorted(_HHS_REGION_STATES[region]),
        }
        for region in sorted(HHS_REGION_HQ)
    ]


# --- State FIPS reference --------------------------------------------------

# Canonical state/territory identity keyed by 2-digit GEOID (FIPS). Lets us
# derive USPS code and name from a GISJOIN alone, independent of which attributes
# a given NHGIS shapefile happens to carry. 50 states + DC + the five territories
# that appear in U.S. census geography.
STATE_FIPS: dict[str, tuple[str, str]] = {
    "01": ("AL", "Alabama"),
    "02": ("AK", "Alaska"),
    "04": ("AZ", "Arizona"),
    "05": ("AR", "Arkansas"),
    "06": ("CA", "California"),
    "08": ("CO", "Colorado"),
    "09": ("CT", "Connecticut"),
    "10": ("DE", "Delaware"),
    "11": ("DC", "District of Columbia"),
    "12": ("FL", "Florida"),
    "13": ("GA", "Georgia"),
    "15": ("HI", "Hawaii"),
    "16": ("ID", "Idaho"),
    "17": ("IL", "Illinois"),
    "18": ("IN", "Indiana"),
    "19": ("IA", "Iowa"),
    "20": ("KS", "Kansas"),
    "21": ("KY", "Kentucky"),
    "22": ("LA", "Louisiana"),
    "23": ("ME", "Maine"),
    "24": ("MD", "Maryland"),
    "25": ("MA", "Massachusetts"),
    "26": ("MI", "Michigan"),
    "27": ("MN", "Minnesota"),
    "28": ("MS", "Mississippi"),
    "29": ("MO", "Missouri"),
    "30": ("MT", "Montana"),
    "31": ("NE", "Nebraska"),
    "32": ("NV", "Nevada"),
    "33": ("NH", "New Hampshire"),
    "34": ("NJ", "New Jersey"),
    "35": ("NM", "New Mexico"),
    "36": ("NY", "New York"),
    "37": ("NC", "North Carolina"),
    "38": ("ND", "North Dakota"),
    "39": ("OH", "Ohio"),
    "40": ("OK", "Oklahoma"),
    "41": ("OR", "Oregon"),
    "42": ("PA", "Pennsylvania"),
    "44": ("RI", "Rhode Island"),
    "45": ("SC", "South Carolina"),
    "46": ("SD", "South Dakota"),
    "47": ("TN", "Tennessee"),
    "48": ("TX", "Texas"),
    "49": ("UT", "Utah"),
    "50": ("VT", "Vermont"),
    "51": ("VA", "Virginia"),
    "53": ("WA", "Washington"),
    "54": ("WV", "West Virginia"),
    "55": ("WI", "Wisconsin"),
    "56": ("WY", "Wyoming"),
    "60": ("AS", "American Samoa"),
    "66": ("GU", "Guam"),
    "69": ("MP", "Northern Mariana Islands"),
    "72": ("PR", "Puerto Rico"),
    "78": ("VI", "U.S. Virgin Islands"),
}


def state_usps(state_geoid: str | int) -> str:
    """Return the USPS code for a 2-digit state GEOID (e.g. ``"13"`` -> ``"GA"``)."""
    geoid = validate_state_geoid(state_geoid)
    try:
        return STATE_FIPS[geoid][0]
    except KeyError:
        raise ValueError(f"unknown state GEOID {state_geoid!r}") from None


def state_name(state_geoid: str | int) -> str:
    """Return the name for a 2-digit state GEOID (e.g. ``"13"`` -> ``"Georgia"``)."""
    geoid = validate_state_geoid(state_geoid)
    try:
        return STATE_FIPS[geoid][1]
    except KeyError:
        raise ValueError(f"unknown state GEOID {state_geoid!r}") from None


# --- Row builders (shapefile attributes -> table rows) ---------------------


def build_state_row(
    gisjoin: str,
    vintage: int,
    *,
    centroid_geo_lon: float | None = None,
    centroid_geo_lat: float | None = None,
    centroid_pop_lon: float | None = None,
    centroid_pop_lat: float | None = None,
    area_land_sqm: float | None = None,
    area_water_sqm: float | None = None,
) -> dict[str, Any]:
    """Assemble a ``geography.us_state`` row from a state GISJOIN plus geometry values.

    Identity (geoid, USPS, name, HHS region) is derived in code so it does not
    depend on which attributes a shapefile carries (ADR 0020). The geographic
    interior point is always supplied; the population-weighted pair is set only
    when a Center of Population covers the unit.
    """
    geoid = gisjoin_to_geoid(gisjoin, "state")
    usps = state_usps(geoid)
    return {
        "geoid": geoid,
        "vintage": int(vintage),
        "gisjoin": gisjoin.strip().upper(),
        "name": state_name(geoid),
        "stusps": usps,
        "hhs_region": hhs_region_for_state(usps),
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "centroid_pop_lon": centroid_pop_lon,
        "centroid_pop_lat": centroid_pop_lat,
        "area_land_sqm": area_land_sqm,
        "area_water_sqm": area_water_sqm,
    }


def build_county_row(
    gisjoin: str,
    vintage: int,
    name: str,
    *,
    centroid_geo_lon: float | None = None,
    centroid_geo_lat: float | None = None,
    centroid_pop_lon: float | None = None,
    centroid_pop_lat: float | None = None,
    area_land_sqm: float | None = None,
    area_water_sqm: float | None = None,
) -> dict[str, Any]:
    """Assemble a ``geography.us_county`` row.

    ``geoid`` and the parent ``state_geoid`` FK are derived from the GISJOIN; the
    county name comes from the shapefile. Geographic interior point always;
    population-weighted pair where a Center of Population exists.
    """
    geoid = gisjoin_to_geoid(gisjoin, "county")
    return {
        "geoid": geoid,
        "vintage": int(vintage),
        "state_geoid": state_geoid_of_county(geoid),
        "gisjoin": gisjoin.strip().upper(),
        "name": name,
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "centroid_pop_lon": centroid_pop_lon,
        "centroid_pop_lat": centroid_pop_lat,
        "area_land_sqm": area_land_sqm,
        "area_water_sqm": area_water_sqm,
    }


def build_tract_row(
    gisjoin: str,
    vintage: int,
    *,
    centroid_geo_lon: float | None = None,
    centroid_geo_lat: float | None = None,
    centroid_pop_lon: float | None = None,
    centroid_pop_lat: float | None = None,
    area_land_sqm: float | None = None,
    area_water_sqm: float | None = None,
) -> dict[str, Any]:
    """Assemble a ``geography.us_tract`` row.

    ``geoid`` (11-digit) and the parent ``county_geoid`` (5) + ``state_geoid`` (2)
    FKs are derived from the GISJOIN. Tracts get both a geographic interior point
    and a population-weighted center (Centers of Population cover tracts).
    """
    geoid = gisjoin_to_geoid(gisjoin, "tract")
    return {
        "geoid": geoid,
        "vintage": int(vintage),
        "state_geoid": geoid[:STATE_GEOID_WIDTH],
        "county_geoid": geoid[:COUNTY_GEOID_WIDTH],
        "gisjoin": gisjoin.strip().upper(),
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "centroid_pop_lon": centroid_pop_lon,
        "centroid_pop_lat": centroid_pop_lat,
        "area_land_sqm": area_land_sqm,
        "area_water_sqm": area_water_sqm,
    }


def build_zcta_row(
    gisjoin: str,
    vintage: int,
    *,
    centroid_geo_lon: float | None = None,
    centroid_geo_lat: float | None = None,
    area_land_sqm: float | None = None,
    area_water_sqm: float | None = None,
) -> dict[str, Any]:
    """Assemble a ``geography.us_zcta`` row.

    ZCTAs are non-nesting (no parent FK) and have no Center of Population, so only
    the geographic interior point is stored.
    """
    return {
        "geoid": gisjoin_to_geoid(gisjoin, "zcta"),
        "vintage": int(vintage),
        "gisjoin": gisjoin.strip().upper(),
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "area_land_sqm": area_land_sqm,
        "area_water_sqm": area_water_sqm,
    }


# --- Crosswalk weight validation -------------------------------------------


def summarize_crosswalk_weights(
    rows: Iterable[dict[str, Any]],
    *,
    source_key: str = "source_geoid",
    weight_key: str = "weight",
) -> dict[str, float]:
    """Sum interpolation weights per source unit in a vintage crosswalk.

    NHGIS crosswalks distribute each source geography across one or more target
    geographies with population-interpolation weights that should sum to ~1.0
    per source unit (ADR 0020).

    Args:
        rows: Crosswalk records, each with a source GEOID and a weight.
        source_key: Key holding the source GEOID in each row.
        weight_key: Key holding the weight in each row.

    Returns:
        Mapping of source GEOID -> total weight.
    """
    totals: dict[str, float] = {}
    for row in rows:
        src = row[source_key]
        totals[src] = totals.get(src, 0.0) + float(row[weight_key])
    return totals


def validate_crosswalk_weights(
    rows: Iterable[dict[str, Any]],
    *,
    source_key: str = "source_geoid",
    weight_key: str = "weight",
    tolerance: float = 1e-3,
) -> list[tuple[str, float]]:
    """Return source units whose interpolation weights don't sum to ~1.0.

    Args:
        rows: Crosswalk records (see :func:`summarize_crosswalk_weights`).
        source_key: Key holding the source GEOID in each row.
        weight_key: Key holding the weight in each row.
        tolerance: Allowed absolute deviation from 1.0.

    Returns:
        A sorted list of ``(source_geoid, total_weight)`` for offending units.
        An empty list means every source unit's weights sum to ~1.0.
    """
    totals = summarize_crosswalk_weights(rows, source_key=source_key, weight_key=weight_key)
    return [(src, total) for src, total in sorted(totals.items()) if abs(total - 1.0) > tolerance]


# --- Crosswalk normalization (long-form output for geography.us_crosswalk; ADR 0021)
# Maps the raw NHGIS weight column names to our controlled weight_kind vocabulary.
NHGIS_WEIGHT_COLUMNS: dict[str, str] = {
    "wt_pop": "pop",
    "wt_hh": "hh",
    "wt_fam": "fam",
    "wt_adult": "adult",
    "parea": "area",
}

CROSSWALK_WEIGHT_KINDS: tuple[str, ...] = ("pop", "hh", "fam", "adult", "area")


def normalize_crosswalk_rows(
    raw_rows: Iterable[dict[str, Any]],
    *,
    source_level: str,
    source_vintage: int,
    target_level: str,
    target_vintage: int,
    source_gj_col: str,
    target_gj_col: str,
    weight_columns: dict[str, str],
) -> list[dict[str, Any]]:
    """Expand raw NHGIS crosswalk records into long-form rows for ``geography.us_crosswalk``.

    NHGIS publishes one row per source-target pair with multiple weight columns
    (``parea``, ``wt_pop``, ``wt_hh``, etc.). We pivot to one row per source ×
    target × weight_kind, deriving GEOIDs from the GISJOIN keys (ADR 0021).
    NaN and non-numeric weights are skipped silently (NHGIS leaves some weight
    cells empty when a denominator is zero).
    """
    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        src_gj = str(raw[source_gj_col]).strip().upper()
        tgt_gj = str(raw[target_gj_col]).strip().upper()
        src_geoid = gisjoin_to_geoid(src_gj, source_level)
        tgt_geoid = gisjoin_to_geoid(tgt_gj, target_level)
        for raw_col, kind in weight_columns.items():
            if raw_col not in raw:
                continue
            try:
                w = float(raw[raw_col])
            except (TypeError, ValueError):
                continue
            if w != w:  # NaN
                continue
            rows.append(
                {
                    "source_level": source_level,
                    "source_vintage": int(source_vintage),
                    "source_geoid": src_geoid,
                    "source_gisjoin": src_gj,
                    "target_level": target_level,
                    "target_vintage": int(target_vintage),
                    "target_geoid": tgt_geoid,
                    "target_gisjoin": tgt_gj,
                    "weight_kind": kind,
                    "weight": w,
                }
            )
    return rows


# --- User-facing hierarchical-filter views (ADR 0028) -----------------------
# Convenience views that denormalize stable parent *display* attributes (state
# name/USPS/HHS region; county name) onto child levels, so analysts can filter
# by the human-readable parent ("counties in Georgia", "tracts in Fulton
# County") without hand-writing the hierarchy joins. The parent *geoids* are
# already on the child tables (us_county.state_geoid; us_tract.state_geoid +
# county_geoid), so code-based filtering needs no view; these add the labels.
# Views (not denormalized base columns) keep the canonical entity tables
# normalized at zero storage/refresh cost. Joins are vintage-keyed and INNER --
# every child has a valid parent (FK integrity, ADR 0023 P0-3) -- and the build
# asserts each view's rowcount equals its base table's to catch any orphan.


def us_enriched_view_definitions(catalog: str) -> dict[str, str]:
    """Return ``{fully_qualified_view_name: CREATE OR REPLACE VIEW sql}`` (ADR 0028).

    - ``us_county_enriched`` = ``us_county`` + state name / USPS / HHS region.

    (``us_tract_enriched`` was retired once ``us_tract`` became an enriched canonical
    carrying ``county_name`` + state labels directly -- ADR 0037 decision 7 / 0040. The
    same supersession applies to ``us_county_enriched`` now that ``us_county`` is enriched;
    this whole views path is slated for retirement once county's view + consumers migrate.)

    ZCTAs are intentionally excluded: they cross county/state lines and have no
    single nesting parent. ``<child>.*`` keeps every base column (the county's
    own ``name`` stays ``name``; the parent's is aliased ``state_name`` etc.).
    """
    g = f"{catalog}.geography"
    county = (
        f"CREATE OR REPLACE VIEW {g}.us_county_enriched AS\n"
        "SELECT c.*,\n"
        "       s.name AS state_name,\n"
        "       s.stusps AS state_stusps,\n"
        "       s.hhs_region AS state_hhs_region\n"
        f"FROM {g}.us_county c\n"
        f"JOIN {g}.us_state s ON c.state_geoid = s.geoid AND c.vintage = s.vintage"
    )
    return {
        f"{g}.us_county_enriched": county,
    }
