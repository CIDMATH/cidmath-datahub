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

import re
import unicodedata
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


# ---------------------------------------------------------------------------
# Slice 3b — country_subdivision helpers (ADR 0022)
# ---------------------------------------------------------------------------

# ISO 3166-2 codes are ``<alpha2>-<local>`` where ``local`` is 1–3 alphanumerics
# (most are 2–3 letters; a handful are digits — e.g., ``US-AS`` vs. ``CN-11``).
_SUBDIVISION_LOCAL_MAX_LEN = 3

# Expected columns on the GADM 4.1 ADM_1 layer. Asserted at runtime so a GADM
# schema change fails locally on the build, not 40 minutes into a Databricks
# job (per CLAUDE.md guidance on third-party API surface).
GADM_ADM1_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"GID_0", "GID_1", "NAME_1", "TYPE_1", "ENGTYPE_1", "HASC_1", "ISO_1"}
)

# ISO 3166-2 codes whose GADM ADM_1 polygon cannot be located by either the
# HASC_1 column ("US.GA") or the ISO_1 column ("US-GA") and therefore need a
# manual ``{iso_3166_2_code: gadm_gid_1}`` override. Ship **empty**: populate
# iteratively from the first-run job's ``_ops.dq_results.details.sample_missing``
# payload (see ``_dq_checks`` in ``build_geography_subdivision``). Known
# suspect categories that may need entries here once we see the data:
#
#   - UK constituent countries (``GB-ENG``, ``GB-SCT``, ``GB-WLS``, ``GB-NIR``):
#     GADM ADM_1 splits England into nine regions, which has no ISO 3166-2
#     counterpart at the constituent-country level.
#   - French overseas departments (``FR-GF``, ``FR-RE``, ``FR-MQ``, ``FR-GP``,
#     ``FR-YT``): GADM may model these as ADM_0 entries instead of ADM_1.
#   - Norway / Svalbard (``NO-21``, ``NO-22``): Svalbard and Jan Mayen are
#     treated separately in some GADM releases.
#   - Finland / Åland (``FI-01``): Åland is a distinct GADM ADM_0 in some
#     releases.
#   - Spain / Ceuta and Melilla (``ES-CE``, ``ES-ML``).
#
# Do NOT seed these from training data — the actual GADM 4.1 release may or
# may not need any of them. Add entries only after seeing them in the missing
# sample.
GADM_ADM1_ISO_FIXUPS: dict[str, str] = {}


def parse_subdivision_code(value: str) -> tuple[str, str]:
    """Parse an ISO 3166-2 code into ``(alpha2, local_code)``.

    ``"US-GA"`` → ``("US", "GA")``. Validates the dash-separated shape; the
    alpha-2 prefix goes through :func:`normalize_alpha2`; the local part must
    be 1–3 alphanumerics (matches the ISO 3166-2 publication, which has
    examples like ``CN-11`` and ``JP-01`` alongside ``US-GA``).

    Raises ``ValueError`` for any deviation.
    """
    if not isinstance(value, str):
        raise ValueError(f"subdivision_code must be a string, got {type(value).__name__}")
    s = value.strip().upper()
    if "-" not in s:
        raise ValueError(f"subdivision_code {value!r} missing '-' separator")
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"subdivision_code {value!r} must have exactly one '-'")
    alpha2_raw, local = parts
    alpha2 = normalize_alpha2(alpha2_raw)
    if not local or len(local) > _SUBDIVISION_LOCAL_MAX_LEN or not local.isalnum():
        raise ValueError(
            f"subdivision_code {value!r} has invalid local part {local!r} "
            f"(expected 1-{_SUBDIVISION_LOCAL_MAX_LEN} alphanumerics)"
        )
    return alpha2, local


# How a subdivision's GADM ADM_1 polygon was resolved (set by
# resolve_subdivision_polygons, stored on each row for auditability — ADR 0023
# review item P0-2). "code" = exact HASC_1/ISO_1 (high confidence); "name" =
# unambiguous within-country name match; "name_ambiguous" = name match where
# >1 GADM ADM_1 in that country shared the normalized name (lower confidence,
# flagged for review); "fixup" = manual override; "none" = no polygon matched.
SUBDIVISION_MATCH_METHODS: frozenset[str] = frozenset(
    {"code", "name", "name_ambiguous", "fixup", "none"}
)


def assemble_subdivision_row(
    *,
    subdivision_code: str,
    country_alpha2: str,
    country_alpha3: str,
    subdivision_name: str,
    subdivision_type_label: str,
    parent_subdivision_code: str | None,
    gadm_gid_1: str | None,
    gadm_match_method: str,
    centroid_geo_lon: float | None,
    centroid_geo_lat: float | None,
    source_file: str,
) -> dict[str, Any]:
    """Assemble one ``geography.country_subdivision`` row from validated inputs.

    Centralizes normalization + nullability the same way
    :func:`assemble_country_row` does for ``geography.country``. The build
    script calls this once per pycountry subdivision; output dict matches the
    Spark schema exactly. Raises ``ValueError`` for malformed identifiers or
    inconsistent inputs.
    """
    alpha2_from_code, local = parse_subdivision_code(subdivision_code)
    a2 = normalize_alpha2(country_alpha2)
    a3 = normalize_alpha3(country_alpha3)

    if gadm_match_method not in SUBDIVISION_MATCH_METHODS:
        raise ValueError(
            f"gadm_match_method {gadm_match_method!r} not in {sorted(SUBDIVISION_MATCH_METHODS)}"
        )

    # The alpha-2 prefix on the code must match the supplied country_alpha2 —
    # pycountry guarantees this, but a hand-built call should fail loudly.
    if alpha2_from_code != a2:
        raise ValueError(
            f"country_alpha2 {a2!r} does not match prefix on subdivision_code {subdivision_code!r}"
        )

    if (centroid_geo_lon is None) != (centroid_geo_lat is None):
        raise ValueError(
            f"centroid lon/lat must both be set or both None (got "
            f"lon={centroid_geo_lon}, lat={centroid_geo_lat})"
        )

    parent = None
    if parent_subdivision_code is not None:
        # Run through parse to validate the shape, then re-emit normalized.
        p_alpha2, p_local = parse_subdivision_code(parent_subdivision_code)
        if p_alpha2 != a2:
            raise ValueError(
                f"parent_subdivision_code {parent_subdivision_code!r} country "
                f"prefix does not match subdivision country {a2!r}"
            )
        parent = f"{p_alpha2}-{p_local}"

    return {
        "subdivision_code": f"{a2}-{local}",
        "country_alpha2": a2,
        "country_alpha3": a3,
        "subdivision_local_code": local,
        "subdivision_name": subdivision_name,
        "subdivision_type_label": subdivision_type_label,
        "parent_subdivision_code": parent,
        "gadm_gid_1": gadm_gid_1,
        "gadm_match_method": gadm_match_method,
        "centroid_geo_lon": centroid_geo_lon,
        "centroid_geo_lat": centroid_geo_lat,
        "source_file": source_file,
    }


def assert_gadm_adm1_columns(columns: Iterable[str]) -> None:
    """Assert the GADM ADM_1 layer has the columns we depend on.

    Raises ``ValueError`` listing the missing columns. Called by the build
    immediately after reading the ADM_1 layer so a GADM schema change fails
    loudly with a clear message rather than producing silently-empty matches.
    """
    have = set(columns)
    missing = sorted(GADM_ADM1_REQUIRED_COLUMNS - have)
    if missing:
        raise ValueError(
            f"GADM ADM_1 layer missing expected columns: {missing}. Got: {sorted(have)}"
        )


def hasc_to_iso_subdivision(hasc_1: Any) -> str | None:
    """Convert a GADM ``HASC_1`` value to an ISO 3166-2 subdivision code.

    GADM stores HASC as ``US.GA``; ISO 3166-2 spells it ``US-GA``. Returns
    ``None`` for null, empty, or malformed HASC values so the caller can
    fall back to the ``ISO_1`` column.
    """
    if hasc_1 is None:
        return None
    if not isinstance(hasc_1, str):
        return None
    s = hasc_1.strip()
    if not s or "." not in s:
        return None
    parts = s.split(".")
    if len(parts) != 2:
        return None
    alpha2_raw, local = parts
    try:
        alpha2 = normalize_alpha2(alpha2_raw)
    except ValueError:
        return None
    if not local or len(local) > _SUBDIVISION_LOCAL_MAX_LEN or not local.isalnum():
        return None
    return f"{alpha2}-{local.upper()}"


def normalize_iso_1(iso_1: Any) -> str | None:
    """Normalize the GADM ``ISO_1`` column to an ISO 3166-2 code, or ``None``.

    Many GADM rows have a blank ``ISO_1``; some carry the value with extra
    whitespace or in alternate case. Returns ``None`` for anything that
    doesn't parse cleanly as ``<alpha2>-<local>``.
    """
    if iso_1 is None or not isinstance(iso_1, str):
        return None
    s = iso_1.strip()
    if not s or "-" not in s:
        return None
    try:
        alpha2, local = parse_subdivision_code(s)
    except ValueError:
        return None
    return f"{alpha2}-{local}"


def match_gadm_adm1(
    gadm_rows: Iterable[dict[str, Any]],
    fixups: dict[str, str] | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Build a ``{iso_3166_2_code: gadm_row}`` lookup from GADM ADM_1 rows.

    Match order per ADR 0022: HASC_1 first (clean for most countries), then
    ISO_1 (fills in the long tail where HASC is blank), then the manual
    ``fixups`` map (``{iso_code: gid_1}``) for countries where ISO and GADM
    legitimately disagree. The fixups map is shipped empty (see
    :data:`GADM_ADM1_ISO_FIXUPS`); operators populate it iteratively from
    first-run DQ output, not from priors.

    ``gadm_rows`` is an iterable of dicts with at least ``GID_0``, ``GID_1``,
    ``NAME_1``, ``TYPE_1``, ``ENGTYPE_1``, ``HASC_1``, ``ISO_1``, and
    ``geometry``. The function is geometry-blind — it just passes the dicts
    through — which lets unit tests use plain dicts without GeoPandas.

    Returns ``(lookup, unmatched_gid_1s)`` where ``unmatched_gid_1s`` is a
    sorted list of GADM ``GID_1`` values that did not resolve to any ISO
    subdivision code; the build records a sample of these in DQ so future
    fixups have ground truth to work from.
    """
    fixups = fixups or {}
    # Build a {gid_1: row} index for the fixups path.
    by_gid: dict[str, dict[str, Any]] = {}
    rows_list: list[dict[str, Any]] = []
    for row in gadm_rows:
        gid_1 = row.get("GID_1")
        if isinstance(gid_1, str) and gid_1:
            by_gid[gid_1] = row
        rows_list.append(row)

    lookup: dict[str, dict[str, Any]] = {}
    matched_gid_1s: set[str] = set()

    for row in rows_list:
        iso_code = hasc_to_iso_subdivision(row.get("HASC_1"))
        if iso_code is None:
            iso_code = normalize_iso_1(row.get("ISO_1"))
        if iso_code is None:
            continue
        if iso_code in lookup:
            # Duplicate HASC/ISO across rows — keep the first deterministic
            # match and let the build's DQ surface the conflict.
            continue
        lookup[iso_code] = row
        gid_1 = row.get("GID_1")
        if isinstance(gid_1, str):
            matched_gid_1s.add(gid_1)

    for iso_code, gid_1 in fixups.items():
        if iso_code in lookup:
            continue  # Don't override a successful HASC/ISO match.
        row = by_gid.get(gid_1)
        if row is None:
            continue  # Fixup points at a GID_1 not in this GADM release.
        lookup[iso_code] = row
        matched_gid_1s.add(gid_1)

    unmatched = sorted(g for g in by_gid if g not in matched_gid_1s)
    return lookup, unmatched


# ---------------------------------------------------------------------------
# Name-based matching (ADR 0023). The HASC_1 / ISO_1 code columns are blank
# for a large share of countries in GADM 4.1 (verified against the slice-3b
# dev run: only ~28% of non-nested subdivisions matched on codes alone, and
# the misses cluster by country — AD, AF, BE, TR, JP, MX … carry no usable
# code but DO carry a NAME_1 that aligns with the ISO 3166-2 grain). So name
# matching within the GADM ``GID_0`` country is the primary recovery path,
# layered after the exact-code path and before the manual fixup map. Genuine
# grain mismatches (e.g. Slovenia: 212 ISO municipalities vs ~12 GADM ADM_1
# regions) legitimately stay unmatched and keep ``gadm_gid_1 = NULL``.
# ---------------------------------------------------------------------------

# Optional GADM column carrying pipe-delimited alternate / transliterated names
# (e.g. "Flanders|Vlaanderen"). Not in GADM_ADM1_REQUIRED_COLUMNS — name
# matching uses it opportunistically when present and never fails on its
# absence.
GADM_ADM1_VARNAME_COLUMN = "VARNAME_1"


def normalize_subdivision_name(name: Any) -> str | None:
    """Normalize a subdivision name for cross-source matching, or ``None``.

    Lower-cases, strips accents/diacritics (NFKD decomposition), and collapses
    any run of non-alphanumerics to a single space (so ``"Côte-d'Or"`` and
    ``"Cote d Or"`` compare equal). Returns ``None`` for non-strings or names
    that normalize to empty. Deliberately conservative — it does not strip
    administrative-type words (``"Province of …"``), because doing so blindly
    causes more false matches than it fixes; alternate spellings are handled
    by also indexing ``VARNAME_1``.
    """
    if not isinstance(name, str):
        return None
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    collapsed = re.sub(r"[^a-z0-9]+", " ", stripped.lower()).strip()
    return collapsed or None


def build_gadm_name_index(
    gadm_rows: Iterable[dict[str, Any]],
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, set[str]]]:
    """Index GADM ADM_1 rows by ``{alpha3: {normalized_name: row}}`` + collisions.

    Keyed on the GADM ``GID_0`` (alpha-3) so name matching is scoped *within*
    a country — two countries can share a subdivision name and we never want a
    cross-country match. Both ``NAME_1`` and the optional pipe-delimited
    ``VARNAME_1`` are indexed. First writer wins on a within-country
    normalized-name collision.

    Returns ``(index, collisions)`` where ``collisions`` maps ``alpha3`` to the
    set of normalized names that **more than one distinct GADM ADM_1 polygon**
    claimed in that country. A name match landing on one of those names is
    ambiguous (we kept an arbitrary first-writer row), so the resolver marks it
    ``name_ambiguous`` and the build's DQ flags it for review — this is the
    precision signal the ADR 0023 review (P0-1) asked for.
    """
    index: dict[str, dict[str, dict[str, Any]]] = {}
    collisions: dict[str, set[str]] = {}
    for row in gadm_rows:
        gid0 = row.get("GID_0")
        if not isinstance(gid0, str) or not gid0.strip():
            continue
        alpha3 = gid0.strip().upper()
        gid1 = row.get("GID_1")
        names: list[str] = []
        primary = row.get("NAME_1")
        if isinstance(primary, str):
            names.append(primary)
        variants = row.get(GADM_ADM1_VARNAME_COLUMN)
        if isinstance(variants, str) and variants:
            names.extend(variants.split("|"))
        country_map = index.setdefault(alpha3, {})
        for raw in names:
            norm = normalize_subdivision_name(raw)
            if not norm:
                continue
            if norm in country_map:
                # Only a genuine collision if a *different* polygon wants it.
                if country_map[norm].get("GID_1") != gid1:
                    collisions.setdefault(alpha3, set()).add(norm)
                continue
            country_map[norm] = row
    return index, collisions


def resolve_subdivision_polygons(
    gadm_rows: Iterable[dict[str, Any]],
    targets: Iterable[dict[str, Any]],
    fixups: dict[str, str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], list[str]]:
    """Resolve each ISO 3166-2 subdivision to a GADM ADM_1 polygon (ADR 0023).

    Three tiers, applied per target in order:

    1. **Exact code** (``method="code"``) — the ``{iso_code: row}`` lookup from
       :func:`match_gadm_adm1` (HASC_1 → ISO_1). Highest confidence.
    2. **Name within country** (``"name"`` / ``"name_ambiguous"``) — normalized
       ``NAME_1`` / ``VARNAME_1`` match scoped to the target's
       ``country_alpha3`` (== GADM ``GID_0``). When the matched name collided
       with another polygon in that country the result is ``name_ambiguous``:
       the method is recorded for review but the polygon is **not** linked
       (excluded from ``resolved``), so we never ship a low-confidence
       attribution into the canonical table.
    3. **Fixup** (``"fixup"``) — manual ``{subdivision_code: gid_1}`` override
       for the residual that neither code nor name resolves.

    Match is always scoped to the country, so a cross-country mis-link is
    impossible by construction; the residual precision risk is within-country
    name ambiguity, which is what ``name_ambiguous`` surfaces.

    Args:
        gadm_rows: GADM ADM_1 row dicts (``GID_0``, ``GID_1``, ``NAME_1``,
            ``HASC_1``, ``ISO_1``, optional ``VARNAME_1``, ``geometry``).
        targets: The subdivisions to resolve — dicts with at least
            ``subdivision_code``, ``country_alpha3``, and ``name``.
        fixups: Optional manual override map; applied last and never displaces
            a code/name match.

    Returns:
        ``(resolved, methods, unmatched_gid_1s)`` — ``resolved`` maps
        ``subdivision_code → gadm_row`` for **confidently-linked** subdivisions
        only (``code`` / ``name`` / ``fixup``; ``name_ambiguous`` is excluded);
        ``methods`` maps ``subdivision_code → match-method string`` (see
        :data:`SUBDIVISION_MATCH_METHODS`) and *does* include the ambiguous
        codes so DQ can surface them; ``unmatched_gid_1s`` is the sorted list of
        GADM ``GID_1`` values not claimed by any target (fixup-seeding ground
        truth recorded in DQ).
    """
    fixups = fixups or {}
    rows_list = list(gadm_rows)
    code_lookup, _ = match_gadm_adm1(rows_list)
    name_index, name_collisions = build_gadm_name_index(rows_list)
    by_gid: dict[str, dict[str, Any]] = {
        r["GID_1"]: r for r in rows_list if isinstance(r.get("GID_1"), str) and r.get("GID_1")
    }

    resolved: dict[str, dict[str, Any]] = {}
    methods: dict[str, str] = {}
    matched_gids: set[str] = set()

    for target in targets:
        code = target["subdivision_code"]
        alpha3 = normalize_alpha3(target["country_alpha3"])
        row: dict[str, Any] | None = code_lookup.get(code)
        method = "code" if row is not None else None

        if row is None:
            norm = normalize_subdivision_name(target.get("name"))
            if norm is not None:
                row = name_index.get(alpha3, {}).get(norm)
                if row is not None:
                    method = (
                        "name_ambiguous" if norm in name_collisions.get(alpha3, set()) else "name"
                    )

        if row is None and code in fixups:
            row = by_gid.get(fixups[code])
            if row is not None:
                method = "fixup"

        if row is not None and method is not None:
            if method == "name_ambiguous":
                # Treat ambiguous name matches as UNLINKED (ADR 0023 review
                # decision): record the method for review/fixup visibility, but
                # don't ship a polygon we can't confidently attribute — two
                # distinct GADM polygons shared the name and we can't tell which
                # is right. The GADM polygon stays in unmatched_gid_1s.
                methods[code] = method
            else:
                resolved[code] = row
                methods[code] = method
                gid = row.get("GID_1")
                if isinstance(gid, str):
                    matched_gids.add(gid)

    unmatched = sorted(g for g in by_gid if g not in matched_gids)
    return resolved, methods, unmatched
