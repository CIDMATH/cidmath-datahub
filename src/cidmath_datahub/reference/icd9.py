"""ICD-9-CM diagnosis code system (frozen reference; ADR 0014, ADR 0031).

Parses the NCHS ICD-9-CM **Tabular List of Diseases** (Volume 1, the ``DTAB``
RTF) into the diagnosis code set used for U.S. coding *before* the 2015-10-01
ICD-10 transition, including the V (``V01``-``V91``) and E (``E000``-``E999``)
supplementary classifications. This module is the single source of truth for
ICD-9-CM parsing, normalization, and format validation; the entrypoint
``bundles/_reference/src/build_icd9.py`` is thin glue over it (ADR 0011/0027).

It is a **standalone** module: ICD-9-CM's source (NCHS RTF), code structure, and
tree-sourcing genuinely differ from ICD-10-CM, so ``reference/icd10.py`` is left
untouched and the two share only the documented *hierarchy contract* (ADR 0031),
not code. It holds **no Spark** -- pure functions over plain Python -- so the
bundle entrypoint converts the result to a Spark DataFrame and writes
``ecdh_model_<env>.codes.icd9`` keyed by ``(icd9_code, edition_year)`` (ADR 0006,
ADR 0015: reference table, no Kimball suffix).

ICD-9-CM is **frozen**: the final update was FY2014 and it is valid for US coding
through 2015-09-30. There is no mid-year overlay (unlike ICD-10-CM), so none of
``icd10.py``'s ``{year}-update`` machinery is ported.

Code structure (canonical dotted form):
    * numeric: ``NNN[.N[N]]`` -- decimal after the 3rd char (``250`` -> ``250.0``
      -> ``250.00``).
    * V codes: ``VNN[.N[N]]`` -- decimal after the 3rd char (``V30`` -> ``V30.00``).
    * E codes: ``ENNN[.N]`` -- decimal after the **4th** char (``E812`` -> ``E812.0``).

Hierarchy (ADR 0031): adjacency / ancestors / depth come from the longest-existing-
prefix rule over the edition's own code set (ICD-9 nests cleanly by string prefix);
chapter/block labels come from Appendix E (``DC_3D`` RTF). Those are added in the
hierarchy slice; this module covers parse / normalize / validate / is_billable.

Source spec (docs-first; NCHS ICD-9-CM distribution):
    * Landing: https://archive.cdc.gov/www_cdc_gov/nchs/icd/icd9cm.htm
    * FTP year dirs: .../ICD9-CM/<dir_year>/  (dir_year = fiscal_year - 1)
    * Per-edition README (read before parsing): .../<dir_year>/Readme<FY2>.txt
    * ``DTAB<FY2>.ZIP`` -> ``DTAB<FY2>.RTF``  (Tabular List of Diseases, Vol 1)
    * ``APPNDX<FY2>.ZIP`` -> ``DC_3D<FY2>.RTF``  (Appendix E, 3-digit categories)
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: NCHS distributes one FTP directory per annual release. The directory is the
#: *calendar* year of the Oct-1 effective date; the fiscal year (our
#: ``edition_year``, for parity with ``codes.icd10``) is ``dir_year + 1``, and
#: the 2-digit filename suffix is ``fiscal_year % 100``. So FY2012 -> dir 2011 ->
#: suffix "12" -> ``DTAB12.ZIP`` / ``DC_3D12.RTF`` / ``Readme12.txt``.
SOURCE_FTP_BASE = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD9-CM"

#: Human-facing landing (archived NCHS ICD-9-CM page) + the per-edition README
#: serves as the data-dictionary; both recorded in registration provenance.
SOURCE_LANDING_URL = "https://archive.cdc.gov/www_cdc_gov/nchs/icd/icd9cm.htm"

#: The RTF members inside the zips. ``DTAB<NN>.RTF`` is the tabular list; Appendix
#: E is ``DC_3D<NN>.RTF`` inside ``APPNDX<NN>.ZIP`` (which also carries the other
#: appendices DMORPH/DDRGCL/DINDST -- excluded by requiring the ``DC_3D`` token).
DTAB_MEMBER_RE = re.compile(r"DTAB\d{2}\.rtf$", re.IGNORECASE)
APPENDIX_E_MEMBER_RE = re.compile(r"DC_3D\d{2}\.rtf$", re.IGNORECASE)


def edition_suffix(edition_year: int) -> str:
    """Return the 2-digit filename suffix for a fiscal-year edition (FY2012 -> '12')."""
    return f"{edition_year % 100:02d}"


def edition_dir_year(edition_year: int) -> int:
    """Return the FTP directory year for a fiscal-year edition (FY2012 -> 2011)."""
    return edition_year - 1


def dtab_zip_url(edition_year: int, base: str = SOURCE_FTP_BASE) -> str:
    """URL of an edition's ``DTAB<NN>.ZIP`` (Tabular List of Diseases, Vol 1)."""
    return f"{base}/{edition_dir_year(edition_year)}/DTAB{edition_suffix(edition_year)}.ZIP"


def appendix_zip_url(edition_year: int, base: str = SOURCE_FTP_BASE) -> str:
    """URL of an edition's ``APPNDX<NN>.ZIP`` (contains Appendix E, ``DC_3D``)."""
    return f"{base}/{edition_dir_year(edition_year)}/APPNDX{edition_suffix(edition_year)}.ZIP"


def readme_url(edition_year: int, base: str = SOURCE_FTP_BASE) -> str:
    """URL of an edition's ``Readme<NN>.txt`` (the data dictionary; read first)."""
    return f"{base}/{edition_dir_year(edition_year)}/Readme{edition_suffix(edition_year)}.txt"


def _select_member(names: Iterable[str], pattern: re.Pattern[str], label: str) -> str:
    matches = sorted(n for n in names if pattern.search(n))
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one ICD-9-CM {label} member; "
            f"found {matches or 'none'} among {sorted(names)}"
        )
    return matches[0]


def select_dtab_member(names: Iterable[str]) -> str:
    """Pick the ``DTAB<NN>.RTF`` member from a zip's name list (ADR 0011 keeps IO out)."""
    return _select_member(names, DTAB_MEMBER_RE, "DTAB (tabular list)")


def select_appendix_e_member(names: Iterable[str]) -> str:
    """Pick the ``DC_3D<NN>.RTF`` (Appendix E) member from the APPNDX zip's name list."""
    return _select_member(names, APPENDIX_E_MEMBER_RE, "DC_3D (Appendix E)")


# ---------------------------------------------------------------------------
# Canonical format (single-sourced here; ADR 0006)
# ---------------------------------------------------------------------------

#: Canonical dotted ICD-9-CM diagnosis code: a numeric 3-digit category with up
#: to 2 decimal digits; a V code (``V`` + 2 digits) with up to 2 decimal digits;
#: or an E code (``E`` + 3 digits) with up to 1 decimal digit. Anchored, uppercase.
ICD9_CODE_RE = re.compile(r"^(?:\d{3}(?:\.\d{1,2})?|V\d{2}(?:\.\d{1,2})?|E\d{3}(?:\.\d)?)$")

#: A token that *could* start a code line (used to spot code lines in the tabular
#: text); the strict check is :func:`validate_code` over the normalized form.
_CODE_TOKEN_RE = re.compile(r"^(\d{3}|V\d{2}|E\d{3})(\.\d{1,2})?$", re.IGNORECASE)


def normalize_code(raw: str) -> str:
    """Normalize a raw ICD-9-CM code to its canonical dotted, uppercase form.

    Strips whitespace, upper-cases, removes any existing decimal, then re-inserts
    the decimal after the 3rd character (numeric / V codes) or the 4th (E codes),
    when the code is longer than its category. Does **not** validate -- pass the
    output to :func:`validate_code`.

    Examples:
        >>> normalize_code("25000")
        '250.00'
        >>> normalize_code("V3000")
        'V30.00'
        >>> normalize_code("E8120")
        'E812.0'
        >>> normalize_code("460")
        '460'
    """
    cleaned = raw.strip().upper().replace(".", "")
    if not cleaned:
        return ""
    category_len = 4 if cleaned.startswith("E") else 3
    if len(cleaned) <= category_len:
        return cleaned
    return f"{cleaned[:category_len]}.{cleaned[category_len:]}"


def validate_code(code: str) -> bool:
    """Return True if ``code`` is a well-formed canonical dotted ICD-9-CM code.

    Validates *format only* (per :data:`ICD9_CODE_RE`), not whether the code
    exists in any edition. Expects already-normalized (uppercase, dotted) input.

    Examples:
        >>> validate_code("250.00")
        True
        >>> validate_code("V30.00")
        True
        >>> validate_code("E812.0")
        True
        >>> validate_code("E812.00")  # E codes take only one decimal digit
        False
    """
    return bool(ICD9_CODE_RE.match(code))


def code_class(code: str) -> str:
    """Return ``"E"``, ``"V"``, or ``"numeric"`` for a (normalized) code."""
    if code[:1] == "E":
        return "E"
    if code[:1] == "V":
        return "V"
    return "numeric"


@dataclass(frozen=True)
class Icd9Record:
    """One ICD-9-CM code for a given edition (pre-hierarchy).

    Mirrors the flat part of the ``codes.icd9`` shape minus audit columns; the
    hierarchy columns are added by the hierarchy slice (ADR 0031).
    """

    icd9_code: str
    edition_year: int
    description: str
    is_billable: bool


# ---------------------------------------------------------------------------
# Tabular (DTAB) parsing -- operates on text after the entrypoint's RTF->text
# step, so it is pure and unit-testable (ADR 0011).
# ---------------------------------------------------------------------------

#: One tabular line: optional indentation, a code token, whitespace, then a
#: non-empty title. Section banners ("INTESTINAL INFECTIOUS DISEASES (001-009)"),
#: chapter headers ("1. INFECTIOUS ..."), and instructional notes ("Excludes:",
#: "Use additional code", ...) do not begin with a code token and are skipped.
#: NOTE: validate the exact RTF->text line shape against a real ``DTAB`` extract;
#: title wrapping / tab-vs-space separation may need a tweak (see ADR 0031).
_DTAB_CODE_LINE_RE = re.compile(r"^\s*((?:\d{3}|V\d{2}|E\d{3})(?:\.\d{1,2})?)\s+(\S.*?)\s*$")


def parse_dtab(text: str) -> list[tuple[str, str]]:
    """Parse the tabular list text into ``(normalized_code, description)`` pairs.

    Operates on the RTF-converted plain text. Selects only real code+title lines
    (a code token at the start of the line, followed by a title), skipping
    chapter/section banners and instructional notes. Codes are normalized but not
    validated here -- run :func:`find_format_violations` over the batch so any
    malformed line surfaces as DQ rather than vanishing (ADR 0009).

    Args:
        text: The DTAB RTF converted to indented plain text.

    Returns:
        ``(code, description)`` pairs in source order (may include duplicates if
        a code is listed twice; the caller deduplicates / DQ-checks).
    """
    pairs: list[tuple[str, str]] = []
    for line in text.splitlines():
        match = _DTAB_CODE_LINE_RE.match(line)
        if not match:
            continue
        raw_code, title = match.group(1), match.group(2).strip()
        if not title:
            continue
        pairs.append((normalize_code(raw_code), title))
    return pairs


def find_billable_codes(codes: Iterable[str]) -> set[str]:
    """Return the billable (leaf) codes: those that are no other code's prefix.

    ICD-9-CM's "code to the highest level of specificity" rule means a code is
    billable iff no more-specific code exists that has it as a prefix (ADR 0031).
    Computed over the whole edition's code set (undotted string prefixes), so the
    caller must pass *all* of an edition's codes.

    Args:
        codes: All normalized codes in one edition.

    Returns:
        The subset that are leaves (billable).
    """
    undotted = {c: c.replace(".", "") for c in codes}
    existing = set(undotted.values())
    non_leaf: set[str] = set()
    for stem in existing:
        for length in range(3, len(stem)):  # proper prefixes; non-code lengths just miss
            prefix = stem[:length]
            if prefix in existing:
                non_leaf.add(prefix)
    return {code for code, stem in undotted.items() if stem not in non_leaf}


def assemble_records(pairs: Iterable[tuple[str, str]], edition_year: int) -> list[Icd9Record]:
    """Build :class:`Icd9Record` rows from ``(code, description)`` pairs.

    Deduplicates on code (first description wins), then stamps ``is_billable`` as
    leaf-of-set over the whole edition (ADR 0031). One record per distinct code.

    Args:
        pairs: ``(code, description)`` from :func:`parse_dtab`.
        edition_year: The fiscal-year edition.

    Returns:
        One record per distinct code, in first-seen order.
    """
    by_code: dict[str, str] = {}
    for code, description in pairs:
        by_code.setdefault(code, description)
    billable = find_billable_codes(by_code.keys())
    return [
        Icd9Record(
            icd9_code=code,
            edition_year=edition_year,
            description=description,
            is_billable=code in billable,
        )
        for code, description in by_code.items()
    ]


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_format_violations(records: list[Icd9Record]) -> list[str]:
    """Return codes that fail canonical ICD-9-CM format validation."""
    return [r.icd9_code for r in records if not validate_code(r.icd9_code)]


def find_missing_descriptions(records: list[Icd9Record]) -> list[tuple[str, int]]:
    """Return ``(icd9_code, edition_year)`` keys whose description is empty."""
    return [
        (r.icd9_code, r.edition_year)
        for r in records
        if not (r.description and r.description.strip())
    ]


def find_duplicate_keys(records: list[Icd9Record]) -> list[tuple[str, int]]:
    """Return ``(icd9_code, edition_year)`` keys appearing more than once."""
    seen: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.icd9_code, r.edition_year)
        seen[key] = seen.get(key, 0) + 1
    return [key for key, count in seen.items() if count > 1]
