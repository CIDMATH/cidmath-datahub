"""ICD-10-PCS procedure coding system (authoritative slow-changing reference; ADR 0014).

Parses the CMS **order file** -- the canonical fixed-width release of the ICD-10-PCS
(Procedure Coding System) inpatient-procedure code set -- into a **flat** reference grain:
one row per code or header, carrying a billable flag, short + long titles, and a small
Section grouping (no classification tree). This is the procedure counterpart to
``codes.icd10cm`` (diagnoses): same agency family, same fiscal-year editions, same order-file
format, same public download, same per-edition replace. This module is the single source of
truth for ICD-10-PCS parsing, normalization, and format validation; the entrypoint
``bundles/_reference/src/build_icd10pcs.py`` is thin glue over it (ADR 0011/0027).

It holds **no Spark** -- pure functions over plain Python, unit tested against real sample
rows -- so the bundle entrypoint does the public HTTPS download + unzip and converts the
output to a Spark DataFrame writing ``ecdh_model_<env>.codes.icd10pcs`` keyed by
``(icd10pcs_code, edition_year)`` (ADR 0006, ADR 0015: reference table, no Kimball suffix).

How PCS differs from CM (so it is not mis-modeled):

* **7-character compositional codes, no decimal.** Codes are stored **as-is** (no dotting,
  unlike CM's ``250.00``). The charset excludes ``I`` and ``O`` (to avoid 1/0 confusion):
  every character is in ``0-9 A-H J-N P-Z``. :func:`validate_code` enforces exactly 7 such
  characters for a **valid** code; header/"title" rows are partial (1-6 chars) by design and
  are charset-checked only (:func:`validate_partial`).
* **No single-tree hierarchy.** PCS is a 7-axis grammar (Section, Body System, Root Operation,
  Body Part, Approach, Device, Qualifier) whose value sets depend on the Section -- there is no
  chapter->block nesting. For v1 we carry only ``section`` (character 1, e.g. ``0`` = Medical
  & Surgical, ``X`` = New Technology) + a static ``section_name``, and ``body_system``
  (character 2). The full axis decomposition / Definitions XML, the tabular XML, the ICD-9<->10
  GEMs, and the alphabetic index are deferred (separate issues).

Versioned per the ICD-10 model (ICD-10-CM precedent; **not** ADR 0032): keyed by
``edition_year`` (the federal fiscal year, e.g. ``2026``, effective Oct 1) with
``snapshot_replace`` (ADR 0024) -- editions are re-pullable, so the table is
vintage-reproducible. Like CM there is an annual Oct-1 base plus an Apr-1 mid-year update
(the New Technology section), overlaid the same way (:func:`overlay_records`).

License: public domain (U.S. Government work); plain HTTPS download, no credential.

Source format (CMS ``icd10pcs_order_<FY>.txt``), fixed character columns (1-indexed, the same
layout family as the CM order file -- ``icd10OrderFiles.pdf`` / ``icd10pcsOrderFile.pdf``):

================  ====================================================
Columns           Content
================  ====================================================
1-5               Order number (right-justified, zero-filled)
6                 (blank)
7-13              ICD-10-PCS code, up to 7 chars, left-justified, no
                  decimal (a valid code fills all 7; a header is shorter
                  and space-padded in this field)
14                (blank)
15                Header/valid flag: ``0`` = header/title row (not valid
                  for billing), ``1`` = valid 7-char code (billable)
16                (blank)
17-76             Short ("abbreviated") title (<=60 chars)
77                (blank)
78-end            Long title
================  ====================================================

Sources:
    * ICD-10 codes (CMS): https://www.cms.gov/medicare/coding-billing/icd-10-codes
    * Order-file format: https://www.cms.gov/files/document/2020-icd-10-pcs-order-file-pdf.pdf
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: Annual **Oct-1 base release** order-file zip on CMS, parameterized by fiscal year so the
#: entrypoint can pull any archived edition (editions are re-pullable; ADR 0024). CMS hosts
#: these under the ``files/zip`` path; the exact slug shifts year to year and the listing page
#: is JavaScript-rendered, so the entrypoint accepts a full ``--order-url`` override -- paste
#: the live link from the CMS ICD-10 page if this default 404s.
ORDER_FILE_ZIP_URL_TEMPLATE = (
    "https://www.cms.gov/files/zip/{year}-icd-10-pcs-order-file-long-abbreviated-titles.zip"
)

#: **Mid-year Apr-1 update** order-file zip (the New Technology section adds codes effective
#: Apr 1-Sep 30). Same order-file format as the base; the entrypoint overlays it onto the base
#: (update wins per code; :func:`overlay_records`). Not every edition has one, and the slug
#: shifts -- a 404 is an expected skip and ``--update-url`` overrides.
UPDATE_FILE_ZIP_URL_TEMPLATE = (
    "https://www.cms.gov/files/zip/april-1-{year}-icd-10-pcs-order-file-long-abbreviated-titles.zip"
)

#: Human-facing landing page and the order-file format documentation (recorded in registration
#: provenance; the fixed-width layout above is transcribed from the latter).
SOURCE_FILES_PAGE_URL = "https://www.cms.gov/medicare/coding-billing/icd-10-codes"
ORDER_FILE_FORMAT_DOC_URL = "https://www.cms.gov/files/document/2020-icd-10-pcs-order-file-pdf.pdf"

#: The order file inside the zip: ``icd10pcs_order_2026.txt``. Matched case-insensitively and
#: separator-agnostically so a layout tweak (``icd10pcs-order-2026.txt``) still resolves, while
#: the change-only ``order_addenda_<FY>.txt`` delta -- which lacks the ``icd10pcs`` prefix and
#: is excluded explicitly -- never wins.
ORDER_FILE_MEMBER_RE = re.compile(r"icd10pcs[-_ ]order[-_ ].*\.txt$", re.IGNORECASE)

#: Fixed-width order file is ASCII; latin-1 keeps a rare stray byte from aborting the parse.
SOURCE_ENCODING = "latin-1"


def order_file_zip_url(edition_year: int, template: str = ORDER_FILE_ZIP_URL_TEMPLATE) -> str:
    """Return the download URL for an edition's base (Oct-1) order-file zip.

    Args:
        edition_year: ICD-10-PCS fiscal-year edition (effective Oct 1), e.g. 2026.
        template: URL template with a ``{year}`` field; defaults to the current CMS layout
            (:data:`ORDER_FILE_ZIP_URL_TEMPLATE`).

    Returns:
        The fully-formed URL.
    """
    return template.format(year=edition_year)


def update_file_zip_url(edition_year: int, template: str = UPDATE_FILE_ZIP_URL_TEMPLATE) -> str:
    """Return the download URL for an edition's mid-year (Apr-1) update zip.

    Not every edition has one; the caller treats a 404 as an expected skip.
    """
    return template.format(year=edition_year)


def select_order_file_member(names: Iterable[str]) -> str:
    """Pick the order-file member from a zip's name list (ADR 0011 keeps IO out).

    Args:
        names: Member names in the downloaded zip.

    Returns:
        The single name matching :data:`ORDER_FILE_MEMBER_RE` (excluding ``*addenda*``).

    Raises:
        ValueError: If zero or more than one member matches -- a sign the release layout
            changed and the entrypoint should be revisited.
    """
    matches = sorted(
        n for n in names if ORDER_FILE_MEMBER_RE.search(n) and "addenda" not in n.lower()
    )
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one ICD-10-PCS order file (icd10pcs*order*.txt, non-addenda); "
            f"found {matches or 'none'} among {sorted(names)}"
        )
    return matches[0]


# ---------------------------------------------------------------------------
# Canonical format definition (single-sourced here; ADR 0006)
# ---------------------------------------------------------------------------

#: PCS characters: digits plus letters excluding ``I`` and ``O`` (1/0 confusion). Applies to
#: every character of every code, valid or header.
PCS_CHARSET = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"

#: A **valid** ICD-10-PCS code: exactly 7 characters from :data:`PCS_CHARSET`, no decimal.
PCS_CODE_RE = re.compile(r"^[0-9A-HJ-NP-Z]{7}$")

#: A **header/title** row's partial code: 1-7 characters from the same charset (a structural
#: node in the PCS table grammar, e.g. ``0`` or ``0DT``). Valid codes also match this.
PCS_PARTIAL_RE = re.compile(r"^[0-9A-HJ-NP-Z]{1,7}$")

#: The 17 ICD-10-PCS Sections (code character 1 -> name). Backs the blocking ``section``
#: controlled-vocab check (ADR 0016) and the denormalized ``section_name`` column.
SECTION_NAMES: dict[str, str] = {
    "0": "Medical and Surgical",
    "1": "Obstetrics",
    "2": "Placement",
    "3": "Administration",
    "4": "Measurement and Monitoring",
    "5": "Extracorporeal or Systemic Assistance and Performance",
    "6": "Extracorporeal or Systemic Therapies",
    "7": "Osteopathic",
    "8": "Other Procedures",
    "9": "Chiropractic",
    "B": "Imaging",
    "C": "Nuclear Medicine",
    "D": "Radiation Therapy",
    "F": "Physical Rehabilitation and Diagnostic Audiology",
    "G": "Mental Health",
    "H": "Substance Abuse Treatment",
    "X": "New Technology",
}

#: The valid Section characters (controlled vocab; ADR 0016).
PCS_SECTIONS: frozenset[str] = frozenset(SECTION_NAMES)

# Fixed-width order-file column spans (0-indexed Python slices); same layout as the CM order
# file (cidmath_datahub.reference.icd10cm).
_COL_CODE = slice(6, 13)
_COL_FLAG = 14
_COL_SHORT_TITLE = slice(16, 76)
_COL_LONG_TITLE = slice(77, None)


@dataclass(frozen=True)
class Icd10pcsRecord:
    """One ICD-10-PCS code (or header) for a given annual edition.

    Mirrors the v1 ``codes.icd10pcs`` table shape minus the audit columns
    (``source_file``/``ingested_at``), which the bundle entrypoint stamps.

    Attributes:
        icd10pcs_code: The code as-is, no decimal (PK component), e.g. ``"0DTJ4ZZ"``. For a
            header row this is the partial structural code (1-6 chars).
        edition_year: ICD-10-PCS fiscal-year edition (effective Oct 1), e.g. 2026.
        short_title: Abbreviated (<=60 char) title.
        long_title: Full title (the non-null description of record).
        is_billable: True for a valid 7-char leaf code (order-file flag ``1``); False for a
            header/title row (flag ``0``) that is not valid for billing.
        section: Code character 1 (e.g. ``"0"``, ``"X"``); always present.
        section_name: The Section's name from :data:`SECTION_NAMES` (None if the section
            character is unknown -- the blocking section check would then fail).
        body_system: Code character 2 (the Body System axis), or None for a 1-char header.
    """

    icd10pcs_code: str
    edition_year: int
    short_title: str
    long_title: str
    is_billable: bool
    section: str
    section_name: str | None
    body_system: str | None


def normalize_code(raw: str) -> str:
    """Normalize a raw ICD-10-PCS code to its canonical (stripped, upper-cased) form.

    PCS codes carry **no decimal** and are stored as-is, so normalization only strips
    surrounding whitespace and upper-cases. Does **not** validate the result -- pass the
    output to :func:`validate_code` / :func:`validate_partial`.

    Args:
        raw: A code as it appears in the source file or upstream data (e.g. ``"0dtj4zz"``,
            ``" 0DT "``).

    Returns:
        The canonical code, e.g. ``"0DTJ4ZZ"``. Empty/whitespace input returns ``""``.

    Examples:
        >>> normalize_code("0dtj4zz")
        '0DTJ4ZZ'
        >>> normalize_code(" 0DT ")
        '0DT'
    """
    return raw.strip().upper()


def validate_code(code: str) -> bool:
    """Return True if ``code`` is a well-formed **valid** ICD-10-PCS code (exactly 7 chars).

    Validates *format only* (per :data:`PCS_CODE_RE`): exactly 7 characters from
    :data:`PCS_CHARSET` (no ``I``/``O``). Expects already-normalized input.

    Examples:
        >>> validate_code("0DTJ4ZZ")
        True
        >>> validate_code("0DT")          # a header/partial code, not a valid 7-char code
        False
        >>> validate_code("0ITJ4ZZ")      # contains 'I'
        False
    """
    return bool(PCS_CODE_RE.match(code))


def validate_partial(code: str) -> bool:
    """Return True if ``code`` is a well-formed code **or** header (1-7 charset chars).

    Used to charset-check header/title rows, which are partial by design.

    Examples:
        >>> validate_partial("0DT")
        True
        >>> validate_partial("0DTJ4ZZ")
        True
        >>> validate_partial("0OT")       # contains 'O'
        False
    """
    return bool(PCS_PARTIAL_RE.match(code))


def section_of(code: str) -> str:
    """Return a code's Section (character 1).

    Examples:
        >>> section_of("0DTJ4ZZ")
        '0'
        >>> section_of("X2C0361")
        'X'
    """
    return code[:1]


def body_system_of(code: str) -> str | None:
    """Return a code's Body System axis (character 2), or None if the code is 1 char.

    Examples:
        >>> body_system_of("0DTJ4ZZ")
        'D'
        >>> body_system_of("0") is None
        True
    """
    return code[1] if len(code) >= 2 else None


def parse_order_line(line: str, edition_year: int) -> Icd10pcsRecord | None:
    """Parse one fixed-width order-file line into an :class:`Icd10pcsRecord`.

    Args:
        line: A single line from ``icd10pcs_order_<FY>.txt``.
        edition_year: The ICD-10-PCS fiscal-year edition this file represents.

    Returns:
        The parsed record, or ``None`` for a blank/too-short line (so the line can be
        skipped). The returned ``icd10pcs_code`` is normalized but **not** validated here --
        run validation over the batch so violations are recorded as DQ rather than silently
        dropped (ADR 0009).

    Raises:
        ValueError: If the header/valid flag column is neither ``0`` nor ``1``.
    """
    if len(line) < 16 or not line.strip():
        return None

    code = normalize_code(line[_COL_CODE])
    flag = line[_COL_FLAG]
    if flag not in ("0", "1"):
        raise ValueError(f"Unexpected header/valid flag {flag!r} in order-file line: {line!r}")

    short_title = line[_COL_SHORT_TITLE].strip()
    long_title = line[_COL_LONG_TITLE].strip()
    # The order file always carries a long title; fall back to the short one only if a row is
    # unexpectedly missing it, so `long_title` is non-null.
    long_title = long_title or short_title

    section = section_of(code)
    return Icd10pcsRecord(
        icd10pcs_code=code,
        edition_year=edition_year,
        short_title=short_title,
        long_title=long_title,
        is_billable=(flag == "1"),
        section=section,
        section_name=SECTION_NAMES.get(section),
        body_system=body_system_of(code),
    )


def parse_order_file(text: str, edition_year: int) -> list[Icd10pcsRecord]:
    """Parse a full ICD-10-PCS order file into records.

    Blank/too-short lines are skipped; every content line is parsed. No deduplication or
    format filtering is applied here -- those are validated as DQ checks downstream (ADR 0009)
    so problems surface rather than vanish.

    Args:
        text: Full contents of ``icd10pcs_order_<FY>.txt``.
        edition_year: The ICD-10-PCS fiscal-year edition this file represents.

    Returns:
        Records in source order.
    """
    records: list[Icd10pcsRecord] = []
    for line in text.splitlines():
        record = parse_order_line(line, edition_year)
        if record is not None:
            records.append(record)
    return records


def overlay_records(
    base: list[Icd10pcsRecord], update: list[Icd10pcsRecord]
) -> list[Icd10pcsRecord]:
    """Merge the mid-year (Apr-1) update onto the base (Oct-1) release.

    The update wins per ``icd10pcs_code``: an update record replaces a matching base record
    (e.g. a revised title) or is appended if it is a new code (New Technology additions). Base
    codes absent from the update are retained. Mid-year updates add/revise but do not delete.

    Both inputs should belong to a single ``edition_year``; the merge keys on ``icd10pcs_code``.

    Returns:
        Merged records: base order preserved, updated codes replaced in place, new update-only
        codes appended. (The caller sorts on write.)
    """
    merged: dict[str, Icd10pcsRecord] = {r.icd10pcs_code: r for r in base}
    for r in update:
        merged[r.icd10pcs_code] = r
    return list(merged.values())


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_keys(records: list[Icd10pcsRecord]) -> list[tuple[str, int]]:
    """Return ``(icd10pcs_code, edition_year)`` keys that appear more than once (blocking PK)."""
    seen: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.icd10pcs_code, r.edition_year)
        seen[key] = seen.get(key, 0) + 1
    return [key for key, count in seen.items() if count > 1]


def find_missing_titles(records: list[Icd10pcsRecord]) -> list[tuple[str, int]]:
    """Return ``(icd10pcs_code, edition_year)`` keys whose long title is blank (blocking).

    ``parse_order_line`` already falls back to the short title, so a hit here means a row had
    neither -- a malformed source line worth failing on.
    """
    return [
        (r.icd10pcs_code, r.edition_year)
        for r in records
        if not (r.long_title and r.long_title.strip())
    ]


def find_invalid_billable_codes(records: list[Icd10pcsRecord]) -> list[str]:
    """Return billable codes that are not a well-formed 7-char PCS code (blocking).

    The PCS-specific shape rule: a *valid* (billable, flag-1) code must be exactly 7 charset
    characters. Header rows are partial by design and are excluded here (charset-checked by
    :func:`find_charset_violations` instead).
    """
    return [
        r.icd10pcs_code for r in records if r.is_billable and not validate_code(r.icd10pcs_code)
    ]


def find_charset_violations(records: list[Icd10pcsRecord]) -> list[str]:
    """Return codes (valid or header) using characters outside the PCS charset (blocking).

    Catches an ``I``/``O`` or an over-length/empty code in either a valid or a header row --
    a sign of a misaligned fixed-width parse.
    """
    return [r.icd10pcs_code for r in records if not validate_partial(r.icd10pcs_code)]


def find_bad_sections(records: list[Icd10pcsRecord]) -> list[tuple[str, str]]:
    """Return ``(icd10pcs_code, section)`` for sections outside the 17 PCS sections.

    Blocking controlled-vocab check (ADR 0016).
    """
    return [(r.icd10pcs_code, r.section) for r in records if r.section not in PCS_SECTIONS]


def section_distribution(records: list[Icd10pcsRecord]) -> dict[str, int]:
    """``{section: count}`` over records (backs the section-distribution WARN)."""
    dist: dict[str, int] = {}
    for r in records:
        dist[r.section] = dist.get(r.section, 0) + 1
    return dist


def billable_share(records: list[Icd10pcsRecord]) -> float:
    """Fraction of records that are billable valid codes (backs the is_billable-share WARN)."""
    if not records:
        return 0.0
    return sum(1 for r in records if r.is_billable) / len(records)
