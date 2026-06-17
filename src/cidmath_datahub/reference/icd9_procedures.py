"""ICD-9-CM Volume 3 procedure code system (frozen reference; ADR 0014).

Parses the CMS ICD-9-CM **Version 32** code-description files (the ``SG`` =
procedure members) into a **flat** reference grain: one row per procedure code with
its short + long title and a small chapter grouping (no classification tree). This is
the ICD-9 procedure counterpart to ``codes.icd10pcs`` (the ICD-10 procedure table) and a
sibling of ``codes.icd9cm`` (ICD-9 diagnoses) -- same agency family, same public domain,
same frozen-after-2015 situation. This module is the single source of truth for ICD-9
procedure parsing, normalization, and format validation; the entrypoint
``bundles/_reference/src/build_icd9_procedures.py`` is thin glue over it (ADR 0011/0027).

It holds **no Spark** -- pure functions over plain Python, unit tested against real sample
rows -- so the bundle entrypoint does the public HTTPS download + unzip and converts the
output to a Spark DataFrame writing ``ecdh_model_<env>.codes.icd9_procedures`` keyed by
``(icd9_procedure_code, edition_year)`` (ADR 0006, ADR 0015: reference table, no Kimball
suffix).

ICD-9-CM is **frozen**: Version 32 (effective 2014-10-01) is the final release, valid for
US coding through 2015-09-30 (ICD-10 took over 2015-10-01). So there is one edition and no
mid-year overlay (unlike ICD-10-CM/PCS); this is a single-snapshot, re-pullable table.

Code structure (canonical dotted form): a 2-digit category with up to 2 decimal digits --
``NN[.N[N]]``. The CMS files store codes undotted (``0001`` -> ``00.01``, ``4701`` ->
``47.01``); :func:`normalize_code` re-inserts the decimal after the 2nd character. Codes
are numeric only (no V/E codes -- those are diagnoses).

``is_billable`` is leaf-of-set: a code is billable iff no more-specific code has it as a
prefix (ICD-9's "code to the highest level of specificity" rule; same as ``codes.icd9cm``).

Grouping (flat, like ``codes.icd10pcs``'s Section -- NOT the ADR-0031 hierarchy): each code
carries its 2-digit ``category`` and the procedure ``chapter`` it falls in (the 18 fixed
Volume-3 chapters by category range, e.g. 01-05 Nervous System, 35-39 Cardiovascular).

License: public domain (U.S. Government work, CMS/NCHS). Plain HTTPS download, no credential.

Sources:
    * ICD-9-CM code titles (CMS): the "ICD-9-CM Diagnosis and Procedure Codes: Abbreviated and
      Full Code Titles" page (``SOURCE_LANDING_URL``).
    * Version 32 master descriptions zip -> ``CMS32_DESC_LONG_SG.txt`` (full titles) and
      ``CMS32_DESC_SHORT_SG.txt`` (abbreviated titles), format ``<code> <title>`` per line.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: The CMS ICD-9-CM Version 32 "master descriptions" zip (final ICD-9-CM release) -- the
#: confirmed CMS direct-download link. If CMS ever relocates it, the entrypoint accepts a full
#: ``--source-url`` override (grab the live link from the CMS ICD-9-CM code-titles page).
SOURCE_ZIP_URL = (
    "https://www.cms.gov/medicare/coding/icd9providerdiagnosticcodes/downloads/"
    "icd-9-cm-v32-master-descriptions.zip"
)

#: Human-facing landing page (recorded in registration provenance).
SOURCE_LANDING_URL = (
    "https://www.cms.gov/medicare/coding-billing/icd-10-codes/"
    "icd-9-cm-diagnosis-procedure-codes-abbreviated-and-full-code-titles"
)

#: The procedure (``SG`` = surgery) members inside the zip. ``DX`` (diagnosis) members are
#: the ``codes.icd9cm`` source family and are ignored here. Long = full titles, Short =
#: abbreviated (<=~28 char) titles. Matched case-insensitively.
LONG_MEMBER_RE = re.compile(r"CMS\d+_DESC_LONG_SG\.txt$", re.IGNORECASE)
SHORT_MEMBER_RE = re.compile(r"CMS\d+_DESC_SHORT_SG\.txt$", re.IGNORECASE)

#: The CMS title files are ASCII/latin-1; latin-1 keeps a rare stray byte from aborting.
SOURCE_ENCODING = "latin-1"


def _select_member(names: Iterable[str], pattern: re.Pattern[str], label: str) -> str:
    matches = sorted(n for n in names if pattern.search(n))
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one ICD-9 procedure {label} member; "
            f"found {matches or 'none'} among {sorted(names)}"
        )
    return matches[0]


def select_long_member(names: Iterable[str]) -> str:
    """Pick the ``CMS<NN>_DESC_LONG_SG.txt`` member from a zip's name list (ADR 0011)."""
    return _select_member(names, LONG_MEMBER_RE, "long-title (CMS*_DESC_LONG_SG.txt)")


def select_short_member(names: Iterable[str]) -> str:
    """Pick the ``CMS<NN>_DESC_SHORT_SG.txt`` member from a zip's name list (ADR 0011)."""
    return _select_member(names, SHORT_MEMBER_RE, "short-title (CMS*_DESC_SHORT_SG.txt)")


# ---------------------------------------------------------------------------
# Canonical format definition (single-sourced here; ADR 0006)
# ---------------------------------------------------------------------------

#: Canonical dotted ICD-9-CM procedure code: a 2-digit category with up to 2 decimal
#: digits. Anchored; numeric only.
ICD9_PROCEDURE_CODE_RE = re.compile(r"^\d{2}(?:\.\d{1,2})?$")

#: The 18 fixed ICD-9-CM Volume 3 procedure chapters, ``(chapter_code, name, low, high)`` by
#: 2-digit category range. ICD-9-CM is frozen, so this is authoritative and complete; chapters
#: are assigned from it rather than parsed. ``chapter_code`` is the category-range string.
_PROCEDURE_CHAPTERS: list[tuple[str, str, int, int]] = [
    ("00", "Procedures and Interventions, Not Elsewhere Classified", 0, 0),
    ("01-05", "Operations on the Nervous System", 1, 5),
    ("06-07", "Operations on the Endocrine System", 6, 7),
    ("08-16", "Operations on the Eye", 8, 16),
    ("17", "Other Miscellaneous Diagnostic and Therapeutic Procedures", 17, 17),
    ("18-20", "Operations on the Ear", 18, 20),
    ("21-29", "Operations on the Nose, Mouth, and Pharynx", 21, 29),
    ("30-34", "Operations on the Respiratory System", 30, 34),
    ("35-39", "Operations on the Cardiovascular System", 35, 39),
    ("40-41", "Operations on the Hemic and Lymphatic System", 40, 41),
    ("42-54", "Operations on the Digestive System", 42, 54),
    ("55-59", "Operations on the Urinary System", 55, 59),
    ("60-64", "Operations on the Male Genital Organs", 60, 64),
    ("65-71", "Operations on the Female Genital Organs", 65, 71),
    ("72-75", "Obstetrical Procedures", 72, 75),
    ("76-84", "Operations on the Musculoskeletal System", 76, 84),
    ("85-86", "Operations on the Integumentary System", 85, 86),
    ("87-99", "Miscellaneous Diagnostic and Therapeutic Procedures", 87, 99),
]

#: The valid chapter codes (controlled vocab; ADR 0016).
PROCEDURE_CHAPTERS: frozenset[str] = frozenset(code for code, *_ in _PROCEDURE_CHAPTERS)


@dataclass(frozen=True)
class Icd9ProcedureRecord:
    """One ICD-9-CM Volume 3 procedure code for a given edition.

    Mirrors the v1 ``codes.icd9_procedures`` table shape minus the audit columns
    (``source_file``/``ingested_at``), which the bundle entrypoint stamps.

    Attributes:
        icd9_procedure_code: Canonical dotted code (PK component), e.g. ``"47.01"``.
        edition_year: ICD-9-CM fiscal-year edition (Version 32 -> 2015, the final release).
        short_title: Abbreviated (<=~28 char) title.
        long_title: Full title (the non-null description of record).
        is_billable: True for a leaf code (no more-specific code has it as a prefix).
        category: The 2-digit category (e.g. ``"47"``); always present.
        chapter_code: The procedure chapter's category-range code (e.g. ``"42-54"``), or None.
        chapter_name: The procedure chapter's name, or None if the category is unmapped.
    """

    icd9_procedure_code: str
    edition_year: int
    short_title: str
    long_title: str
    is_billable: bool
    category: str
    chapter_code: str | None
    chapter_name: str | None


def normalize_code(raw: str) -> str:
    """Normalize a raw ICD-9-CM procedure code to its canonical dotted, uppercase form.

    Strips whitespace, removes any existing decimal, then re-inserts the decimal after the
    2nd character when the code is longer than its 2-digit category. Does **not** validate --
    pass the output to :func:`validate_code`.

    Examples:
        >>> normalize_code("0001")
        '00.01'
        >>> normalize_code("4701")
        '47.01'
        >>> normalize_code("470")
        '47.0'
        >>> normalize_code("47")
        '47'
        >>> normalize_code("")
        ''
    """
    cleaned = raw.strip().replace(".", "")
    if not cleaned:
        return ""
    if len(cleaned) <= 2:
        return cleaned
    return f"{cleaned[:2]}.{cleaned[2:]}"


def validate_code(code: str) -> bool:
    """Return True if ``code`` is a well-formed canonical dotted ICD-9 procedure code.

    Validates *format only* (per :data:`ICD9_PROCEDURE_CODE_RE`): a 2-digit category with up
    to 2 decimal digits. Expects already-normalized input.

    Examples:
        >>> validate_code("47.01")
        True
        >>> validate_code("47")
        True
        >>> validate_code("470")     # not normalized (missing decimal)
        False
        >>> validate_code("V30.0")   # V codes are diagnoses, not procedures
        False
    """
    return bool(ICD9_PROCEDURE_CODE_RE.match(code))


def category_of(code: str) -> str:
    """Return a code's 2-digit category (the part before the decimal).

    Examples:
        >>> category_of("47.01")
        '47'
        >>> category_of("00")
        '00'
    """
    return code.split(".", 1)[0]


def chapter_for(code: str) -> tuple[str, str] | None:
    """Return ``(chapter_code, chapter_name)`` for a code, or ``None`` if unmapped.

    Maps the 2-digit category into one of the 18 fixed Volume-3 chapters by range.

    Examples:
        >>> chapter_for("47.01")
        ('42-54', 'Operations on the Digestive System')
        >>> chapter_for("36.0")
        ('35-39', 'Operations on the Cardiovascular System')
    """
    try:
        category = int(category_of(code))
    except ValueError:
        return None
    for chapter_code, chapter_name, low, high in _PROCEDURE_CHAPTERS:
        if low <= category <= high:
            return (chapter_code, chapter_name)
    return None


# ---------------------------------------------------------------------------
# Parsing (CMS title files: "<code> <title>" per line, undotted code)
# ---------------------------------------------------------------------------


def parse_titles(text: str) -> list[tuple[str, str]]:
    """Parse a CMS ``DESC_*_SG`` title file into ``(normalized_code, title)`` pairs.

    Each line is an undotted code, whitespace, then the title (the file pads the code
    field for alignment, so the gap may be 1+ spaces). Codes are normalized but **not**
    validated here -- run :func:`find_format_violations` over the batch so a malformed line
    surfaces as DQ rather than vanishing (ADR 0009).

    Args:
        text: Full contents of ``CMS<NN>_DESC_LONG_SG.txt`` or ``..._SHORT_SG.txt``.

    Returns:
        ``(code, title)`` pairs in source order.
    """
    pairs: list[tuple[str, str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=1)
        code = normalize_code(parts[0])
        title = parts[1].strip() if len(parts) > 1 else ""
        pairs.append((code, title))
    return pairs


def find_billable_codes(codes: Iterable[str]) -> set[str]:
    """Return the billable (leaf) codes: those that are no other code's prefix.

    ICD-9-CM's "code to the highest level of specificity" rule -- a code is billable iff no
    more-specific code exists that has it as a prefix (same approach as ``codes.icd9cm``,
    with the 2-digit procedure category). Computed over undotted strings, so the caller must
    pass *all* of the edition's codes.

    Args:
        codes: All normalized codes in the edition.

    Returns:
        The subset that are leaves (billable).
    """
    undotted = {c: c.replace(".", "") for c in codes}
    existing = set(undotted.values())
    non_leaf: set[str] = set()
    for stem in existing:
        for length in range(2, len(stem)):  # proper prefixes from the 2-digit category up
            prefix = stem[:length]
            if prefix in existing:
                non_leaf.add(prefix)
    return {code for code, stem in undotted.items() if stem not in non_leaf}


def assemble_records(
    long_pairs: Iterable[tuple[str, str]],
    short_pairs: Iterable[tuple[str, str]],
    edition_year: int,
) -> list[Icd9ProcedureRecord]:
    """Join long + short titles by code into :class:`Icd9ProcedureRecord` rows.

    The long file is authoritative for the code set; the short file supplies abbreviated
    titles (joined by code, blank if absent). Deduplicates on code (first long title wins),
    stamps ``is_billable`` as leaf-of-set over the whole edition, and derives category +
    chapter. One record per distinct code, in first-seen order.

    Args:
        long_pairs: ``(code, long_title)`` from :func:`parse_titles` on the LONG file.
        short_pairs: ``(code, short_title)`` from :func:`parse_titles` on the SHORT file.
        edition_year: The fiscal-year edition (Version 32 -> 2015).

    Returns:
        One record per distinct code.
    """
    short_by_code: dict[str, str] = {}
    for code, title in short_pairs:
        short_by_code.setdefault(code, title)

    long_by_code: dict[str, str] = {}
    for code, title in long_pairs:
        long_by_code.setdefault(code, title)

    billable = find_billable_codes(long_by_code.keys())
    records: list[Icd9ProcedureRecord] = []
    for code, long_title in long_by_code.items():
        chapter = chapter_for(code)
        records.append(
            Icd9ProcedureRecord(
                icd9_procedure_code=code,
                edition_year=edition_year,
                short_title=short_by_code.get(code, ""),
                long_title=long_title,
                is_billable=code in billable,
                category=category_of(code),
                chapter_code=chapter[0] if chapter else None,
                chapter_name=chapter[1] if chapter else None,
            )
        )
    return records


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_keys(records: list[Icd9ProcedureRecord]) -> list[tuple[str, int]]:
    """Duplicate ``(icd9_procedure_code, edition_year)`` keys (blocking PK uniqueness)."""
    seen: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.icd9_procedure_code, r.edition_year)
        seen[key] = seen.get(key, 0) + 1
    return [key for key, count in seen.items() if count > 1]


def find_missing_long_titles(records: list[Icd9ProcedureRecord]) -> list[tuple[str, int]]:
    """Return ``(icd9_procedure_code, edition_year)`` keys whose long title is blank (blocking)."""
    return [
        (r.icd9_procedure_code, r.edition_year)
        for r in records
        if not (r.long_title and r.long_title.strip())
    ]


def find_format_violations(records: list[Icd9ProcedureRecord]) -> list[str]:
    """Return codes that fail canonical ICD-9 procedure format validation (blocking)."""
    return [r.icd9_procedure_code for r in records if not validate_code(r.icd9_procedure_code)]


def find_bad_chapters(records: list[Icd9ProcedureRecord]) -> list[tuple[str, str]]:
    """Return ``(code, category)`` for codes whose chapter didn't resolve (blocking; ADR 0016).

    The 18 chapters span the whole 00-99 category space, so a real procedure code always
    resolves; a hit means a category outside 00-99 -- a parse/format anomaly.
    """
    return [(r.icd9_procedure_code, r.category) for r in records if r.chapter_code is None]


def chapter_distribution(records: list[Icd9ProcedureRecord]) -> dict[str, int]:
    """``{chapter_code: count}`` over records (backs the chapter-distribution WARN)."""
    dist: dict[str, int] = {}
    for r in records:
        key = r.chapter_code or "<none>"
        dist[key] = dist.get(key, 0) + 1
    return dist


def billable_share(records: list[Icd9ProcedureRecord]) -> float:
    """Fraction of records that are billable leaf codes (backs the is_billable-share WARN)."""
    if not records:
        return 0.0
    return sum(1 for r in records if r.is_billable) / len(records)
