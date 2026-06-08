"""ICD-10-CM diagnosis code system (authoritative slow-changing reference; ADR 0014).

Parses the CDC NCHS **order file** -- the canonical fixed-width release of the
ICD-10-CM (Clinical Modification) diagnosis code set used for U.S. public-health
and clinical coding. One row per code or header, carrying a billable flag and a
short + long description. This module is the single source of truth for
ICD-10-CM parsing, normalization, and format validation (the entrypoint
``bundles/_reference/src/build_icd10.py`` is thin glue over it; ADR 0011/0027).

It holds **no Spark** -- pure functions over plain Python data structures, unit
tested against real sample rows -- so the bundle entrypoint converts the output
to a Spark DataFrame and writes ``ecdh_model_<env>.codes.icd10`` keyed by
``(icd10_code, edition_year)`` (ADR 0006, ADR 0015: reference table, no Kimball
suffix).

Variant: this is **ICD-10-CM** (CDC/NCHS clinical-modification diagnosis codes),
*not* WHO ICD-10 or ICD-10-PCS (procedures).

Source format (CDC NCHS ``icd10cm_order_<year>.txt``), fixed character columns
(1-indexed, per the order-file specification):

================  ====================================================
Columns           Content
================  ====================================================
1-5               Order number (right-justified, zero-filled)
6                 (blank)
7-13              ICD-10-CM code, 7 chars, left-justified, no decimal
14                (blank)
15                Header/billable flag: ``0`` = header (not valid for
                  billing), ``1`` = valid leaf code (billable)
16                (blank)
17-76             Short description (<=60 chars)
77                (blank)
78-end            Long description
================  ====================================================

Codes are stored **without** a decimal in the source; the canonical
``icd10_code`` is the dotted form (decimal after the 3rd character when the code
is longer than 3), e.g. source ``U071`` -> ``U07.1``, ``J189`` -> ``J18.9``,
``A00`` -> ``A00``.

This module also derives the ICD-10-CM **classification hierarchy** (ADR 0030)
from the **tabular XML** (:func:`parse_tabular_tree`): adjacency
(``parent_icd10_code``), a materialized path (``ancestor_codes``), depth
(``node_level``), and chapter/block labels all come from the XML's
``chapter -> section -> diag`` nesting. Seventh-character codes (e.g. ``S72.001A``)
are not XML nodes, so they fall back to their nearest listed ancestor by prefix
(:func:`resolve_ancestors`). :func:`build_hierarchy` combines it all, and
:func:`find_adjacency_mismatches` cross-checks the XML tree against the prefix
rule. With the XML skipped, adjacency degrades gracefully to the prefix rule.

Sources:
    * Files page: https://www.cdc.gov/nchs/icd/icd-10-cm/files.html
    * Release archive: https://ftp.cdc.gov/pub/health_statistics/nchs/Publications/ICD10CM/
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: Annual **Oct-1 base release** (``.../ICD10CM/{year}/``). The order file ships
#: inside the "Code Descriptions" zip. Parameterized by edition year so the
#: entrypoint can pull any archived edition (editions are re-pullable, so the
#: table is vintage-reproducible; ADR 0022/0024). Overridable from the entrypoint
#: for editions whose zip name predates this template.
ORDER_FILE_ZIP_URL_TEMPLATE = (
    "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/"
    "ICD10CM/{year}/icd10cm-Code Descriptions-{year}.zip"
)

#: **Mid-year Apr-1 update** (``.../ICD10CM/{year}-update/``), published since
#: FY2025. Same "Code Descriptions in Tabular Order" order-file format as the
#: base, carrying the additions/revisions effective Apr 1-Sep 30. The entrypoint
#: overlays it onto the base (update wins per ``icd10_code``; :func:`overlay_records`)
#: so an edition reflects the latest within-fiscal-year release. Not every year
#: has one (pre-FY2025 editions 404 here -- an expected skip).
UPDATE_FILE_ZIP_URL_TEMPLATE = (
    "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/"
    "ICD10CM/{year}-update/icd10cm-Code Descriptions-April-1-{year}.zip"
)

#: Tabular XML -- the classification *tree* (chapter -> section -> diag ...). Ships
#: in the "table and index" zip (note: the base zip name uses spaces, the update
#: zip uses hyphens). We read only the chapter/section -> 3-char-category map from
#: it; the order file drives parent / ancestors / level (ADR 0030).
TABULAR_ZIP_URL_TEMPLATE = (
    "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/"
    "ICD10CM/{year}/icd10cm-table and index-{year}.zip"
)
UPDATE_TABULAR_ZIP_URL_TEMPLATE = (
    "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/"
    "ICD10CM/{year}-update/icd10cm-table-and-index-April-1-{year}.zip"
)

#: Human-facing files page and the order-file format documentation (recorded in
#: registration provenance; the format below is transcribed from the latter).
SOURCE_FILES_PAGE_URL = "https://www.cdc.gov/nchs/icd/icd-10-cm/files.html"
ORDER_FILE_FORMAT_DOC_URL = (
    "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/2019/icd10OrderFiles.pdf"
)

#: The order file inside either zip: ``icd10cm-order-2026.txt`` (base),
#: ``icd10cm-order-April-1-2026.txt`` (update), or the older ``icd10cm_order_2024.txt``.
#: Matched case-insensitively and separator-agnostically so we pick the full order
#: file -- which carries the billable flag and both descriptions -- and never the
#: ``icd10cm-codes-*.txt`` (billable-only) file. The zip also ships change-only
#: ``icd10cm-order-addenda-*.txt`` deltas; :func:`select_order_file_member` drops
#: those (they also match this pattern) so only the full order file remains.
ORDER_FILE_MEMBER_RE = re.compile(r"icd10cm[-_ ]order[-_ ].*\.txt$", re.IGNORECASE)

#: The tabular XML inside the "table and index" zip, e.g. ``icd10cm-tabular-2026.xml``
#: (or ``icd10cm-tabular-April-1-2026.xml``). The "tabular" token excludes the
#: sibling index / drug / neoplasm XMLs.
TABULAR_XML_MEMBER_RE = re.compile(r"icd10cm[-_ ]tabular.*\.xml$", re.IGNORECASE)


def order_file_zip_url(edition_year: int, template: str = ORDER_FILE_ZIP_URL_TEMPLATE) -> str:
    """Return the download URL for an edition's base (Oct-1) "Code Descriptions" zip.

    Args:
        edition_year: ICD-10-CM fiscal-year edition (effective Oct 1), e.g. 2026.
        template: URL template with a ``{year}`` field; defaults to the current
            CDC NCHS layout (:data:`ORDER_FILE_ZIP_URL_TEMPLATE`).

    Returns:
        The fully-formed URL.
    """
    return template.format(year=edition_year)


def update_file_zip_url(edition_year: int, template: str = UPDATE_FILE_ZIP_URL_TEMPLATE) -> str:
    """Return the download URL for an edition's mid-year (Apr-1) update zip.

    Not every edition has one (pre-FY2025 editions have no ``{year}-update/``
    directory); the caller treats a 404 as an expected skip.

    Args:
        edition_year: ICD-10-CM fiscal-year edition, e.g. 2026.
        template: URL template with a ``{year}`` field; defaults to
            :data:`UPDATE_FILE_ZIP_URL_TEMPLATE`.

    Returns:
        The fully-formed URL.
    """
    return template.format(year=edition_year)


def select_order_file_member(names: Iterable[str]) -> str:
    """Pick the order-file member from a zip's name list (ADR 0011 keeps IO out).

    Args:
        names: Member names in the downloaded zip.

    Returns:
        The single name matching :data:`ORDER_FILE_MEMBER_RE`.

    Raises:
        ValueError: If zero or more than one member matches -- either is a sign
            the release layout changed and the entrypoint should be revisited.
    """
    # Exclude the change-only *-addenda-* delta files; we want the full order file.
    matches = sorted(
        n for n in names if ORDER_FILE_MEMBER_RE.search(n) and "addenda" not in n.lower()
    )
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one ICD-10-CM order file (icd10cm*order*.txt, non-addenda); "
            f"found {matches or 'none'} among {sorted(names)}"
        )
    return matches[0]


def tabular_zip_url(edition_year: int, template: str = TABULAR_ZIP_URL_TEMPLATE) -> str:
    """Return the download URL for an edition's base (Oct-1) "table and index" zip."""
    return template.format(year=edition_year)


def update_tabular_zip_url(
    edition_year: int, template: str = UPDATE_TABULAR_ZIP_URL_TEMPLATE
) -> str:
    """Return the download URL for an edition's mid-year (Apr-1) "table and index" zip."""
    return template.format(year=edition_year)


def select_tabular_xml_member(names: Iterable[str]) -> str:
    """Pick the tabular-XML member from a zip's name list (ADR 0011 keeps IO out).

    Raises:
        ValueError: If zero or more than one member matches
            :data:`TABULAR_XML_MEMBER_RE` -- a sign the release layout changed.
    """
    # Exclude any *-addenda-* delta files; we want the full tabular XML.
    matches = sorted(
        n for n in names if TABULAR_XML_MEMBER_RE.search(n) and "addenda" not in n.lower()
    )
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one ICD-10-CM tabular XML (icd10cm*tabular*.xml, non-addenda); "
            f"found {matches or 'none'} among {sorted(names)}"
        )
    return matches[0]


# ---------------------------------------------------------------------------
# Canonical format definition (single-sourced here; ADR 0006)
# ---------------------------------------------------------------------------

#: Canonical dotted ICD-10-CM code: a letter, a digit, a digit-or-letter, then
#: an optional decimal followed by 1-4 alphanumerics (codes are 3-7 chars
#: excluding the decimal). Anchored and uppercase-only -- callers normalize
#: first via :func:`normalize_code`.
ICD10CM_CODE_RE = re.compile(r"^[A-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$")

# Fixed-width order-file column spans (0-indexed Python slices).
_COL_CODE = slice(6, 13)
_COL_FLAG = 14
_COL_SHORT_DESC = slice(16, 76)
_COL_LONG_DESC = slice(77, None)


@dataclass(frozen=True)
class Icd10Record:
    """One ICD-10-CM code for a given annual edition.

    Mirrors the v1 ``codes.icd10`` table shape minus the audit columns
    (``source_file``/``ingested_at``), which the bundle entrypoint stamps.

    Attributes:
        icd10_code: Canonical dotted code (PK component), e.g. ``"U07.1"``.
        edition_year: ICD-10-CM fiscal-year edition (effective Oct 1), e.g. 2025.
        description: Long description for the code.
        is_billable: True for a valid leaf code (order-file flag ``1``); False
            for a header/category row (flag ``0``) that is not valid for billing.
        short_description: Abbreviated (<=60 char) description; kept for callers
            even though the v1 table only persists ``description``.
    """

    icd10_code: str
    edition_year: int
    description: str
    is_billable: bool
    short_description: str


def normalize_code(raw: str) -> str:
    """Normalize a raw ICD-10-CM code to its canonical dotted, uppercase form.

    Strips surrounding whitespace, upper-cases, removes any existing decimal,
    then re-inserts the decimal after the third character when the code is
    longer than three characters. Does **not** validate the result -- pass the
    output to :func:`validate_code`.

    Args:
        raw: A code as it appears in the source file or upstream data, with or
            without a decimal (e.g. ``"u071"``, ``"U07.1"``, ``" J189 "``).

    Returns:
        The canonical dotted code, e.g. ``"U07.1"``. A 3-character category code
        is returned undotted (``"A00"``). An empty/whitespace input returns
        ``""``.

    Examples:
        >>> normalize_code("U071")
        'U07.1'
        >>> normalize_code(" j18.9 ")
        'J18.9'
        >>> normalize_code("a00")
        'A00'
    """
    cleaned = raw.strip().upper().replace(".", "")
    if not cleaned:
        return ""
    if len(cleaned) <= 3:
        return cleaned
    return f"{cleaned[:3]}.{cleaned[3:]}"


def validate_code(code: str) -> bool:
    """Return True if ``code`` is a well-formed canonical dotted ICD-10-CM code.

    Validates *format only* (per :data:`ICD10CM_CODE_RE`), not whether the code
    exists in any edition. Expects already-normalized input; a raw code with a
    lower-case letter or missing decimal will fail.

    Args:
        code: A candidate code, normally the output of :func:`normalize_code`.

    Returns:
        True if the code matches the canonical ICD-10-CM pattern.

    Examples:
        >>> validate_code("U07.1")
        True
        >>> validate_code("A00")
        True
        >>> validate_code("u071")
        False
    """
    return bool(ICD10CM_CODE_RE.match(code))


def parse_order_line(line: str, edition_year: int) -> Icd10Record | None:
    """Parse one fixed-width order-file line into an :class:`Icd10Record`.

    Args:
        line: A single line from ``icd10cm_order_<year>.txt``.
        edition_year: The ICD-10-CM fiscal-year edition this file represents.

    Returns:
        The parsed record, or ``None`` for a blank/too-short line (so the line
        can be skipped). The returned ``icd10_code`` is normalized but **not**
        validated here -- run format validation over the batch so violations are
        recorded as DQ rather than silently dropped (ADR 0009).

    Raises:
        ValueError: If the billable flag column is neither ``0`` nor ``1``.
    """
    if len(line) < 16 or not line.strip():
        return None

    code = normalize_code(line[_COL_CODE])
    flag = line[_COL_FLAG]
    if flag not in ("0", "1"):
        raise ValueError(f"Unexpected billable flag {flag!r} in order-file line: {line!r}")

    short_desc = line[_COL_SHORT_DESC].strip()
    long_desc = line[_COL_LONG_DESC].strip()
    # The order file always carries a long description; fall back to the short
    # one only if a row is unexpectedly missing it, so `description` is non-null.
    description = long_desc or short_desc

    return Icd10Record(
        icd10_code=code,
        edition_year=edition_year,
        description=description,
        is_billable=(flag == "1"),
        short_description=short_desc,
    )


def parse_order_file(text: str, edition_year: int) -> list[Icd10Record]:
    """Parse a full ICD-10-CM order file into records.

    Blank/too-short lines are skipped; every content line is parsed. No
    deduplication or format filtering is applied here -- those are validated as
    DQ checks downstream (ADR 0009) so problems surface rather than vanish.

    Args:
        text: Full contents of ``icd10cm_order_<year>.txt``.
        edition_year: The ICD-10-CM fiscal-year edition this file represents.

    Returns:
        Records in source order.
    """
    records: list[Icd10Record] = []
    for line in text.splitlines():
        record = parse_order_line(line, edition_year)
        if record is not None:
            records.append(record)
    return records


def overlay_records(base: list[Icd10Record], update: list[Icd10Record]) -> list[Icd10Record]:
    """Merge the mid-year (Apr-1) update onto the base (Oct-1) release.

    The update wins per ``icd10_code``: an update record replaces a matching base
    record (e.g. a revised description) or is appended if it is a new code. Base
    codes absent from the update are retained. This is correct whether the update
    file is a full re-issue or a delta of changed entries, and whether or not it
    introduces new codes -- mid-year updates add/revise but do not delete codes.

    Both inputs should belong to a single ``edition_year`` (the caller overlays
    one edition at a time); the merge keys on ``icd10_code`` alone.

    Args:
        base: Records parsed from the Oct-1 base order file.
        update: Records parsed from the Apr-1 update order file (possibly empty).

    Returns:
        Merged records: base order preserved, updated codes replaced in place,
        new update-only codes appended. (The caller sorts on write.)
    """
    merged: dict[str, Icd10Record] = {r.icd10_code: r for r in base}
    for r in update:
        merged[r.icd10_code] = r
    return list(merged.values())


# ---------------------------------------------------------------------------
# Hierarchy (ADR 0030): adjacency + materialized path + chapter/block from the
# tabular XML's nesting (chapter -> section -> diag -> diag ...). The XML is the
# source of truth for the parent of every *listed* code; seventh-character codes
# (e.g. S72.001A) are not XML nodes, so they fall back to the nearest listed
# ancestor by prefix. All pure / no Spark.
# ---------------------------------------------------------------------------

#: A trailing code-range parenthetical on a chapter/section description, e.g.
#: " (A00-B99)" -- stripped so chapter_name/block_name are clean labels.
_RANGE_SUFFIX_RE = re.compile(r"\s*\([A-Z0-9][A-Z0-9.\-]*\)\s*$")


@dataclass(frozen=True)
class CategoryGroup:
    """Chapter + block (section) a three-character category belongs to (ADR 0030)."""

    chapter_code: str
    chapter_name: str
    block_code: str
    block_name: str


@dataclass(frozen=True)
class Icd10Node:
    """An ICD-10-CM code enriched with its place in the classification tree.

    Extends the flat :class:`Icd10Record` with adjacency (``parent_icd10_code``),
    a materialized path (``ancestor_codes``, root->parent), depth (``node_level``),
    and the denormalized chapter/block labels. ``ancestor_codes`` is a tuple so
    the node stays hashable/immutable; the entrypoint passes it straight to a
    Spark ``ARRAY<STRING>`` column.
    """

    icd10_code: str
    edition_year: int
    description: str
    is_billable: bool
    parent_icd10_code: str | None
    node_level: int
    ancestor_codes: tuple[str, ...]
    chapter_code: str | None
    chapter_name: str | None
    block_code: str | None
    block_name: str | None


def category_of(code: str) -> str:
    """Return a code's three-character category (the part before the decimal).

    Examples:
        >>> category_of("S72.001A")
        'S72'
        >>> category_of("A00")
        'A00'
    """
    return code.split(".", 1)[0]


def code_prefixes(code: str) -> list[str]:
    """Return a code's proper dotted prefixes, shortest (category) first.

    Trims the undotted code one trailing character at a time down to the
    three-character category, re-dotting after the third character. Used to find
    a code's ancestors among the codes that actually exist in an edition.

    Examples:
        >>> code_prefixes("S72.001A")
        ['S72', 'S72.0', 'S72.00', 'S72.001']
        >>> code_prefixes("A00.0")
        ['A00']
        >>> code_prefixes("A00")
        []
    """
    undotted = code.replace(".", "")
    prefixes: list[str] = []
    for length in range(3, len(undotted)):
        stem = undotted[:length]
        prefixes.append(stem if length <= 3 else f"{stem[:3]}.{stem[3:]}")
    return prefixes


def ancestors_for(code: str, code_set: set[str]) -> list[str]:
    """Return ``code``'s ancestors: the proper prefixes that exist in ``code_set``.

    The longest-existing-prefix rule (ADR 0030). Correct across seventh-character
    and ``X``-placeholder expansions: ``S02.0XXA``'s nearest existing ancestor is
    ``S02.0`` because ``S02.0XX`` / ``S02.0X`` are not themselves listed codes.

    Args:
        code: The code whose ancestors are wanted.
        code_set: All codes in the same edition (membership truth set).

    Returns:
        Ancestor codes, root->parent order (empty for a top-level category).
    """
    return [p for p in code_prefixes(code) if p in code_set]


def _strip_range(desc: str | None) -> str:
    """Strip a trailing code-range parenthetical from a chapter/section desc."""
    if not desc:
        return ""
    return _RANGE_SUFFIX_RE.sub("", desc).strip()


@dataclass(frozen=True)
class TabularTree:
    """The classification tree parsed from the tabular XML (ADR 0030).

    Attributes:
        category_map: three-character category -> its chapter/block.
        parent_of: every *listed* code -> its parent code in the XML nesting
            (``None`` for a three-character category at the top of a section).
            Seventh-character expansion codes are absent (they are not XML nodes).
    """

    category_map: dict[str, CategoryGroup]
    parent_of: dict[str, str | None]


def _walk_diag(
    diag: ET.Element,
    parent: str | None,
    group: CategoryGroup,
    category_map: dict[str, CategoryGroup],
    parent_of: dict[str, str | None],
) -> None:
    """Recurse a ``<diag>`` subtree, recording each code's parent (ADR 0030)."""
    code = normalize_code(diag.findtext("name") or "")
    if not code:
        return
    parent_of[code] = parent
    if parent is None:  # a top-level diag under a section is a 3-char category
        category_map[code] = group
    for child in diag.findall("diag"):
        _walk_diag(child, code, group, category_map, parent_of)


def parse_tabular_tree(xml_text: str) -> TabularTree:
    """Parse the tabular XML into the classification tree (ADR 0030).

    Walks ``chapter -> section -> diag -> diag ...`` recursively, so ``parent_of``
    captures the authoritative parent of every listed code (not just the
    three-character categories), and ``category_map`` carries the chapter/block
    labels. Seventh-character codes are represented in the XML by ``sevenChrDef``
    rather than as ``diag`` nodes, so they do not appear in ``parent_of``; the
    caller resolves them to their nearest listed ancestor.

    Args:
        xml_text: Full contents of ``icd10cm-tabular-<year>.xml``.

    Returns:
        The parsed :class:`TabularTree`.
    """
    root = ET.fromstring(xml_text)
    category_map: dict[str, CategoryGroup] = {}
    parent_of: dict[str, str | None] = {}
    for chapter in root.findall("chapter"):
        chapter_code = (chapter.findtext("name") or "").strip()
        chapter_name = _strip_range(chapter.findtext("desc"))
        for section in chapter.findall("section"):
            group = CategoryGroup(
                chapter_code=chapter_code,
                chapter_name=chapter_name,
                block_code=(section.get("id") or "").strip(),
                block_name=_strip_range(section.findtext("desc")),
            )
            for diag in section.findall("diag"):
                _walk_diag(diag, None, group, category_map, parent_of)
    return TabularTree(category_map=category_map, parent_of=parent_of)


def parse_tabular_category_map(xml_text: str) -> dict[str, CategoryGroup]:
    """Convenience: the ``category -> CategoryGroup`` map from :func:`parse_tabular_tree`."""
    return parse_tabular_tree(xml_text).category_map


def resolve_ancestors(code: str, parent_of: dict[str, str | None], code_set: set[str]) -> list[str]:
    """Return ``code``'s ancestors, root->parent, preferring the XML tree (ADR 0030).

    If ``code`` is a listed node, its ancestors are read straight from the XML
    nesting (``parent_of``) -- the authoritative tree. Otherwise (a
    seventh-character expansion absent from the XML, or ``parent_of`` empty
    because the XML was skipped) the nearest *existing* prefix is used as an
    anchor and its ancestors are resolved the same way; ``existing`` is the union
    of XML nodes and the edition's order-file ``code_set``.

    Args:
        code: The code whose ancestors are wanted.
        parent_of: ``code -> parent`` from :func:`parse_tabular_tree` (may be empty).
        code_set: All codes in the same edition (order-file truth set).

    Returns:
        Ancestor codes, root->parent order (empty for a top-level category).
    """
    if code in parent_of:
        chain: list[str] = []
        cursor = parent_of.get(code)
        while cursor is not None:
            chain.append(cursor)
            cursor = parent_of.get(cursor)
        return list(reversed(chain))

    existing = code_set | set(parent_of)
    anchor: str | None = None
    for prefix in code_prefixes(code):  # shortest -> longest; keep the longest match
        if prefix in existing:
            anchor = prefix
    if anchor is None:
        return []
    return resolve_ancestors(anchor, parent_of, code_set) + [anchor]


def build_hierarchy(
    records: list[Icd10Record],
    category_map: dict[str, CategoryGroup],
    parent_of: dict[str, str | None] | None = None,
) -> list[Icd10Node]:
    """Enrich one edition's records with adjacency + path + chapter/block (ADR 0030).

    Adjacency comes from the XML tree (``parent_of``) for listed codes, falling
    back to the nearest listed ancestor for seventh-character expansions. When
    ``parent_of`` is ``None``/empty (``--hierarchy skip``), adjacency degrades to
    the pure prefix rule over the edition's own code set and chapter/block are
    left null. ``records`` must be a single edition.

    Args:
        records: The edition's flat code records (post-overlay).
        category_map: ``category -> CategoryGroup`` (empty when the XML was skipped).
        parent_of: ``code -> parent`` from :func:`parse_tabular_tree` (empty/None
            to derive adjacency from the code set instead).

    Returns:
        One :class:`Icd10Node` per input record, in input order.
    """
    parents = parent_of or {}
    code_set = {r.icd10_code for r in records}
    nodes: list[Icd10Node] = []
    for r in records:
        ancestors = tuple(resolve_ancestors(r.icd10_code, parents, code_set))
        grp = category_map.get(category_of(r.icd10_code))
        nodes.append(
            Icd10Node(
                icd10_code=r.icd10_code,
                edition_year=r.edition_year,
                description=r.description,
                is_billable=r.is_billable,
                parent_icd10_code=ancestors[-1] if ancestors else None,
                node_level=len(ancestors),
                ancestor_codes=ancestors,
                chapter_code=grp.chapter_code if grp else None,
                chapter_name=grp.chapter_name if grp else None,
                block_code=grp.block_code if grp else None,
                block_name=grp.block_name if grp else None,
            )
        )
    return nodes


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_format_violations(records: list[Icd10Record]) -> list[str]:
    """Return the codes in ``records`` that fail canonical format validation.

    Backs the blocking ICD-10-CM code-format DQ check. Order-preserving; may
    contain duplicates if a malformed code recurs across editions.

    Args:
        records: Parsed records.

    Returns:
        The offending ``icd10_code`` values (empty if all are well-formed).
    """
    return [r.icd10_code for r in records if not validate_code(r.icd10_code)]


def find_missing_descriptions(records: list[Icd10Record]) -> list[tuple[str, int]]:
    """Return the ``(icd10_code, edition_year)`` keys whose description is empty.

    Backs the blocking non-null ``description`` DQ check. ``parse_order_line``
    already falls back to the short description, so a hit here means a row had
    neither -- a malformed source line worth failing on.

    Args:
        records: Parsed records.

    Returns:
        The keys with a blank/whitespace-only description, in first-seen order.
    """
    return [
        (r.icd10_code, r.edition_year)
        for r in records
        if not (r.description and r.description.strip())
    ]


def find_duplicate_keys(records: list[Icd10Record]) -> list[tuple[str, int]]:
    """Return ``(icd10_code, edition_year)`` keys that appear more than once.

    Backs the blocking primary-key uniqueness DQ check.

    Args:
        records: Parsed records.

    Returns:
        The duplicated key tuples (each reported once), in first-seen order.
    """
    seen: dict[tuple[str, int], int] = {}
    for r in records:
        key = (r.icd10_code, r.edition_year)
        seen[key] = seen.get(key, 0) + 1
    return [key for key, count in seen.items() if count > 1]


def find_unmapped_categories(nodes: list[Icd10Node]) -> list[str]:
    """Return the distinct categories whose chapter/block didn't resolve (ADR 0030).

    Backs a WARN check: a category absent from the tabular-XML map (e.g. a brand-new
    mid-year category, or the XML having been skipped) yields null chapter/block
    rather than a failed build.
    """
    return sorted({category_of(n.icd10_code) for n in nodes if n.chapter_code is None})


def find_orphan_codes(nodes: list[Icd10Node]) -> list[str]:
    """Return subcategory codes (dotted) that found no parent in their edition.

    Backs a WARN check: a 4+ character code whose ancestors are all absent from
    the code set is an orphan -- normally impossible (the order file lists every
    category), so a hit signals a malformed or partial source.
    """
    return [n.icd10_code for n in nodes if "." in n.icd10_code and not n.ancestor_codes]


def find_adjacency_mismatches(
    records: list[Icd10Record], parent_of: dict[str, str | None]
) -> list[str]:
    """Return listed codes whose XML parent disagrees with the prefix-derived one.

    A cross-check (ADR 0030): for every code that is a node in the tabular XML,
    its XML parent should equal the longest existing prefix in the order file's
    code set. They coincide whenever the XML nesting follows the code string and
    every XML node is also an order-file code -- so a non-empty result flags a
    real source anomaly (e.g. an XML node missing from the order file, or an
    unexpected nesting). Backs a WARN check; never blocks.

    Args:
        records: The edition's records (order-file truth set).
        parent_of: ``code -> parent`` from :func:`parse_tabular_tree`.

    Returns:
        The offending codes, sorted (empty when XML and prefix agree).
    """
    code_set = {r.icd10_code for r in records}
    mismatches: list[str] = []
    for code, xml_parent in parent_of.items():
        if code not in code_set:
            continue
        prefix_ancestors = ancestors_for(code, code_set)
        prefix_parent = prefix_ancestors[-1] if prefix_ancestors else None
        if xml_parent != prefix_parent:
            mismatches.append(code)
    return sorted(mismatches)
