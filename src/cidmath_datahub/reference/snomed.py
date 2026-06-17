"""SNOMED CT US Edition concepts (versioned reference; ADR 0014).

Parses the SNOMED CT US Edition **RF2 Snapshot** files into a **flat** reference
grain: one row per active concept with its Fully Specified Name (FSN), preferred
term, and semantic tag. The IS-A relationship graph (a polyhierarchy / DAG of
millions of edges) is deferred to a separate major effort (it doesn't fit the
single-tree hierarchy contract); v1 is concept + names only.

This module is the single source of truth for RF2 parsing, SCTID validation, and
the FSN/preferred-term reduction; the entrypoint
``bundles/_reference/src/build_snomed.py`` is thin glue over it (ADR 0011/0027). It
holds **no Spark and no network** -- pure functions over plain Python, unit tested
against real RF2 sample rows -- so the bundle entrypoint does the authenticated UMLS
download + unzip and converts the output to a Spark DataFrame writing
``ecdh_model_<env>.codes.snomed``.

Versioning model (ICD-10 per-version, NOT ADR 0032). SNOMED CT US Edition ships
semi-annually (~Mar 1 / Sep 1) and the NLM archives every release, so it is
re-pullable: ``codes.snomed`` is keyed by ``snomed_version`` (the release effective
date, e.g. ``"20260301"``) with ``snapshot_replace`` -- replace this release's rows,
retain others (ADR 0024). Uses the RF2 **Snapshot** view (current state of each
component as of the release), not Full (history) or Delta.

Three RF2 Snapshot files are joined to build each row:
  * ``sct2_Concept_Snapshot`` -> the concept (id, active, moduleId, effectiveTime).
  * ``sct2_Description_Snapshot-en`` -> descriptions (term, typeId = FSN vs Synonym).
  * ``der2_cRefset_LanguageSnapshot-en`` -> the US-English language refset, which
    marks one synonym per concept as *preferred*.
The FSN carries the semantic tag in trailing parentheses (e.g. "Diabetes mellitus
(disorder)"), which is the flat grouping (the SNOMED analog of LOINC ``CLASS``).

Licensed source: SNOMED CT is proprietary (SNOMED International); the US Edition is
free for US use via the NLM under the UMLS Metathesaurus License (which includes the
SNOMED CT affiliate license). Both register ``access_tier="restricted"`` /
``dua_required=True``; no external redistribution.

Sources:
    * SNOMED CT US Edition (NLM): https://www.nlm.nih.gov/healthit/snomedct/
    * RF2 release format: https://confluence.ihtsdotools.org/display/DOCRELFMT
    * UTS automating downloads: https://documentation.uts.nlm.nih.gov/automating-downloads.html
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec + RF2 metadata constants (single-sourced here; ADR 0011)
# ---------------------------------------------------------------------------

SOURCE_LANDING_URL = "https://www.nlm.nih.gov/healthit/snomedct/"
SOURCE_DOCUMENTATION_URL = "https://documentation.uts.nlm.nih.gov/automating-downloads.html"
SOURCE_DATA_DICTIONARY_URL = "https://confluence.ihtsdotools.org/display/DOCRELFMT"
#: UTS download proxy: GET ?url=<NLM file url>&apiKey=<UMLS key> returns the file.
UTS_DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"

#: RF2 files are UTF-8, tab-delimited, with a header row.
SOURCE_ENCODING = "utf-8"

#: RF2 Snapshot member patterns inside the US Edition release zip (matched on the
#: basename, case-insensitively). Snapshot = current state as of the release.
CONCEPT_MEMBER_RE = re.compile(r"sct2_Concept_Snapshot.*\.txt$", re.IGNORECASE)
DESCRIPTION_MEMBER_RE = re.compile(r"sct2_Description_Snapshot-en.*\.txt$", re.IGNORECASE)
LANGUAGE_MEMBER_RE = re.compile(r"der2_cRefset_LanguageSnapshot-en.*\.txt$", re.IGNORECASE)

#: SNOMED metadata concept IDs used to interpret the RF2 rows.
FSN_TYPE_ID = "900000000000003001"  # description typeId: Fully Specified Name
SYNONYM_TYPE_ID = "900000000000013009"  # description typeId: Synonym
PREFERRED_ACCEPTABILITY_ID = "900000000000548007"  # language refset: Preferred
US_ENGLISH_REFSET_ID = "900000000000509007"  # US English language reference set

#: Expected active-concept count band (US Edition ~360k+); coarse cardinality WARN.
ACTIVE_CONCEPT_MIN = 300_000

#: The trailing "(semantic tag)" on an FSN, e.g. "... (disorder)".
_SEMANTIC_TAG_RE = re.compile(r"\(([^()]+)\)\s*$")

#: Published SNOMED CT semantic tags (a.k.a. hierarchy tags), per the SNOMED Editorial Guide /
#: the Machine-Readable Concept Model, verified present in the US Edition. This is the
#: authority for "is this a real tag": :func:`assemble_concepts` keeps a parsed FSN tag only if
#: it is recognized (in this set OR carried by an active concept this release -- see there).
#: SNOMED adds a tag only rarely; ``find_active_unrecognized_tags`` WARNs when an active concept
#: carries a tag missing from this set, so it can be refreshed. Curating in only confident-real
#: tags is deliberate: a wrong entry here would let junk survive, whereas a *missing* real tag
#: is still kept (via active usage) and surfaced by the WARN.
SNOMED_SEMANTIC_TAGS: frozenset[str] = frozenset(
    {
        # Clinical finding
        "finding",
        "disorder",
        # Procedure
        "procedure",
        "regime/therapy",
        # Body structure
        "body structure",
        "morphologic abnormality",
        "cell",
        "cell structure",
        # Organism / substance / product
        "organism",
        "substance",
        "product",
        "medicinal product",
        "medicinal product form",
        "clinical drug",
        "virtual clinical drug",
        "physical object",
        "physical force",
        # Specimen / observable / event / situation
        "specimen",
        "observable entity",
        "event",
        "situation",
        # Qualifier / attribute / value sets
        "qualifier value",
        "attribute",
        "administration method",
        "basic dose form",
        "dose form",
        "intended site",
        "unit of presentation",
        "disposition",
        "state of matter",
        "release characteristic",
        "transformation",
        "supplier",
        "product name",
        # Scales / staging
        "assessment scale",
        "staging scale",
        "tumor staging",
        # Social / context
        "social concept",
        "person",
        "ethnic group",
        "racial group",
        "occupation",
        "religion/philosophy",
        "life style",
        "role",
        "administrative concept",
        # Environment / location
        "environment",
        "environment / location",
        "geographic location",
        # Record / special / navigational
        "record artifact",
        "special concept",
        "navigational concept",
        "context-dependent category",
        "inactive concept",
        "biological function",
        "calculation",
        # Metadata model
        "namespace concept",
        "core metadata concept",
        "foundation metadata concept",
        "linkage concept",
        "link assertion",
        "OWL metadata concept",
        "metadata",
    }
)


# ---------------------------------------------------------------------------
# SCTID validation (Verhoeff check digit; the error-prone bit)
# ---------------------------------------------------------------------------

# Verhoeff dihedral-group tables (D5). SNOMED SCTIDs carry a Verhoeff check digit
# as their last digit, so a valid SCTID checksums to 0 over all its digits.
_VERHOEFF_D = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0),
)
_VERHOEFF_P = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8),
)


def _verhoeff_ok(number: str) -> bool:
    """Return True if ``number`` (digits incl. the trailing check digit) checksums to 0."""
    c = 0
    for i, ch in enumerate(reversed(number)):
        c = _VERHOEFF_D[c][_VERHOEFF_P[i % 8][int(ch)]]
    return c == 0


def validate_sctid(sctid: str) -> bool:
    """Return True if ``sctid`` is a well-formed SNOMED identifier.

    Checks: all digits, length 6-18, no leading zero, and a valid Verhoeff check
    digit. (Does not assert the partition identifier is specifically a *concept*
    partition -- that is a softer rule left out of v1 validity.)

    Examples:
        >>> validate_sctid("73211009")   # Diabetes mellitus (disorder)
        True
        >>> validate_sctid("73211008")   # wrong check digit
        False
        >>> validate_sctid("12x")
        False
    """
    if not sctid.isdigit() or not (6 <= len(sctid) <= 18) or sctid[0] == "0":
        return False
    return _verhoeff_ok(sctid)


def parse_semantic_tag(fsn: str) -> str:
    """Return the trailing ``(semantic tag)`` text from an FSN, or ``""`` if none.

    Examples:
        >>> parse_semantic_tag("Diabetes mellitus (disorder)")
        'disorder'
        >>> parse_semantic_tag("Appendectomy (procedure)")
        'procedure'
        >>> parse_semantic_tag("No tag here")
        ''
    """
    match = _SEMANTIC_TAG_RE.search(fsn)
    return match.group(1).strip() if match else ""


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnomedConceptRow:
    """One raw row from ``sct2_Concept_Snapshot``."""

    concept_id: str
    active: bool
    module_id: str
    effective_time: str
    definition_status_id: str


@dataclass(frozen=True)
class SnomedDescription:
    """One raw row from ``sct2_Description_Snapshot-en``."""

    description_id: str
    active: bool
    concept_id: str
    type_id: str
    term: str
    language_code: str


@dataclass(frozen=True)
class SnomedConcept:
    """One assembled output row (PK ``(concept_id, snomed_version)``).

    ``snomed_version`` / ``source_file`` / ``loaded_at`` are stamped by the bundle
    entrypoint. ``fsn`` is the active Fully Specified Name; ``preferred_term`` is the
    US-English preferred synonym; ``semantic_tag`` is parsed from the FSN.
    """

    concept_id: str
    fsn: str
    preferred_term: str
    semantic_tag: str
    active: bool
    module_id: str
    effective_time: str


# ---------------------------------------------------------------------------
# Parsing (RF2 tab-delimited, header-driven)
# ---------------------------------------------------------------------------


def _read(text: str) -> tuple[list[list[str]], Callable[..., str]]:
    """Return ``(data_rows, get)`` from tab-delimited RF2 ``text`` with a header getter."""
    lines = text.split("\n")
    if not lines:
        return [], (lambda row, *names: "")
    header = [h.strip() for h in lines[0].split("\t")]
    pos = {name: i for i, name in enumerate(header)}
    rows = [ln.split("\t") for ln in lines[1:] if ln.strip()]

    def get(row: list[str], *names: str) -> str:
        for name in names:
            i = pos.get(name)
            if i is not None and i < len(row):
                return row[i].strip()
        return ""

    return rows, get


def parse_concepts(text: str) -> list[SnomedConceptRow]:
    """Parse ``sct2_Concept_Snapshot`` into concept rows (header-driven)."""
    rows, get = _read(text)
    return [
        SnomedConceptRow(
            concept_id=get(row, "id"),
            active=get(row, "active") == "1",
            module_id=get(row, "moduleId"),
            effective_time=get(row, "effectiveTime"),
            definition_status_id=get(row, "definitionStatusId"),
        )
        for row in rows
    ]


def parse_descriptions(text: str) -> list[SnomedDescription]:
    """Parse ``sct2_Description_Snapshot-en`` into description rows (header-driven)."""
    rows, get = _read(text)
    return [
        SnomedDescription(
            description_id=get(row, "id"),
            active=get(row, "active") == "1",
            concept_id=get(row, "conceptId"),
            type_id=get(row, "typeId"),
            term=get(row, "term"),
            language_code=get(row, "languageCode"),
        )
        for row in rows
    ]


def parse_preferred_description_ids(text: str, refset_id: str = US_ENGLISH_REFSET_ID) -> set[str]:
    """Return the set of description ids marked *preferred* in the language refset.

    Reads ``der2_cRefset_LanguageSnapshot-en`` and keeps active rows for ``refset_id``
    whose ``acceptabilityId`` is Preferred; the ``referencedComponentId`` is the
    description id.
    """
    rows, get = _read(text)
    preferred: set[str] = set()
    for row in rows:
        if (
            get(row, "active") == "1"
            and get(row, "refsetId") == refset_id
            and get(row, "acceptabilityId") == PREFERRED_ACCEPTABILITY_ID
        ):
            preferred.add(get(row, "referencedComponentId"))
    return preferred


# ---------------------------------------------------------------------------
# Assembly: one row per concept with FSN + preferred term + semantic tag
# ---------------------------------------------------------------------------


def _active_descriptions_by_concept(
    descriptions: Iterable[SnomedDescription],
) -> dict[str, list[SnomedDescription]]:
    by_concept: dict[str, list[SnomedDescription]] = defaultdict(list)
    for d in descriptions:
        if d.active:
            by_concept[d.concept_id].append(d)
    return by_concept


def assemble_concepts(
    concepts: list[SnomedConceptRow],
    descriptions: list[SnomedDescription],
    preferred_description_ids: set[str],
) -> list[SnomedConcept]:
    """Reduce the RF2 rows to one :class:`SnomedConcept` per concept.

    For each concept, the FSN is its active ``FSN``-typed description and the preferred
    term is its active ``Synonym``-typed description whose id is in
    ``preferred_description_ids`` (the US-English language refset). Concepts with no
    active FSN get an empty ``fsn`` (surfaced by DQ for active concepts); the entrypoint
    keeps the ``active`` flag so inactive concepts are distinguishable.

    Semantic tag: the trailing ``(...)`` is only a true semantic tag on *some* FSNs --
    legacy/inactive concepts (e.g. Read-code-derived "O/E - abdominal movement (& wall)")
    can end in a parenthetical that is part of the term, which the structural parser would
    otherwise mis-read as ``semantic_tag = "& wall"``. So a parsed tag is kept only when
    **recognized**: in the published :data:`SNOMED_SEMANTIC_TAGS` set (the authority for
    "is this a real tag"), OR carried by an active concept in this release. The second arm
    is authoritative too -- SNOMED guarantees active FSNs end in an approved tag -- and lets
    a newly-introduced tag flow through before the published constant is refreshed (the
    entrypoint WARNs on those via :func:`find_active_unrecognized_tags`). An inactive concept
    thus keeps a genuine ``(disorder)`` but drops ``(& wall)``; the FSN itself is untouched.

    Args:
        concepts: Parsed ``sct2_Concept_Snapshot`` rows.
        descriptions: Parsed ``sct2_Description_Snapshot-en`` rows.
        preferred_description_ids: From :func:`parse_preferred_description_ids`.

    Returns:
        One row per input concept, in input order.
    """
    by_concept = _active_descriptions_by_concept(descriptions)

    # FSN per concept (first active FSN-typed description, else "").
    fsn_by_id: dict[str, str] = {}
    for c in concepts:
        fsns = [d.term for d in by_concept.get(c.concept_id, []) if d.type_id == FSN_TYPE_ID]
        fsn_by_id[c.concept_id] = fsns[0] if fsns else ""

    # Recognized tags = the published set plus any tag an active concept carries this release.
    active_tags = {parse_semantic_tag(fsn_by_id[c.concept_id]) for c in concepts if c.active}
    active_tags.discard("")
    recognized_tags = SNOMED_SEMANTIC_TAGS | active_tags

    out: list[SnomedConcept] = []
    for c in concepts:
        descs = by_concept.get(c.concept_id, [])
        preferred = [
            d.term
            for d in descs
            if d.type_id == SYNONYM_TYPE_ID and d.description_id in preferred_description_ids
        ]
        fsn = fsn_by_id[c.concept_id]
        parsed_tag = parse_semantic_tag(fsn)
        out.append(
            SnomedConcept(
                concept_id=c.concept_id,
                fsn=fsn,
                preferred_term=preferred[0] if preferred else "",
                semantic_tag=parsed_tag if parsed_tag in recognized_tags else "",
                active=c.active,
                module_id=c.module_id,
                effective_time=c.effective_time,
            )
        )
    return out


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_concept_ids(rows: list[SnomedConcept]) -> list[str]:
    """Duplicate ``concept_id`` within a release (blocking PK uniqueness)."""
    seen: dict[str, int] = {}
    for r in rows:
        seen[r.concept_id] = seen.get(r.concept_id, 0) + 1
    return [cid for cid, n in seen.items() if n > 1]


def find_invalid_sctids(rows: list[SnomedConcept]) -> list[str]:
    """``concept_id`` values that fail SCTID validation (blocking)."""
    return [r.concept_id for r in rows if not validate_sctid(r.concept_id)]


def find_active_missing_fsn(rows: list[SnomedConcept]) -> list[str]:
    """Active concepts with a blank ``fsn`` (blocking non-null FSN for active concepts)."""
    return [r.concept_id for r in rows if r.active and not r.fsn.strip()]


def find_active_fsn_count_anomalies(
    concepts: list[SnomedConceptRow], descriptions: list[SnomedDescription]
) -> list[str]:
    """Active concepts that don't have exactly one active FSN description (blocking)."""
    by_concept = _active_descriptions_by_concept(descriptions)
    bad: list[str] = []
    for c in concepts:
        if not c.active:
            continue
        n_fsn = sum(1 for d in by_concept.get(c.concept_id, []) if d.type_id == FSN_TYPE_ID)
        if n_fsn != 1:
            bad.append(c.concept_id)
    return bad


def find_active_preferred_count_anomalies(
    concepts: list[SnomedConceptRow],
    descriptions: list[SnomedDescription],
    preferred_description_ids: set[str],
) -> list[str]:
    """Active concepts that don't have exactly one preferred synonym (blocking)."""
    by_concept = _active_descriptions_by_concept(descriptions)
    bad: list[str] = []
    for c in concepts:
        if not c.active:
            continue
        n_pref = sum(
            1
            for d in by_concept.get(c.concept_id, [])
            if d.type_id == SYNONYM_TYPE_ID and d.description_id in preferred_description_ids
        )
        if n_pref != 1:
            bad.append(c.concept_id)
    return bad


def inactive_count(rows: list[SnomedConcept]) -> int:
    """Number of inactive concepts (backs the inactive-share WARN)."""
    return sum(1 for r in rows if not r.active)


def semantic_tag_distribution(rows: list[SnomedConcept]) -> dict[str, int]:
    """``{semantic_tag: count}`` over active concepts (backs the distribution WARN)."""
    dist: dict[str, int] = {}
    for r in rows:
        if r.active:
            tag = r.semantic_tag or "<none>"
            dist[tag] = dist.get(tag, 0) + 1
    return dist


def find_active_missing_preferred(rows: list[SnomedConcept]) -> list[str]:
    """Active concepts with a blank ``preferred_term`` (backs a coverage WARN)."""
    return [r.concept_id for r in rows if r.active and not r.preferred_term.strip()]


def find_active_unrecognized_tags(rows: list[SnomedConcept]) -> list[tuple[str, str]]:
    """``(concept_id, semantic_tag)`` for active concepts whose tag is outside the published set.

    These are kept (an active FSN's tag is authoritative) but flagged so the published set can
    be refreshed when SNOMED introduces a tag -- or a parse anomaly investigated. Backs a WARN.
    """
    return [
        (r.concept_id, r.semantic_tag)
        for r in rows
        if r.active and r.semantic_tag and r.semantic_tag not in SNOMED_SEMANTIC_TAGS
    ]
