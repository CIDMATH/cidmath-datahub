"""NLM RxNorm normalized drug concepts (versioned reference; ADR 0014).

Parses ``RXNCONSO.RRF`` from the RxNorm full monthly release into a **flat** reference
grain: one row per RxNorm concept (``RXCUI``) with its normalized name and term type
(``TTY``). Relationships (``RXNREL``), attributes incl. the RxNorm<->NDC crosswalk
(``RXNSAT``), source-vocab atoms (``SAB != RXNORM``), and the atom/synonym grain are
deferred to separate efforts that reuse the same downloaded RRF.

This module is the single source of truth for RRF parsing + the RxCUI reduction; the
entrypoint ``bundles/_reference/src/build_rxnorm.py`` is thin glue over it (ADR 0011/0027).
It holds **no Spark and no network** -- pure functions over plain Python, unit tested
against real RRF sample lines -- so the bundle entrypoint does the authenticated UMLS/UTS
download + unzip and converts the output to a Spark DataFrame writing
``ecdh_model_<env>.codes.rxnorm``.

Versioning model (ICD-10 per-version, NOT ADR 0032). RxNorm ships monthly and the NLM
archives every release back to 2005, so it is re-pullable: ``codes.rxnorm`` is keyed by
``rxnorm_version`` (the release identifier, e.g. ``"04072025"``) with ``snapshot_replace``
-- replace this release's rows, retain others (ADR 0024).

Scope = ``SAB=RXNORM``, ``SUPPRESS=N`` (RxNorm's own atoms, not the licensed source
vocabularies the full RRF also carries) -- which keeps the stored data non-proprietary.
A single RXCUI has several atoms (the concept's name plus synonyms / a prescribable name /
tall-man synonyms); the canonical row uses the concept's **defining** atom (a non-synonym
TTY), so synonyms (``SY``/``TMSY``/``PSN``) do not become the name.

License: the RxNorm vocabulary is non-proprietary (NLM); the UTS *download* is gated by a
UMLS account, but ``SAB=RXNORM``-scoped data carries no redistribution restriction.

Sources:
    * RxNorm (NLM): https://www.nlm.nih.gov/research/umls/rxnorm/
    * RRF file format: https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html
    * UTS automating downloads: https://documentation.uts.nlm.nih.gov/automating-downloads.html
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Source spec + RRF layout (single-sourced here; the entrypoint does the IO)
# ---------------------------------------------------------------------------

SOURCE_LANDING_URL = "https://www.nlm.nih.gov/research/umls/rxnorm/"
SOURCE_DOCUMENTATION_URL = "https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html"
SOURCE_DATA_DICTIONARY_URL = SOURCE_DOCUMENTATION_URL
#: UTS download proxy: GET ?url=<NLM file url>&apiKey=<UMLS key> returns the file.
UTS_DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"

#: RRF is pipe-delimited UTF-8 with NO header row. The full release ships ``RXNCONSO.RRF``
#: under ``rrf/`` AND a smaller "Prescribable Content" subset under ``prescribe/rrf/``; we use
#: the full file (see :func:`select_conso_member`).
SOURCE_ENCODING = "utf-8"
CONSO_MEMBER = "RXNCONSO.RRF"

#: We keep only RxNorm's own active atoms.
RXNORM_SAB = "RXNORM"
SUPPRESS_OK = "N"

# RXNCONSO.RRF column positions (0-indexed; RRF has no header). The file ends each
# line with a trailing '|', so a split yields one extra empty trailing field.
_COL_RXCUI = 0
_COL_ISPREF = 6
_COL_RXAUI = 7
_COL_SAB = 11
_COL_TTY = 12
_COL_STR = 14
_COL_SUPPRESS = 16
_MIN_COLS = 17  # RXNCONSO has 18 fields; require at least through SUPPRESS

#: Known RxNorm term types (``SAB=RXNORM``), per the NLM RxNorm "Appendix 5" term-type list.
#: Backs the ``tty`` *recognition* WARN -- NOT blocking: TTY is descriptive source metadata and
#: the RxNorm TTY set can grow, so an unrecognized value is surfaced for review, never fatal.
#: Includes the entry-term (``ET``) and synonym (``SY``/``TMSY``/``PSN``) types.
RXNORM_TTY_VALUES: frozenset[str] = frozenset(
    {
        "IN",
        "PIN",
        "MIN",
        "BN",
        "SCDC",
        "SCDF",
        "SCDFP",
        "SCDG",
        "SCDGP",
        "SCD",
        "SBDC",
        "SBDF",
        "SBDFP",
        "SBDG",
        "SBD",
        "BPCK",
        "GPCK",
        "DF",
        "DFG",
        "SY",
        "TMSY",
        "PSN",
        "ET",
    }
)

#: TTYs that are alternate *names of* a concept (synonyms / prescribable name / tall-man /
#: entry term) rather than the concept's own defining atom; these are deprioritized when
#: choosing the canonical name/TTY for an RXCUI, so a defining atom (IN/SCD/SBD/...) wins.
_SYNONYM_TTYS: frozenset[str] = frozenset({"SY", "TMSY", "PSN", "ET"})

#: Sanity band for the active SAB=RXNORM concept count (hundreds of thousands).
CARDINALITY_MIN = 100_000


def select_conso_member(names: Iterable[str]) -> str:
    """Pick the full-release ``RXNCONSO.RRF`` from a zip's name list (ADR 0011 keeps IO out).

    The RxNorm full monthly release bundles two ``RXNCONSO.RRF`` files: the full file at
    ``rrf/RXNCONSO.RRF`` and a smaller **Prescribable Content** subset at
    ``prescribe/rrf/RXNCONSO.RRF``. Match by basename, then exclude any member under a
    ``prescribe`` path segment so we always load the full file.

    Args:
        names: Member names in the downloaded zip.

    Returns:
        The single full-release ``RXNCONSO.RRF`` member name.

    Raises:
        ValueError: If zero or more than one full member remains after excluding the
            ``prescribe/`` subset -- a sign the release layout changed.
    """
    matches = [n for n in names if n.replace("\\", "/").split("/")[-1].upper() == CONSO_MEMBER]
    full = [n for n in matches if "prescribe" not in n.lower().replace("\\", "/").split("/")]
    if len(full) != 1:
        raise ValueError(
            f"Expected exactly one full {CONSO_MEMBER} (excluding the prescribe/ subset); "
            f"found full={full}, all={matches}"
        )
    return full[0]


@dataclass(frozen=True)
class RxnormAtom:
    """One ``SAB=RXNORM``, ``SUPPRESS=N`` atom from ``RXNCONSO.RRF``."""

    rxcui: str
    tty: str
    name: str
    rxaui: str
    is_pref: bool


@dataclass(frozen=True)
class RxnormConcept:
    """One reduced output row (PK ``(rxcui, rxnorm_version)``).

    ``rxnorm_version`` / ``source_file`` / ``loaded_at`` are stamped by the bundle
    entrypoint. ``name`` is the concept's defining-atom string; ``tty`` its term type.
    """

    rxcui: str
    name: str
    tty: str


# ---------------------------------------------------------------------------
# Parsing (RRF: pipe-delimited, positional, no header)
# ---------------------------------------------------------------------------


def parse_rxnconso(text: str) -> list[RxnormAtom]:
    """Parse ``RXNCONSO.RRF``, keeping only ``SAB=RXNORM`` / ``SUPPRESS=N`` atoms.

    RRF is positional (no header) and pipe-delimited; fields never contain an
    unescaped pipe, so a plain split is safe. Filtering here keeps the in-memory set
    to RxNorm's own active atoms.

    Args:
        text: Full contents of ``RXNCONSO.RRF``.

    Returns:
        The kept atoms, in file order.
    """
    atoms: list[RxnormAtom] = []
    for line in text.split("\n"):
        if not line:
            continue
        f = line.split("|")
        if len(f) <= _MIN_COLS:
            continue
        if f[_COL_SAB] != RXNORM_SAB or f[_COL_SUPPRESS] != SUPPRESS_OK:
            continue
        atoms.append(
            RxnormAtom(
                rxcui=f[_COL_RXCUI].strip(),
                tty=f[_COL_TTY].strip(),
                name=f[_COL_STR].strip(),
                rxaui=f[_COL_RXAUI].strip(),
                is_pref=f[_COL_ISPREF].strip().upper() == "Y",
            )
        )
    return atoms


def reduce_to_concepts(atoms: list[RxnormAtom]) -> list[RxnormConcept]:
    """Reduce atoms to one :class:`RxnormConcept` per RXCUI (deterministic).

    Each RxNorm concept has one defining atom (its name + TTY) plus optional synonyms
    (``SY``/``TMSY``) and a prescribable name (``PSN``). The canonical row prefers a
    *non-synonym* atom; ties (rare for ``SAB=RXNORM``) break on the smallest ``RXAUI``
    so the result is stable. Output is sorted by ``rxcui`` (numeric where possible).

    Args:
        atoms: Parsed ``SAB=RXNORM`` atoms from :func:`parse_rxnconso`.

    Returns:
        One row per RXCUI.
    """
    by_rxcui: dict[str, list[RxnormAtom]] = defaultdict(list)
    for a in atoms:
        by_rxcui[a.rxcui].append(a)

    def _sort_key(a: RxnormAtom) -> tuple[int, str]:
        # Non-synonym atoms first; then a stable RXAUI tiebreak.
        synonym_rank = 1 if a.tty in _SYNONYM_TTYS else 0
        return (synonym_rank, a.rxaui.zfill(12))

    concepts: list[RxnormConcept] = []
    for rxcui, group in by_rxcui.items():
        chosen = min(group, key=_sort_key)
        concepts.append(RxnormConcept(rxcui=rxcui, name=chosen.name, tty=chosen.tty))

    concepts.sort(key=lambda c: (len(c.rxcui), c.rxcui))
    return concepts


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_rxcui(concepts: list[RxnormConcept]) -> list[str]:
    """Duplicate ``rxcui`` within a release (blocking PK uniqueness; should be empty)."""
    seen: dict[str, int] = {}
    for c in concepts:
        seen[c.rxcui] = seen.get(c.rxcui, 0) + 1
    return [rxcui for rxcui, n in seen.items() if n > 1]


def find_missing_fields(concepts: list[RxnormConcept]) -> list[tuple[str, str]]:
    """``(rxcui, field)`` for blank required fields (blocking).

    Required: ``rxcui``, ``name``, ``tty``.
    """
    missing: list[tuple[str, str]] = []
    for c in concepts:
        for field, value in (("rxcui", c.rxcui), ("name", c.name), ("tty", c.tty)):
            if not value.strip():
                missing.append((c.rxcui, field))
    return missing


def find_bad_tty(concepts: list[RxnormConcept]) -> list[tuple[str, str]]:
    """``(rxcui, tty)`` for term types outside the RxNorm vocabulary (blocking; ADR 0016)."""
    return [(c.rxcui, c.tty) for c in concepts if c.tty not in RXNORM_TTY_VALUES]


def tty_distribution(concepts: list[RxnormConcept]) -> dict[str, int]:
    """``{tty: count}`` over concepts (backs the TTY-distribution WARN)."""
    dist: dict[str, int] = {}
    for c in concepts:
        dist[c.tty] = dist.get(c.tty, 0) + 1
    return dist
