"""LOINC core terms + deprecated->replacement map (versioned reference; ADR 0014).

Parses two files from a LOINC release zip into **flat** reference grains (the
multi-axial hierarchy is deferred -- it is a poly-hierarchy/DAG that doesn't fit
the single-tree contract, modeled separately later):

* ``LoincTableCore/LoincTableCore.csv`` -> :class:`LoincTerm` (the core term table:
  lab tests, measurements, clinical observations).
* ``MapTo.csv`` -> :class:`LoincMapTo` (when LOINC retires a term it publishes the
  successor here, so retired codes can be remapped to current ones).

This module is the single source of truth for LOINC parsing, normalization, and
validation; the entrypoint ``bundles/_reference/src/build_loinc.py`` is thin glue
over it (ADR 0011/0027). It holds **no Spark and no network** -- pure functions
over plain Python, unit tested against real sample rows -- so the bundle entrypoint
does the authenticated download + MD5 verify + unzip and converts the output to
Spark DataFrames writing ``ecdh_model_<env>.codes.loinc`` and ``codes.loinc_map_to``.

Versioning model (ICD-10 per-version, NOT ADR 0032). LOINC ships discrete versions
(~2/year) and the Download API serves every past release, so it is
vintage-reproducible: both tables are keyed by ``loinc_version`` (the release
string, e.g. ``"2.82"``) with ``snapshot_replace`` -- replace this version's rows,
retain other versions (the geography per-vintage / ICD-10 per-edition pattern, ADR
0024). No history-snapshot machinery; the source preserves versions.

Licensed source (Regenstrief): free but attribution-required and
redistribution-restricted, so both tables register ``access_tier="restricted"`` /
``dua_required=True`` (mirroring the NHGIS pattern). Some terms carry third-party
copyright (``EXTERNAL_COPYRIGHT_NOTICE``) -- the column is kept and the restricted
posture covers it.

Sources:
    * LOINC: https://loinc.org/
    * Download API: https://loinc.org/kb/api/download (auth: https://loinc.org/kb/api/auth)
    * License: https://loinc.org/license/
"""

from __future__ import annotations

import csv
import io
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: LOINC Download API base. ``GET /Loinc?version=<v>`` returns release metadata
#: (``numberOfLoincs``, ``downloadMD5Hash``, ...); ``GET /Loinc/Download?version=<v>``
#: returns the release zip. Auth is HTTP Basic (LOINC username/password).
API_BASE_URL = "https://loinc.regenstrief.org/api/v1"

SOURCE_LANDING_URL = "https://loinc.org/"
SOURCE_DOCUMENTATION_URL = "https://loinc.org/kb/api/download"
#: LOINC Database Structure / file readmes serve as the data dictionary.
SOURCE_DATA_DICTIONARY_URL = "https://loinc.org/kb/file-structures/"
LICENSE_URL = "https://loinc.org/license/"

#: Member basenames inside the release zip (selected case-insensitively by the
#: entrypoint; ``MapTo.csv`` lives under ``LoincTableCore/`` or ``LoincTable/``).
CORE_MEMBER = "LoincTableCore.csv"
MAP_TO_MEMBER = "MapTo.csv"

#: LOINC release CSVs are UTF-8 (sometimes with a BOM); the entrypoint decodes
#: ``utf-8-sig`` so a leading BOM never corrupts the first column name.
SOURCE_ENCODING = "utf-8-sig"


class LoincStatus(StrEnum):
    """Controlled vocabulary for ``status`` (normalized from LOINC ``STATUS``)."""

    ACTIVE = "active"
    TRIAL = "trial"
    DISCOURAGED = "discouraged"
    DEPRECATED = "deprecated"


#: Membership set for the blocking ``status`` controlled-vocabulary check (ADR 0016).
LOINC_STATUS_VALUES: frozenset[str] = frozenset(s.value for s in LoincStatus)

#: Statuses a deprecated term in MapTo should carry in the core table (WARN check).
_RETIRED_STATUSES: frozenset[str] = frozenset({LoincStatus.DEPRECATED, LoincStatus.DISCOURAGED})

#: Expected size band for the core table (LOINC 2.82 has ~109k terms). The exact
#: count is checked against the API's ``numberOfLoincs`` by the entrypoint; this is
#: a coarse fallback band.
CORE_CARDINALITY_MIN = 90_000


def normalize_status(raw: str) -> str:
    """Normalize a raw LOINC ``STATUS`` to its lower-case controlled form.

    Does **not** validate membership -- pass records to
    :func:`find_status_violations` so an unrecognized status surfaces as DQ.

    Examples:
        >>> normalize_status("ACTIVE")
        'active'
        >>> normalize_status("DEPRECATED")
        'deprecated'
        >>> normalize_status("")
        ''
    """
    return raw.strip().lower()


def normalize_loinc_num(raw: str) -> str:
    """Normalize a LOINC number to its canonical string form (whitespace-stripped).

    LOINC numbers are kept as strings (e.g. ``"2160-0"``) -- the trailing digit is a
    Mod-10 check digit, and they are never used arithmetically.
    """
    return raw.strip()


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LoincTerm:
    """One LOINC core term (PK ``(loinc_num, loinc_version)``).

    ``loinc_version`` / ``source_file`` / ``loaded_at`` are stamped by the bundle
    entrypoint. ``loinc_class`` carries the source ``CLASS`` (renamed to avoid the
    reserved word); ``status`` is the normalized controlled value and
    ``status_raw`` preserves the source value for DQ.
    """

    loinc_num: str
    component: str
    property: str
    time_aspct: str
    system: str
    scale_typ: str
    method_typ: str
    loinc_class: str
    classtype: str
    long_common_name: str
    shortname: str
    external_copyright_notice: str
    status: str
    version_first_released: str
    version_last_changed: str
    status_raw: str


@dataclass(frozen=True)
class LoincMapTo:
    """One deprecated->replacement mapping (PK ``(loinc_num, loinc_version)``).

    ``loinc_num`` is the deprecated code; ``map_to_loinc_num`` is the replacement,
    which FK-references :class:`LoincTerm` in the same version.
    """

    loinc_num: str
    map_to_loinc_num: str
    comment: str


# ---------------------------------------------------------------------------
# Parsing (CSV, header-driven; LONG_COMMON_NAME contains commas so use csv)
# ---------------------------------------------------------------------------


def _read(text: str) -> tuple[list[list[str]], Callable[..., str]]:
    """Return ``(data_rows, get)`` from CSV ``text`` with a header-name getter.

    Uses the ``csv`` module (quoted fields, embedded commas), upper-cases the
    header, and returns ``get(row, *names)`` reading the first present column.
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = [h.strip().lstrip("﻿").upper() for h in next(reader)]
    except StopIteration:
        return [], (lambda row, *names: "")
    pos = {name: i for i, name in enumerate(header)}
    rows = [r for r in reader if any(c.strip() for c in r)]

    def get(row: list[str], *names: str) -> str:
        for name in names:
            i = pos.get(name)
            if i is not None and i < len(row):
                return row[i].strip()
        return ""

    return rows, get


def parse_loinc_core(text: str) -> list[LoincTerm]:
    """Parse ``LoincTableCore.csv`` into :class:`LoincTerm` records (header-driven)."""
    rows, get = _read(text)
    records: list[LoincTerm] = []
    for row in rows:
        status_raw = get(row, "STATUS")
        records.append(
            LoincTerm(
                loinc_num=normalize_loinc_num(get(row, "LOINC_NUM")),
                component=get(row, "COMPONENT"),
                property=get(row, "PROPERTY"),
                time_aspct=get(row, "TIME_ASPCT"),
                system=get(row, "SYSTEM"),
                scale_typ=get(row, "SCALE_TYP"),
                method_typ=get(row, "METHOD_TYP"),
                loinc_class=get(row, "CLASS"),
                classtype=get(row, "CLASSTYPE"),
                long_common_name=get(row, "LONG_COMMON_NAME"),
                shortname=get(row, "SHORTNAME"),
                external_copyright_notice=get(row, "EXTERNAL_COPYRIGHT_NOTICE"),
                status=normalize_status(status_raw),
                version_first_released=get(row, "VERSIONFIRSTRELEASED", "VERSION_FIRST_RELEASED"),
                version_last_changed=get(row, "VERSIONLASTCHANGED", "VERSION_LAST_CHANGED"),
                status_raw=status_raw,
            )
        )
    return records


def parse_map_to(text: str) -> list[LoincMapTo]:
    """Parse ``MapTo.csv`` into :class:`LoincMapTo` records (header-driven)."""
    rows, get = _read(text)
    records: list[LoincMapTo] = []
    for row in rows:
        records.append(
            LoincMapTo(
                loinc_num=normalize_loinc_num(get(row, "LOINC")),
                map_to_loinc_num=normalize_loinc_num(get(row, "MAP_TO")),
                comment=get(row, "COMMENT"),
            )
        )
    return records


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def _duplicates(keys) -> list:
    seen: dict = {}
    for k in keys:
        seen[k] = seen.get(k, 0) + 1
    return [k for k, n in seen.items() if n > 1]


def find_duplicate_loinc_nums(terms: list[LoincTerm]) -> list[str]:
    """Duplicate ``loinc_num`` within a version (blocking PK uniqueness)."""
    return _duplicates(t.loinc_num for t in terms)


def find_missing_term_fields(terms: list[LoincTerm]) -> list[tuple[str, str]]:
    """``(loinc_num, field)`` for blank required term fields (blocking).

    Required: ``loinc_num``, ``long_common_name``, ``status``.
    """
    missing: list[tuple[str, str]] = []
    for t in terms:
        for field, value in (
            ("loinc_num", t.loinc_num),
            ("long_common_name", t.long_common_name),
            ("status", t.status),
        ):
            if not value.strip():
                missing.append((t.loinc_num, field))
    return missing


def find_status_violations(terms: list[LoincTerm]) -> list[tuple[str, str]]:
    """``(loinc_num, status_raw)`` for statuses outside the controlled vocab (blocking)."""
    return [(t.loinc_num, t.status_raw) for t in terms if t.status not in LOINC_STATUS_VALUES]


def find_duplicate_map_keys(maps: list[LoincMapTo]) -> list[tuple[str, str]]:
    """Duplicate ``(loinc_num, map_to_loinc_num)`` within a version (blocking PK).

    A deprecated LOINC can map to several replacements, so the deprecated ``loinc_num``
    alone is not unique; the (deprecated, replacement) pair is.
    """
    return _duplicates((m.loinc_num, m.map_to_loinc_num) for m in maps)


def find_missing_map_fields(maps: list[LoincMapTo]) -> list[tuple[str, str]]:
    """``(loinc_num, field)`` for blank ``loinc_num`` / ``map_to_loinc_num`` (blocking)."""
    missing: list[tuple[str, str]] = []
    for m in maps:
        for field, value in (("loinc_num", m.loinc_num), ("map_to_loinc_num", m.map_to_loinc_num)):
            if not value.strip():
                missing.append((m.loinc_num, field))
    return missing


def find_map_target_orphans(
    maps: list[LoincMapTo], term_loinc_nums: set[str]
) -> list[tuple[str, str]]:
    """``(loinc_num, map_to_loinc_num)`` where the replacement isn't a term (blocking FK).

    Every ``map_to_loinc_num`` must resolve to a ``codes.loinc`` row in the same version.
    """
    return [
        (m.loinc_num, m.map_to_loinc_num) for m in maps if m.map_to_loinc_num not in term_loinc_nums
    ]


def find_map_source_not_retired(
    maps: list[LoincMapTo], status_by_loinc: dict[str, str]
) -> list[tuple[str, str]]:
    """``(loinc_num, status)`` where a mapped (deprecated) code isn't retired (WARN).

    A code that appears in MapTo should be ``deprecated``/``discouraged`` in the core
    table; anything else (or absent) is a WARN-level anomaly, not a build blocker.
    """
    offenders: list[tuple[str, str]] = []
    for m in maps:
        status = status_by_loinc.get(m.loinc_num)
        if status is None or status not in _RETIRED_STATUSES:
            offenders.append((m.loinc_num, status or "<absent>"))
    return offenders


def find_missing_name_axes(terms: list[LoincTerm]) -> list[str]:
    """Non-deprecated ``loinc_num`` missing any of the six name axes (WARN).

    The axes (``component``/``property``/``time_aspct``/``system``/``scale_typ``/
    ``method_typ``) are expected for active terms; ``method_typ`` is legitimately
    blank for many terms, so it is excluded from the required set.
    """
    bad: list[str] = []
    for t in terms:
        if t.status == LoincStatus.DEPRECATED:
            continue
        if not (t.component and t.property and t.time_aspct and t.system and t.scale_typ):
            bad.append(t.loinc_num)
    return bad


def status_distribution(terms: list[LoincTerm]) -> dict[str, int]:
    """Return ``{status: count}`` (backs an INFO/WARN status-distribution record)."""
    dist: dict[str, int] = {}
    for t in terms:
        dist[t.status] = dist.get(t.status, 0) + 1
    return dist
