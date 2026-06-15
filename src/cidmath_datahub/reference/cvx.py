"""CVX vaccine-administered code set (revise-in-place reference; ADR 0014, ADR 0032).

Parses the CDC IIS **CVX** ("Vaccine Administered") code set from the machine-
readable *XML-new* report into a **flat** code set: one row per CVX code, no
classification hierarchy (unlike ``codes.icd10cm`` / ``codes.icd9cm`` -- CVX has no
chapter/block/parent structure). This module is the single source of truth for
CVX parsing, normalization, and validation; the entrypoint
``bundles/_reference/src/build_cvx.py`` is thin glue over it (ADR 0011/0027).

It holds **no Spark** -- pure functions over plain Python data structures, unit
tested against real XML-new records -- so the bundle entrypoint converts the
output to a Spark DataFrame and writes ``ecdh_model_<env>.codes.cvx`` keyed by
``(cvx_code, snapshot_date)`` (ADR 0006, ADR 0015: reference table, no Kimball
suffix).

History model (ADR 0032). CVX is **revised in place**: codes change status,
descriptions are edited, and new codes appear, but CDC publishes only the
*current* list (with a per-code "Last Updated" date) -- there is no published
archive of past states. So we preserve history ourselves: the entrypoint writes
the raw XML verbatim to an immutable, date-stamped UC Volume file, and the table
keeps every snapshot, keyed by ``snapshot_date`` with ``snapshot_replace``
semantics (the geography per-vintage replace, ADR 0024, with ``snapshot_date``
as the vintage key). "Current" is the latest ``snapshot_date``.

Source format (``XML2.asp?rpt=cvx``), ISO-8859-1, e.g.::

    <CVXCodes>
      <CVXInfo>
        <ShortDescription>Adenovirus types 4 and 7</ShortDescription>
        <FullVaccinename>Adenovirus, type 4 and type 7, live, oral</FullVaccinename>
        <CVXCode>143       </CVXCode>
        <Notes>This vaccine is administered as 2 tablets.</Notes>
        <Status>Active</Status>
        <LastUpdated>3/20/2011</LastUpdated>
      </CVXInfo>
      ...
    </CVXCodes>

Note that the XML-new payload carries only the six elements above; it does **not**
expose the HTML page's "Nonvaccine" column or any vaccine-group mapping. We do not
synthesize a non-vaccine flag (the source provides none); every record is loaded
as published, and administrative codes such as ``998`` ("no vaccine administered")
are kept alongside the rest, distinguishable by ``cvx_code`` / description. The
CVX->vaccine-group mapping is out of scope and arrives via a separate CDSi job later.

Sources:
    * Landing page: https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx
    * XML-new download: https://www2a.cdc.gov/vaccines/iis/iisstandards/XML2.asp?rpt=cvx
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: The machine-readable "XML-new format" download on the CVX report page. This is
#: the authoritative parsed source (decision: parse the XML, do not scrape the
#: HTML table). Served as ``text/xml`` with an ISO-8859-1 declaration.
SOURCE_XML_NEW_URL = "https://www2a.cdc.gov/vaccines/iis/iisstandards/XML2.asp?rpt=cvx"

#: Encoding declared by the XML-new payload (used by the entrypoint to decode the
#: fetched bytes before parsing, and to write the verbatim Volume snapshot).
SOURCE_ENCODING = "ISO-8859-1"

#: Human-facing CVX report page; also serves as the data dictionary (it documents
#: the columns and the Vaccine Status vocabulary). Recorded in registration
#: provenance.
SOURCE_LANDING_URL = "https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx"
SOURCE_DATA_DICTIONARY_URL = SOURCE_LANDING_URL


class VaccineStatus(StrEnum):
    """Controlled vocabulary for ``vaccine_status`` (normalized from CVX "Status").

    The CVX report documents five statuses: ``Active``, ``Inactive``,
    ``Pending``, ``Non-US``, ``Never Active``. They are normalized to snake_case
    here (``Non-US`` -> ``non_us``, ``Never Active`` -> ``never_active``) so the
    column is a stable controlled vocabulary (cf. ADR 0016 controlled-vocabulary
    enforcement). ``Pending`` is documented by CDC but may be absent from any
    given snapshot; it stays in the vocabulary regardless.
    """

    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    NON_US = "non_us"
    NEVER_ACTIVE = "never_active"


#: Membership set for the blocking ``vaccine_status`` controlled-vocabulary check.
VACCINE_STATUS_VALUES: frozenset[str] = frozenset(s.value for s in VaccineStatus)

#: Expected size band for a current snapshot (a few hundred codes). Backs the
#: cardinality WARN; a real snapshot well outside this signals a parse/source
#: problem.
CARDINALITY_MIN = 200
CARDINALITY_MAX = 350

#: CVX "Last Updated" is published as ``M/D/YYYY`` (zero-padding not guaranteed).
_LAST_UPDATED_FORMAT = "%m/%d/%Y"

#: Collapses runs of whitespace/hyphens to a single underscore when normalizing
#: a "Status" label to its controlled-vocabulary form.
_STATUS_SEP_RE = re.compile(r"[\s\-]+")


@dataclass(frozen=True)
class CvxRecord:
    """One CVX code parsed from a single XML-new snapshot.

    Mirrors the persisted ``codes.cvx`` shape minus the snapshot/audit columns
    (``snapshot_date`` / ``source_file`` / ``loaded_at``), which the bundle
    entrypoint stamps. Flat by design -- CVX has no hierarchy.

    Attributes:
        cvx_code: CVX code as a string (PK component with ``snapshot_date``),
            whitespace-stripped from the space-padded source value, e.g. ``"143"``.
        short_description: Abbreviated vaccine description.
        full_vaccine_name: Full vaccine name.
        vaccine_status: Normalized controlled-vocabulary status (see
            :class:`VaccineStatus`); the un-normalized source value is preserved
            in ``vaccine_status_raw`` for DQ reporting.
        cvx_last_updated: Parsed "Last Updated" date, or ``None`` when the source
            value is blank or unparseable.
        vaccine_status_raw: The source "Status" text before normalization
            (e.g. ``"Non-US"``); kept for DQ, not persisted.
        cvx_last_updated_raw: The source "Last Updated" text before parsing; kept
            for DQ (distinguishing "blank" from "present but unparseable"), not
            persisted.
    """

    cvx_code: str
    short_description: str
    full_vaccine_name: str
    vaccine_status: str
    cvx_last_updated: date | None
    vaccine_status_raw: str
    cvx_last_updated_raw: str


def normalize_cvx_code(raw: str) -> str:
    """Normalize a raw CVX code to its canonical string form.

    The XML-new ``<CVXCode>`` value is space-padded (e.g. ``"143       "``); the
    canonical code is kept as a whitespace-stripped **string** (codes-schema
    consistency, and CVX codes are not used arithmetically).

    Args:
        raw: A code as it appears in the source element.

    Returns:
        The stripped code string (``""`` for blank input).

    Examples:
        >>> normalize_cvx_code("143       ")
        '143'
        >>> normalize_cvx_code(" 54 ")
        '54'
    """
    return raw.strip()


def normalize_status(raw: str) -> str:
    """Normalize a raw CVX "Status" label to its snake_case controlled form.

    Lower-cases and collapses whitespace/hyphens to underscores. Does **not**
    validate membership -- pass the records to :func:`find_status_violations` so
    an unrecognized status surfaces as DQ rather than being silently dropped.

    Args:
        raw: The source status text (e.g. ``"Active"``, ``"Non-US"``,
            ``"Never Active"``).

    Returns:
        The normalized status (e.g. ``"active"``, ``"non_us"``,
        ``"never_active"``); ``""`` for blank input.

    Examples:
        >>> normalize_status("Active")
        'active'
        >>> normalize_status("Non-US")
        'non_us'
        >>> normalize_status("Never Active")
        'never_active'
    """
    cleaned = raw.strip()
    if not cleaned:
        return ""
    return _STATUS_SEP_RE.sub("_", cleaned).lower()


def parse_last_updated(raw: str) -> date | None:
    """Parse a CVX "Last Updated" value (``M/D/YYYY``) into a date.

    Args:
        raw: The source date text, possibly blank.

    Returns:
        The parsed :class:`datetime.date`, or ``None`` if the value is blank or
        does not match the expected ``M/D/YYYY`` format. A non-blank value that
        returns ``None`` is reported by :func:`find_unparseable_last_updated`.

    Examples:
        >>> parse_last_updated("3/20/2011")
        datetime.date(2011, 3, 20)
        >>> parse_last_updated("") is None
        True
    """
    cleaned = raw.strip()
    if not cleaned:
        return None
    try:
        return datetime.strptime(cleaned, _LAST_UPDATED_FORMAT).date()
    except ValueError:
        return None


def parse_cvx_xml(xml_text: str) -> list[CvxRecord]:
    """Parse a full CVX XML-new payload into records.

    Walks ``<CVXCodes>/<CVXInfo>`` and extracts the six published elements per
    code, normalizing each. No deduplication or validation is applied here --
    those are recorded as DQ downstream (ADR 0009) so problems surface rather
    than vanish.

    Args:
        xml_text: The full XML-new document (already decoded to ``str``).

    Returns:
        Records in source order.

    Raises:
        xml.etree.ElementTree.ParseError: If the payload is not well-formed XML.
    """
    root = ET.fromstring(xml_text)
    records: list[CvxRecord] = []
    for info in root.findall("CVXInfo"):
        code = normalize_cvx_code(info.findtext("CVXCode") or "")
        status_raw = (info.findtext("Status") or "").strip()
        last_updated_raw = (info.findtext("LastUpdated") or "").strip()
        records.append(
            CvxRecord(
                cvx_code=code,
                short_description=(info.findtext("ShortDescription") or "").strip(),
                full_vaccine_name=(info.findtext("FullVaccinename") or "").strip(),
                vaccine_status=normalize_status(status_raw),
                cvx_last_updated=parse_last_updated(last_updated_raw),
                vaccine_status_raw=status_raw,
                cvx_last_updated_raw=last_updated_raw,
            )
        )
    return records


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def find_duplicate_codes(records: list[CvxRecord]) -> list[str]:
    """Return ``cvx_code`` values that appear more than once in one snapshot.

    Backs the blocking ``(cvx_code, snapshot_date)`` uniqueness check: within a
    single snapshot, each ``cvx_code`` must be unique.

    Returns:
        The duplicated codes (each reported once), in first-seen order.
    """
    seen: dict[str, int] = {}
    for r in records:
        seen[r.cvx_code] = seen.get(r.cvx_code, 0) + 1
    return [code for code, count in seen.items() if count > 1]


def find_missing_required(records: list[CvxRecord]) -> list[tuple[str, str]]:
    """Return ``(cvx_code, field)`` for any blank required field.

    Backs the blocking non-null checks for ``cvx_code``, ``short_description``,
    ``full_vaccine_name``, and ``vaccine_status`` (``snapshot_date`` is stamped
    by the entrypoint and checked there).

    Returns:
        The offending ``(cvx_code, field_name)`` pairs, in first-seen order.
    """
    missing: list[tuple[str, str]] = []
    for r in records:
        for field, value in (
            ("cvx_code", r.cvx_code),
            ("short_description", r.short_description),
            ("full_vaccine_name", r.full_vaccine_name),
            ("vaccine_status", r.vaccine_status),
        ):
            if not value.strip():
                missing.append((r.cvx_code, field))
    return missing


def find_status_violations(records: list[CvxRecord]) -> list[tuple[str, str]]:
    """Return ``(cvx_code, raw_status)`` for statuses outside the controlled vocab.

    Backs the blocking ``vaccine_status`` controlled-vocabulary check. The raw
    (un-normalized) source value is reported so an unexpected new status is
    legible in the DQ record.

    Returns:
        The offending ``(cvx_code, vaccine_status_raw)`` pairs, in source order.
    """
    return [
        (r.cvx_code, r.vaccine_status_raw)
        for r in records
        if r.vaccine_status not in VACCINE_STATUS_VALUES
    ]


def find_unparseable_last_updated(records: list[CvxRecord]) -> list[tuple[str, str]]:
    """Return ``(cvx_code, raw)`` where "Last Updated" was present but unparseable.

    Backs a WARN check (the column is nullable): a blank value is fine and
    excluded; a non-blank value that did not parse to a date is reported.

    Returns:
        The offending ``(cvx_code, cvx_last_updated_raw)`` pairs, in source order.
    """
    return [
        (r.cvx_code, r.cvx_last_updated_raw)
        for r in records
        if r.cvx_last_updated_raw and r.cvx_last_updated is None
    ]


def find_future_last_updated(records: list[CvxRecord], as_of: date) -> list[tuple[str, date]]:
    """Return ``(cvx_code, cvx_last_updated)`` where the date is after ``as_of``.

    Backs a WARN check: a "Last Updated" in the future relative to the run date
    signals a source anomaly.

    Args:
        records: Parsed records.
        as_of: The reference date (typically the snapshot/run date).

    Returns:
        The offending ``(cvx_code, date)`` pairs, in source order.
    """
    return [
        (r.cvx_code, r.cvx_last_updated)
        for r in records
        if r.cvx_last_updated is not None and r.cvx_last_updated > as_of
    ]
