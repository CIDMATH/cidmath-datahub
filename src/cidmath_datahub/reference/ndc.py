"""FDA NDC Directory (finished drugs) reference (revise-in-place; ADR 0014, ADR 0032).

Parses the FDA National Drug Code Directory "Text Version" (``ndctext.zip`` ->
``product.txt`` + ``package.txt``, tab-delimited) into two **flat** reference
grains -- products and packages -- so drug/pharmacy data (claims, dispensing,
vaccine NDCs) can be conformed to canonical national drug codes and their
attributes. This module is the single source of truth for NDC parsing,
normalization, and validation; the entrypoint ``bundles/_reference/src/build_ndc.py``
is thin glue over it (ADR 0011/0027).

It holds **no Spark** -- pure functions over plain Python -- so the bundle
entrypoint converts the output to Spark DataFrames and writes
``ecdh_model_<env>.codes.ndc_product`` and ``codes.ndc_package`` (ADR 0006,
ADR 0015: reference tables, no Kimball suffix).

History model (ADR 0032). The NDC Directory is **revised in place** (FDA updates
it daily and publishes only the current list), so we preserve history ourselves:
the entrypoint writes the raw ``ndctext.zip`` verbatim to an immutable,
date-stamped UC Volume file, and both tables keep every snapshot, keyed by
``snapshot_date`` with ``snapshot_replace`` semantics (the geography per-vintage
replace, ADR 0024). The source already carries ``STARTMARKETINGDATE`` /
``ENDMARKETINGDATE``, so real-world "was this NDC on the market on date X" is
answerable from a single snapshot; snapshots are pulled quarterly.

Key design points (confirmed against the FDA file-definition pages):

* **``ProductID``** (NDCproductcode + SPL documentID) is the stable join/dedup key
  -- FDA includes it specifically because ``PRODUCTNDC`` is not unique per
  snapshot (a re-listed NDC recurs under a new SPL doc). So products are keyed by
  ``product_id`` and packages link to products by ``product_id``; ``product_ndc``
  is carried as an attribute.
* **NDC normalization** is the error-prone core. Source NDCs are hyphenated with
  variable-length segments. We keep the verbatim value *and* compute a zero-padded
  canonical form: the package NDC to the 11-digit 5-4-2 form (``package_ndc_11``),
  the product NDC to the 9-digit 5-4 form (``product_ndc_normalized`` -- a product
  NDC has only the labeler+product segments, so it cannot be 11 digits).

Sources:
    * Landing: https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory
    * Text zip: https://www.accessdata.fda.gov/cder/ndctext.zip
    * Product file definitions:
      https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions
    * Package file definitions:
      https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-package-file-definitions
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime

# ---------------------------------------------------------------------------
# Source spec (single-sourced here; the entrypoint does the IO -- ADR 0011)
# ---------------------------------------------------------------------------

#: The NDC Directory "Text Version" zip (finished drugs): tab-delimited
#: ``product.txt`` + ``package.txt``. FDA refreshes it daily; we pull quarterly.
SOURCE_TEXT_ZIP_URL = "https://www.accessdata.fda.gov/cder/ndctext.zip"

#: Human-facing landing + the two data dictionaries (recorded in provenance).
SOURCE_LANDING_URL = (
    "https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory"
)
PRODUCT_DEFINITIONS_URL = (
    "https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions"
)
PACKAGE_DEFINITIONS_URL = (
    "https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-package-file-definitions"
)

#: The text files are historically Latin-1 / Windows-1252, not UTF-8. latin-1
#: decodes any byte without error, so a stray non-ASCII byte never aborts a parse.
SOURCE_ENCODING = "latin-1"

#: Member basenames inside ``ndctext.zip`` (matched case-insensitively).
PRODUCT_MEMBER = "product.txt"
PACKAGE_MEMBER = "package.txt"

#: DEA controlled-substance schedules (``DEASCHEDULE``); blank for non-scheduled.
DEA_SCHEDULE_VALUES: frozenset[str] = frozenset({"CI", "CII", "CIII", "CIV", "CV"})

#: ``YYYYMMDD`` source date format (``STARTMARKETINGDATE`` etc.).
_DATE_FORMAT = "%Y%m%d"

#: Sanity bands for the current-snapshot cardinality WARNs.
PRODUCT_CARDINALITY_MIN = 80_000
PACKAGE_CARDINALITY_MIN = 200_000


# ---------------------------------------------------------------------------
# Normalization primitives (the tested core)
# ---------------------------------------------------------------------------


def normalize_ndc(code: str, segment_lengths: tuple[int, ...]) -> str | None:
    """Zero-pad a hyphenated NDC to a canonical fixed-width digit string.

    The NDC's hyphen-separated segments are left-zero-padded to ``segment_lengths``
    and concatenated. This implements the FDA/CMS 10-digit -> 11-digit rule
    uniformly: for a package code (target ``(5, 4, 2)``) a ``4-4-2`` pads the
    labeler, ``5-3-2`` pads the product, and ``5-4-1`` pads the package, all to
    ``5-4-2``; for a product code (target ``(5, 4)``) a ``4-4`` or ``5-3`` pads to
    ``5-4``.

    Args:
        code: The hyphenated source NDC (e.g. ``"0517-0801-25"``).
        segment_lengths: Target width per segment (``(5, 4, 2)`` for a package
            NDC, ``(5, 4)`` for a product NDC).

    Returns:
        The padded digit string (length ``sum(segment_lengths)``), or ``None`` if
        the code is malformed -- wrong segment count, a non-digit segment, or a
        segment already longer than its target width. ``None`` is surfaced as a DQ
        violation rather than silently dropped (ADR 0009).

    Examples:
        >>> normalize_ndc("0517-0801-25", (5, 4, 2))
        '00517080125'
        >>> normalize_ndc("12345-678-90", (5, 4, 2))
        '12345067890'
        >>> normalize_ndc("12345-6789-0", (5, 4, 2))
        '12345678900'
        >>> normalize_ndc("0002-7510", (5, 4))
        '000027510'
        >>> normalize_ndc("bad", (5, 4, 2)) is None
        True
    """
    parts = code.strip().split("-")
    if len(parts) != len(segment_lengths):
        return None
    out: list[str] = []
    for part, width in zip(parts, segment_lengths, strict=True):
        if not part.isdigit() or len(part) > width:
            return None
        out.append(part.zfill(width))
    return "".join(out)


def normalize_package_ndc(code: str) -> str | None:
    """Normalize a package ``NDCPACKAGECODE`` to the 11-digit 5-4-2 form."""
    return normalize_ndc(code, (5, 4, 2))


def normalize_product_ndc(code: str) -> str | None:
    """Normalize a ``PRODUCTNDC`` (labeler-product) to the 9-digit 5-4 form."""
    return normalize_ndc(code, (5, 4))


def parse_ndc_date(raw: str) -> date | None:
    """Parse a ``YYYYMMDD`` source date; ``None`` for blank or unparseable input.

    Examples:
        >>> parse_ndc_date("20240115")
        datetime.date(2024, 1, 15)
        >>> parse_ndc_date("") is None
        True
    """
    cleaned = raw.strip()
    if not cleaned:
        return None
    try:
        return datetime.strptime(cleaned, _DATE_FORMAT).date()
    except ValueError:
        return None


def yn_to_bool(raw: str) -> bool:
    """Map a ``Y``/``N`` flag (e.g. ``SAMPLE_PACKAGE``) to a boolean (blank -> False)."""
    return raw.strip().upper() == "Y"


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NdcProduct:
    """One product-level NDC row (PK ``(product_id, snapshot_date)``).

    ``snapshot_date`` / ``source_file`` / ``loaded_at`` are stamped by the bundle
    entrypoint. ``product_ndc_normalized`` is the 9-digit 5-4 form;
    ``product_ndc`` keeps the verbatim hyphenated source value.
    """

    product_id: str
    product_ndc: str
    product_ndc_normalized: str | None
    product_type_name: str
    proprietary_name: str
    proprietary_name_suffix: str
    nonproprietary_name: str
    dosage_form_name: str
    route_name: str
    start_marketing_date: date | None
    end_marketing_date: date | None
    marketing_category_name: str
    application_number: str
    labeler_name: str
    substance_name: str
    active_numerator_strength: str
    active_ingred_unit: str
    pharm_classes: str
    dea_schedule: str
    ndc_exclude_flag: str
    listing_record_certified_through: date | None


@dataclass(frozen=True)
class NdcPackage:
    """One package-level NDC row (PK ``(product_id, ndc_package_code, snapshot_date)``).

    Links to :class:`NdcProduct` by ``product_id``. ``package_ndc_11`` is the
    11-digit 5-4-2 form; ``ndc_package_code`` keeps the verbatim source value.
    """

    product_id: str
    product_ndc: str
    ndc_package_code: str
    package_ndc_11: str | None
    package_description: str
    start_marketing_date: date | None
    end_marketing_date: date | None
    ndc_exclude_flag: str
    sample_package: bool


# ---------------------------------------------------------------------------
# Parsing (header-driven; tab-delimited text after the entrypoint decodes it)
# ---------------------------------------------------------------------------


def _split_rows(text: str) -> tuple[list[str], list[list[str]]]:
    """Return ``(header, data_rows)`` from tab-delimited text (header upper-cased)."""
    lines = text.splitlines()
    if not lines:
        return [], []
    header = [h.strip().upper() for h in lines[0].split("\t")]
    rows = [line.split("\t") for line in lines[1:] if line.strip()]
    return header, rows


def _indexer(header: list[str]):
    """Return ``get(row, *names)`` reading the first present column by name (else '')."""
    pos = {name: i for i, name in enumerate(header)}

    def get(row: list[str], *names: str) -> str:
        for name in names:
            i = pos.get(name)
            if i is not None and i < len(row):
                return row[i].strip()
        return ""

    return get


def parse_product_file(text: str) -> list[NdcProduct]:
    """Parse ``product.txt`` into :class:`NdcProduct` records (header-driven).

    Column names are matched case-insensitively by header, so column reordering or
    additions don't break the parse. NDCs are normalized but not validated here --
    run the DQ helpers over the batch so problems surface (ADR 0009).
    """
    header, rows = _split_rows(text)
    get = _indexer(header)
    records: list[NdcProduct] = []
    for row in rows:
        product_ndc = get(row, "PRODUCTNDC")
        records.append(
            NdcProduct(
                product_id=get(row, "PRODUCTID"),
                product_ndc=product_ndc,
                product_ndc_normalized=normalize_product_ndc(product_ndc),
                product_type_name=get(row, "PRODUCTTYPENAME"),
                proprietary_name=get(row, "PROPRIETARYNAME"),
                proprietary_name_suffix=get(row, "PROPRIETARYNAMESUFFIX"),
                nonproprietary_name=get(row, "NONPROPRIETARYNAME"),
                dosage_form_name=get(row, "DOSAGEFORMNAME"),
                route_name=get(row, "ROUTENAME"),
                start_marketing_date=parse_ndc_date(get(row, "STARTMARKETINGDATE")),
                end_marketing_date=parse_ndc_date(get(row, "ENDMARKETINGDATE")),
                marketing_category_name=get(row, "MARKETINGCATEGORYNAME"),
                application_number=get(row, "APPLICATIONNUMBER"),
                labeler_name=get(row, "LABELERNAME"),
                substance_name=get(row, "SUBSTANCENAME"),
                active_numerator_strength=get(row, "ACTIVE_NUMERATOR_STRENGTH", "STRENGTHNUMBER"),
                active_ingred_unit=get(row, "ACTIVE_INGRED_UNIT", "STRENGTHUNIT"),
                pharm_classes=get(row, "PHARM_CLASSES"),
                dea_schedule=get(row, "DEASCHEDULE"),
                ndc_exclude_flag=get(row, "NDC_EXCLUDE_FLAG"),
                listing_record_certified_through=parse_ndc_date(
                    get(row, "LISTING_RECORD_CERTIFIED_THROUGH")
                ),
            )
        )
    return records


def parse_package_file(text: str) -> list[NdcPackage]:
    """Parse ``package.txt`` into :class:`NdcPackage` records (header-driven)."""
    header, rows = _split_rows(text)
    get = _indexer(header)
    records: list[NdcPackage] = []
    for row in rows:
        package_code = get(row, "NDCPACKAGECODE")
        records.append(
            NdcPackage(
                product_id=get(row, "PRODUCTID"),
                product_ndc=get(row, "PRODUCTNDC"),
                ndc_package_code=package_code,
                package_ndc_11=normalize_package_ndc(package_code),
                package_description=get(row, "PACKAGEDESCRIPTION"),
                start_marketing_date=parse_ndc_date(get(row, "STARTMARKETINGDATE")),
                end_marketing_date=parse_ndc_date(get(row, "ENDMARKETINGDATE")),
                ndc_exclude_flag=get(row, "NDC_EXCLUDE_FLAG"),
                sample_package=yn_to_bool(get(row, "SAMPLE_PACKAGE")),
            )
        )
    return records


# ---------------------------------------------------------------------------
# DQ helpers (pure; the entrypoint records the results via ctx.recorder, ADR 0009)
# ---------------------------------------------------------------------------


def _duplicates(keys: Iterable) -> list:
    seen: dict = {}
    for k in keys:
        seen[k] = seen.get(k, 0) + 1
    return [k for k, n in seen.items() if n > 1]


def find_duplicate_product_keys(products: list[NdcProduct]) -> list[str]:
    """Duplicate ``product_id`` values within a snapshot (blocking PK uniqueness)."""
    return _duplicates(p.product_id for p in products)


def find_duplicate_package_keys(packages: list[NdcPackage]) -> list[tuple[str, str]]:
    """Duplicate ``(product_id, ndc_package_code)`` within a snapshot (blocking PK)."""
    return _duplicates((p.product_id, p.ndc_package_code) for p in packages)


def find_missing_product_fields(products: list[NdcProduct]) -> list[tuple[str, str]]:
    """``(product_id, field)`` for blank required product fields (blocking)."""
    missing: list[tuple[str, str]] = []
    for p in products:
        for field, value in (
            ("product_id", p.product_id),
            ("product_ndc", p.product_ndc),
            ("labeler_name", p.labeler_name),
        ):
            if not value.strip():
                missing.append((p.product_id, field))
    return missing


def find_missing_package_fields(packages: list[NdcPackage]) -> list[tuple[str, str]]:
    """``(ndc_package_code, field)`` for blank required package fields (blocking)."""
    missing: list[tuple[str, str]] = []
    for p in packages:
        for field, value in (
            ("product_id", p.product_id),
            ("ndc_package_code", p.ndc_package_code),
            ("product_ndc", p.product_ndc),
        ):
            if not value.strip():
                missing.append((p.ndc_package_code, field))
    return missing


def find_bad_product_ndc(products: list[NdcProduct]) -> list[str]:
    """``product_ndc`` values that don't normalize to a 9-digit 5-4 form (blocking)."""
    return [
        p.product_ndc
        for p in products
        if p.product_ndc_normalized is None or len(p.product_ndc_normalized) != 9
    ]


def find_bad_package_ndc(packages: list[NdcPackage]) -> list[str]:
    """``ndc_package_code`` values that don't normalize to 11 digits (blocking)."""
    return [
        p.ndc_package_code
        for p in packages
        if p.package_ndc_11 is None or len(p.package_ndc_11) != 11
    ]


def find_package_orphans(
    packages: list[NdcPackage], product_ids: set[str]
) -> list[tuple[str, str]]:
    """``(ndc_package_code, product_id)`` for packages with no product in the snapshot.

    Backs the blocking package -> product FK check (same snapshot).
    """
    return [(p.ndc_package_code, p.product_id) for p in packages if p.product_id not in product_ids]


def find_bad_marketing_date_order(
    records: list,
) -> list[str]:
    """NDC ids where ``end_marketing_date`` precedes ``start_marketing_date`` (WARN).

    Works for either record type (both carry the two dates); the identifier is the
    package code or product NDC.
    """
    bad: list[str] = []
    for r in records:
        start, end = r.start_marketing_date, r.end_marketing_date
        if start is not None and end is not None and end < start:
            ident = getattr(r, "ndc_package_code", None) or r.product_ndc
            bad.append(ident)
    return bad


def find_bad_dea_schedule(products: list[NdcProduct]) -> list[tuple[str, str]]:
    """``(product_ndc, dea_schedule)`` for DEA schedules outside the vocab (WARN).

    Blank is allowed (non-scheduled); any other non-CI..CV value is reported.
    """
    return [
        (p.product_ndc, p.dea_schedule)
        for p in products
        if p.dea_schedule.strip() and p.dea_schedule.strip() not in DEA_SCHEDULE_VALUES
    ]
