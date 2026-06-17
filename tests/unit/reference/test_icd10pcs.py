"""Unit tests for `cidmath_datahub.reference.icd10pcs`.

Anchored against real ICD-10-PCS codes and the CMS order-file fixed-width layout:
  - 0DTJ4ZZ (Resection of Appendix, Percutaneous Endoscopic Approach) -- a valid
    Medical & Surgical (section 0) 7-char code; billable leaf (flag 1).
  - XW033E5 (Introduction of Remdesivir Anti-infective into Peripheral Vein,
    Percutaneous Approach, New Technology Group 5) -- a valid New Technology
    (section X) code; billable leaf (flag 1).
  - 0DT (a structural "title" header row) -- not a valid code (flag 0); partial
    (3 chars), so it exercises the header/valid distinction PCS makes.

The fixed-width layout is the same family as the CM order file; the `_order_line`
helper rebuilds it at the spec's character positions, independently of the
parser's slice constants, so the two must agree.

Source format reference: https://www.cms.gov/files/document/2020-icd-10-pcs-order-file-pdf.pdf
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import icd10pcs

EDITION = 2026


def _order_line(order_no: str, code: str, flag: str, short_title: str, long_title: str) -> str:
    """Compose one CMS order-file line at the spec's fixed character positions.

    Layout (1-indexed): 1-5 order no (right/zero), 6 blank, 7-13 code (left,
    space-padded), 14 blank, 15 flag, 16 blank, 17-76 short title (left), 77
    blank, 78+ long title. A valid code fills cols 7-13; a header is shorter and
    space-padded in that field.
    """
    return (
        f"{order_no:0>5}"  # cols 1-5
        + " "  # col 6
        + f"{code:<7}"  # cols 7-13
        + " "  # col 14
        + flag  # col 15
        + " "  # col 16
        + f"{short_title:<60}"  # cols 17-76
        + " "  # col 77
        + long_title  # cols 78+
    )


SAMPLE_LINES = [
    _order_line(
        "1",
        "0DT",  # a header/title row: partial code, not billable
        "0",
        "Resection, Gastrointestinal System",
        "Medical and Surgical, Gastrointestinal System, Resection",
    ),
    _order_line(
        "2745",
        "0DTJ4ZZ",
        "1",
        "Resection of Appendix, Perc Endo",
        "Resection of Appendix, Percutaneous Endoscopic Approach",
    ),
    _order_line(
        "78901",
        "XW033E5",
        "1",
        "Introduce Remdesivir in Periph Vein, Perc, New Tech 5",
        "Introduction of Remdesivir Anti-infective into Peripheral Vein, "
        "Percutaneous Approach, New Technology Group 5",
    ),
]
SAMPLE_FILE = "\n".join(SAMPLE_LINES) + "\n"


def _rec(
    code: str,
    long_title: str = "x",
    *,
    edition: int = EDITION,
    short_title: str = "x",
    is_billable: bool = True,
    section: str = "0",
    section_name: str | None = "Medical and Surgical",
    body_system: str | None = "D",
) -> icd10pcs.Icd10pcsRecord:
    """Build a record with sensible defaults; tests override only what they assert on."""
    return icd10pcs.Icd10pcsRecord(
        code, edition, short_title, long_title, is_billable, section, section_name, body_system
    )


@pytest.mark.unit
class TestNormalizeCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("0dtj4zz", "0DTJ4ZZ"),
            ("XW033E5", "XW033E5"),
            (" 0DT ", "0DT"),  # trims + upper-cases; no decimal manipulation
            ("xw033e5", "XW033E5"),
            ("", ""),  # empty in, empty out
        ],
    )
    def test_normalization(self, raw, expected):
        assert icd10pcs.normalize_code(raw) == expected


@pytest.mark.unit
class TestValidateCode:
    @pytest.mark.parametrize("code", ["0DTJ4ZZ", "XW033E5", "00X0ZZ4", "B0451ZZ"])
    def test_valid(self, code):
        assert icd10pcs.validate_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "0DT",  # header/partial, not a valid 7-char code
            "0DTJ4Z",  # only 6 chars
            "0DTJ4ZZZ",  # 8 chars
            "0ITJ4ZZ",  # contains 'I' (excluded from the charset)
            "0OTJ4ZZ",  # contains 'O' (excluded from the charset)
            "0dtj4zz",  # not normalized (lower-case)
            "",  # empty
        ],
    )
    def test_invalid(self, code):
        assert icd10pcs.validate_code(code) is False


@pytest.mark.unit
class TestValidatePartial:
    @pytest.mark.parametrize("code", ["0", "0D", "0DT", "0DTJ4ZZ", "X"])
    def test_valid_partials(self, code):
        assert icd10pcs.validate_partial(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "0OT",  # 'O' not in the charset
            "0IT",  # 'I' not in the charset
            "0DTJ4ZZZ",  # over length
            "",  # empty
        ],
    )
    def test_invalid_partials(self, code):
        assert icd10pcs.validate_partial(code) is False


@pytest.mark.unit
class TestSectionAndBodySystem:
    def test_section_of(self):
        assert icd10pcs.section_of("0DTJ4ZZ") == "0"
        assert icd10pcs.section_of("XW033E5") == "X"

    def test_body_system_of(self):
        assert icd10pcs.body_system_of("0DTJ4ZZ") == "D"
        assert icd10pcs.body_system_of("XW033E5") == "W"
        assert icd10pcs.body_system_of("0") is None  # 1-char header

    def test_all_17_sections_named(self):
        assert len(icd10pcs.PCS_SECTIONS) == 17
        assert icd10pcs.SECTION_NAMES["0"] == "Medical and Surgical"
        assert icd10pcs.SECTION_NAMES["X"] == "New Technology"
        assert icd10pcs.SECTION_NAMES["B"] == "Imaging"


@pytest.mark.unit
class TestParseOrderLine:
    def test_billable_leaf_medical_surgical(self):
        rec = icd10pcs.parse_order_line(SAMPLE_LINES[1], EDITION)  # 0DTJ4ZZ
        assert rec is not None
        assert rec.icd10pcs_code == "0DTJ4ZZ"
        assert rec.edition_year == EDITION
        assert rec.long_title == "Resection of Appendix, Percutaneous Endoscopic Approach"
        assert rec.is_billable is True
        assert rec.section == "0"
        assert rec.section_name == "Medical and Surgical"
        assert rec.body_system == "D"

    def test_billable_leaf_new_technology(self):
        rec = icd10pcs.parse_order_line(SAMPLE_LINES[2], EDITION)  # XW033E5
        assert rec is not None
        assert rec.icd10pcs_code == "XW033E5"
        assert rec.is_billable is True
        assert rec.section == "X"
        assert rec.section_name == "New Technology"
        assert rec.body_system == "W"
        assert "Remdesivir" in rec.long_title

    def test_header_not_billable_and_partial(self):
        rec = icd10pcs.parse_order_line(SAMPLE_LINES[0], EDITION)  # 0DT header
        assert rec is not None
        assert rec.icd10pcs_code == "0DT"
        assert rec.is_billable is False
        assert rec.section == "0"
        assert rec.section_name == "Medical and Surgical"
        assert icd10pcs.validate_code(rec.icd10pcs_code) is False  # partial, not 7-char
        assert icd10pcs.validate_partial(rec.icd10pcs_code) is True

    def test_blank_and_short_lines_skipped(self):
        assert icd10pcs.parse_order_line("", EDITION) is None
        assert icd10pcs.parse_order_line("   ", EDITION) is None
        assert icd10pcs.parse_order_line("00001 0DTJ4ZZ", EDITION) is None  # too short for a flag

    def test_bad_flag_raises(self):
        bad = _order_line("3", "0DTJ4ZZ", "X", "bad flag", "bad flag")
        with pytest.raises(ValueError, match="Unexpected header/valid flag"):
            icd10pcs.parse_order_line(bad, EDITION)


@pytest.mark.unit
class TestParseOrderFile:
    def test_parses_all_content_lines(self):
        recs = icd10pcs.parse_order_file(SAMPLE_FILE, EDITION)
        assert len(recs) == 3

    def test_known_codes_present_undotted(self):
        recs = icd10pcs.parse_order_file(SAMPLE_FILE, EDITION)
        by_code = {r.icd10pcs_code: r for r in recs}
        assert "0DTJ4ZZ" in by_code
        assert "XW033E5" in by_code
        assert "." not in by_code["0DTJ4ZZ"].icd10pcs_code  # no decimal
        assert all(r.edition_year == EDITION for r in recs)

    def test_billable_split(self):
        recs = icd10pcs.parse_order_file(SAMPLE_FILE, EDITION)
        assert sum(r.is_billable for r in recs) == 2  # the two valid codes
        assert sum(not r.is_billable for r in recs) == 1  # the 0DT header


@pytest.mark.unit
class TestDQHelpers:
    def _recs(self):
        return icd10pcs.parse_order_file(SAMPLE_FILE, EDITION)

    def test_find_duplicate_keys(self):
        recs = self._recs()
        assert icd10pcs.find_duplicate_keys(recs) == []
        dup = recs + [recs[1]]  # repeat 0DTJ4ZZ for the same edition
        assert icd10pcs.find_duplicate_keys(dup) == [("0DTJ4ZZ", EDITION)]

    def test_same_code_different_editions_not_duplicate(self):
        a = _rec("0DTJ4ZZ", edition=2025)
        b = _rec("0DTJ4ZZ", edition=2026)
        assert icd10pcs.find_duplicate_keys([a, b]) == []

    def test_find_missing_titles(self):
        recs = self._recs()
        assert icd10pcs.find_missing_titles(recs) == []
        blank = recs + [_rec("0DTJ0ZZ", "", short_title="  ")]
        assert icd10pcs.find_missing_titles(blank) == [("0DTJ0ZZ", EDITION)]

    def test_find_invalid_billable_codes(self):
        recs = self._recs()
        # The 0DT header is partial but flag 0, so it is NOT a billable violation.
        assert icd10pcs.find_invalid_billable_codes(recs) == []
        # A billable row whose code is not a 7-char code is a violation.
        bad = recs + [_rec("0DTJ")]
        assert icd10pcs.find_invalid_billable_codes(bad) == ["0DTJ"]

    def test_find_charset_violations(self):
        recs = self._recs()
        assert icd10pcs.find_charset_violations(recs) == []
        # An 'I' or 'O' anywhere (even in a header) is a charset violation.
        bad = recs + [_rec("0ITJ4ZZ", body_system="I")]
        assert icd10pcs.find_charset_violations(bad) == ["0ITJ4ZZ"]

    def test_find_bad_sections(self):
        recs = self._recs()
        assert icd10pcs.find_bad_sections(recs) == []
        # 'A' and 'E' are not valid PCS sections.
        bad = recs + [_rec("AXTJ4ZZ", section="A", section_name=None, body_system="X")]
        assert icd10pcs.find_bad_sections(bad) == [("AXTJ4ZZ", "A")]

    def test_section_distribution(self):
        dist = icd10pcs.section_distribution(self._recs())
        assert dist == {"0": 2, "X": 1}  # 0DT + 0DTJ4ZZ, XW033E5

    def test_billable_share(self):
        assert icd10pcs.billable_share(self._recs()) == pytest.approx(2 / 3)
        assert icd10pcs.billable_share([]) == 0.0


@pytest.mark.unit
class TestSourceLocators:
    def test_order_file_zip_url_default_template(self):
        url = icd10pcs.order_file_zip_url(2026)
        assert "2026-icd-10-pcs-order-file" in url
        assert url.startswith("https://www.cms.gov/")

    def test_order_file_zip_url_custom_template(self):
        url = icd10pcs.order_file_zip_url(2024, template="file:///data/pcs/{year}/order-{year}.zip")
        assert url == "file:///data/pcs/2024/order-2024.zip"

    def test_update_file_zip_url_default_template(self):
        url = icd10pcs.update_file_zip_url(2026)
        assert "april-1-2026-icd-10-pcs-order-file" in url

    @pytest.mark.parametrize(
        "name",
        [
            "icd10pcs_order_2026.txt",  # CMS underscore style
            "icd10pcs-order-2026.txt",  # hyphen variant
            "ICD10PCS_Order_2025.TXT",  # case-insensitive
        ],
    )
    def test_select_order_file_member_picks_order_file(self, name):
        members = ["icd10pcs_codes_2026.txt", name, "icd10pcsOrderFile.pdf"]
        assert icd10pcs.select_order_file_member(members) == name

    def test_select_order_file_member_ignores_addenda(self):
        members = [
            "icd10pcsOrderFile.pdf",
            "icd10pcs_codes_2026.txt",
            "icd10pcs_order_2026.txt",
            "order_addenda_2026.txt",
        ]
        assert icd10pcs.select_order_file_member(members) == "icd10pcs_order_2026.txt"

    def test_select_order_file_member_rejects_none(self):
        with pytest.raises(ValueError, match="order file"):
            icd10pcs.select_order_file_member(["icd10pcs_codes_2026.txt", "readme.txt"])

    def test_select_order_file_member_rejects_ambiguous(self):
        with pytest.raises(ValueError, match="exactly one"):
            icd10pcs.select_order_file_member(
                ["icd10pcs_order_2025.txt", "icd10pcs_order_2026.txt"]
            )


@pytest.mark.unit
class TestOverlayRecords:
    """The mid-year (Apr-1) New Technology update overlaid onto the Oct-1 base, update-wins."""

    def _base(self):
        return [
            _rec("0DT", "Med/Surg, Gastrointestinal System, Resection", is_billable=False),
            _rec("0DTJ4ZZ", "Resection of Appendix, Perc Endo"),
        ]

    def test_empty_update_returns_base(self):
        base = self._base()
        assert icd10pcs.overlay_records(base, []) == base

    def test_update_revises_existing_code(self):
        base = self._base()
        update = [_rec("0DTJ4ZZ", "Resection of Appendix (revised)")]
        merged = {r.icd10pcs_code: r for r in icd10pcs.overlay_records(base, update)}
        assert len(merged) == 2  # revision, not a new code
        assert merged["0DTJ4ZZ"].long_title == "Resection of Appendix (revised)"

    def test_update_adds_new_technology_code(self):
        base = self._base()
        update = [_rec("XW033E5", "Introduction of Remdesivir ...", section="X", body_system="W")]
        merged = {r.icd10pcs_code: r for r in icd10pcs.overlay_records(base, update)}
        assert len(merged) == 3
        assert merged["XW033E5"].section == "X"
        assert merged["0DTJ4ZZ"].long_title == "Resection of Appendix, Perc Endo"  # base retained

    def test_overlay_keys_unique(self):
        base = self._base()
        update = [_rec("0DTJ4ZZ")]
        merged = icd10pcs.overlay_records(base, update)
        codes = [r.icd10pcs_code for r in merged]
        assert len(codes) == len(set(codes))
