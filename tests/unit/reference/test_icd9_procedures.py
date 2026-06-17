"""Unit tests for `cidmath_datahub.reference.icd9_procedures`.

Anchored on real ICD-9-CM Volume 3 procedure codes and the CMS Version 32 title-file
layout (``<undotted code> <title>`` per line):
  - 47.01 (Laparoscopic appendectomy) -- a billable leaf in the Digestive chapter.
  - 47.0 / 47 (Other appendectomy / Operations on appendix) -- non-leaf headers.
  - 36.06 (coronary stent) -- Cardiovascular chapter.
  - 00.01 (Therapeutic ultrasound ...) -- the "00" chapter.

Source format reference: CMS ICD-9-CM Version 32 ``CMS32_DESC_{LONG,SHORT}_SG.txt``.
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import icd9_procedures as proc

EDITION = 2015

# CMS title-file sample lines: undotted code, whitespace (1+, padded for alignment), title.
LONG_TEXT = (
    "\n".join(
        [
            "0001 Therapeutic ultrasound of vessels of head and neck",
            "47   Operations on appendix",
            "470  Other appendectomy",
            "4701 Laparoscopic appendectomy",
            "3606 Insertion of non-drug-eluting coronary artery stent(s)",
        ]
    )
    + "\n"
)

SHORT_TEXT = (
    "\n".join(
        [
            "0001 Ther ult head & neck ves",
            "47   Ops on appendix",
            "470  Appendectomy NEC",
            "4701 Lap appendectomy",
            "3606 Ins non-drug-el cor stent",
        ]
    )
    + "\n"
)


@pytest.mark.unit
class TestNormalizeCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("0001", "00.01"),
            ("4701", "47.01"),
            ("470", "47.0"),
            ("47", "47"),
            (" 47.01 ", "47.01"),  # trims + tolerates an already-dotted code
            ("", ""),
        ],
    )
    def test_normalization(self, raw, expected):
        assert proc.normalize_code(raw) == expected


@pytest.mark.unit
class TestValidateCode:
    @pytest.mark.parametrize("code", ["47.01", "47", "00", "36.06", "00.01"])
    def test_valid(self, code):
        assert proc.validate_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "470",  # not normalized (missing decimal)
            "4.01",  # 1-digit category
            "47.012",  # too many decimal digits
            "V30.0",  # V codes are diagnoses, not procedures
            "",
        ],
    )
    def test_invalid(self, code):
        assert proc.validate_code(code) is False


@pytest.mark.unit
class TestCategoryAndChapter:
    def test_category_of(self):
        assert proc.category_of("47.01") == "47"
        assert proc.category_of("00") == "00"

    def test_chapter_for(self):
        assert proc.chapter_for("47.01") == ("42-54", "Operations on the Digestive System")
        assert proc.chapter_for("36.06") == ("35-39", "Operations on the Cardiovascular System")
        assert proc.chapter_for("00.01")[0] == "00"

    def test_all_18_chapters_cover_00_to_99(self):
        assert len(proc.PROCEDURE_CHAPTERS) == 18
        # Every 2-digit category resolves to exactly one chapter.
        for n in range(0, 100):
            assert proc.chapter_for(f"{n:02d}") is not None


@pytest.mark.unit
class TestParseTitles:
    def test_parses_code_title_pairs(self):
        pairs = dict(proc.parse_titles(LONG_TEXT))
        assert pairs["47.01"] == "Laparoscopic appendectomy"
        assert pairs["00.01"] == "Therapeutic ultrasound of vessels of head and neck"
        assert pairs["47"] == "Operations on appendix"  # multi-space alignment handled

    def test_skips_blank_lines(self):
        assert proc.parse_titles("\n\n  \n") == []


@pytest.mark.unit
class TestFindBillable:
    def test_leaf_of_set(self):
        # 47 and 47.0 are prefixes of 47.01 -> non-leaf; the rest are leaves.
        billable = proc.find_billable_codes(["47", "47.0", "47.01", "36.06", "00.01"])
        assert billable == {"47.01", "36.06", "00.01"}


@pytest.mark.unit
class TestAssemble:
    def _records(self):
        long_pairs = proc.parse_titles(LONG_TEXT)
        short_pairs = proc.parse_titles(SHORT_TEXT)
        return {
            r.icd9_procedure_code: r
            for r in proc.assemble_records(long_pairs, short_pairs, EDITION)
        }

    def test_billable_leaf(self):
        r = self._records()["47.01"]
        assert r.long_title == "Laparoscopic appendectomy"
        assert r.short_title == "Lap appendectomy"
        assert r.is_billable is True
        assert r.category == "47"
        assert r.chapter_code == "42-54"
        assert r.chapter_name == "Operations on the Digestive System"
        assert r.edition_year == EDITION

    def test_header_not_billable(self):
        rows = self._records()
        assert rows["47"].is_billable is False
        assert rows["47.0"].is_billable is False

    def test_short_title_joined(self):
        rows = self._records()
        assert rows["36.06"].short_title == "Ins non-drug-el cor stent"
        # A code missing from the short file would still get a record with a blank short title.

    def test_one_record_per_code(self):
        rows = self._records()
        assert len(rows) == 5


@pytest.mark.unit
class TestDQHelpers:
    def _records(self):
        return proc.assemble_records(
            proc.parse_titles(LONG_TEXT), proc.parse_titles(SHORT_TEXT), EDITION
        )

    def test_find_duplicate_keys(self):
        recs = self._records()
        assert proc.find_duplicate_keys(recs) == []
        assert proc.find_duplicate_keys(recs + [recs[0]])[0] == (
            recs[0].icd9_procedure_code,
            EDITION,
        )

    def test_same_code_different_editions_not_duplicate(self):
        a = proc.Icd9ProcedureRecord("47.01", 2014, "s", "l", True, "47", "42-54", "Digestive")
        b = proc.Icd9ProcedureRecord("47.01", 2015, "s", "l", True, "47", "42-54", "Digestive")
        assert proc.find_duplicate_keys([a, b]) == []

    def test_find_missing_long_titles(self):
        recs = self._records()
        assert proc.find_missing_long_titles(recs) == []
        blank = recs + [
            proc.Icd9ProcedureRecord("99.99", EDITION, "x", "  ", True, "99", "87-99", "Misc")
        ]
        assert proc.find_missing_long_titles(blank) == [("99.99", EDITION)]

    def test_find_format_violations(self):
        recs = self._records()
        assert proc.find_format_violations(recs) == []
        bad = recs + [
            proc.Icd9ProcedureRecord("470", EDITION, "x", "x", True, "47", "42-54", "Digestive")
        ]
        assert proc.find_format_violations(bad) == ["470"]

    def test_find_bad_chapters(self):
        recs = self._records()
        assert proc.find_bad_chapters(recs) == []
        bad = recs + [proc.Icd9ProcedureRecord("AB.0", EDITION, "x", "x", True, "AB", None, None)]
        assert proc.find_bad_chapters(bad) == [("AB.0", "AB")]

    def test_billable_share(self):
        recs = self._records()
        assert proc.billable_share(recs) == pytest.approx(3 / 5)  # 47.01, 36.06, 00.01
        assert proc.billable_share([]) == 0.0


@pytest.mark.unit
class TestMemberSelectors:
    def test_selects_sg_long_and_short_ignoring_dx(self):
        names = [
            "CMS32_DESC_LONG_DX.txt",
            "CMS32_DESC_SHORT_DX.txt",
            "CMS32_DESC_LONG_SG.txt",
            "CMS32_DESC_SHORT_SG.txt",
            "readme.txt",
        ]
        assert proc.select_long_member(names) == "CMS32_DESC_LONG_SG.txt"
        assert proc.select_short_member(names) == "CMS32_DESC_SHORT_SG.txt"

    def test_long_member_rejects_missing(self):
        with pytest.raises(ValueError, match="long-title"):
            proc.select_long_member(["CMS32_DESC_SHORT_SG.txt", "readme.txt"])
