"""Unit tests for `cidmath_datahub.reference.icd10cm`.

Anchored against real ICD-10-CM codes and the CDC NCHS order-file fixed-width
layout:
  - U07.1 (COVID-19) — added FY2021; billable leaf (flag 1).
  - J18.9 (Pneumonia, unspecified organism) — billable leaf.
  - A00   (Cholera) — a 3-char category header (flag 0, not billable).
  - A00.0 (Cholera due to Vibrio cholerae 01, biovar cholerae) — billable leaf.

Source format reference: https://www.cdc.gov/nchs/icd/icd-10-cm/files.html
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import icd10cm

EDITION = 2025


def _order_line(order_no: str, code: str, flag: str, short_desc: str, long_desc: str) -> str:
    """Compose one CDC order-file line at the spec's fixed character positions.

    Layout (1-indexed): 1-5 order no (right/zero), 6 blank, 7-13 code (left),
    14 blank, 15 flag, 16 blank, 17-76 short desc (left), 77 blank, 78+ long.
    Built independently of the parser's slice constants so the two must agree.
    """
    return (
        f"{order_no:0>5}"  # cols 1-5
        + " "  # col 6
        + f"{code:<7}"  # cols 7-13
        + " "  # col 14
        + flag  # col 15
        + " "  # col 16
        + f"{short_desc:<60}"  # cols 17-76
        + " "  # col 77
        + long_desc  # cols 78+
    )


SAMPLE_LINES = [
    _order_line("1", "A00", "0", "Cholera", "Cholera"),
    _order_line(
        "2",
        "A000",
        "1",
        "Cholera d/t Vibrio cholerae 01, biovar cholerae",
        "Cholera due to Vibrio cholerae 01, biovar cholerae",
    ),
    _order_line(
        "39850", "J189", "1", "Pneumonia, unspecified organism", "Pneumonia, unspecified organism"
    ),
    _order_line("95922", "U071", "1", "COVID-19", "COVID-19"),
]
SAMPLE_FILE = "\n".join(SAMPLE_LINES) + "\n"


@pytest.mark.unit
class TestNormalizeCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("U071", "U07.1"),
            ("J189", "J18.9"),
            ("A00", "A00"),  # 3-char category stays undotted
            ("A000", "A00.0"),
            ("S72001A", "S72.001A"),  # 7-char code
            (" j18.9 ", "J18.9"),  # trims + upper-cases + already dotted
            ("u071", "U07.1"),
            ("", ""),  # empty in, empty out
        ],
    )
    def test_normalization(self, raw, expected):
        assert icd10cm.normalize_code(raw) == expected


@pytest.mark.unit
class TestValidateCode:
    @pytest.mark.parametrize(
        "code",
        [
            "U07.1",
            "J18.9",
            "A00",
            "A00.0",
            "S72.001A",
            "C4A",  # letter in the 3rd position (melanoma)
            "QA0",  # letter in the 2nd position (present in CDC's FY2026 file)
            "QA0.0101",
        ],
    )
    def test_valid(self, code):
        assert icd10cm.validate_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "u071",  # not normalized (lower-case, no dot)
            "U071",  # 4 chars but no decimal
            "123",  # no leading letter
            "U07.",  # dot with nothing after
            "U07.12345",  # too many post-decimal chars
            "",  # empty
        ],
    )
    def test_invalid(self, code):
        assert icd10cm.validate_code(code) is False


@pytest.mark.unit
class TestParseOrderLine:
    def test_billable_leaf(self):
        rec = icd10cm.parse_order_line(SAMPLE_LINES[3], EDITION)  # U07.1
        assert rec is not None
        assert rec.icd10cm_code == "U07.1"
        assert rec.edition_year == EDITION
        assert rec.description == "COVID-19"
        assert rec.is_billable is True

    def test_header_not_billable(self):
        rec = icd10cm.parse_order_line(SAMPLE_LINES[0], EDITION)  # A00 header
        assert rec is not None
        assert rec.icd10cm_code == "A00"
        assert rec.is_billable is False

    def test_blank_and_short_lines_skipped(self):
        assert icd10cm.parse_order_line("", EDITION) is None
        assert icd10cm.parse_order_line("   ", EDITION) is None
        assert icd10cm.parse_order_line("00001 A00", EDITION) is None  # too short for a flag

    def test_bad_flag_raises(self):
        bad = _order_line("3", "B001", "X", "bad flag", "bad flag")
        with pytest.raises(ValueError, match="Unexpected billable flag"):
            icd10cm.parse_order_line(bad, EDITION)


@pytest.mark.unit
class TestParseOrderFile:
    def test_parses_all_content_lines(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        assert len(recs) == 4

    def test_known_codes_present_and_dotted(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        by_code = {r.icd10cm_code: r for r in recs}
        assert "U07.1" in by_code
        assert "J18.9" in by_code
        assert by_code["J18.9"].description == "Pneumonia, unspecified organism"
        assert all(r.edition_year == EDITION for r in recs)

    def test_billable_split(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        assert sum(r.is_billable for r in recs) == 3  # A00.0, J18.9, U07.1
        assert sum(not r.is_billable for r in recs) == 1  # A00 header


@pytest.mark.unit
class TestDQHelpers:
    def test_find_format_violations(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        assert icd10cm.find_format_violations(recs) == []  # clean sample
        bad = recs + [icd10cm.Icd10Record("bogus!", EDITION, "x", True, "x")]
        assert icd10cm.find_format_violations(bad) == ["bogus!"]

    def test_find_missing_descriptions(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        assert icd10cm.find_missing_descriptions(recs) == []  # all populated
        blank = recs + [icd10cm.Icd10Record("Z99.9", EDITION, "  ", True, "")]
        assert icd10cm.find_missing_descriptions(blank) == [("Z99.9", EDITION)]

    def test_find_duplicate_keys(self):
        recs = icd10cm.parse_order_file(SAMPLE_FILE, EDITION)
        assert icd10cm.find_duplicate_keys(recs) == []  # unique per edition
        dup = recs + [recs[0]]  # repeat A00 for the same edition
        assert icd10cm.find_duplicate_keys(dup) == [("A00", EDITION)]

    def test_same_code_different_editions_not_duplicate(self):
        a = icd10cm.Icd10Record("U07.1", 2021, "COVID-19", True, "COVID-19")
        b = icd10cm.Icd10Record("U07.1", 2025, "COVID-19", True, "COVID-19")
        assert icd10cm.find_duplicate_keys([a, b]) == []


@pytest.mark.unit
class TestSourceLocators:
    def test_order_file_zip_url_default_template(self):
        url = icd10cm.order_file_zip_url(2026)
        assert url.endswith("/ICD10CM/2026/icd10cm-Code Descriptions-2026.zip")
        assert url.startswith("https://ftp.cdc.gov/")

    def test_order_file_zip_url_custom_template(self):
        url = icd10cm.order_file_zip_url(
            2021, template="file:///data/icd10/{year}/order-{year}.zip"
        )
        assert url == "file:///data/icd10/2021/order-2021.zip"

    def test_update_file_zip_url_default_template(self):
        url = icd10cm.update_file_zip_url(2026)
        assert url.endswith("/ICD10CM/2026-update/icd10cm-Code Descriptions-April-1-2026.zip")
        assert url.startswith("https://ftp.cdc.gov/")

    @pytest.mark.parametrize(
        "name",
        [
            "icd10cm-order-2026.txt",  # base
            "icd10cm_order_2024.txt",  # older separator style
            "ICD10CM-Order-2025.TXT",  # case-insensitive
            "icd10cm-order-April-1-2026.txt",  # mid-year update naming
        ],
    )
    def test_select_order_file_member_picks_order_file(self, name):
        members = ["icd10cm-codes-2026.txt", name, "readme.txt"]
        assert icd10cm.select_order_file_member(members) == name

    def test_select_order_file_member_rejects_codes_only(self):
        # The billable-only "codes" file must never be mistaken for the order file.
        with pytest.raises(ValueError, match="order file"):
            icd10cm.select_order_file_member(["icd10cm-codes-2026.txt", "readme.txt"])

    def test_select_order_file_member_rejects_ambiguous(self):
        with pytest.raises(ValueError, match="exactly one"):
            icd10cm.select_order_file_member(["icd10cm-order-2025.txt", "icd10cm-order-2026.txt"])

    def test_select_order_file_member_ignores_addenda(self):
        # The real FY2026 "Code Descriptions" zip ships the full order file AND a
        # change-only order-addenda file; pick the full one, drop the addenda.
        members = [
            "icd10OrderFiles.pdf",
            "icd10cm-codes-2026.txt",
            "icd10cm-codes-addenda-2026.txt",
            "icd10cm-order-2026.txt",
            "icd10cm-order-addenda-2026.txt",
            "icd10cmCodesFile.pdf",
        ]
        assert icd10cm.select_order_file_member(members) == "icd10cm-order-2026.txt"


@pytest.mark.unit
class TestOverlayRecords:
    """The mid-year (Apr-1) update overlaid onto the Oct-1 base, update-wins."""

    def _base(self):
        return [
            icd10cm.Icd10Record("A00", 2026, "Cholera", False, "Cholera"),
            icd10cm.Icd10Record(
                "J18.9", 2026, "Pneumonia, unspecified organism", True, "Pneumonia"
            ),
            icd10cm.Icd10Record("U07.1", 2026, "COVID-19", True, "COVID-19"),
        ]

    def test_empty_update_returns_base(self):
        base = self._base()
        assert icd10cm.overlay_records(base, []) == base

    def test_update_revises_existing_code(self):
        base = self._base()
        update = [
            icd10cm.Icd10Record("J18.9", 2026, "Pneumonia, unspecified (revised)", True, "Pn")
        ]
        merged = {r.icd10cm_code: r for r in icd10cm.overlay_records(base, update)}
        assert len(merged) == 3  # no new code, just a revision
        assert merged["J18.9"].description == "Pneumonia, unspecified (revised)"

    def test_update_adds_new_code(self):
        base = self._base()
        update = [icd10cm.Icd10Record("U07.2", 2026, "Vaping-related disorder", True, "Vaping")]
        merged = {r.icd10cm_code: r for r in icd10cm.overlay_records(base, update)}
        assert len(merged) == 4
        assert merged["U07.2"].description == "Vaping-related disorder"
        assert merged["A00"].description == "Cholera"  # base codes retained

    def test_base_only_codes_retained_and_keys_unique(self):
        base = self._base()
        update = [
            icd10cm.Icd10Record("A00", 2026, "Cholera (rev)", False, "Cholera"),
            icd10cm.Icd10Record("B99.9", 2026, "Unspecified infectious disease", True, "Inf"),
        ]
        merged = icd10cm.overlay_records(base, update)
        codes = [r.icd10cm_code for r in merged]
        assert sorted(codes) == ["A00", "B99.9", "J18.9", "U07.1"]
        assert len(codes) == len(set(codes))  # overlay never duplicates a code


# A small but real-shaped tabular XML covering three chapters/branches, used by
# the hierarchy tests below. Mirrors the CDC tabular structure
# (chapter -> section[id] -> diag -> nested diag), with code-range parentheticals
# on the chapter/section descriptions that the parser must strip.
TABULAR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ICD10CM.tabular>
  <version>2026</version>
  <chapter>
    <name>19</name>
    <desc>Injury, poisoning and certain other consequences of external causes (S00-T88)</desc>
    <section id="S70-S79">
      <desc>Injuries to the hip and thigh (S70-S79)</desc>
      <diag><name>S72</name><desc>Fracture of femur</desc>
        <diag><name>S72.0</name><desc>Fracture of head and neck of femur</desc>
          <diag><name>S72.00</name><desc>Fracture of unspecified part of neck of femur</desc>
            <diag><name>S72.001</name><desc>Fx of neck of right femur</desc></diag>
          </diag>
        </diag>
      </diag>
    </section>
  </chapter>
  <chapter>
    <name>22</name>
    <desc>Codes for special purposes (U00-U85)</desc>
    <section id="U00-U49">
      <desc>Provisional assignment of new diseases of uncertain etiology (U00-U49)</desc>
      <diag><name>U07</name><desc>Emergency use of U07</desc></diag>
    </section>
  </chapter>
  <chapter>
    <name>1</name>
    <desc>Certain infectious and parasitic diseases (A00-B99)</desc>
    <section id="A00-A09">
      <desc>Intestinal infectious diseases (A00-A09)</desc>
      <diag><name>A00</name><desc>Cholera</desc></diag>
    </section>
  </chapter>
</ICD10CM.tabular>"""


@pytest.mark.unit
class TestTabularSourceLocators:
    def test_tabular_zip_url_default(self):
        url = icd10cm.tabular_zip_url(2026)
        assert url.endswith("/ICD10CM/2026/icd10cm-table and index-2026.zip")

    def test_update_tabular_zip_url_default(self):
        url = icd10cm.update_tabular_zip_url(2026)
        assert url.endswith("/ICD10CM/2026-update/icd10cm-table-and-index-April-1-2026.zip")

    @pytest.mark.parametrize(
        "name", ["icd10cm-tabular-2026.xml", "icd10cm-tabular-April-1-2026.xml"]
    )
    def test_select_tabular_xml_member(self, name):
        members = ["icd10cm-index-2026.xml", name, "icd10cm-drug-2026.xml", "readme.txt"]
        assert icd10cm.select_tabular_xml_member(members) == name

    def test_select_tabular_xml_member_rejects_index_only(self):
        with pytest.raises(ValueError, match="tabular"):
            icd10cm.select_tabular_xml_member(["icd10cm-index-2026.xml", "readme.txt"])


@pytest.mark.unit
class TestPrefixesAndAncestors:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("S72.001A", ["S72", "S72.0", "S72.00", "S72.001"]),
            ("A00.0", ["A00"]),
            ("A00", []),  # a category has no proper prefixes
            ("U07.1", ["U07"]),
        ],
    )
    def test_code_prefixes(self, code, expected):
        assert icd10cm.code_prefixes(code) == expected

    def test_category_of(self):
        assert icd10cm.category_of("S72.001A") == "S72"
        assert icd10cm.category_of("A00") == "A00"

    def test_ancestors_only_existing_prefixes(self):
        code_set = {"S72", "S72.0", "S72.00", "S72.001", "S72.001A"}
        assert icd10cm.ancestors_for("S72.001A", code_set) == ["S72", "S72.0", "S72.00", "S72.001"]

    def test_ancestors_skip_x_placeholder_stems(self):
        # S02.0XX / S02.0X are not listed codes -> nearest existing ancestor is S02.0.
        code_set = {"S02", "S02.0", "S02.0XXA"}
        assert icd10cm.ancestors_for("S02.0XXA", code_set) == ["S02", "S02.0"]


@pytest.mark.unit
class TestTabularCategoryMap:
    def test_maps_categories_with_clean_labels(self):
        m = icd10cm.parse_tabular_category_map(TABULAR_XML)
        assert m["S72"].chapter_code == "19"
        # the "(S00-T88)" range parenthetical is stripped from the chapter name
        assert (
            m["S72"].chapter_name
            == "Injury, poisoning and certain other consequences of external causes"
        )
        assert m["S72"].block_code == "S70-S79"
        assert m["S72"].block_name == "Injuries to the hip and thigh"
        assert m["U07"].chapter_code == "22"
        assert m["A00"].block_code == "A00-A09"

    def test_only_top_level_categories_are_keys(self):
        m = icd10cm.parse_tabular_category_map(TABULAR_XML)
        # nested diag S72.0 is not a category key; only 3-char categories are
        assert "S72.0" not in m
        assert set(m) == {"S72", "U07", "A00"}


@pytest.mark.unit
class TestBuildHierarchy:
    def _records(self):
        rec = icd10cm.Icd10Record
        return [
            rec("S72", 2026, "Fracture of femur", False, ""),
            rec("S72.0", 2026, "Fracture of head and neck of femur", False, ""),
            rec("S72.00", 2026, "Fracture of unspecified part of neck of femur", False, ""),
            rec("S72.001", 2026, "Fracture of unspecified part of neck of right femur", False, ""),
            rec("S72.001A", 2026, "...initial encounter for closed fracture", True, ""),
            rec("U07", 2026, "Emergency use of U07", False, ""),
            rec("U07.1", 2026, "COVID-19", True, ""),
            rec("A00", 2026, "Cholera", False, ""),
            rec("A00.0", 2026, "Cholera due to Vibrio cholerae 01, biovar cholerae", True, ""),
        ]

    def test_deep_leaf_full_chain(self):
        cm = icd10cm.parse_tabular_category_map(TABULAR_XML)
        nodes = {n.icd10cm_code: n for n in icd10cm.build_hierarchy(self._records(), cm)}
        n = nodes["S72.001A"]
        assert n.parent_icd10cm_code == "S72.001"
        assert n.node_level == 4
        assert n.ancestor_codes == ("S72", "S72.0", "S72.00", "S72.001")
        assert n.chapter_code == "19"
        assert n.block_code == "S70-S79"
        assert n.is_billable is True

    def test_category_root_has_null_parent(self):
        cm = icd10cm.parse_tabular_category_map(TABULAR_XML)
        nodes = {n.icd10cm_code: n for n in icd10cm.build_hierarchy(self._records(), cm)}
        assert nodes["U07"].parent_icd10cm_code is None
        assert nodes["U07"].node_level == 0
        assert nodes["U07.1"].parent_icd10cm_code == "U07"
        assert nodes["A00.0"].ancestor_codes == ("A00",)
        assert nodes["A00"].chapter_code == "1"

    def test_empty_category_map_leaves_chapter_block_null(self):
        nodes = icd10cm.build_hierarchy(self._records(), {})
        # adjacency still computed from the code set even with no XML
        by = {n.icd10cm_code: n for n in nodes}
        assert by["S72.001A"].parent_icd10cm_code == "S72.001"
        assert all(n.chapter_code is None and n.block_code is None for n in nodes)

    def test_unmapped_and_orphan_dq_helpers(self):
        cm = icd10cm.parse_tabular_category_map(TABULAR_XML)
        # B99.9's category B99 is not in the map -> unmapped; and with no B99 in
        # the record set, B99.9 is also an orphan (no parent in its edition).
        recs = [icd10cm.Icd10Record("B99.9", 2026, "Unspecified infectious disease", True, "")]
        nodes = icd10cm.build_hierarchy(recs, cm)
        assert icd10cm.find_unmapped_categories(nodes) == ["B99"]
        assert icd10cm.find_orphan_codes(nodes) == ["B99.9"]
        # the clean sample has neither
        clean = icd10cm.build_hierarchy(self._records(), cm)
        assert icd10cm.find_unmapped_categories(clean) == []
        assert icd10cm.find_orphan_codes(clean) == []


@pytest.mark.unit
class TestTabularTree:
    """parse_tabular_tree reads the authoritative parent of every listed code."""

    def test_parent_of_from_nesting(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        assert tree.parent_of["S72"] is None  # 3-char category = section root
        assert tree.parent_of["S72.0"] == "S72"
        assert tree.parent_of["S72.00"] == "S72.0"
        assert tree.parent_of["S72.001"] == "S72.00"
        assert tree.parent_of["U07"] is None

    def test_category_map_only_top_level(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        assert set(tree.category_map) == {"S72", "U07", "A00"}
        # nested diags are in parent_of but not category_map
        assert "S72.0" in tree.parent_of and "S72.0" not in tree.category_map


@pytest.mark.unit
class TestXmlSourcedAdjacency:
    """build_hierarchy reads adjacency from the XML tree, prefix only as fallback."""

    def _records(self):
        rec = icd10cm.Icd10Record
        return [
            rec("S72", 2026, "Fracture of femur", False, ""),
            rec("S72.0", 2026, "Fracture of head and neck of femur", False, ""),
            rec("S72.00", 2026, "Fracture of unspecified part of neck of femur", False, ""),
            rec("S72.001", 2026, "Fracture of unsp part of neck of right femur", False, ""),
            rec("S72.001A", 2026, "...initial encounter for closed fracture", True, ""),
            rec("U07", 2026, "Emergency use of U07", False, ""),
            rec("U07.1", 2026, "COVID-19", True, ""),
        ]

    def test_listed_node_parent_from_xml(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        nodes = {
            n.icd10cm_code: n
            for n in icd10cm.build_hierarchy(self._records(), tree.category_map, tree.parent_of)
        }
        # S72.001 is an XML node: parent/ancestors read straight from the nesting
        assert nodes["S72.001"].parent_icd10cm_code == "S72.00"
        assert nodes["S72.001"].ancestor_codes == ("S72", "S72.0", "S72.00")

    def test_seventh_char_falls_back_to_nearest_listed(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        nodes = {
            n.icd10cm_code: n
            for n in icd10cm.build_hierarchy(self._records(), tree.category_map, tree.parent_of)
        }
        # S72.001A is NOT an XML node -> anchors to S72.001 and inherits its chain
        n = nodes["S72.001A"]
        assert n.parent_icd10cm_code == "S72.001"
        assert n.ancestor_codes == ("S72", "S72.0", "S72.00", "S72.001")
        assert n.node_level == 4
        # U07.1 is not listed in the XML (only U07 is) -> falls back to U07
        assert nodes["U07.1"].parent_icd10cm_code == "U07"

    def test_resolve_ancestors_direct(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        code_set = {r.icd10cm_code for r in self._records()}
        assert icd10cm.resolve_ancestors("S72.001", tree.parent_of, code_set) == [
            "S72",
            "S72.0",
            "S72.00",
        ]
        # empty parent_of (XML skipped) -> prefix rule over the code set
        assert icd10cm.resolve_ancestors("S72.001A", {}, code_set) == [
            "S72",
            "S72.0",
            "S72.00",
            "S72.001",
        ]

    def test_adjacency_cross_check(self):
        tree = icd10cm.parse_tabular_tree(TABULAR_XML)
        recs = self._records()
        # XML and prefix agree on the clean sample
        assert icd10cm.find_adjacency_mismatches(recs, tree.parent_of) == []
        # corrupt one XML parent -> the cross-check flags exactly that code
        bad = dict(tree.parent_of)
        bad["S72.0"] = "S99"
        assert icd10cm.find_adjacency_mismatches(recs, bad) == ["S72.0"]
