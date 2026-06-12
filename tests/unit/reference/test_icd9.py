"""Unit tests for `cidmath_datahub.reference.icd9` (Slice 1: parse/normalize/billable).

Anchored on real-shaped ICD-9-CM tabular rows incl. V and E codes:
  - 250 / 250.0 / 250.00 (diabetes mellitus) -- numeric category -> sub -> billable leaf
  - V30 / V30.00 (single liveborn) -- V supplementary classification
  - E812 / E812.0 (motor vehicle accident) -- E code (decimal after the 4th char)
  - E810 -- a 4-char E code with no subdivisions (billable leaf)

NOTE: the DTAB sample below mimics the RTF->text shape; confirm against a real
DTAB extract during the dev run and capture it as a fixture (ADR 0011/0031).
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import icd9

EDITION = 2012

# A small, real-shaped slice of the Tabular List (Volume 1) after RTF->text:
# chapter header, section banners, code+title lines, and an instructional note.
DTAB_SAMPLE = """\
1. INFECTIOUS AND PARASITIC DISEASES (001-139)

   INTESTINAL INFECTIOUS DISEASES (001-009)

   001     Cholera
   001.0     Due to Vibrio cholerae
   001.9     Cholera, unspecified

   ENDOCRINE, NUTRITIONAL AND METABOLIC DISEASES (240-279)

   250     Diabetes mellitus
           Excludes: gestational diabetes (648.8)
   250.0     Diabetes mellitus without mention of complication
   250.00     Type II or unspecified type, not stated as uncontrolled

   SUPPLEMENTARY CLASSIFICATION OF FACTORS (V01-V91)

   V30     Single liveborn
   V30.0     Born in hospital
   V30.00     Delivered without mention of cesarean delivery

   SUPPLEMENTARY CLASSIFICATION OF EXTERNAL CAUSES (E000-E999)

   E810     Motor vehicle traffic accident involving collision with train
   E812     Other motor vehicle traffic accident involving collision
   E812.0     Driver of motor vehicle other than motorcycle
"""


@pytest.mark.unit
class TestUrlBuilder:
    def test_suffix_and_dir_mapping(self):
        # FY2012 -> dir 2011, suffix "12"
        assert icd9.edition_suffix(2012) == "12"
        assert icd9.edition_dir_year(2012) == 2011
        assert icd9.edition_suffix(2010) == "10"
        assert icd9.edition_dir_year(2010) == 2009

    def test_zip_and_readme_urls(self):
        assert icd9.dtab_zip_url(2012).endswith("/ICD9-CM/2011/Dtab12.zip")
        assert icd9.appendix_zip_url(2012).endswith("/ICD9-CM/2011/Appndx12.zip")
        assert icd9.readme_url(2012).endswith("/ICD9-CM/2011/Readme12.txt")


@pytest.mark.unit
class TestSelectMembers:
    def test_select_dtab(self):
        members = ["DTAB12.RTF", "DINDEX12.RTF", "PREFAC12.RTF"]
        assert icd9.select_dtab_member(members) == "DTAB12.RTF"

    def test_select_appendix_e(self):
        # APPNDX zip also ships the other appendices; pick only DC_3D
        members = ["DMORPH12.RTF", "DDRGCL12.RTF", "DINDST12.RTF", "DC_3D12.RTF"]
        assert icd9.select_appendix_e_member(members) == "DC_3D12.RTF"

    def test_select_dtab_missing_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            icd9.select_dtab_member(["DINDEX12.RTF", "PTAB12.RTF"])


@pytest.mark.unit
class TestNormalizeCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("25000", "250.00"),
            ("2500", "250.0"),
            ("250", "250"),
            ("460", "460"),  # 3-digit, no subdivision
            ("V3000", "V30.00"),
            ("V30", "V30"),
            ("E8120", "E812.0"),  # E decimal after the 4th char
            ("E812", "E812"),
            (" 250.00 ", "250.00"),  # trims, already dotted
            ("v30.0", "V30.0"),  # upper-cases
            ("", ""),
        ],
    )
    def test_normalization(self, raw, expected):
        assert icd9.normalize_code(raw) == expected


@pytest.mark.unit
class TestValidateCode:
    @pytest.mark.parametrize(
        "code", ["250", "250.0", "250.00", "460", "V30", "V30.00", "E810", "E812.0"]
    )
    def test_valid(self, code):
        assert icd9.validate_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "25000",  # not normalized (no decimal)
            "v30.0",  # lower-case
            "25",  # too short (numeric needs 3 digits)
            "250.000",  # too many decimal digits (numeric max 2)
            "E812.00",  # E codes take only one decimal digit
            "E81",  # E needs 3 digits
            "",
        ],
    )
    def test_invalid(self, code):
        assert icd9.validate_code(code) is False

    def test_code_class(self):
        assert icd9.code_class("250.00") == "numeric"
        assert icd9.code_class("V30") == "V"
        assert icd9.code_class("E812.0") == "E"


@pytest.mark.unit
class TestParseDtab:
    def test_extracts_only_code_lines(self):
        pairs = icd9.parse_dtab(DTAB_SAMPLE)
        codes = [c for c, _ in pairs]
        # every real code present, in all three classes
        for c in ["001", "001.0", "250", "250.00", "V30", "V30.00", "E810", "E812.0"]:
            assert c in codes
        # banners / chapter header / notes excluded
        assert all(not desc.startswith("Excludes") for _, desc in pairs)
        assert "139" not in codes and "279" not in codes  # range banners not codes

    def test_titles_captured(self):
        by_code = dict(icd9.parse_dtab(DTAB_SAMPLE))
        assert by_code["250"] == "Diabetes mellitus"
        assert by_code["V30"] == "Single liveborn"
        assert by_code["E812.0"] == "Driver of motor vehicle other than motorcycle"


@pytest.mark.unit
class TestBillableLeafOfSet:
    def test_leaf_set(self):
        codes = [c for c, _ in icd9.parse_dtab(DTAB_SAMPLE)]
        billable = icd9.find_billable_codes(codes)
        # leaves are billable
        assert {"001.0", "001.9", "250.00", "V30.00", "E810", "E812.0"} <= billable
        # parents/headers are not
        assert billable.isdisjoint({"001", "250", "250.0", "V30", "V30.0", "E812"})

    def test_three_digit_with_no_subdivision_is_billable(self):
        # 460 has no children in this set -> it is a leaf -> billable
        assert icd9.find_billable_codes(["460", "250", "250.0"]) == {"460", "250.0"}


@pytest.mark.unit
class TestAssembleRecords:
    def test_records_and_billable_flag(self):
        recs = {
            r.icd9_code: r for r in icd9.assemble_records(icd9.parse_dtab(DTAB_SAMPLE), EDITION)
        }
        assert recs["250.00"].is_billable is True
        assert recs["250"].is_billable is False
        assert recs["E810"].is_billable is True
        assert recs["E812"].is_billable is False
        assert all(r.edition_year == EDITION for r in recs.values())

    def test_dedup_first_wins(self):
        recs = icd9.assemble_records([("250", "First"), ("250", "Second")], EDITION)
        assert len(recs) == 1 and recs[0].description == "First"


# A slice of Appendix E (DC_3D, "List of Three-Digit Categories") in the real
# striprtf shape (verified against the 2012 edition): chapter headers are
# "<n>.<TAB>ALL-CAPS NAME" with NO code range; block headers are sentence-case
# "Name (low-high)"; category lines are "<code><TAB>Title". (V/E categories are
# intentionally omitted to exercise the unmapped-block WARN path.)
APPENDIX_E_SAMPLE = (
    "\n".join(
        [
            "Appendix E",
            "List of Three-Digit Categories",
            "1.\tINFECTIOUS AND PARASITIC DISEASES",
            "Intestinal infectious diseases (001-009)",
            "001\tCholera",
            "002\tTyphoid and paratyphoid fevers",
            "3.\tENDOCRINE, NUTRITIONAL AND METABOLIC DISEASES",
            "Diseases of other endocrine glands (249-259)",
            "250\tDiabetes mellitus",
        ]
    )
    + "\n"
)


@pytest.mark.unit
class TestCodePrefixesAndAncestors:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("250.00", ["250", "250.0"]),
            ("250.0", ["250"]),
            ("250", []),  # a category has no proper prefix
            ("V30.00", ["V30", "V30.0"]),
            ("E812.0", ["E812"]),  # E category is 4 chars -> decimal after the 4th
            ("E812", []),
        ],
    )
    def test_code_prefixes(self, code, expected):
        assert icd9.code_prefixes(code) == expected

    def test_category_of(self):
        assert icd9.category_of("250.00") == "250"
        assert icd9.category_of("V30.0") == "V30"
        assert icd9.category_of("E812.0") == "E812"

    def test_ancestors_only_existing(self):
        code_set = {"250", "250.0", "250.00"}
        assert icd9.ancestors_for("250.00", code_set) == ["250", "250.0"]
        # if an intermediate is missing, it's skipped
        assert icd9.ancestors_for("250.00", {"250", "250.00"}) == ["250"]


@pytest.mark.unit
class TestChapterMap:
    @pytest.mark.parametrize(
        "code,chapter_code",
        [
            ("001", "1"),
            ("250.00", "3"),  # endocrine, 240-279
            ("460", "8"),  # respiratory, 460-519
            ("999.9", "17"),  # injury and poisoning, 800-999
            ("V30.00", "V"),  # supplementary -- factors influencing health status
            ("E812.0", "E"),  # supplementary -- external causes
        ],
    )
    def test_chapter_for(self, code, chapter_code):
        result = icd9.chapter_for(code)
        assert result is not None and result[0] == chapter_code

    def test_chapter_names_present(self):
        assert "Endocrine" in icd9.chapter_for("250")[1]
        assert icd9.chapter_for("V30")[1].startswith("Supplementary")
        assert icd9.chapter_for("E812")[1].startswith("Supplementary")


@pytest.mark.unit
class TestAppendixE:
    def test_category_to_block(self):
        m = icd9.parse_appendix_e(APPENDIX_E_SAMPLE)
        assert m["250"] == ("249-259", "Diseases of other endocrine glands")
        assert m["001"] == ("001-009", "Intestinal infectious diseases")
        assert "002" in m  # all categories under a block are mapped
        # chapter headers (no range) and preamble lines don't leak in as blocks
        assert "139" not in m and "240" not in m


@pytest.mark.unit
class TestBuildHierarchy:
    def _nodes(self):
        records = icd9.assemble_records(icd9.parse_dtab(DTAB_SAMPLE), EDITION)
        category_map = icd9.parse_appendix_e(APPENDIX_E_SAMPLE)
        return {n.icd9_code: n for n in icd9.build_hierarchy(records, category_map)}

    def test_adjacency_and_path(self):
        n = self._nodes()["250.00"]
        assert n.parent_icd9_code == "250.0"
        assert n.ancestor_codes == ("250", "250.0")
        assert n.node_level == 2
        assert n.chapter_code == "3"
        assert n.block_code == "249-259"
        assert n.is_billable is True

    def test_category_root_has_null_parent(self):
        nodes = self._nodes()
        assert nodes["250"].parent_icd9_code is None
        assert nodes["250"].node_level == 0
        assert nodes["250.0"].ancestor_codes == ("250",)

    def test_contract_node_level_equals_len_ancestors(self):
        # the shared code-system contract (ADR 0031), identical to codes.icd10
        for n in self._nodes().values():
            assert n.node_level == len(n.ancestor_codes)

    def test_subtree_semantics_match_icd10(self):
        # WHERE array_contains(ancestor_codes, '250') selects the 250 subtree
        nodes = self._nodes().values()
        subtree = {n.icd9_code for n in nodes if "250" in n.ancestor_codes}
        assert subtree == {"250.0", "250.00"}  # descendants, not 250 itself

    def test_empty_category_map_leaves_block_null_chapter_static(self):
        records = icd9.assemble_records(icd9.parse_dtab(DTAB_SAMPLE), EDITION)
        nodes = {n.icd9_code: n for n in icd9.build_hierarchy(records, {})}
        # adjacency still computed; chapter from the static map; only block null
        assert nodes["250.00"].parent_icd9_code == "250.0"
        assert nodes["250.00"].chapter_code == "3"
        assert all(n.block_code is None for n in nodes.values())


@pytest.mark.unit
class TestHierarchyDQ:
    def _nodes(self):
        records = icd9.assemble_records(icd9.parse_dtab(DTAB_SAMPLE), EDITION)
        category_map = icd9.parse_appendix_e(APPENDIX_E_SAMPLE)
        return icd9.build_hierarchy(records, category_map)

    def test_dangling_parents_empty_by_construction(self):
        assert icd9.find_dangling_parents(self._nodes()) == []

    def test_orphans_empty(self):
        assert icd9.find_orphan_codes(self._nodes()) == []

    def test_all_categories_get_a_chapter(self):
        # chapters come from the static frozen map, so every code resolves a chapter
        # (incl. V/E) -- find_unmapped_categories surfaces only true anomalies
        assert icd9.find_unmapped_categories(self._nodes()) == []

    def test_unmapped_blocks_flag_ve(self):
        # blocks come from Appendix E; the sample maps 001 and 250 but omits the V/E
        # categories -> those have a null block and are flagged (WARN, not blocking)
        unmapped = icd9.find_unmapped_blocks(self._nodes())
        assert unmapped == ["E810", "E812", "V30"]
        assert "001" not in unmapped and "250" not in unmapped  # both in Appendix E
