"""Unit tests for `cidmath_datahub.reference.loinc` (LOINC core + MapTo; ADR 0014).

Anchored on real-shaped rows in ``tests/fixtures/``: an ACTIVE lab term (the real
``2160-0`` Creatinine), an ACTIVE term carrying an ``EXTERNAL_COPYRIGHT_NOTICE``, a
DEPRECATED term and its ``MapTo`` successor, and a name with an embedded comma (so
the CSV-quoting path is exercised). The representative rows should be re-verified
against a real LOINC release during the dev run (HTTP/MD5/zip is entrypoint glue).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cidmath_datahub.reference import loinc

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
CORE_FIXTURE = _FIXTURES / "loinc_core_sample.csv"
MAPTO_FIXTURE = _FIXTURES / "loinc_mapto_sample.csv"


@pytest.fixture(scope="module")
def terms() -> list[loinc.LoincTerm]:
    return loinc.parse_loinc_core(CORE_FIXTURE.read_text(encoding="utf-8-sig"))


@pytest.fixture(scope="module")
def maps() -> list[loinc.LoincMapTo]:
    return loinc.parse_map_to(MAPTO_FIXTURE.read_text(encoding="utf-8-sig"))


@pytest.mark.unit
class TestStatus:
    def test_vocab(self):
        assert loinc.LOINC_STATUS_VALUES == {"active", "trial", "discouraged", "deprecated"}

    @pytest.mark.parametrize(
        "raw,expected",
        [("ACTIVE", "active"), ("DEPRECATED", "deprecated"), ("Trial", "trial"), ("", "")],
    )
    def test_normalize(self, raw: str, expected: str):
        assert loinc.normalize_status(raw) == expected


@pytest.mark.unit
class TestParseCore:
    def test_row_count(self, terms: list[loinc.LoincTerm]):
        assert len(terms) == 4

    def test_active_real_term(self, terms: list[loinc.LoincTerm]):
        t = next(t for t in terms if t.loinc_num == "2160-0")
        assert t.status == "active"
        assert t.component == "Creatinine"
        assert t.loinc_class == "CHEM"
        assert t.long_common_name == "Creatinine [Mass/volume] in Serum or Plasma"

    def test_external_copyright_kept(self, terms: list[loinc.LoincTerm]):
        t = next(t for t in terms if t.loinc_num == "44249-1")
        assert t.external_copyright_notice.startswith("Copyright")

    def test_comma_in_name_preserved(self, terms: list[loinc.LoincTerm]):
        # CSV (not naive split) so the comma in LONG_COMMON_NAME survives.
        t = next(t for t in terms if t.loinc_num == "10000-8")
        assert t.long_common_name == "R wave duration in lead AVR, by EKG"

    def test_deprecated_term(self, terms: list[loinc.LoincTerm]):
        assert next(t for t in terms if t.loinc_num == "10999-9").status == "deprecated"


@pytest.mark.unit
class TestParseMapTo:
    def test_row(self, maps: list[loinc.LoincMapTo]):
        assert len(maps) == 1
        assert maps[0].loinc_num == "10999-9"
        assert maps[0].map_to_loinc_num == "10000-8"


@pytest.mark.unit
class TestDq:
    def test_clean_core_passes_blocking(self, terms: list[loinc.LoincTerm]):
        assert loinc.find_duplicate_loinc_nums(terms) == []
        assert loinc.find_missing_term_fields(terms) == []
        assert loinc.find_status_violations(terms) == []

    def test_clean_map_passes_blocking(
        self, terms: list[loinc.LoincTerm], maps: list[loinc.LoincMapTo]
    ):
        assert loinc.find_duplicate_map_keys(maps) == []
        assert loinc.find_missing_map_fields(maps) == []
        assert loinc.find_map_target_orphans(maps, {t.loinc_num for t in terms}) == []

    def test_map_source_is_retired(
        self, terms: list[loinc.LoincTerm], maps: list[loinc.LoincMapTo]
    ):
        status_by = {t.loinc_num: t.status for t in terms}
        assert loinc.find_map_source_not_retired(maps, status_by) == []

    def test_status_violation_flagged(self):
        bad = loinc.parse_loinc_core("LOINC_NUM,LONG_COMMON_NAME,STATUS\n1-1,Foo,BOGUS\n")
        assert loinc.find_status_violations(bad) == [("1-1", "BOGUS")]

    def test_map_target_orphan_flagged(self, terms: list[loinc.LoincTerm]):
        bad = loinc.parse_map_to("LOINC,MAP_TO,COMMENT\n2160-0,99999-9,x\n")
        assert loinc.find_map_target_orphans(bad, {t.loinc_num for t in terms}) == [
            ("2160-0", "99999-9")
        ]

    def test_map_source_not_retired_flagged(self, terms: list[loinc.LoincTerm]):
        # 2160-0 is ACTIVE, so mapping FROM it is an anomaly (WARN).
        bad = loinc.parse_map_to("LOINC,MAP_TO,COMMENT\n2160-0,10000-8,x\n")
        status_by = {t.loinc_num: t.status for t in terms}
        assert loinc.find_map_source_not_retired(bad, status_by) == [("2160-0", "active")]

    def test_status_distribution(self, terms: list[loinc.LoincTerm]):
        assert loinc.status_distribution(terms) == {"active": 3, "deprecated": 1}
