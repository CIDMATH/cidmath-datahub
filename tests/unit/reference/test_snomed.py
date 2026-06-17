"""Unit tests for `cidmath_datahub.reference.snomed` (RF2 Snapshot -> concepts; ADR 0014).

Anchored on real-shaped RF2 Snapshot rows in ``tests/fixtures/``: an active disorder
(``73211009`` Diabetes mellitus) and an active finding (``386661006`` Fever), each with
an FSN + a preferred synonym (via the US-English language refset), plus a simulated
inactive concept. SCTID Verhoeff validation is the error-prone bit, so it's tested
directly. The representative rows should be re-verified against a real US Edition release
during the dev run (the UMLS download/unzip is entrypoint glue).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cidmath_datahub.reference import snomed

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
CONCEPT_FIXTURE = _FIXTURES / "snomed_concept_sample.txt"
DESCRIPTION_FIXTURE = _FIXTURES / "snomed_description_sample.txt"
LANGUAGE_FIXTURE = _FIXTURES / "snomed_language_sample.txt"


@pytest.fixture(scope="module")
def concepts() -> list[snomed.SnomedConceptRow]:
    return snomed.parse_concepts(CONCEPT_FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def descriptions() -> list[snomed.SnomedDescription]:
    return snomed.parse_descriptions(DESCRIPTION_FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def preferred_ids() -> set[str]:
    return snomed.parse_preferred_description_ids(LANGUAGE_FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def rows(concepts, descriptions, preferred_ids) -> list[snomed.SnomedConcept]:
    return snomed.assemble_concepts(concepts, descriptions, preferred_ids)


@pytest.mark.unit
class TestSctidValidation:
    @pytest.mark.parametrize("sctid", ["73211009", "386661006", "195967001"])
    def test_real_sctids_pass(self, sctid: str):
        assert snomed.validate_sctid(sctid)

    @pytest.mark.parametrize(
        "sctid",
        [
            "73211008",  # wrong check digit
            "12x",  # non-digit
            "12345",  # too short
            "073211009",  # leading zero
        ],
    )
    def test_bad_sctids_fail(self, sctid: str):
        assert not snomed.validate_sctid(sctid)


@pytest.mark.unit
class TestSemanticTag:
    @pytest.mark.parametrize(
        "fsn,expected",
        [
            ("Diabetes mellitus (disorder)", "disorder"),
            ("Appendectomy (procedure)", "procedure"),
            ("Fever (finding)", "finding"),
            ("No tag", ""),
        ],
    )
    def test_parse(self, fsn: str, expected: str):
        assert snomed.parse_semantic_tag(fsn) == expected


@pytest.mark.unit
class TestParseAndAssemble:
    def test_counts(self, concepts, descriptions, preferred_ids):
        assert len(concepts) == 3
        assert len(descriptions) == 5
        assert preferred_ids == {"102", "202"}

    def test_active_disorder(self, rows):
        d = next(r for r in rows if r.concept_id == "73211009")
        assert d.fsn == "Diabetes mellitus (disorder)"
        assert d.preferred_term == "Diabetes mellitus"
        assert d.semantic_tag == "disorder"
        assert d.active is True
        assert d.module_id == "731000124108"

    def test_active_finding(self, rows):
        f = next(r for r in rows if r.concept_id == "386661006")
        assert f.semantic_tag == "finding"
        assert f.preferred_term == "Fever"

    def test_inactive_carried(self, rows):
        i = next(r for r in rows if r.concept_id == "195967001")
        assert i.active is False
        assert i.semantic_tag == "disorder"  # still parsed from its (active) FSN


@pytest.mark.unit
class TestDq:
    def test_clean_fixture_passes_blocking(self, rows, concepts, descriptions, preferred_ids):
        assert snomed.find_duplicate_concept_ids(rows) == []
        assert snomed.find_invalid_sctids(rows) == []
        assert snomed.find_active_missing_fsn(rows) == []
        assert snomed.find_active_fsn_count_anomalies(concepts, descriptions) == []
        assert (
            snomed.find_active_preferred_count_anomalies(concepts, descriptions, preferred_ids)
            == []
        )

    def test_inactive_count(self, rows):
        assert snomed.inactive_count(rows) == 1

    def test_semantic_tag_distribution_is_active_only(self, rows):
        # The inactive disorder is excluded from the distribution.
        assert snomed.semantic_tag_distribution(rows) == {"disorder": 1, "finding": 1}

    def test_invalid_sctid_flagged(self):
        bad = [snomed.SnomedConcept("73211008", "X (disorder)", "X", "disorder", True, "m", "t")]
        assert snomed.find_invalid_sctids(bad) == ["73211008"]

    def test_active_missing_fsn_flagged(self):
        rows = [snomed.SnomedConcept("73211009", "", "", "", True, "m", "t")]
        assert snomed.find_active_missing_fsn(rows) == ["73211009"]

    def test_active_fsn_count_anomaly_flagged(self):
        # An active concept with two active FSN descriptions is an anomaly.
        concepts = [snomed.SnomedConceptRow("73211009", True, "m", "t", "d")]
        descs = [
            snomed.SnomedDescription(
                "1", True, "73211009", snomed.FSN_TYPE_ID, "A (disorder)", "en"
            ),
            snomed.SnomedDescription(
                "2", True, "73211009", snomed.FSN_TYPE_ID, "B (disorder)", "en"
            ),
        ]
        assert snomed.find_active_fsn_count_anomalies(concepts, descs) == ["73211009"]
