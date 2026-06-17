"""Unit tests for `cidmath_datahub.reference.rxnorm` (RXNCONSO -> concepts; ADR 0014).

Anchored on real-shaped ``RXNCONSO.RRF`` lines in ``tests/fixtures/``: the ingredient
``161`` Acetaminophen (``IN``) with a ``SY`` synonym that must NOT win, a clinical drug
(``SCD``), a branded drug (``SBD``), plus a ``SUPPRESS=O`` line and a non-RXNORM atom that
must both be filtered out. The representative lines should be re-verified against a real
release during the dev run (the UMLS download/unzip is entrypoint glue).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cidmath_datahub.reference import rxnorm

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
CONSO_FIXTURE = _FIXTURES / "rxnorm_rxnconso_sample.rrf"


@pytest.fixture(scope="module")
def atoms() -> list[rxnorm.RxnormAtom]:
    return rxnorm.parse_rxnconso(CONSO_FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def concepts(atoms) -> list[rxnorm.RxnormConcept]:
    return rxnorm.reduce_to_concepts(atoms)


@pytest.mark.unit
class TestParseAndFilter:
    def test_filters_suppressed_and_non_rxnorm(self, atoms):
        # 6 source lines -> keep 4 (IN, SY, SCD, SBD); drop SUPPRESS=O and SAB=SNOMEDCT_US.
        assert len(atoms) == 4
        assert all(a.rxcui != "999999" for a in atoms)  # the SNOMED atom is gone

    def test_atom_fields(self, atoms):
        a = next(a for a in atoms if a.tty == "IN")
        assert a.rxcui == "161" and a.name == "Acetaminophen" and a.is_pref


@pytest.mark.unit
class TestReduce:
    def test_one_row_per_rxcui(self, concepts):
        assert len(concepts) == 3
        assert sorted(c.rxcui for c in concepts) == ["1049221", "161", "209387"]

    def test_ingredient_picks_concept_atom_not_synonym(self, concepts):
        # 161 has IN ("Acetaminophen") + SY ("APAP"); the concept atom must win.
        c = next(c for c in concepts if c.rxcui == "161")
        assert c.tty == "IN"
        assert c.name == "Acetaminophen"

    def test_scd_and_sbd(self, concepts):
        by = {c.rxcui: c for c in concepts}
        assert by["1049221"].tty == "SCD"
        assert by["209387"].tty == "SBD"
        assert by["209387"].name == "Acetaminophen 325 MG Oral Tablet [Tylenol]"


@pytest.mark.unit
class TestDq:
    def test_clean_fixture_passes_blocking(self, concepts):
        assert rxnorm.find_duplicate_rxcui(concepts) == []
        assert rxnorm.find_missing_fields(concepts) == []
        assert rxnorm.find_bad_tty(concepts) == []

    def test_tty_distribution(self, concepts):
        assert rxnorm.tty_distribution(concepts) == {"IN": 1, "SCD": 1, "SBD": 1}

    def test_bad_tty_flagged(self):
        bad = [rxnorm.RxnormConcept("1", "Foo", "ZZZ")]
        assert rxnorm.find_bad_tty(bad) == [("1", "ZZZ")]

    def test_missing_fields_flagged(self):
        bad = [rxnorm.RxnormConcept("161", "", "IN")]
        assert rxnorm.find_missing_fields(bad) == [("161", "name")]

    def test_all_concept_ttys_are_in_vocab(self, concepts):
        assert all(c.tty in rxnorm.RXNORM_TTY_VALUES for c in concepts)
