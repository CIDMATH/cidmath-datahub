"""Unit tests for ``cidmath_datahub.reference.ruca``.

Anchored against real USDA ERS RUCA data and the published code definitions:

  - ZIP rows are **verbatim** from the 2020 RUCA ZIP-code CSV (header
    ``ZIPCode,State,ZIPCodeType,POName,PrimaryRUCA,SecondaryRUCA``): ``00001`` (N Dillingham,
    AK) primary 10 / secondary written as ``10``; ``00006`` primary 10 / secondary ``10.1`` --
    exercising the bare-integer secondary the ZIP file uses for ``X.0``.
  - Tract rows are constructed with the ERS 2020 census-tract column names (the parser resolves
    columns by alias, so the exact header spelling/year suffix is not load-bearing); they
    exercise GEOID derivation, code normalization, and population/density disambiguation rather
    than asserting any specific tract's true RUCA code.
  - Code sets are the documented Primary (1-10, 99) and Secondary (1.0 .. 10.3, 99) values.

Source: https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/documentation
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import ruca

# --- real ZIP rows (verbatim from the 2020 RUCA ZIP-code CSV) ----------------
ZIP_HEADER = ["ZIPCode", "State", "ZIPCodeType", "POName", "PrimaryRUCA", "SecondaryRUCA"]
ZIP_ROWS = [
    {
        "ZIPCode": "00001",
        "State": "AK",
        "ZIPCodeType": "ZIP Code Area",
        "POName": "N Dillingham",
        "PrimaryRUCA": "10",
        "SecondaryRUCA": "10",
    },
    {
        "ZIPCode": "00006",
        "State": "AK",
        "ZIPCodeType": "ZIP Code Area",
        "POName": "Matanuska-Sustina Borough",
        "PrimaryRUCA": "10",
        "SecondaryRUCA": "10.1",
    },
]

# --- constructed tract rows using the ERS 2020 census-tract column names ------
TRACT_HEADER = [
    "State-County FIPS Code",
    "Select State",
    "Select County",
    "State-County-Tract FIPS Code",
    "Primary RUCA Code 2020",
    "Secondary RUCA Code 2020",
    "Tract Population 2020",
    "Land Area (square miles) 2020",
    "Population Density (per square mile) 2020",
]


def _tract_row(geoid, state, county, primary, secondary, pop, land, density):
    return {
        "State-County FIPS Code": geoid[:5],
        "Select State": state,
        "Select County": county,
        "State-County-Tract FIPS Code": geoid,
        "Primary RUCA Code 2020": primary,
        "Secondary RUCA Code 2020": secondary,
        "Tract Population 2020": pop,
        "Land Area (square miles) 2020": land,
        "Population Density (per square mile) 2020": density,
    }


TRACT_ROWS = [
    _tract_row("13121001100", "GA", "Fulton", "1", "1.0", "3,000", "0.45", "6666.7"),
    _tract_row("13121980000", "GA", "Fulton", "10", "10.0", "850", "120.5", "7.1"),
    # A "99" not-coded tract: water / zero population and zero land area.
    _tract_row("02016000100", "AK", "Aleutians West", "99", "99", "0", "0", ""),
]


@pytest.mark.unit
class TestNormalizePrimaryCode:
    @pytest.mark.parametrize("raw,expected", [("1", 1), ("10", 10), (1.0, 1), (99, 99), (" 7 ", 7)])
    def test_normalization(self, raw, expected):
        assert ruca.normalize_primary_code(raw) == expected

    def test_blank_raises(self):
        with pytest.raises(ValueError):
            ruca.normalize_primary_code("  ")


@pytest.mark.unit
class TestNormalizeSecondaryCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("10", "10.0"),  # the ZIP file writes 10 for 10.0
            ("1", "1.0"),
            (1.1, "1.1"),
            ("10.3", "10.3"),
            (" 7.2 ", "7.2"),
            ("99", "99"),  # special code preserved
        ],
    )
    def test_normalization(self, raw, expected):
        assert ruca.normalize_secondary_code(raw) == expected


@pytest.mark.unit
class TestValidateCodes:
    @pytest.mark.parametrize("code", [1, 5, 10, 99])
    def test_valid_primary(self, code):
        assert ruca.validate_primary_code(code) is True

    @pytest.mark.parametrize("code", [0, 11, 100, -1])
    def test_invalid_primary(self, code):
        assert ruca.validate_primary_code(code) is False

    @pytest.mark.parametrize("code", ["1.0", "1.1", "7.2", "10.3", "99"])
    def test_valid_secondary(self, code):
        assert ruca.validate_secondary_code(code) is True

    @pytest.mark.parametrize("code", ["10.9", "3.1", "11.0", "1", "10"])
    def test_invalid_secondary(self, code):
        # "1"/"10" are pre-normalization forms; validate expects canonical dotted input.
        assert ruca.validate_secondary_code(code) is False

    def test_code_set_sizes(self):
        # 10 primary classes + 99; 21 published secondary codes + 99.
        assert len(ruca.PRIMARY_RUCA_CODES) == 11
        assert len(ruca.SECONDARY_RUCA_CODES) == 22


@pytest.mark.unit
class TestNormalizeIdentifiers:
    def test_tract_geoid_zero_pads_to_11(self):
        assert ruca.normalize_tract_geoid("2016000100") == "02016000100"

    def test_tract_geoid_keeps_leading_zero(self):
        assert ruca.normalize_tract_geoid("02016000100") == "02016000100"

    def test_zip_zero_pads_to_5(self):
        assert ruca.normalize_zip_code(601) == "00601"

    def test_overlong_id_raises(self):
        with pytest.raises(ValueError):
            ruca.normalize_zip_code("123456")


@pytest.mark.unit
class TestResolveColumn:
    def test_compact_zip_headers(self):
        zc = ruca.resolve_column(ZIP_HEADER, equals=("zipcode",), contains=("zipcode",))
        assert zc == "ZIPCode"
        assert (
            ruca.resolve_column(ZIP_HEADER, equals=("primaryruca",), contains=("primaryruca",))
            == "PrimaryRUCA"
        )

    def test_year_suffixed_tract_headers(self):
        # The tract id is the header containing "tract"; the 5-digit county FIPS column does not.
        geoid = ruca.resolve_column(TRACT_HEADER, equals=(), contains=("tract",))
        assert geoid == "State-County-Tract FIPS Code"

    def test_population_vs_density_disambiguation(self):
        cols = ruca._resolve_population_columns(TRACT_HEADER)
        assert cols["population"] == "Tract Population 2020"
        assert cols["population_density"] == "Population Density (per square mile) 2020"
        assert cols["land_area_sqmi"] == "Land Area (square miles) 2020"

    def test_missing_column_returns_none(self):
        assert ruca.resolve_column(["A", "B"], equals=("zipcode",), contains=("zipcode",)) is None


@pytest.mark.unit
class TestParseZipRows:
    def test_parses_real_rows(self):
        recs = ruca.parse_zip_rows(ZIP_ROWS, 2020)
        assert len(recs) == 2
        first = recs[0]
        assert first.zip_code == "00001"
        assert first.vintage == 2020
        assert first.state == "AK"
        assert first.zip_code_type == "ZIP Code Area"
        assert first.po_name == "N Dillingham"
        assert first.primary_ruca == 10
        assert first.secondary_ruca == "10.0"  # bare "10" normalized to canonical 10.0
        assert recs[1].secondary_ruca == "10.1"

    def test_blank_rows_skipped(self):
        rows = [*ZIP_ROWS, {k: "" for k in ZIP_HEADER}]
        assert len(ruca.parse_zip_rows(rows, 2020)) == 2

    def test_missing_required_column_raises(self):
        with pytest.raises(ValueError):
            ruca.parse_zip_rows([{"State": "AK", "PrimaryRUCA": "1", "SecondaryRUCA": "1.0"}], 2020)

    def test_na_zip_row_skipped(self):
        na = {
            "ZIPCode": "N/A",
            "State": "",
            "ZIPCodeType": "",
            "POName": "",
            "PrimaryRUCA": "",
            "SecondaryRUCA": "",
        }
        assert len(ruca.parse_zip_rows([*ZIP_ROWS, na], 2020)) == 2


@pytest.mark.unit
class TestParseTractRows:
    def test_derives_parents_and_normalizes(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS, 2020)
        assert len(recs) == 3
        metro = recs[0]
        assert metro.geoid == "13121001100"
        assert metro.state_geoid == "13"
        assert metro.county_geoid == "13121"
        assert metro.state == "GA"
        assert metro.county == "Fulton"
        assert metro.primary_ruca == 1
        assert metro.secondary_ruca == "1.0"
        assert metro.population == 3000  # thousands separator stripped
        assert metro.land_area_sqmi == pytest.approx(0.45)
        assert metro.population_density == pytest.approx(6666.7)

    def test_rural_and_water_tracts(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS, 2020)
        rural, water = recs[1], recs[2]
        assert rural.primary_ruca == 10 and rural.secondary_ruca == "10.0"
        assert water.primary_ruca == 99 and water.secondary_ruca == "99"
        assert water.population == 0
        assert water.population_density is None  # blank density -> None, not 0

    def test_leading_zero_geoid_preserved(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS, 2020)
        assert recs[2].geoid == "02016000100"
        assert recs[2].state_geoid == "02"

    def test_na_geoid_row_skipped(self):
        # ERS files carry footer / unassigned rows with a sentinel FIPS ("N/A"); skip, don't crash.
        rows = [*TRACT_ROWS, _tract_row("N/A", "", "", "", "", "", "", "")]
        recs = ruca.parse_tract_rows(rows, 2020)
        assert len(recs) == 3

    def test_malformed_geoid_kept_for_dq(self):
        # A non-sentinel but malformed GEOID is kept (not dropped) so blocking DQ flags it.
        rows = [_tract_row("13ABC", "GA", "X", "1", "1.0", "1", "1", "1")]
        recs = ruca.parse_tract_rows(rows, 2020)
        assert len(recs) == 1
        assert ruca.find_bad_tract_geoids(recs) == ["13ABC"]


@pytest.mark.unit
class TestDQHelpers:
    def test_duplicate_tract_keys(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS + [TRACT_ROWS[0]], 2020)
        dups = ruca.find_duplicate_tract_keys(recs)
        assert dups == [("13121001100", 2020)]

    def test_same_geoid_different_vintage_not_duplicate(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS, 2020) + ruca.parse_tract_rows(TRACT_ROWS, 2010)
        assert ruca.find_duplicate_tract_keys(recs) == []

    def test_duplicate_zip_keys(self):
        recs = ruca.parse_zip_rows(ZIP_ROWS + [ZIP_ROWS[0]], 2020)
        assert ruca.find_duplicate_zip_keys(recs) == [("00001", 2020)]

    def test_bad_tract_geoid_flagged(self):
        bad = ruca.RucaTractRecord(
            geoid="139",
            vintage=2020,
            state_geoid="13",
            county_geoid="139",
            state=None,
            county=None,
            primary_ruca=1,
            secondary_ruca="1.0",
            population=None,
            land_area_sqmi=None,
            population_density=None,
        )
        assert ruca.find_bad_tract_geoids([bad]) == ["139"]

    def test_invalid_primary_and_secondary_flagged(self):
        recs = ruca.parse_zip_rows(ZIP_ROWS, 2020)
        bad = ruca.RucaZipRecord(
            zip_code="99999",
            vintage=2020,
            state=None,
            zip_code_type=None,
            po_name=None,
            primary_ruca=42,
            secondary_ruca="9.9",
        )
        assert ruca.find_invalid_primary_codes([*recs, bad]) == [("99999", 42)]
        assert ruca.find_invalid_secondary_codes([*recs, bad]) == [("99999", "9.9")]

    def test_primary_distribution(self):
        recs = ruca.parse_tract_rows(TRACT_ROWS, 2020)
        assert ruca.primary_distribution(recs) == {1: 1, 10: 1, 99: 1}
