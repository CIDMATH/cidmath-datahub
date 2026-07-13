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
        # The un-suffixed aliases are the current (v3_x) vocabulary: 10 primary + 99; 21 secondary + 99.
        assert len(ruca.PRIMARY_RUCA_CODES) == 11
        assert len(ruca.SECONDARY_RUCA_CODES) == 22
        assert ruca.PRIMARY_RUCA_CODES == ruca.PRIMARY_RUCA_CODES_BY_VERSION["v3_x"]
        assert ruca.SECONDARY_RUCA_CODES == ruca.SECONDARY_RUCA_CODES_BY_VERSION["v3_x"]


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


# --- versioned code vocabulary (ADR 0038) ------------------------------------
@pytest.mark.unit
class TestVersionedVocabulary:
    def test_vintage_to_version_map(self):
        assert ruca.RUCA_VERSION_BY_VINTAGE == {
            1990: "v1_11", 2000: "v2_0", 2010: "v3_x", 2020: "v3_x"
        }

    @pytest.mark.parametrize(
        "vintage,version",
        [(1990, "v1_11"), (2000, "v2_0"), (2010, "v3_x"), (2020, "v3_x")],
    )
    def test_version_for_vintage(self, vintage, version):
        assert ruca.version_for_vintage(vintage) == version

    def test_version_for_unknown_vintage_defaults_to_current(self):
        assert ruca.version_for_vintage(1980) == ruca.DEFAULT_RUCA_VERSION == "v3_x"

    def test_primary_codes_identical_across_versions(self):
        # Every version has primary 1-10 + 99; only the terminology (descriptions) differs.
        expected = frozenset(range(1, 11)) | {99}
        for version in ruca.RUCA_VERSIONS:
            assert ruca.PRIMARY_RUCA_CODES_BY_VERSION[version] == expected

    def test_v1_11_secondary_deltas(self):
        # v1.11 uniquely has 2.2 and lacks 4.2 / 5.2 / 6.1 / 10.6.
        v1 = ruca.SECONDARY_RUCA_CODES_BY_VERSION["v1_11"]
        assert "2.2" in v1
        assert {"4.2", "5.2", "6.1", "10.6"}.isdisjoint(v1)

    def test_v2_0_secondary_deltas(self):
        # v2.0 carries the wider secondary set the modern v3_x product drops.
        v2 = ruca.SECONDARY_RUCA_CODES_BY_VERSION["v2_0"]
        assert {"4.2", "5.2", "6.1", "10.6"}.issubset(v2)
        assert "2.2" not in v2

    def test_v3_x_matches_current_alias(self):
        assert ruca.SECONDARY_RUCA_CODES_BY_VERSION["v3_x"] == ruca.SECONDARY_RUCA_CODES

    @pytest.mark.parametrize(
        "code,version,expected",
        [
            ("2.2", "v1_11", True),   # unique to v1.11
            ("2.2", "v2_0", False),
            ("2.2", "v3_x", False),
            ("10.6", "v2_0", True),   # present in v2.0
            ("10.6", "v1_11", False),
            ("10.6", "v3_x", False),
            ("4.2", "v2_0", True),
            ("4.2", "v3_x", False),
            ("10.3", "v3_x", True),   # modern set unchanged
        ],
    )
    def test_validate_secondary_is_version_aware(self, code, version, expected):
        assert ruca.validate_secondary_code(code, version) is expected

    def test_validate_secondary_defaults_to_current_version(self):
        # No version arg -> v3_x, so a v1.11-only code is rejected.
        assert ruca.validate_secondary_code("2.2") is False
        assert ruca.validate_secondary_code("10.3") is True

    def test_validate_primary_accepts_version_arg(self):
        assert ruca.validate_primary_code(2, "v1_11") is True
        assert ruca.validate_primary_code(99, "v2_0") is True


# --- version-aware batch DQ over parsed records ------------------------------
@pytest.mark.unit
class TestVersionAwareDQ:
    def test_v1_11_row_with_2_2_secondary_passes_at_1990(self):
        # A 1990 tract with the v1.11-only 2.2 secondary must validate against v1.11 (not flagged).
        rows = [_tract_row("53033010100", "WA", "King", "2", "2.2", "4,000", "1.2", "3333.3")]
        recs = ruca.parse_tract_rows(rows, 1990)
        assert recs[0].primary_ruca == 2
        assert recs[0].secondary_ruca == "2.2"
        assert ruca.find_invalid_secondary_codes(recs) == []
        assert ruca.find_invalid_primary_codes(recs) == []

    def test_same_2_2_secondary_flagged_at_2020(self):
        # The identical code at a modern vintage IS out of vocab (v3_x has no 2.2).
        rows = [_tract_row("53033010100", "WA", "King", "2", "2.2", "4,000", "1.2", "3333.3")]
        recs = ruca.parse_tract_rows(rows, 2020)
        assert ruca.find_invalid_secondary_codes(recs) == [("53033010100", "2.2")]

    def test_v2_0_row_with_10_6_secondary_passes_at_2000(self):
        # A 2000 tract with the v2.0 10.6 secondary must validate against v2.0 (not flagged).
        rows = [
            _tract_row("30001000100", "MT", "Beaverhead", "10", "10.6", "1,200", "500.0", "2.4"),
            _tract_row("30001000200", "MT", "Beaverhead", "4", "4.2", "8,000", "10.0", "800.0"),
        ]
        recs = ruca.parse_tract_rows(rows, 2000)
        assert {r.secondary_ruca for r in recs} == {"10.6", "4.2"}
        assert ruca.find_invalid_secondary_codes(recs) == []

    def test_v2_0_10_6_flagged_at_2020(self):
        rows = [_tract_row("30001000100", "MT", "Beaverhead", "10", "10.6", "1,200", "500.0", "2.4")]
        recs = ruca.parse_tract_rows(rows, 2020)
        assert ruca.find_invalid_secondary_codes(recs) == [("30001000100", "10.6")]

    def test_mixed_vintages_each_validated_against_own_version(self):
        # 1990 (2.2 ok) + 2020 (2.2 not ok) in one batch: only the 2020 row is flagged.
        recs = (
            ruca.parse_tract_rows(
                [_tract_row("53033010100", "WA", "King", "2", "2.2", "1", "1", "1")], 1990
            )
            + ruca.parse_tract_rows(
                [_tract_row("53033010100", "WA", "King", "2", "2.2", "1", "1", "1")], 2020
            )
        )
        assert ruca.find_invalid_secondary_codes(recs) == [("53033010100", "2.2")]


# --- code-definitions lookup projection --------------------------------------
@pytest.mark.unit
class TestCodeDefinitions:
    def test_covers_all_versions_and_levels(self):
        defs = ruca.code_definitions()
        versions = {d.ruca_version for d in defs}
        levels = {d.code_level for d in defs}
        assert versions == set(ruca.RUCA_VERSIONS)
        assert levels == {"primary", "secondary"}

    def test_row_count_matches_registries(self):
        expected = sum(
            len(ruca.PRIMARY_RUCA_DESCRIPTIONS_BY_VERSION[v])
            + len(ruca.SECONDARY_RUCA_DESCRIPTIONS_BY_VERSION[v])
            for v in ruca.RUCA_VERSIONS
        )
        assert len(ruca.code_definitions()) == expected

    def test_pk_is_unique(self):
        keys = [(d.ruca_version, d.code_level, d.code) for d in ruca.code_definitions()]
        assert len(keys) == len(set(keys))

    def test_primary_codes_stored_as_plain_strings(self):
        defs = ruca.code_definitions()
        primary = {d.code for d in defs if d.code_level == "primary"}
        assert "1" in primary and "10" in primary and "99" in primary
        assert "1.0" not in primary  # primary codes are not dotted

    def test_version_specific_description_present(self):
        defs = {(d.ruca_version, d.code_level, d.code): d.description for d in ruca.code_definitions()}
        # v1.11's 2.2 has its distinct "combined flows" wording; absent from v3_x.
        assert "combined flows" in defs[("v1_11", "secondary", "2.2")].lower()
        assert ("v3_x", "secondary", "2.2") not in defs
        # Terminology differs at the primary level across versions.
        assert "urban core" in defs[("v1_11", "primary", "1")].lower()
        assert "metropolitan" in defs[("v3_x", "primary", "1")].lower()
