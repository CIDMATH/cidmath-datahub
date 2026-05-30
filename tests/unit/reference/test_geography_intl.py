"""Unit tests for ``cidmath_datahub.reference.geography_intl``.

Anchors:
  - ISO 3166-1 alpha-2 / alpha-3 are exactly 2 / 3 letters, case-insensitive
    on input, upper-case on output.
  - Numeric codes preserve leading zeros (Afghanistan = ``"004"``).
  - GADM X-prefixed GID_0 values are explicitly non-ISO and excluded.
  - WHO region vocabulary is AFR/AMR/EMR/EUR/SEAR/WPR (unsuffixed).
  - assemble_country_row enforces centroid lon/lat presence as a pair.
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import geography_intl as gi


@pytest.mark.unit
class TestNormalizeAlpha3:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("USA", "USA"),
            ("usa", "USA"),
            (" Bra ", "BRA"),
            ("JPN", "JPN"),
        ],
    )
    def test_valid(self, given, expected):
        assert gi.normalize_alpha3(given) == expected

    @pytest.mark.parametrize("bad", ["US", "USAA", "US1", "", "12 ", 840])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            gi.normalize_alpha3(bad)


@pytest.mark.unit
class TestNormalizeAlpha2:
    def test_valid(self):
        assert gi.normalize_alpha2("us") == "US"
        assert gi.normalize_alpha2("BR") == "BR"

    @pytest.mark.parametrize("bad", ["U", "USA", "U1", "", 8])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            gi.normalize_alpha2(bad)


@pytest.mark.unit
class TestNormalizeNumeric:
    @pytest.mark.parametrize(
        "given,expected",
        [
            (4, "004"),  # Afghanistan
            ("4", "004"),
            ("004", "004"),
            (840, "840"),  # United States
            ("840", "840"),
            (76, "076"),  # Brazil
        ],
    )
    def test_preserves_leading_zeros(self, given, expected):
        assert gi.normalize_numeric(given) == expected

    @pytest.mark.parametrize("bad", ["abc", "1234", "-1", "", None, 1.5])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            gi.normalize_numeric(bad)


@pytest.mark.unit
class TestIsIsoGid0:
    @pytest.mark.parametrize("gid0", ["USA", "BRA", "JPN", "AFG", "ESH", "TWN", "PSE"])
    def test_real_iso_codes_pass(self, gid0):
        assert gi.is_iso_gid0(gid0) is True

    @pytest.mark.parametrize("gid0", ["XKO", "XNC", "XAD", "XCA", "XCL", "XPI", "XSP"])
    def test_gadm_coined_codes_rejected(self, gid0):
        assert gi.is_iso_gid0(gid0) is False

    @pytest.mark.parametrize("bad", ["", "US", "USAA", None, 123])
    def test_malformed_rejected(self, bad):
        assert gi.is_iso_gid0(bad) is False

    def test_any_x_prefix_rejected_even_if_not_in_known_set(self):
        # If GADM coins a new X-prefixed code, we still want to exclude it.
        assert gi.is_iso_gid0("XZZ") is False


@pytest.mark.unit
class TestAssembleCountryRow:
    @staticmethod
    def _base_args(**overrides):
        defaults = dict(
            alpha2="US",
            alpha3="USA",
            numeric="840",
            name="United States",
            official_name="United States of America",
            who_region="AMR",
            un_region="Americas",
            un_subregion="Northern America",
            is_un_member=True,
            is_sovereign=True,
            iso_3166_3_predecessor=None,
            centroid_geo_lon=-98.5,
            centroid_geo_lat=39.5,
            source_file="gadm_410-levels.gpkg",
        )
        defaults.update(overrides)
        return defaults

    def test_assembles_full_row(self):
        row = gi.assemble_country_row(**self._base_args())
        assert row["country_alpha3"] == "USA"
        assert row["country_alpha2"] == "US"
        assert row["country_numeric"] == "840"
        assert row["who_region"] == "AMR"
        assert row["un_region"] == "Americas"
        assert row["is_un_member"] is True
        assert row["centroid_geo_lon"] == -98.5

    def test_normalizes_lowercase_codes(self):
        row = gi.assemble_country_row(**self._base_args(alpha2="us", alpha3="usa"))
        assert row["country_alpha2"] == "US"
        assert row["country_alpha3"] == "USA"

    def test_pads_numeric(self):
        row = gi.assemble_country_row(**self._base_args(numeric=4, alpha3="AFG", alpha2="AF"))
        assert row["country_numeric"] == "004"

    def test_who_region_none_allowed(self):
        # Non-WHO-member territories (Taiwan, Vatican, etc.) get null who_region.
        row = gi.assemble_country_row(**self._base_args(who_region=None, alpha3="TWN", alpha2="TW"))
        assert row["who_region"] is None

    def test_bad_who_region_rejected(self):
        with pytest.raises(ValueError, match="who_region"):
            gi.assemble_country_row(**self._base_args(who_region="AFRO"))  # suffixed form

    def test_bad_un_region_rejected(self):
        with pytest.raises(ValueError, match="un_region"):
            gi.assemble_country_row(**self._base_args(un_region="North America"))

    def test_centroid_requires_both_or_neither(self):
        with pytest.raises(ValueError, match="centroid"):
            gi.assemble_country_row(
                **self._base_args(centroid_geo_lon=-98.5, centroid_geo_lat=None)
            )
        with pytest.raises(ValueError, match="centroid"):
            gi.assemble_country_row(**self._base_args(centroid_geo_lon=None, centroid_geo_lat=39.5))

    def test_centroid_both_none_allowed(self):
        # pycountry entries with no GADM match should still produce a row, with null centroid.
        row = gi.assemble_country_row(
            **self._base_args(centroid_geo_lon=None, centroid_geo_lat=None)
        )
        assert row["centroid_geo_lon"] is None
        assert row["centroid_geo_lat"] is None


@pytest.mark.unit
class TestParseSubdivisionCode:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("US-GA", ("US", "GA")),
            ("us-ga", ("US", "GA")),
            (" BR-SP ", ("BR", "SP")),
            ("CN-11", ("CN", "11")),  # ISO uses numeric locals for many CN/JP/RU subs
            ("JP-01", ("JP", "01")),
        ],
    )
    def test_valid(self, given, expected):
        assert gi.parse_subdivision_code(given) == expected

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "USGA",  # no dash
            "US-",  # empty local
            "-GA",  # empty alpha2
            "US-GAAA",  # local too long
            "US-G@",  # non-alnum local
            "U-GA",  # alpha2 wrong length
            "US-GA-X",  # two dashes
            123,  # not a string
        ],
    )
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            gi.parse_subdivision_code(bad)


@pytest.mark.unit
class TestAssembleSubdivisionRow:
    @staticmethod
    def _base_args(**overrides):
        defaults = dict(
            subdivision_code="US-GA",
            country_alpha2="US",
            country_alpha3="USA",
            subdivision_name="Georgia",
            subdivision_type_label="State",
            parent_subdivision_code=None,
            gadm_gid_1="USA.10_1",
            gadm_match_method="code",
            centroid_geo_lon=-83.0,
            centroid_geo_lat=32.7,
            source_file="gadm_410-levels.gpkg",
        )
        defaults.update(overrides)
        return defaults

    def test_assembles_full_row(self):
        row = gi.assemble_subdivision_row(**self._base_args())
        assert row["subdivision_code"] == "US-GA"
        assert row["country_alpha2"] == "US"
        assert row["country_alpha3"] == "USA"
        assert row["subdivision_local_code"] == "GA"
        assert row["subdivision_type_label"] == "State"
        assert row["parent_subdivision_code"] is None
        assert row["gadm_gid_1"] == "USA.10_1"
        assert row["gadm_match_method"] == "code"
        assert row["centroid_geo_lon"] == -83.0

    def test_invalid_match_method_rejected(self):
        with pytest.raises(ValueError, match="gadm_match_method"):
            gi.assemble_subdivision_row(**self._base_args(gadm_match_method="guessed"))

    def test_normalizes_lowercase(self):
        row = gi.assemble_subdivision_row(
            **self._base_args(subdivision_code="us-ga", country_alpha2="us", country_alpha3="usa")
        )
        assert row["subdivision_code"] == "US-GA"
        assert row["country_alpha2"] == "US"
        assert row["country_alpha3"] == "USA"
        assert row["subdivision_local_code"] == "GA"

    def test_country_prefix_must_match(self):
        with pytest.raises(ValueError, match="does not match prefix"):
            gi.assemble_subdivision_row(
                **self._base_args(subdivision_code="US-GA", country_alpha2="BR")
            )

    def test_centroid_requires_both_or_neither(self):
        with pytest.raises(ValueError, match="centroid"):
            gi.assemble_subdivision_row(
                **self._base_args(centroid_geo_lon=-83.0, centroid_geo_lat=None)
            )

    def test_centroid_both_none_allowed(self):
        row = gi.assemble_subdivision_row(
            **self._base_args(centroid_geo_lon=None, centroid_geo_lat=None)
        )
        assert row["centroid_geo_lon"] is None
        assert row["centroid_geo_lat"] is None

    def test_gadm_gid_1_nullable(self):
        # Subdivisions without an ADM_1 polygon (and nested subdivisions
        # whose match is on the parent's polygon) get null gadm_gid_1.
        row = gi.assemble_subdivision_row(**self._base_args(gadm_gid_1=None))
        assert row["gadm_gid_1"] is None

    def test_nested_parent_recorded(self):
        # AZ-BAB rolls up to AZ-NX in pycountry.
        row = gi.assemble_subdivision_row(
            **self._base_args(
                subdivision_code="AZ-BAB",
                country_alpha2="AZ",
                country_alpha3="AZE",
                subdivision_name="Babək",
                subdivision_type_label="Rayon",
                parent_subdivision_code="AZ-NX",
                gadm_gid_1=None,
            )
        )
        assert row["parent_subdivision_code"] == "AZ-NX"

    def test_nested_parent_country_mismatch_rejected(self):
        with pytest.raises(ValueError, match="parent_subdivision_code"):
            gi.assemble_subdivision_row(
                **self._base_args(
                    subdivision_code="AZ-BAB",
                    country_alpha2="AZ",
                    country_alpha3="AZE",
                    parent_subdivision_code="US-GA",
                )
            )


@pytest.mark.unit
class TestAssertGadmAdm1Columns:
    def test_all_present_passes(self):
        gi.assert_gadm_adm1_columns(
            ["GID_0", "GID_1", "NAME_1", "TYPE_1", "ENGTYPE_1", "HASC_1", "ISO_1", "geometry"]
        )

    def test_missing_columns_raises(self):
        with pytest.raises(ValueError, match="HASC_1"):
            gi.assert_gadm_adm1_columns(
                ["GID_0", "GID_1", "NAME_1", "TYPE_1", "ENGTYPE_1", "ISO_1"]
            )


@pytest.mark.unit
class TestHascToIsoSubdivision:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("US.GA", "US-GA"),
            ("BR.SP", "BR-SP"),
            (" jp.01 ", "JP-01"),
        ],
    )
    def test_valid(self, given, expected):
        assert gi.hasc_to_iso_subdivision(given) == expected

    @pytest.mark.parametrize("bad", [None, "", "US-GA", "US.GA.X", "U.GA", "US.GAAA", 123])
    def test_invalid_returns_none(self, bad):
        assert gi.hasc_to_iso_subdivision(bad) is None


@pytest.mark.unit
class TestNormalizeIso1:
    @pytest.mark.parametrize(
        "given,expected",
        [("US-GA", "US-GA"), (" us-ga ", "US-GA"), ("BR-SP", "BR-SP")],
    )
    def test_valid(self, given, expected):
        assert gi.normalize_iso_1(given) == expected

    @pytest.mark.parametrize("bad", [None, "", "USGA", "US.GA", 7])
    def test_invalid_returns_none(self, bad):
        assert gi.normalize_iso_1(bad) is None


@pytest.mark.unit
class TestMatchGadmAdm1:
    @staticmethod
    def _row(gid_0, gid_1, hasc_1=None, iso_1=None, name="X"):
        return {
            "GID_0": gid_0,
            "GID_1": gid_1,
            "NAME_1": name,
            "TYPE_1": "State",
            "ENGTYPE_1": "State",
            "HASC_1": hasc_1,
            "ISO_1": iso_1,
            "geometry": None,
        }

    def test_hasc_match_preferred(self):
        rows = [self._row("USA", "USA.10_1", hasc_1="US.GA", iso_1="US-GA")]
        lookup, unmatched = gi.match_gadm_adm1(rows)
        assert "US-GA" in lookup
        assert lookup["US-GA"]["GID_1"] == "USA.10_1"
        assert unmatched == []

    def test_iso_fallback_when_hasc_blank(self):
        rows = [self._row("BRA", "BRA.25_1", hasc_1=None, iso_1="BR-SP")]
        lookup, unmatched = gi.match_gadm_adm1(rows)
        assert "BR-SP" in lookup
        assert unmatched == []

    def test_unmatched_rows_reported(self):
        rows = [
            self._row("GBR", "GBR.1_1", hasc_1=None, iso_1=None, name="North East England"),
            self._row("USA", "USA.10_1", hasc_1="US.GA", iso_1="US-GA"),
        ]
        lookup, unmatched = gi.match_gadm_adm1(rows)
        assert "US-GA" in lookup
        assert unmatched == ["GBR.1_1"]

    def test_fixups_fill_in_unmatched(self):
        rows = [
            self._row("GBR", "GBR.1_1", hasc_1=None, iso_1=None, name="North East England"),
        ]
        lookup, unmatched = gi.match_gadm_adm1(rows, fixups={"GB-ENG": "GBR.1_1"})
        assert lookup["GB-ENG"]["GID_1"] == "GBR.1_1"
        assert unmatched == []

    def test_fixup_pointing_at_missing_gid_ignored(self):
        rows = [self._row("USA", "USA.10_1", hasc_1="US.GA")]
        lookup, unmatched = gi.match_gadm_adm1(rows, fixups={"XX-YY": "ZZZ.999_1"})
        assert "XX-YY" not in lookup
        assert "US-GA" in lookup

    def test_fixup_does_not_override_successful_match(self):
        # If HASC/ISO already produced a match, a fixup for the same code
        # should not silently displace it.
        rows = [
            self._row("USA", "USA.10_1", hasc_1="US.GA"),
            self._row("USA", "USA.99_1", hasc_1=None),
        ]
        lookup, _ = gi.match_gadm_adm1(rows, fixups={"US-GA": "USA.99_1"})
        assert lookup["US-GA"]["GID_1"] == "USA.10_1"

    def test_fixups_default_empty_dict(self):
        # GADM_ADM1_ISO_FIXUPS ships empty; match_gadm_adm1 should handle the
        # default-arg path cleanly.
        rows = [self._row("USA", "USA.10_1", hasc_1="US.GA")]
        lookup, unmatched = gi.match_gadm_adm1(rows)
        assert "US-GA" in lookup
        assert unmatched == []

    def test_module_constant_ships_empty(self):
        # Documented in the ADR / build: GADM_ADM1_ISO_FIXUPS starts empty
        # and is populated from observed DQ misses, not training-data priors.
        assert gi.GADM_ADM1_ISO_FIXUPS == {}


@pytest.mark.unit
class TestCheckJoinCoverage:
    def test_full_coverage(self):
        matched, total, missing = gi.check_join_coverage(
            ["USA", "BRA", "JPN"], {"USA", "BRA", "JPN"}
        )
        assert matched == 3
        assert total == 3
        assert missing == []

    def test_partial_coverage(self):
        matched, total, missing = gi.check_join_coverage(
            ["USA", "BRA", "VAT", "JPN"], {"USA", "BRA", "JPN"}
        )
        assert matched == 3
        assert total == 4
        assert missing == ["VAT"]

    def test_sample_truncated_to_ten(self):
        iso_list = [f"X{i:02d}" for i in range(15)]
        matched, total, missing = gi.check_join_coverage(iso_list, set())
        assert matched == 0
        assert total == 15
        assert len(missing) == 10
        # First ten in sorted order
        assert missing == sorted(iso_list)[:10]


@pytest.mark.unit
class TestNormalizeSubdivisionName:
    @pytest.mark.parametrize(
        "given,expected",
        [
            ("Georgia", "georgia"),
            ("  North  East  England ", "north east england"),
            ("Côte-d'Or", "cote d or"),
            ("Bavīria", "baviria"),
            ("Île-de-France", "ile de france"),
            ("São Paulo", "sao paulo"),
            ("Region 7", "region 7"),
        ],
    )
    def test_normalizes(self, given, expected):
        assert gi.normalize_subdivision_name(given) == expected

    @pytest.mark.parametrize("bad", [None, 123, "", "   ", "---", "．"])
    def test_empty_or_nonstring_returns_none(self, bad):
        assert gi.normalize_subdivision_name(bad) is None

    def test_accent_insensitive_equality(self):
        assert gi.normalize_subdivision_name("Vlaanderen") == gi.normalize_subdivision_name(
            "vlaanderen"
        )


@pytest.mark.unit
class TestBuildGadmNameIndex:
    @staticmethod
    def _row(gid_0, gid_1, name, varname=None):
        return {"GID_0": gid_0, "GID_1": gid_1, "NAME_1": name, "VARNAME_1": varname}

    def test_indexes_by_country_and_normalized_name(self):
        rows = [self._row("USA", "USA.10_1", "Georgia")]
        index, _ = gi.build_gadm_name_index(rows)
        assert index["USA"]["georgia"]["GID_1"] == "USA.10_1"

    def test_varname_alternates_indexed(self):
        rows = [self._row("BEL", "BEL.1_1", "Flanders", varname="Vlaanderen|Flandre")]
        index, _ = gi.build_gadm_name_index(rows)
        assert "flanders" in index["BEL"]
        assert "vlaanderen" in index["BEL"]
        assert "flandre" in index["BEL"]

    def test_same_name_different_countries_not_collapsed(self):
        rows = [
            self._row("USA", "USA.10_1", "Georgia"),
            self._row("GEO", "GEO.1_1", "Georgia"),  # the country, but illustrates scoping
        ]
        index, _ = gi.build_gadm_name_index(rows)
        assert index["USA"]["georgia"]["GID_1"] == "USA.10_1"
        assert index["GEO"]["georgia"]["GID_1"] == "GEO.1_1"

    def test_missing_gid0_skipped(self):
        rows = [self._row(None, "X.1_1", "Nowhere")]
        index, collisions = gi.build_gadm_name_index(rows)
        assert index == {}
        assert collisions == {}


@pytest.mark.unit
class TestResolveSubdivisionPolygons:
    @staticmethod
    def _gadm(gid_0, gid_1, name="X", hasc_1=None, iso_1=None, varname=None):
        return {
            "GID_0": gid_0,
            "GID_1": gid_1,
            "NAME_1": name,
            "TYPE_1": "State",
            "ENGTYPE_1": "State",
            "HASC_1": hasc_1,
            "ISO_1": iso_1,
            "VARNAME_1": varname,
            "geometry": None,
        }

    @staticmethod
    def _target(code, alpha3, name):
        return {"subdivision_code": code, "country_alpha3": alpha3, "name": name}

    def test_exact_code_path_wins(self):
        gadm_rows = [self._gadm("USA", "USA.10_1", name="Georgia", hasc_1="US.GA")]
        targets = [self._target("US-GA", "USA", "Georgia")]
        resolved, _, unmatched = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert resolved["US-GA"]["GID_1"] == "USA.10_1"
        assert unmatched == []

    def test_name_path_recovers_code_blank_country(self):
        # Afghanistan-style: HASC/ISO blank, but NAME_1 aligns with ISO name.
        gadm_rows = [self._gadm("AFG", "AFG.1_1", name="Balkh")]
        targets = [self._target("AF-BAL", "AFG", "Balkh")]
        resolved, _, unmatched = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert resolved["AF-BAL"]["GID_1"] == "AFG.1_1"
        assert unmatched == []

    def test_name_match_is_accent_insensitive(self):
        gadm_rows = [self._gadm("BRA", "BRA.25_1", name="São Paulo")]
        targets = [self._target("BR-SP", "BRA", "Sao Paulo")]
        resolved, _, _ = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert resolved["BR-SP"]["GID_1"] == "BRA.25_1"

    def test_name_match_scoped_within_country(self):
        # A same-named subdivision in another country must not match.
        gadm_rows = [self._gadm("USA", "USA.10_1", name="Georgia")]
        targets = [self._target("XX-GA", "XXX", "Georgia")]
        resolved, _, unmatched = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert "XX-GA" not in resolved
        assert unmatched == ["USA.10_1"]

    def test_fixup_is_last_resort(self):
        gadm_rows = [self._gadm("GBR", "GBR.1_1", name="North East")]
        targets = [self._target("GB-ENG", "GBR", "England")]  # no code, no name match
        resolved, methods, _ = gi.resolve_subdivision_polygons(
            gadm_rows, targets, fixups={"GB-ENG": "GBR.1_1"}
        )
        assert resolved["GB-ENG"]["GID_1"] == "GBR.1_1"
        assert methods["GB-ENG"] == "fixup"

    def test_unmatched_gids_reported_sorted(self):
        gadm_rows = [
            self._gadm("SVN", "SVN.2_1", name="Gorenjska"),
            self._gadm("SVN", "SVN.1_1", name="Pomurska"),
        ]
        # ISO grain mismatch: municipality codes that match neither region.
        targets = [self._target("SI-001", "SVN", "Ajdovščina")]
        resolved, _, unmatched = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert resolved == {}
        assert unmatched == ["SVN.1_1", "SVN.2_1"]

    def test_priority_code_over_name(self):
        # If both a code row and a name row exist, the exact-code row wins.
        gadm_rows = [
            self._gadm("USA", "USA.10_1", name="Georgia", hasc_1="US.GA"),
            self._gadm("USA", "USA.99_1", name="Georgia"),
        ]
        targets = [self._target("US-GA", "USA", "Georgia")]
        resolved, _, _ = gi.resolve_subdivision_polygons(gadm_rows, targets)
        assert resolved["US-GA"]["GID_1"] == "USA.10_1"

    def test_method_code_reported(self):
        gadm_rows = [self._gadm("USA", "USA.10_1", name="Georgia", hasc_1="US.GA")]
        _, methods, _ = gi.resolve_subdivision_polygons(
            gadm_rows, [self._target("US-GA", "USA", "Georgia")]
        )
        assert methods["US-GA"] == "code"

    def test_method_name_reported(self):
        gadm_rows = [self._gadm("AFG", "AFG.1_1", name="Balkh")]
        _, methods, _ = gi.resolve_subdivision_polygons(
            gadm_rows, [self._target("AF-BAL", "AFG", "Balkh")]
        )
        assert methods["AF-BAL"] == "name"

    def test_method_name_ambiguous_when_collision(self):
        # Two distinct GADM polygons in the same country normalize to the same
        # name -> flagged ambiguous, method recorded, but NOT linked (review
        # decision): no polygon shipped, both GADM rows stay unmatched.
        gadm_rows = [
            self._gadm("XYZ", "XYZ.1_1", name="Central"),
            self._gadm("XYZ", "XYZ.2_1", name="Central"),
        ]
        resolved, methods, unmatched = gi.resolve_subdivision_polygons(
            gadm_rows, [self._target("XY-C", "XYZ", "Central")]
        )
        assert methods["XY-C"] == "name_ambiguous"
        assert "XY-C" not in resolved
        assert unmatched == ["XYZ.1_1", "XYZ.2_1"]

    def test_collision_surfaced_by_name_index(self):
        rows = [
            {"GID_0": "XYZ", "GID_1": "XYZ.1_1", "NAME_1": "Central", "VARNAME_1": None},
            {"GID_0": "XYZ", "GID_1": "XYZ.2_1", "NAME_1": "Central", "VARNAME_1": None},
        ]
        _, collisions = gi.build_gadm_name_index(rows)
        assert "central" in collisions["XYZ"]


@pytest.mark.unit
class TestAssertGadmAdm2Columns:
    def test_all_present_passes(self):
        gi.assert_gadm_adm2_columns(
            ["GID_0", "GID_1", "GID_2", "NAME_2", "TYPE_2", "ENGTYPE_2", "geometry"]
        )

    def test_missing_columns_raises(self):
        with pytest.raises(ValueError, match="GID_2"):
            gi.assert_gadm_adm2_columns(["GID_0", "GID_1", "NAME_2", "TYPE_2", "ENGTYPE_2"])


@pytest.mark.unit
class TestGadmLevelFromGid:
    @pytest.mark.parametrize(
        "gid,level",
        [
            ("USA", 0),
            ("USA.10_1", 1),
            ("USA.10.121_1", 2),
            ("USA.10.121.4_1", 3),
            (" BRA.25.300_1 ", 2),
        ],
    )
    def test_level(self, gid, level):
        assert gi.gadm_level_from_gid(gid) == level

    @pytest.mark.parametrize("bad", [None, "", "   ", 123])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError):
            gi.gadm_level_from_gid(bad)


@pytest.mark.unit
class TestBuildGid1ToSubdivisionCode:
    def test_maps_matched_rows(self):
        rows = [
            {"gadm_gid_1": "USA.10_1", "subdivision_code": "US-GA"},
            {"gadm_gid_1": "BRA.25_1", "subdivision_code": "BR-SP"},
        ]
        assert gi.build_gid1_to_subdivision_code(rows) == {
            "USA.10_1": "US-GA",
            "BRA.25_1": "BR-SP",
        }

    def test_skips_unmatched_rows(self):
        rows = [
            {"gadm_gid_1": None, "subdivision_code": "SI-001"},  # no polygon match
            {"gadm_gid_1": "USA.10_1", "subdivision_code": "US-GA"},
        ]
        assert gi.build_gid1_to_subdivision_code(rows) == {"USA.10_1": "US-GA"}

    def test_first_writer_wins_on_dup_gid1(self):
        rows = [
            {"gadm_gid_1": "USA.10_1", "subdivision_code": "US-GA"},
            {"gadm_gid_1": "USA.10_1", "subdivision_code": "US-XX"},
        ]
        assert gi.build_gid1_to_subdivision_code(rows) == {"USA.10_1": "US-GA"}


@pytest.mark.unit
class TestAssembleSubnationalRow:
    @staticmethod
    def _base_args(**overrides):
        defaults = dict(
            gadm_gid="USA.10.121_1",
            gadm_level=2,
            subnational_name="Fulton",
            subnational_type_label="County",
            parent_gid="USA.10_1",
            country_alpha3="USA",
            subdivision_code="US-GA",
            centroid_geo_lon=-84.4,
            centroid_geo_lat=33.8,
            source_file="gadm_410-levels.gpkg",
        )
        defaults.update(overrides)
        return defaults

    def test_assembles_full_row(self):
        row = gi.assemble_subnational_row(**self._base_args())
        assert row["gadm_gid"] == "USA.10.121_1"
        assert row["gadm_level"] == 2
        assert row["parent_gid"] == "USA.10_1"
        assert row["country_alpha3"] == "USA"
        assert row["subdivision_code"] == "US-GA"
        assert row["subnational_type_label"] == "County"

    def test_gid_suffix_preserved(self):
        # Stripping GADM's _N suffix breaks the polygon join (ADR 0022 guardrail).
        row = gi.assemble_subnational_row(**self._base_args())
        assert row["gadm_gid"].endswith("_1")

    def test_level_must_agree_with_gid(self):
        with pytest.raises(ValueError, match="disagrees with GID"):
            gi.assemble_subnational_row(**self._base_args(gadm_gid="USA.10.121_1", gadm_level=3))

    def test_level_out_of_range_rejected(self):
        with pytest.raises(ValueError, match="gadm_level"):
            gi.assemble_subnational_row(**self._base_args(gadm_gid="USA.10_1", gadm_level=1))

    def test_subdivision_code_optional(self):
        row = gi.assemble_subnational_row(**self._base_args(subdivision_code=None))
        assert row["subdivision_code"] is None

    def test_subdivision_code_normalized(self):
        row = gi.assemble_subnational_row(**self._base_args(subdivision_code="us-ga"))
        assert row["subdivision_code"] == "US-GA"

    def test_parent_gid_optional(self):
        row = gi.assemble_subnational_row(**self._base_args(parent_gid=None))
        assert row["parent_gid"] is None

    def test_centroid_requires_both_or_neither(self):
        with pytest.raises(ValueError, match="centroid"):
            gi.assemble_subnational_row(
                **self._base_args(centroid_geo_lon=-84.4, centroid_geo_lat=None)
            )

    def test_empty_gid_rejected(self):
        with pytest.raises(ValueError, match="gadm_gid"):
            gi.assemble_subnational_row(**self._base_args(gadm_gid="  "))
