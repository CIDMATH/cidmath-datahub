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
