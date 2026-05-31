"""Tests for cidmath_datahub.weather.nclimgrid (ADR 0025) — raw nClimGrid parse.

Synthetic rows mirror the real file shape (verified against NOAA 2026 files):
headerless, 37 fields = region_type, region_code, region_name, year, month,
VARIABLE, then 31 daily values; ``-999.99`` is the missing/padding sentinel.
"""

from __future__ import annotations

from datetime import date

import pytest

from cidmath_datahub.weather import nclimgrid as ncl


def _row(region_type, code, name, year, month, variable, daily):
    assert len(daily) == 31, "nClimGrid rows always carry 31 day columns"
    return ",".join([region_type, code, name, str(year), f"{month:02d}", variable, *daily])


@pytest.mark.unit
class TestParseAverageFilename:
    @pytest.mark.parametrize(
        "name,expected",
        [
            (
                "tavg-202401-cty-scaled.csv",
                {
                    "variable": "tavg",
                    "year": 2024,
                    "month": 1,
                    "region_type": "cty",
                    "status": "scaled",
                },
            ),
            (
                "prcp-202602-ste-prelim.csv",
                {
                    "variable": "prcp",
                    "year": 2026,
                    "month": 2,
                    "region_type": "ste",
                    "status": "prelim",
                },
            ),
        ],
    )
    def test_valid(self, name, expected):
        assert ncl.parse_average_filename(name) == expected

    @pytest.mark.parametrize(
        "bad",
        [
            "ncdd-202601-version.txt",
            "garbage.csv",
            "tavg-2024-cty-scaled.csv",
            "",
            None,
            "tavg-202401-cty-final.csv",
        ],
    )
    def test_invalid_returns_none(self, bad):
        assert ncl.parse_average_filename(bad) is None


@pytest.mark.unit
class TestParseAverageCsv:
    def test_state_january_full_month(self):
        daily = ["0.00"] * 31
        daily[2] = "10.22"
        rows = ncl.parse_average_csv(
            [_row("ste", "01", "Alabama", 2026, 1, "PRCP", daily)], source_file="f.csv"
        )
        assert len(rows) == 31
        assert rows[2]["value"] == 10.22
        assert rows[2]["obs_date"] == date(2026, 1, 3)
        assert rows[0]["region_code"] == "01"
        assert rows[0]["region_name"] == "Alabama"
        assert rows[0]["variable"] == "prcp"  # lower-cased to the controlled set

    def test_february_padding_dropped(self):
        daily = ["1.0"] * 28 + ["-999.99", "-999.99", "-999.99"]
        rows = ncl.parse_average_csv(
            [_row("cty", "01001", "AL: Autauga", 2026, 2, "PRCP", daily)], source_file="f.csv"
        )
        assert len(rows) == 28
        assert max(r["obs_date"].day for r in rows) == 28
        assert all(r["value"] == 1.0 for r in rows)

    def test_sentinel_on_real_day_is_none(self):
        daily = ["-999.99"] + ["0.0"] * 30
        rows = ncl.parse_average_csv(
            [_row("ste", "02", "Arizona", 2026, 1, "TAVG", daily)], source_file="f.csv"
        )
        assert rows[0]["value"] is None
        assert rows[0]["obs_date"] == date(2026, 1, 1)

    def test_space_padded_values_parse(self):
        daily = ["     0.00"] * 31
        daily[0] = "    44.58"
        rows = ncl.parse_average_csv(
            [_row("ste", "01", "Alabama", 2026, 1, "PRCP", daily)], source_file="f.csv"
        )
        assert rows[0]["value"] == 44.58

    def test_wrong_field_count_raises(self):
        with pytest.raises(ValueError, match="expected 37"):
            ncl.parse_average_csv(["ste,01,Alabama,2026,01,PRCP,0.0"], source_file="f.csv")

    def test_blank_lines_skipped(self):
        daily = ["0.0"] * 31
        rows = ncl.parse_average_csv(
            ["", _row("ste", "01", "Alabama", 2026, 1, "PRCP", daily), "  "], source_file="f.csv"
        )
        assert len(rows) == 31


@pytest.mark.unit
class TestExtractCsvLinks:
    def test_extracts_csv_basenames_only(self):
        html = (
            '<a href="/data/.../">Parent Directory</a>\n'
            '<a href="ncdd-202601-version.txt">ncdd-202601-version.txt</a>\n'
            '<a href="prcp-202601-cty-scaled.csv">prcp-202601-cty-scaled.csv</a>\n'
            '<a href="tavg-202601-ste-scaled.csv">tavg-202601-ste-scaled.csv</a>'
        )
        assert ncl.extract_csv_links(html) == [
            "prcp-202601-cty-scaled.csv",
            "tavg-202601-ste-scaled.csv",
        ]

    def test_dedup_preserves_order(self):
        html = '<a href="a.csv">a</a><a href="b.csv">b</a><a href="a.csv">a2</a>'
        assert ncl.extract_csv_links(html) == ["a.csv", "b.csv"]

    def test_non_string_returns_empty(self):
        assert ncl.extract_csv_links(None) == []


@pytest.mark.unit
class TestParseNceiFipsCrosswalk:
    _CSV = [
        "state_name,postal_code,NCEI_code,FIPS_code",
        "Alabama,AL,01,01",
        "Arizona,AZ,02,04",
        "Indiana,IL,11,17",  # upstream name bug; numeric cols are correct (IL=11->17)
        "Illinois,IN,12,18",
        "Wyoming,WY,48,56",
    ]

    def test_maps_numeric_codes(self):
        m = ncl.parse_ncei_fips_crosswalk(self._CSV)
        assert m["01"] == "01"
        assert m["02"] == "04"
        assert m["48"] == "56"

    def test_keyed_on_numeric_not_swapped_name(self):
        # Despite the swapped state_name column, the numeric mapping is right.
        m = ncl.parse_ncei_fips_crosswalk(self._CSV)
        assert m["11"] == "17"
        assert m["12"] == "18"

    def test_zero_padded(self):
        m = ncl.parse_ncei_fips_crosswalk(["state_name,postal_code,NCEI_code,FIPS_code", "X,X,1,1"])
        assert m == {"01": "01"}


@pytest.mark.unit
class TestConformRegion:
    M = {"01": "01", "02": "04", "06": "09", "44": "51"}

    def test_state(self):
        assert ncl.conform_region("ste", "02", self.M) == "04"

    def test_county_arizona(self):
        assert ncl.conform_region("cty", "02001", self.M) == "04001"

    def test_county_alabama_coincides(self):
        assert ncl.conform_region("cty", "01001", self.M) == "01001"

    def test_county_va_independent_city(self):
        assert ncl.conform_region("cty", "44510", self.M) == "51510"

    def test_unknown_ncei_state_returns_none(self):
        assert ncl.conform_region("cty", "99001", self.M) is None

    def test_bad_county_suffix_returns_none(self):
        assert ncl.conform_region("cty", "0200X", self.M) is None

    def test_dc_filed_under_maryland_override(self):
        # NCEI files DC under Maryland (state 18) as county 511; real geoid 11001.
        # Override fires before the state cross-reference (empty map still maps it).
        assert ncl.conform_region("cty", "18511", {}) == "11001"

    def test_unknown_region_type_returns_none(self):
        assert ncl.conform_region("div", "02001", self.M) is None


@pytest.mark.unit
class TestConstants:
    def test_region_types_and_variables(self):
        assert ncl.REGION_TYPES == frozenset({"cty", "ste"})
        assert ncl.VARIABLES == frozenset({"prcp", "tavg", "tmax", "tmin"})
        assert ncl.SENTINEL == -999.99
