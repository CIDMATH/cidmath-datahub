"""Unit tests for `cidmath_datahub.reference.geography`.

Anchors:
  - GEOIDs are zero-padded strings; leading zeros are significant.
  - A county GEOID's first two digits are its state GEOID (Fulton Co GA 13121 -> 13).
  - HHS region assignments are the fixed federal grouping (Georgia -> Region 4).
  - Crosswalk interpolation weights sum to ~1.0 per source unit.
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import geography as geo

# 50 states + DC, for the HHS coverage test.
_STATES_AND_DC = (
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN "
    "MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA "
    "WV WI WY"
).split()


@pytest.mark.unit
class TestNormalizeGeoid:
    @pytest.mark.parametrize(
        "value,width,expected",
        [
            (1, 2, "01"),
            ("1", 2, "01"),
            ("01", 2, "01"),
            (13, 2, "13"),
            (1001, 5, "01001"),
            ("01001", 5, "01001"),
            ("13121", 5, "13121"),
        ],
    )
    def test_zero_pads_and_preserves(self, value, width, expected):
        assert geo.normalize_geoid(value, width) == expected

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError):
            geo.normalize_geoid("1A", 2)

    def test_rejects_too_long(self):
        with pytest.raises(ValueError):
            geo.normalize_geoid("123", 2)

    def test_level_helpers(self):
        assert geo.validate_state_geoid(1) == "01"
        assert geo.validate_county_geoid(1001) == "01001"


@pytest.mark.unit
class TestStateGeoidOfCounty:
    @pytest.mark.parametrize(
        "county,state",
        [
            ("13121", "13"),  # Fulton County, GA
            ("01001", "01"),  # Autauga County, AL
            (1001, "01"),  # integer input, leading zero restored
            ("06037", "06"),  # Los Angeles County, CA
        ],
    )
    def test_derives_parent_state(self, county, state):
        assert geo.state_geoid_of_county(county) == state

    def test_rejects_bad_county(self):
        with pytest.raises(ValueError):
            geo.state_geoid_of_county("ABCDE")


@pytest.mark.unit
class TestGisjoinToGeoid:
    @pytest.mark.parametrize(
        "gisjoin,level,expected",
        [
            ("G080", "state", "08"),  # Colorado
            ("G360", "state", "36"),  # New York
            ("g130", "state", "13"),  # Georgia, lowercase
            ("G0800010", "county", "08001"),  # Adams County, CO
            ("G3600610", "county", "36061"),  # New York County, NY
            ("G1301210", "county", "13121"),  # Fulton County, GA
            ("G1800910040900", "tract", "18091040900"),  # tract 040900, Indiana
            ("G0100010020100", "tract", "01001020100"),  # tract 020100, Autauga Co AL
            ("G30307", "zcta", "30307"),  # Atlanta ZCTA
            ("g01001", "zcta", "01001"),  # leading-zero ZCTA, lowercase
            ("G18009100409001", "bg", "180910409001"),  # bg 1 in an Indiana tract (state 18)
        ],
    )
    def test_parses_known_gisjoins(self, gisjoin, level, expected):
        assert geo.gisjoin_to_geoid(gisjoin, level) == expected

    def test_rejects_missing_g_prefix(self):
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("0800010", "county")

    def test_rejects_wrong_length_for_level(self):
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G080", "county")
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G0800010", "state")
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G080", "tract")
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G080", "zcta")
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G080", "bg")

    def test_rejects_unknown_level(self):
        with pytest.raises(ValueError):
            geo.gisjoin_to_geoid("G080", "block")


@pytest.mark.unit
class TestTractParents:
    def test_parent_derivations(self):
        assert geo.state_geoid_of_tract("18091040900") == "18"
        assert geo.county_geoid_of_tract("18091040900") == "18091"

    def test_validate_widths(self):
        assert geo.validate_tract_geoid("18091040900") == "18091040900"
        assert geo.validate_zcta_geoid(1001) == "01001"

    def test_rejects_overlong_tract(self):
        with pytest.raises(ValueError):
            geo.validate_tract_geoid("180910409001")


@pytest.mark.unit
class TestHhsRegions:
    @pytest.mark.parametrize(
        "stusps,region",
        [
            ("GA", 4),
            ("ga", 4),  # case-insensitive
            ("CA", 9),
            ("NY", 2),
            ("CT", 1),
            ("TX", 6),
            ("IL", 5),
            ("MO", 7),
            ("CO", 8),
            ("WA", 10),
            ("DC", 3),
        ],
    )
    def test_known_assignments(self, stusps, region):
        assert geo.hhs_region_for_state(stusps) == region

    def test_unknown_state_raises(self):
        with pytest.raises(ValueError):
            geo.hhs_region_for_state("ZZ")

    def test_every_state_and_dc_has_a_region(self):
        for s in _STATES_AND_DC:
            assert 1 <= geo.hhs_region_for_state(s) <= 10

    def test_region_name(self):
        assert geo.hhs_region_name(4) == "Atlanta"
        assert geo.hhs_region_name(1) == "Boston"

    def test_region_name_invalid(self):
        with pytest.raises(ValueError):
            geo.hhs_region_name(11)

    def test_generate_returns_ten_rows(self):
        rows = geo.generate_hhs_regions()
        assert len(rows) == 10
        assert [r["hhs_region"] for r in rows] == list(range(1, 11))
        atlanta = next(r for r in rows if r["hhs_region"] == 4)
        assert atlanta["name"] == "Atlanta"
        assert "GA" in atlanta["member_states"]


@pytest.mark.unit
class TestStateFips:
    def test_known_lookups(self):
        assert geo.state_usps("13") == "GA"
        assert geo.state_name("13") == "Georgia"
        assert geo.state_usps(6) == "CA"  # integer input, leading zero restored
        assert geo.state_name("11") == "District of Columbia"

    def test_unknown_geoid_raises(self):
        with pytest.raises(ValueError):
            geo.state_usps("99")

    def test_covers_50_states_and_dc(self):
        assert len(_STATES_AND_DC) == 51
        known = {v[0] for v in geo.STATE_FIPS.values()}
        for s in _STATES_AND_DC:
            assert s in known

    def test_hhs_members_are_all_known_fips(self):
        known = {v[0] for v in geo.STATE_FIPS.values()}
        for members in geo._HHS_REGION_STATES.values():
            for usps in members:
                assert usps in known


@pytest.mark.unit
class TestRowBuilders:
    def test_build_state_row(self):
        row = geo.build_state_row("G130", 2020, centroid_geo_lon=-83.6, centroid_geo_lat=32.6)
        assert row["geoid"] == "13"
        assert row["stusps"] == "GA"
        assert row["name"] == "Georgia"
        assert row["hhs_region"] == 4
        assert row["vintage"] == 2020
        assert row["centroid_geo_lon"] == -83.6
        assert row["centroid_pop_lon"] is None

    def test_build_county_row(self):
        row = geo.build_county_row("G1301210", 2020, "Fulton")
        assert row["geoid"] == "13121"
        assert row["state_geoid"] == "13"
        assert row["name"] == "Fulton"
        assert row["centroid_pop_lat"] is None

    def test_pop_centroid_flows_through(self):
        s = geo.build_state_row(
            "G130",
            2020,
            centroid_geo_lon=-83.6,
            centroid_geo_lat=33.0,
            centroid_pop_lon=-84.2,
            centroid_pop_lat=33.7,
        )
        assert s["centroid_geo_lon"] == -83.6
        assert s["centroid_pop_lon"] == -84.2
        assert s["centroid_pop_lat"] == 33.7

    def test_build_tract_row(self):
        row = geo.build_tract_row(
            "G1800910040900",
            2020,
            centroid_geo_lon=-86.5,
            centroid_geo_lat=39.1,
            centroid_pop_lon=-86.4,
            centroid_pop_lat=39.2,
        )
        assert row["geoid"] == "18091040900"
        assert row["state_geoid"] == "18"
        assert row["county_geoid"] == "18091"
        assert row["centroid_pop_lon"] == -86.4

    def test_build_zcta_row(self):
        row = geo.build_zcta_row("G30307", 2020, centroid_geo_lon=-84.3, centroid_geo_lat=33.8)
        assert row["geoid"] == "30307"
        assert row["centroid_geo_lon"] == -84.3
        assert "centroid_pop_lon" not in row


@pytest.mark.unit
class TestCrosswalkWeights:
    def test_well_formed_crosswalk_has_no_offenders(self):
        rows = [
            {"source_geoid": "09001", "target_geoid": "09110", "weight": 0.6},
            {"source_geoid": "09001", "target_geoid": "09120", "weight": 0.4},
            {"source_geoid": "09003", "target_geoid": "09130", "weight": 1.0},
        ]
        assert geo.validate_crosswalk_weights(rows) == []

    def test_summarize_totals_per_source(self):
        rows = [
            {"source_geoid": "09001", "target_geoid": "09110", "weight": 0.6},
            {"source_geoid": "09001", "target_geoid": "09120", "weight": 0.4},
        ]
        totals = geo.summarize_crosswalk_weights(rows)
        assert totals == {"09001": pytest.approx(1.0)}

    def test_detects_underweight_source(self):
        rows = [
            {"source_geoid": "09001", "target_geoid": "09110", "weight": 0.5},
            {"source_geoid": "09001", "target_geoid": "09120", "weight": 0.4},
        ]
        offenders = geo.validate_crosswalk_weights(rows)
        assert len(offenders) == 1
        assert offenders[0][0] == "09001"
        assert offenders[0][1] == pytest.approx(0.9)

    def test_tolerance_is_respected(self):
        rows = [{"source_geoid": "09001", "target_geoid": "09110", "weight": 0.9995}]
        assert geo.validate_crosswalk_weights(rows, tolerance=1e-2) == []
        assert geo.validate_crosswalk_weights(rows, tolerance=1e-4) != []

    def test_custom_keys(self):
        rows = [{"src": "1", "wt": 1.0}]
        assert geo.validate_crosswalk_weights(rows, source_key="src", weight_key="wt") == []


@pytest.mark.unit
class TestNormalizeCrosswalkRows:
    def test_long_form_expansion(self):
        raw = [
            {
                "bg2010gj": "G18009100409001",
                "co2020gj": "G1800910",
                "parea": 0.5,
                "wt_pop": 0.6,
                "wt_hh": 0.4,
            },
            {
                "bg2010gj": "G18009100409001",
                "co2020gj": "G1800930",
                "parea": 0.5,
                "wt_pop": 0.4,
                "wt_hh": 0.6,
            },
        ]
        rows = geo.normalize_crosswalk_rows(
            raw,
            source_level="bg",
            source_vintage=2010,
            target_level="county",
            target_vintage=2020,
            source_gj_col="bg2010gj",
            target_gj_col="co2020gj",
            weight_columns=geo.NHGIS_WEIGHT_COLUMNS,
        )
        # 2 raw rows x 3 weight kinds present (parea, wt_pop, wt_hh) = 6 long-form rows
        assert len(rows) == 6
        assert {r["weight_kind"] for r in rows} == {"area", "pop", "hh"}
        first = next(
            r for r in rows if r["target_gisjoin"] == "G1800910" and r["weight_kind"] == "pop"
        )
        assert first["source_level"] == "bg"
        assert first["source_geoid"] == "180910409001"
        assert first["target_geoid"] == "18091"
        assert first["weight"] == 0.6

    def test_skips_nan_and_nonnumeric(self):
        raw = [
            {
                "bg2010gj": "G18009100409001",
                "tr2020gj": "G1800910040900",
                "wt_pop": float("nan"),
                "wt_hh": "x",
            },
        ]
        rows = geo.normalize_crosswalk_rows(
            raw,
            source_level="bg",
            source_vintage=2010,
            target_level="tract",
            target_vintage=2020,
            source_gj_col="bg2010gj",
            target_gj_col="tr2020gj",
            weight_columns={"wt_pop": "pop", "wt_hh": "hh"},
        )
        assert rows == []

    def test_weight_kinds_vocabulary(self):
        assert set(geo.NHGIS_WEIGHT_COLUMNS.values()) <= set(geo.CROSSWALK_WEIGHT_KINDS)


@pytest.mark.unit
class TestUsEnrichedViewDefinitions:
    CAT = "ecdh_model_dev"

    def test_returns_county_and_tract_views(self):
        defs = geo.us_enriched_view_definitions(self.CAT)
        assert set(defs) == {
            f"{self.CAT}.geography.us_county_enriched",
            f"{self.CAT}.geography.us_tract_enriched",
        }

    def test_county_view_joins_state_on_geoid_and_vintage(self):
        sql = geo.us_enriched_view_definitions(self.CAT)[f"{self.CAT}.geography.us_county_enriched"]
        assert f"CREATE OR REPLACE VIEW {self.CAT}.geography.us_county_enriched AS" in sql
        assert f"FROM {self.CAT}.geography.us_county c" in sql
        assert f"JOIN {self.CAT}.geography.us_state s ON " in sql
        assert "c.state_geoid = s.geoid AND c.vintage = s.vintage" in sql
        assert "s.stusps AS state_stusps" in sql
        assert "s.name AS state_name" in sql

    def test_tract_view_joins_county_then_state_vintage_keyed(self):
        sql = geo.us_enriched_view_definitions(self.CAT)[f"{self.CAT}.geography.us_tract_enriched"]
        assert "co.name AS county_name" in sql
        assert "t.county_geoid = co.geoid AND t.vintage = co.vintage" in sql
        assert "t.state_geoid = s.geoid AND t.vintage = s.vintage" in sql

    def test_zcta_is_not_enriched(self):
        defs = geo.us_enriched_view_definitions(self.CAT)
        assert not any("zcta" in name for name in defs)
