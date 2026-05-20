"""Unit tests for `cidmath_datahub.reference.time`.

MMWR expectations are pinned against published CDC values. Key anchors:
  - 2023-01-01 (Sunday) is 2023W01.
  - 2020 has 53 MMWR weeks; 2020-12-31 is 2020W53.
  - 2021-01-03 (Sunday) is 2021W01 (the prior week, 2020W53, ends 2021-01-02).
"""

from __future__ import annotations

from datetime import date

import pytest

from cidmath_datahub.reference import time as rt


@pytest.mark.unit
class TestMMWRWeek:
    @pytest.mark.parametrize(
        "d,expected",
        [
            (date(2023, 1, 1), (2023, 1)),    # Sunday, week 1 of 2023
            (date(2023, 1, 7), (2023, 1)),    # Saturday, still week 1
            (date(2023, 1, 8), (2023, 2)),    # next Sunday, week 2
            (date(2020, 12, 31), (2020, 53)), # 2020 is a 53-week year
            (date(2021, 1, 2), (2020, 53)),   # Saturday belongs to 2020W53
            (date(2021, 1, 3), (2021, 1)),    # Sunday starts 2021W01
        ],
    )
    def test_known_mmwr_values(self, d, expected):
        assert rt.mmwr_week(d) == expected

    def test_2020_has_53_weeks(self):
        weeks = rt.generate_epi_weeks(2020, 2020)
        assert weeks[-1]["epi_week"] == 53
        assert len(weeks) == 53

    def test_most_years_have_52_weeks(self):
        weeks = rt.generate_epi_weeks(2023, 2023)
        assert len(weeks) == 52


@pytest.mark.unit
class TestEpiWeekId:
    def test_zero_padded(self):
        assert rt.epi_week_id(2024, 1) == "2024W01"
        assert rt.epi_week_id(2024, 53) == "2024W53"


@pytest.mark.unit
class TestGenerateCalendar:
    def test_row_count_matches_date_span(self):
        rows = rt.generate_calendar(date(2024, 1, 1), date(2024, 1, 31))
        assert len(rows) == 31

    def test_first_row_fields(self):
        rows = rt.generate_calendar(date(2024, 7, 4), date(2024, 7, 4))
        row = rows[0]
        assert row["date"] == date(2024, 7, 4)
        assert row["year"] == 2024
        assert row["quarter"] == 3
        assert row["month"] == 7
        assert row["month_name"] == "July"
        assert row["day_name"] == "Thursday"
        assert row["day_of_week_iso"] == 4
        assert row["is_weekend"] is False

    def test_weekend_flag(self):
        # 2024-07-06 is a Saturday, 2024-07-07 a Sunday.
        rows = rt.generate_calendar(date(2024, 7, 6), date(2024, 7, 7))
        assert rows[0]["is_weekend"] is True
        assert rows[1]["is_weekend"] is True

    def test_leap_day_present(self):
        rows = rt.generate_calendar(date(2024, 2, 28), date(2024, 3, 1))
        dates = [r["date"] for r in rows]
        assert date(2024, 2, 29) in dates

    def test_rejects_inverted_range(self):
        with pytest.raises(ValueError):
            rt.generate_calendar(date(2024, 2, 1), date(2024, 1, 1))


@pytest.mark.unit
class TestGenerateEpiWeeks:
    def test_weeks_are_contiguous_sunday_to_saturday(self):
        weeks = rt.generate_epi_weeks(2023, 2023)
        for w in weeks:
            # start is Sunday (weekday 6), end is Saturday (weekday 5)
            assert w["start_date"].weekday() == 6
            assert w["end_date"].weekday() == 5
            assert (w["end_date"] - w["start_date"]).days == 6

    def test_weeks_sorted_by_start(self):
        weeks = rt.generate_epi_weeks(2022, 2024)
        starts = [w["start_date"] for w in weeks]
        assert starts == sorted(starts)

    def test_rejects_inverted_year_range(self):
        with pytest.raises(ValueError):
            rt.generate_epi_weeks(2025, 2020)
