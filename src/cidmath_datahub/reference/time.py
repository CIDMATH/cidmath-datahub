"""Computational time reference data (ADR 0014, class: computational reference).

Generates the canonical `time.calendar_date` and `time.epi_week` tables. These
are deterministic — produced from rules, not ingested from a source — so they
live in pure functions that return plain Python data structures. The bundle
entrypoint converts the output to Spark DataFrames and writes them.

MMWR (CDC epidemiological) weeks are computed with the `epiweeks` package
(CDC system: Sunday-start weeks; week 1 is the week with at least four days in
the new calendar year). The unit tests pin behavior against known CDC values
(e.g., 2020 has 53 MMWR weeks; 2023-01-01 is 2023W01).
"""

from __future__ import annotations

import calendar as _calendar
from datetime import date, timedelta
from typing import Any

from epiweeks import Week, Year


def mmwr_week(d: date) -> tuple[int, int]:
    """Return ``(epi_year, epi_week)`` for a date using CDC MMWR rules.

    Args:
        d: The date to classify.

    Returns:
        A tuple of the epidemiological year and week number (1-53).

    Examples:
        >>> mmwr_week(date(2023, 1, 1))
        (2023, 1)
        >>> mmwr_week(date(2020, 12, 31))
        (2020, 53)
    """
    w = Week.fromdate(d)  # defaults to CDC (MMWR) system
    return w.year, w.week


def epi_week_id(epi_year: int, epi_week: int) -> str:
    """Return the canonical epi-week identifier, e.g. ``"2024W01"``."""
    return f"{epi_year}W{epi_week:02d}"


def generate_calendar(start: date, end: date) -> list[dict[str, Any]]:
    """Generate one calendar row per date in ``[start, end]`` inclusive.

    Args:
        start: First date (inclusive).
        end: Last date (inclusive).

    Returns:
        A list of dicts, one per day, with calendar attributes including
        ISO week and MMWR epi-week fields.

    Raises:
        ValueError: If ``start`` is after ``end``.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    rows: list[dict[str, Any]] = []
    d = start
    one_day = timedelta(days=1)
    while d <= end:
        iso_year, iso_week, iso_weekday = d.isocalendar()
        epi_year, epi_week = mmwr_week(d)
        rows.append(
            {
                "date": d,
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "month_name": _calendar.month_name[d.month],
                "day_of_month": d.day,
                "day_of_week_iso": iso_weekday,  # 1=Monday .. 7=Sunday
                "day_name": _calendar.day_name[d.weekday()],
                "day_of_year": d.timetuple().tm_yday,
                "iso_year": iso_year,
                "iso_week": iso_week,
                "epi_year": epi_year,
                "epi_week": epi_week,
                "epi_week_id": epi_week_id(epi_year, epi_week),
                "is_weekend": d.weekday() >= 5,  # Saturday(5) or Sunday(6)
            }
        )
        d += one_day
    return rows


def generate_epi_weeks(start_year: int, end_year: int) -> list[dict[str, Any]]:
    """Generate one row per MMWR epi-week for epi-years ``[start_year, end_year]``.

    Uses ``epiweeks.Year(y).iterweeks()`` so each epi-week appears exactly once,
    keyed by the epi-year that owns it (a week may start in the prior calendar
    year — e.g., 2021W01 starts 2021-01-03 while 2020W53 ends 2021-01-02).

    Args:
        start_year: First epi-year to cover (inclusive).
        end_year: Last epi-year to cover (inclusive).

    Returns:
        A list of dicts, one per epi-week, ordered by start date.

    Raises:
        ValueError: If ``start_year`` is after ``end_year``.
    """
    if start_year > end_year:
        raise ValueError(f"start_year {start_year} is after end_year {end_year}")

    rows: list[dict[str, Any]] = []
    for y in range(start_year, end_year + 1):
        for w in Year(y).iterweeks():  # CDC system by default
            rows.append(
                {
                    "epi_week_id": epi_week_id(w.year, w.week),
                    "epi_year": w.year,
                    "epi_week": w.week,
                    "start_date": w.startdate(),  # Sunday
                    "end_date": w.enddate(),  # Saturday
                    "label": f"{w.year}-W{w.week:02d}",
                }
            )
    rows.sort(key=lambda r: r["start_date"])
    return rows
