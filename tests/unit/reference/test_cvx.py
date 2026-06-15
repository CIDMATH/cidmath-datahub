"""Unit tests for `cidmath_datahub.reference.cvx` (flat CVX code set; ADR 0032).

Anchored on real CVX XML-new records captured in ``tests/fixtures/cvx_sample.xml``
(ADR 0011) -- one of each shape the parser must handle:
  - 143 Active           (a live vaccine code)
  - 54  Inactive         (a retired code)
  - 173 Non-US           (a non-US code -> normalized "non_us")
  - 226 Never Active     (withdrawn -> normalized "never_active")
  - 998 administrative   ("no vaccine administered"; Status Inactive -- loaded as published)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cidmath_datahub.reference import cvx

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "cvx_sample.xml"


@pytest.fixture(scope="module")
def sample_xml() -> str:
    return FIXTURE.read_text(encoding="ISO-8859-1")


@pytest.fixture(scope="module")
def sample_records(sample_xml: str) -> list[cvx.CvxRecord]:
    return cvx.parse_cvx_xml(sample_xml)


@pytest.mark.unit
class TestSourceSpec:
    def test_xml_new_url_is_the_xml2_report(self):
        assert cvx.SOURCE_XML_NEW_URL.endswith("XML2.asp?rpt=cvx")

    def test_status_vocab_is_the_five_documented_values(self):
        assert cvx.VACCINE_STATUS_VALUES == {
            "active",
            "inactive",
            "pending",
            "non_us",
            "never_active",
        }


@pytest.mark.unit
class TestNormalizeCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("143       ", "143"),
            (" 54 ", "54"),
            ("998", "998"),
            ("", ""),
        ],
    )
    def test_strips_padding(self, raw: str, expected: str):
        assert cvx.normalize_cvx_code(raw) == expected


@pytest.mark.unit
class TestNormalizeStatus:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Active", "active"),
            ("Inactive", "inactive"),
            ("Pending", "pending"),
            ("Non-US", "non_us"),
            ("Never Active", "never_active"),
            ("  Non-US  ", "non_us"),
            ("", ""),
        ],
    )
    def test_normalizes_to_controlled_vocab(self, raw: str, expected: str):
        assert cvx.normalize_status(raw) == expected

    def test_every_documented_status_is_a_valid_vocab_member(self):
        for raw in ("Active", "Inactive", "Pending", "Non-US", "Never Active"):
            assert cvx.normalize_status(raw) in cvx.VACCINE_STATUS_VALUES


@pytest.mark.unit
class TestParseLastUpdated:
    def test_parses_m_d_yyyy(self):
        assert cvx.parse_last_updated("3/20/2011") == date(2011, 3, 20)
        assert cvx.parse_last_updated("12/1/2024") == date(2024, 12, 1)

    def test_blank_is_none(self):
        assert cvx.parse_last_updated("") is None
        assert cvx.parse_last_updated("   ") is None

    def test_unparseable_is_none(self):
        assert cvx.parse_last_updated("not-a-date") is None
        assert cvx.parse_last_updated("2011-03-20") is None  # ISO form is not the source format


@pytest.mark.unit
class TestParseCvxXml:
    def test_parses_all_records(self, sample_records: list[cvx.CvxRecord]):
        assert len(sample_records) == 5
        assert [r.cvx_code for r in sample_records] == ["143", "54", "173", "226", "998"]

    def test_active_record(self, sample_records: list[cvx.CvxRecord]):
        r = next(r for r in sample_records if r.cvx_code == "143")
        assert r.short_description == "Adenovirus types 4 and 7"
        assert r.full_vaccine_name == "Adenovirus, type 4 and type 7, live, oral"
        assert r.vaccine_status == "active"
        assert r.cvx_last_updated == date(2011, 3, 20)

    def test_inactive_record(self, sample_records: list[cvx.CvxRecord]):
        r = next(r for r in sample_records if r.cvx_code == "54")
        assert r.vaccine_status == "inactive"
        assert r.cvx_last_updated == date(2010, 5, 28)

    def test_non_us_record(self, sample_records: list[cvx.CvxRecord]):
        r = next(r for r in sample_records if r.cvx_code == "173")
        assert r.vaccine_status == "non_us"
        assert r.vaccine_status_raw == "Non-US"

    def test_never_active_record(self, sample_records: list[cvx.CvxRecord]):
        r = next(r for r in sample_records if r.cvx_code == "226")
        assert r.vaccine_status == "never_active"

    def test_administrative_998_record(self, sample_records: list[cvx.CvxRecord]):
        r = next(r for r in sample_records if r.cvx_code == "998")
        # 998 ("no vaccine administered") is loaded as published, like any other code.
        assert r.vaccine_status == "inactive"
        assert r.short_description == "no vaccine administered"

    def test_all_parsed_statuses_are_in_the_controlled_vocab(
        self, sample_records: list[cvx.CvxRecord]
    ):
        assert all(r.vaccine_status in cvx.VACCINE_STATUS_VALUES for r in sample_records)


@pytest.mark.unit
class TestDqHelpers:
    def test_clean_sample_passes_all_blocking_checks(self, sample_records: list[cvx.CvxRecord]):
        assert cvx.find_duplicate_codes(sample_records) == []
        assert cvx.find_missing_required(sample_records) == []
        assert cvx.find_status_violations(sample_records) == []

    def test_finds_duplicate_codes(self):
        xml = (
            "<CVXCodes>"
            "<CVXInfo><ShortDescription>a</ShortDescription>"
            "<FullVaccinename>a</FullVaccinename><CVXCode>10</CVXCode>"
            "<Status>Active</Status><LastUpdated>1/1/2020</LastUpdated></CVXInfo>"
            "<CVXInfo><ShortDescription>b</ShortDescription>"
            "<FullVaccinename>b</FullVaccinename><CVXCode>10</CVXCode>"
            "<Status>Active</Status><LastUpdated>1/1/2020</LastUpdated></CVXInfo>"
            "</CVXCodes>"
        )
        assert cvx.find_duplicate_codes(cvx.parse_cvx_xml(xml)) == ["10"]

    def test_finds_missing_required(self):
        xml = (
            "<CVXCodes>"
            "<CVXInfo><ShortDescription></ShortDescription>"
            "<FullVaccinename>b</FullVaccinename><CVXCode>11</CVXCode>"
            "<Status>Active</Status><LastUpdated>1/1/2020</LastUpdated></CVXInfo>"
            "</CVXCodes>"
        )
        assert cvx.find_missing_required(cvx.parse_cvx_xml(xml)) == [("11", "short_description")]

    def test_finds_status_violations(self):
        xml = (
            "<CVXCodes>"
            "<CVXInfo><ShortDescription>a</ShortDescription>"
            "<FullVaccinename>a</FullVaccinename><CVXCode>12</CVXCode>"
            "<Status>Bogus</Status><LastUpdated>1/1/2020</LastUpdated></CVXInfo>"
            "</CVXCodes>"
        )
        assert cvx.find_status_violations(cvx.parse_cvx_xml(xml)) == [("12", "Bogus")]

    def test_finds_unparseable_last_updated_but_not_blank(self):
        xml = (
            "<CVXCodes>"
            "<CVXInfo><ShortDescription>a</ShortDescription>"
            "<FullVaccinename>a</FullVaccinename><CVXCode>13</CVXCode>"
            "<Status>Active</Status><LastUpdated>garbage</LastUpdated></CVXInfo>"
            "<CVXInfo><ShortDescription>b</ShortDescription>"
            "<FullVaccinename>b</FullVaccinename><CVXCode>14</CVXCode>"
            "<Status>Active</Status><LastUpdated></LastUpdated></CVXInfo>"
            "</CVXCodes>"
        )
        recs = cvx.parse_cvx_xml(xml)
        assert cvx.find_unparseable_last_updated(recs) == [("13", "garbage")]

    def test_finds_future_last_updated(self, sample_records: list[cvx.CvxRecord]):
        # The latest sample record is 3/9/2023; nothing is after a 2024 as_of.
        assert cvx.find_future_last_updated(sample_records, date(2024, 1, 1)) == []
        # With an early as_of, every record updated after 1/1/2011 shows as "future"
        # (54 was updated 5/28/2010, so it is excluded).
        future = cvx.find_future_last_updated(sample_records, date(2011, 1, 1))
        assert {code for code, _ in future} == {"143", "173", "226", "998"}
