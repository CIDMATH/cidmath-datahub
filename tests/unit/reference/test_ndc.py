"""Unit tests for `cidmath_datahub.reference.ndc` (FDA NDC Directory; ADR 0032).

Anchored on real-shaped product + package rows in ``tests/fixtures/`` covering the
three NDC segment configurations the normalizer must handle, a re-listed
``PRODUCTNDC`` (same code, two ``ProductID``s -> exercises the product_id keying
decision), a discontinued product (``ENDMARKETINGDATE`` set), and a DEA-scheduled
product. The 10->11-digit padding is the error-prone core, so it's tested
exhaustively.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cidmath_datahub.reference import ndc

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
PRODUCT_FIXTURE = _FIXTURES / "ndc_product_sample.txt"
PACKAGE_FIXTURE = _FIXTURES / "ndc_package_sample.txt"


@pytest.fixture(scope="module")
def products() -> list[ndc.NdcProduct]:
    return ndc.parse_product_file(PRODUCT_FIXTURE.read_text(encoding="latin-1"))


@pytest.fixture(scope="module")
def packages() -> list[ndc.NdcPackage]:
    return ndc.parse_package_file(PACKAGE_FIXTURE.read_text(encoding="latin-1"))


@pytest.mark.unit
class TestNormalizeNdc:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("0517-0801-25", "00517080125"),  # 4-4-2 -> pad labeler
            ("12345-678-90", "12345067890"),  # 5-3-2 -> pad product
            ("50090-1234-1", "50090123401"),  # 5-4-1 -> pad package
            ("12345-6789-01", "12345678901"),  # already 5-4-2
        ],
    )
    def test_package_to_11_digits(self, code: str, expected: str):
        result = ndc.normalize_package_ndc(code)
        assert result == expected
        assert len(result) == 11

    @pytest.mark.parametrize(
        "code,expected",
        [
            ("0517-0801", "005170801"),  # 4-4 -> pad labeler
            ("12345-678", "123450678"),  # 5-3 -> pad product
            ("50090-1234", "500901234"),  # already 5-4
        ],
    )
    def test_product_to_9_digits(self, code: str, expected: str):
        result = ndc.normalize_product_ndc(code)
        assert result == expected
        assert len(result) == 9

    @pytest.mark.parametrize(
        "code",
        [
            "",  # empty
            "12345",  # no hyphens / wrong segment count
            "12345-678",  # only two segments for a package
            "123456-789-01",  # labeler segment too long
            "12345-678-9X",  # non-digit segment
        ],
    )
    def test_malformed_package_returns_none(self, code: str):
        assert ndc.normalize_package_ndc(code) is None


@pytest.mark.unit
class TestScalarParsers:
    def test_parse_date(self):
        assert ndc.parse_ndc_date("20240115") == date(2024, 1, 15)
        assert ndc.parse_ndc_date("") is None
        assert ndc.parse_ndc_date("2024-01-15") is None  # wrong format

    def test_yn_to_bool(self):
        assert ndc.yn_to_bool("Y") is True
        assert ndc.yn_to_bool("N") is False
        assert ndc.yn_to_bool("") is False


@pytest.mark.unit
class TestParseProduct:
    def test_row_count(self, products: list[ndc.NdcProduct]):
        assert len(products) == 4

    def test_fields_and_normalization(self, products: list[ndc.NdcProduct]):
        by_id = {p.product_id: p for p in products}
        a = by_id["0517-0801_1"]
        assert a.product_ndc == "0517-0801"
        assert a.product_ndc_normalized == "005170801"
        assert a.labeler_name == "American Regent, Inc."
        assert a.start_marketing_date == date(2010, 1, 1)
        assert a.end_marketing_date is None

    def test_dea_schedule_parsed(self, products: list[ndc.NdcProduct]):
        controlled = next(p for p in products if p.product_id == "12345-678_1")
        assert controlled.dea_schedule == "CII"

    def test_discontinued_product_has_end_date(self, products: list[ndc.NdcProduct]):
        disc = next(p for p in products if p.product_id == "50090-1234_1")
        assert disc.end_marketing_date == date(2023, 12, 31)

    def test_product_ndc_is_not_unique_but_product_id_is(self, products: list[ndc.NdcProduct]):
        # "0517-0801" is re-listed under two ProductIDs -> product_ndc collides,
        # product_id does not. This is why the table keys on product_id (FDA dedup).
        assert ndc.find_duplicate_product_keys(products) == []  # product_id unique
        product_ndcs = [p.product_ndc for p in products]
        assert product_ndcs.count("0517-0801") == 2


@pytest.mark.unit
class TestParsePackage:
    def test_row_count(self, packages: list[ndc.NdcPackage]):
        assert len(packages) == 4

    def test_normalization_and_flags(self, packages: list[ndc.NdcPackage]):
        by_code = {p.ndc_package_code: p for p in packages}
        assert by_code["0517-0801-25"].package_ndc_11 == "00517080125"
        assert by_code["0517-0801-50"].sample_package is True
        assert by_code["0517-0801-25"].sample_package is False

    def test_all_package_ndc_11_are_11_digits(self, packages: list[ndc.NdcPackage]):
        assert all(p.package_ndc_11 and len(p.package_ndc_11) == 11 for p in packages)


@pytest.mark.unit
class TestDqHelpers:
    def test_clean_fixture_passes_blocking_checks(
        self, products: list[ndc.NdcProduct], packages: list[ndc.NdcPackage]
    ):
        assert ndc.find_duplicate_product_keys(products) == []
        assert ndc.find_duplicate_package_keys(packages) == []
        assert ndc.find_missing_product_fields(products) == []
        assert ndc.find_missing_package_fields(packages) == []
        assert ndc.find_bad_product_ndc(products) == []
        assert ndc.find_bad_package_ndc(packages) == []

    def test_package_fk_resolves_in_fixture(
        self, products: list[ndc.NdcProduct], packages: list[ndc.NdcPackage]
    ):
        product_ids = {p.product_id for p in products}
        assert ndc.find_package_orphans(packages, product_ids) == []

    def test_finds_package_orphan(self):
        pkgs = ndc.parse_package_file(
            "PRODUCTID\tPRODUCTNDC\tNDCPACKAGECODE\tPACKAGEDESCRIPTION\t"
            "STARTMARKETINGDATE\tENDMARKETINGDATE\tNDC_EXCLUDE_FLAG\tSAMPLE_PACKAGE\n"
            "missing_1\t9999-9999\t9999-9999-99\t1 BOTTLE\t20200101\t\t\tN\n"
        )
        assert ndc.find_package_orphans(pkgs, {"0517-0801_1"}) == [("9999-9999-99", "missing_1")]

    def test_finds_bad_package_ndc(self):
        pkgs = ndc.parse_package_file(
            "PRODUCTID\tPRODUCTNDC\tNDCPACKAGECODE\tPACKAGEDESCRIPTION\t"
            "STARTMARKETINGDATE\tENDMARKETINGDATE\tNDC_EXCLUDE_FLAG\tSAMPLE_PACKAGE\n"
            "x_1\tBAD\tnot-an-ndc\t1 BOTTLE\t20200101\t\t\tN\n"
        )
        assert ndc.find_bad_package_ndc(pkgs) == ["not-an-ndc"]

    def test_finds_bad_marketing_date_order(self, packages: list[ndc.NdcPackage]):
        assert ndc.find_bad_marketing_date_order(packages) == []
        bad = ndc.parse_package_file(
            "PRODUCTID\tPRODUCTNDC\tNDCPACKAGECODE\tPACKAGEDESCRIPTION\t"
            "STARTMARKETINGDATE\tENDMARKETINGDATE\tNDC_EXCLUDE_FLAG\tSAMPLE_PACKAGE\n"
            "x_1\t0517-0801\t0517-0801-25\t1 BOTTLE\t20240101\t20200101\t\tN\n"
        )
        assert ndc.find_bad_marketing_date_order(bad) == ["0517-0801-25"]

    def test_finds_bad_dea_schedule(self, products: list[ndc.NdcProduct]):
        assert ndc.find_bad_dea_schedule(products) == []  # CII + blanks are valid
        bad = ndc.parse_product_file(
            "PRODUCTID\tPRODUCTNDC\tLABELERNAME\tDEASCHEDULE\nx_1\t0517-0801\tAcme\tCVI\n"
        )
        assert ndc.find_bad_dea_schedule(bad) == [("0517-0801", "CVI")]
