"""Unit tests for `cidmath_datahub.common.grants`."""

from __future__ import annotations

import pytest

from cidmath_datahub.common import grants


@pytest.mark.unit
class TestCatalogUsage:
    def test_single_use_catalog_statement(self):
        stmts = grants.catalog_usage_statements("ecdh_model_dev", "ecdh-analysts")
        assert stmts == ["GRANT USE CATALOG ON CATALOG ecdh_model_dev TO `ecdh-analysts`"]


@pytest.mark.unit
class TestSchemaStatements:
    def test_reader_tier_is_use_schema_and_select_only(self):
        stmts = grants.reader_schema_statements("ecdh_model_dev", "time", "ecdh-analysts")
        assert stmts == [
            "GRANT USE SCHEMA ON SCHEMA ecdh_model_dev.time TO `ecdh-analysts`",
            "GRANT SELECT ON SCHEMA ecdh_model_dev.time TO `ecdh-analysts`",
        ]

    def test_engineer_tier_includes_modify_and_create(self):
        stmts = grants.engineer_schema_statements("ecdh_dev", "_ops", "ecdh-data-engineers")
        privileges = [s.split(" ON ")[0].replace("GRANT ", "") for s in stmts]
        assert privileges == ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]

    def test_reader_tier_never_grants_modify(self):
        stmts = grants.reader_schema_statements("ecdh_model_dev", "geography", "ecdh-analysts")
        joined = " ".join(stmts)
        assert "MODIFY" not in joined
        assert "CREATE TABLE" not in joined

    def test_principal_is_backtick_quoted(self):
        stmts = grants.reader_schema_statements("c", "s", "ecdh-analysts")
        assert all(s.endswith("TO `ecdh-analysts`") for s in stmts)


@pytest.mark.unit
class TestVolumeStatements:
    def test_read_volume_statement(self):
        stmts = grants.volume_read_statements(
            "ecdh_model_dev", "codes", "cvx_raw", "ecdh-data-engineers"
        )
        assert stmts == [
            "GRANT READ VOLUME ON VOLUME ecdh_model_dev.codes.cvx_raw TO `ecdh-data-engineers`"
        ]

    def test_grant_volume_reader_executes_statement(self):
        executed = []

        class FakeSpark:
            def sql(self, stmt):
                executed.append(stmt)

        grants.grant_volume_reader(
            FakeSpark(), "ecdh_model_dev", "codes", "cvx_raw", "ecdh-data-engineers"
        )
        assert executed == [
            "GRANT READ VOLUME ON VOLUME ecdh_model_dev.codes.cvx_raw TO `ecdh-data-engineers`"
        ]


@pytest.mark.unit
class TestApply:
    def test_apply_executes_each_statement(self):
        executed = []

        class FakeSpark:
            def sql(self, stmt):
                executed.append(stmt)

        grants.apply(FakeSpark(), ["GRANT A", "GRANT B"])
        assert executed == ["GRANT A", "GRANT B"]

    def test_grant_schema_reader_runs_two_statements(self):
        executed = []

        class FakeSpark:
            def sql(self, stmt):
                executed.append(stmt)

        grants.grant_schema_reader(FakeSpark(), "ecdh_model_dev", "time", "ecdh-analysts")
        assert len(executed) == 2
        assert all("ecdh_model_dev.time" in s for s in executed)


# --- Verification helpers ---


class _FakeRow:
    """Stands in for a Spark Row with an asDict() method."""

    def __init__(self, data: dict):
        self._data = data

    def asDict(self):
        return dict(self._data)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _FakeGrantsSpark:
    """Returns a fixed set of SHOW GRANTS rows for any query."""

    def __init__(self, rows: list[dict]):
        self._rows = [_FakeRow(r) for r in rows]

    def sql(self, stmt):
        return _FakeResult(self._rows)


def _grant_row(principal: str, action: str) -> dict:
    return {
        "Principal": principal,
        "ActionType": action,
        "ObjectType": "SCHEMA",
        "ObjectKey": "c.s",
    }


@pytest.mark.unit
class TestParseGrantRows:
    def test_normalizes_underscores_to_spaces(self):
        rows = [_grant_row("ecdh-analysts", "USE_SCHEMA"), _grant_row("ecdh-analysts", "SELECT")]
        assert grants._parse_grant_rows(rows, "ecdh-analysts") == {"USE SCHEMA", "SELECT"}

    def test_filters_to_requested_principal(self):
        rows = [
            {"principal": "ecdh-analysts", "action_type": "SELECT"},
            {"principal": "someone-else", "action_type": "MODIFY"},
        ]
        assert grants._parse_grant_rows(rows, "ecdh-analysts") == {"SELECT"}

    def test_empty_when_no_rows(self):
        assert grants._parse_grant_rows([], "ecdh-analysts") == set()


@pytest.mark.unit
class TestVerifySchemaReader:
    def test_passes_for_exact_reader(self):
        spark = _FakeGrantsSpark(
            [_grant_row("ecdh-analysts", "USE SCHEMA"), _grant_row("ecdh-analysts", "SELECT")]
        )
        grants.verify_schema_reader(spark, "c", "time", "ecdh-analysts")  # no raise

    def test_fails_when_over_granted(self):
        spark = _FakeGrantsSpark(
            [
                _grant_row("ecdh-analysts", "USE SCHEMA"),
                _grant_row("ecdh-analysts", "SELECT"),
                _grant_row("ecdh-analysts", "MODIFY"),
            ]
        )
        with pytest.raises(grants.GrantVerificationError):
            grants.verify_schema_reader(spark, "c", "time", "ecdh-analysts")

    def test_fails_when_missing_select(self):
        spark = _FakeGrantsSpark([_grant_row("ecdh-analysts", "USE SCHEMA")])
        with pytest.raises(grants.GrantVerificationError):
            grants.verify_schema_reader(spark, "c", "time", "ecdh-analysts")

    def test_subset_mode_tolerates_extra(self):
        spark = _FakeGrantsSpark(
            [
                _grant_row("ecdh-data-engineers", "USE SCHEMA"),
                _grant_row("ecdh-data-engineers", "SELECT"),
                _grant_row("ecdh-data-engineers", "MODIFY"),
            ]
        )
        grants.verify_schema_reader(spark, "c", "time", "ecdh-data-engineers", exact=False)


@pytest.mark.unit
class TestVerifySchemaNoAccess:
    def test_passes_when_empty(self):
        grants.verify_schema_no_access(_FakeGrantsSpark([]), "c", "_ops", "ecdh-analysts")

    def test_fails_when_any_grant_present(self):
        spark = _FakeGrantsSpark([_grant_row("ecdh-analysts", "SELECT")])
        with pytest.raises(grants.GrantVerificationError):
            grants.verify_schema_no_access(spark, "c", "_ops", "ecdh-analysts")


@pytest.mark.unit
class TestVerifySchemaEngineer:
    def test_passes_for_exact_engineer_tier(self):
        spark = _FakeGrantsSpark(
            [
                _grant_row("ecdh-data-engineers", "USE SCHEMA"),
                _grant_row("ecdh-data-engineers", "SELECT"),
                _grant_row("ecdh-data-engineers", "MODIFY"),
                _grant_row("ecdh-data-engineers", "CREATE TABLE"),
            ]
        )
        grants.verify_schema_engineer(spark, "c", "_ops", "ecdh-data-engineers")  # no raise

    def test_fails_when_missing_create_table(self):
        spark = _FakeGrantsSpark(
            [
                _grant_row("ecdh-data-engineers", "USE SCHEMA"),
                _grant_row("ecdh-data-engineers", "SELECT"),
                _grant_row("ecdh-data-engineers", "MODIFY"),
            ]
        )
        with pytest.raises(grants.GrantVerificationError):
            grants.verify_schema_engineer(spark, "c", "_ops", "ecdh-data-engineers")
