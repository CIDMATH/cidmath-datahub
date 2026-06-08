"""Unit tests for `cidmath_datahub.common.dq`.

The recorder's persistence step is intentionally untested here — it's a
one-line ``df.write.mode("append").saveAsTable(...)`` and exercising it
in unit tests would require spinning up a real Spark session. The
buffering, vocabulary validation, and rate computation are pure-Python
and fully covered.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity


def _recorder(run_id: str = "run-1") -> DQRecorder:
    return DQRecorder(
        spark=MagicMock(),
        catalog="ecdh_model_dev",
        run_id=run_id,
        pipeline_reference="build_test",
    )


@pytest.mark.unit
class TestNewRunId:
    def test_returns_uuid4_string(self):
        rid = new_run_id()
        # UUID4 strings are 36 chars: 8-4-4-4-12 with dashes.
        assert len(rid) == 36
        assert rid.count("-") == 4

    def test_each_call_unique(self):
        assert new_run_id() != new_run_id()


@pytest.mark.unit
class TestDQRecorderBuffering:
    def test_starts_empty(self):
        rec = _recorder()
        assert rec.buffered == 0
        assert rec.run_id == "run-1"

    def test_record_appends_to_buffer(self):
        rec = _recorder()
        rec.record(
            table_name="geography.us_state",
            check_name="state_geoid_unique",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=True,
            total_row_count=51,
            failing_row_count=0,
        )
        assert rec.buffered == 1
        row = rec._buffer[0]
        assert row["table_name"] == "geography.us_state"
        assert row["check_name"] == "state_geoid_unique"
        assert row["category"] == "uniqueness"
        assert row["severity"] == "fail"
        assert row["passed"] is True
        assert row["total_row_count"] == 51
        assert row["failing_row_count"] == 0
        assert row["failure_rate"] == 0.0
        assert row["run_id"] == "run-1"
        assert row["pipeline_reference"] == "build_test"

    def test_multiple_records_accumulate(self):
        rec = _recorder()
        for i in range(5):
            rec.record(
                table_name=f"geography.lvl{i}",
                check_name="x",
                category=DQCategory.UNIQUENESS,
                severity=DQSeverity.WARN,
                passed=True,
            )
        assert rec.buffered == 5


@pytest.mark.unit
class TestRateDerivation:
    def test_rate_computed_when_both_counts_present(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.REFERENTIAL,
            severity=DQSeverity.FAIL,
            passed=False,
            failing_row_count=3,
            total_row_count=10,
        )
        assert rec._buffer[0]["failure_rate"] == pytest.approx(0.3)

    def test_rate_null_when_total_missing(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=True,
            failing_row_count=0,
        )
        assert rec._buffer[0]["failure_rate"] is None

    def test_rate_null_when_failing_missing(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=True,
            total_row_count=100,
        )
        assert rec._buffer[0]["failure_rate"] is None

    def test_rate_null_when_total_zero(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.INFO,
            passed=True,
            failing_row_count=0,
            total_row_count=0,
        )
        assert rec._buffer[0]["failure_rate"] is None


@pytest.mark.unit
class TestDetailsEncoding:
    def test_dict_is_json_encoded(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=False,
            details={"sample": ["a", "b"], "count": 2},
        )
        encoded = rec._buffer[0]["details"]
        assert json.loads(encoded) == {"sample": ["a", "b"], "count": 2}

    def test_string_passthrough(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.WARN,
            passed=True,
            details="freeform note",
        )
        assert rec._buffer[0]["details"] == "freeform note"

    def test_none_stays_none(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.INFO,
            passed=True,
        )
        assert rec._buffer[0]["details"] is None


@pytest.mark.unit
class TestVocabularyValidation:
    def test_enum_inputs_accepted(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.BUSINESS_RULE,
            severity=DQSeverity.QUARANTINE,
            passed=False,
        )
        assert rec._buffer[0]["category"] == "business_rule"
        assert rec._buffer[0]["severity"] == "quarantine"

    def test_string_inputs_accepted_when_valid(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category="referential",
            severity="warn",
            passed=True,
        )
        assert rec._buffer[0]["category"] == "referential"
        assert rec._buffer[0]["severity"] == "warn"

    def test_rejects_unknown_category(self):
        rec = _recorder()
        with pytest.raises(ValueError, match="Unknown DQ category"):
            rec.record(
                table_name="t",
                check_name="c",
                category="not_a_real_category",
                severity=DQSeverity.FAIL,
                passed=True,
            )

    def test_rejects_unknown_severity(self):
        rec = _recorder()
        with pytest.raises(ValueError, match="Unknown DQ severity"):
            rec.record(
                table_name="t",
                check_name="c",
                category=DQCategory.UNIQUENESS,
                severity="critical",
                passed=False,
            )


@pytest.mark.unit
class TestFlushAndContextManager:
    def test_flush_calls_spark_write_and_clears_buffer(self):
        rec = _recorder()
        rec.record(
            table_name="t",
            check_name="c",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=True,
        )
        # createDataFrame returns a mock; .write.mode().saveAsTable() must be chained.
        df_mock = MagicMock()
        rec._spark.createDataFrame.return_value = df_mock
        n = rec.flush()
        assert n == 1
        assert rec.buffered == 0
        rec._spark.createDataFrame.assert_called_once()
        df_mock.write.mode.assert_called_once_with("append")
        df_mock.write.mode.return_value.saveAsTable.assert_called_once_with(
            "ecdh_model_dev._ops.dq_results"
        )

    def test_flush_empty_is_noop(self):
        rec = _recorder()
        assert rec.flush() == 0
        rec._spark.createDataFrame.assert_not_called()

    def test_context_manager_flushes_on_exit(self):
        rec = _recorder()
        df_mock = MagicMock()
        rec._spark.createDataFrame.return_value = df_mock
        with rec:
            rec.record(
                table_name="t",
                check_name="c",
                category=DQCategory.UNIQUENESS,
                severity=DQSeverity.FAIL,
                passed=True,
            )
        # Exit triggered flush
        df_mock.write.mode.return_value.saveAsTable.assert_called_once()

    def test_context_manager_flushes_even_when_block_raises(self):
        rec = _recorder()
        df_mock = MagicMock()
        rec._spark.createDataFrame.return_value = df_mock
        with pytest.raises(RuntimeError, match="boom"):
            with rec:
                rec.record(
                    table_name="t",
                    check_name="c",
                    category=DQCategory.UNIQUENESS,
                    severity=DQSeverity.FAIL,
                    passed=False,
                )
                raise RuntimeError("boom")
        # Even though the block raised, the failed check should have been flushed.
        df_mock.write.mode.return_value.saveAsTable.assert_called_once()

    def test_context_manager_swallows_flush_errors_to_preserve_original_exception(self):
        rec = _recorder()
        rec._spark.createDataFrame.side_effect = RuntimeError("spark exploded")
        # Flush failure should NOT mask the original exception.
        with pytest.raises(RuntimeError, match="original"):
            with rec:
                rec.record(
                    table_name="t",
                    check_name="c",
                    category=DQCategory.UNIQUENESS,
                    severity=DQSeverity.FAIL,
                    passed=False,
                )
                raise RuntimeError("original")


# ---------------------------------------------------------------------------
# Reusable DQ check helpers (ADR 0029)
# ---------------------------------------------------------------------------
from cidmath_datahub.common.dq import (  # noqa: E402
    TableDQ,
    count_sql,
    duplicate_count_sql,
    null_count_sql,
    orphan_count_sql,
)


@pytest.mark.unit
class TestSqlBuilders:
    """The query builders are pure -- assert exact SQL so a refactor that
    changes generated SQL is caught without a Spark session."""

    def test_count_sql_no_filter(self):
        assert count_sql("c.s.t") == "SELECT COUNT(*) AS n FROM c.s.t"

    def test_count_sql_with_filter(self):
        assert count_sql("c.s.t", "year = 2020") == (
            "SELECT COUNT(*) AS n FROM c.s.t WHERE year = 2020"
        )

    def test_duplicate_count_sql(self):
        sql = duplicate_count_sql("c.s.t", ["geoid", "date"])
        assert sql == (
            "SELECT COUNT(*) AS n FROM ("
            "SELECT geoid, date FROM c.s.t "
            "GROUP BY geoid, date HAVING COUNT(*) > 1)"
        )

    def test_duplicate_count_sql_with_filter(self):
        sql = duplicate_count_sql("c.s.t", ["geoid"], "year = 2020")
        assert "FROM c.s.t WHERE year = 2020 GROUP BY geoid" in sql

    def test_null_count_sql_single_column(self):
        assert null_count_sql("c.s.t", ["geoid"]) == (
            "SELECT COUNT(*) AS n FROM c.s.t WHERE geoid IS NULL"
        )

    def test_null_count_sql_multi_column_ors(self):
        sql = null_count_sql("c.s.t", ["a", "b"])
        assert sql == "SELECT COUNT(*) AS n FROM c.s.t WHERE a IS NULL OR b IS NULL"

    def test_null_count_sql_combines_filter_with_and(self):
        sql = null_count_sql("c.s.t", ["a"], "year = 2020")
        assert sql == "SELECT COUNT(*) AS n FROM c.s.t WHERE (year = 2020) AND (a IS NULL)"

    def test_orphan_count_sql_anti_join(self):
        sql = orphan_count_sql("c.s.child", "geoid", "c.s.parent", "geoid")
        assert sql == (
            "SELECT COUNT(*) AS n FROM "
            "(SELECT DISTINCT geoid FROM c.s.child) c "
            "LEFT ANTI JOIN "
            "(SELECT geoid FROM c.s.parent) p "
            "ON c.geoid = p.geoid"
        )

    def test_orphan_count_sql_with_parent_where(self):
        sql = orphan_count_sql(
            "c.s.child", "geoid", "c.s.parent", "geoid", parent_where="vintage = 2020"
        )
        assert "FROM c.s.parent WHERE vintage = 2020) p" in sql


class _FakeSpark:
    """Returns pre-seeded scalar counts for successive ``sql().collect()[0]["n"]``."""

    def __init__(self, *counts: int):
        self._counts = list(counts)
        self.queries: list[str] = []

    def sql(self, query: str):
        self.queries.append(query)
        df = MagicMock()
        df.collect.return_value = [{"n": self._counts.pop(0)}]
        return df


def _table_dq(spark, where=None):
    return TableDQ(
        recorder=_recorder(),
        spark=spark,
        query_table="ecdh_dev.weather_processed.noaa_nclimgrid_daily",
        record_table="weather_processed.noaa_nclimgrid_daily",
        where=where,
    )


@pytest.mark.unit
class TestTableDQUnique:
    def test_pass_records_uniqueness_and_no_raise(self):
        spark = _FakeSpark(0, 100)  # dups, total
        dq = _table_dq(spark)
        assert dq.unique(keys=["geoid", "date"], check_name="nk_unique") is True
        row = dq.recorder._buffer[0]
        assert row["category"] == "uniqueness"
        assert row["severity"] == "fail"
        assert row["passed"] is True
        assert row["table_name"] == "weather_processed.noaa_nclimgrid_daily"
        assert row["total_row_count"] == 100
        assert row["failing_row_count"] == 0
        assert row["details"] is None  # details suppressed on pass

    def test_fail_raises_on_blocking_severity(self):
        spark = _FakeSpark(2, 100)
        dq = _table_dq(spark)
        with pytest.raises(ValueError, match="nk_unique"):
            dq.unique(keys=["geoid"], check_name="nk_unique")
        row = dq.recorder._buffer[0]
        assert row["passed"] is False
        assert row["failing_row_count"] == 2
        assert json.loads(row["details"])["duplicate_key_groups"] == 2

    def test_fail_without_raise_returns_false(self):
        spark = _FakeSpark(2, 100)
        dq = _table_dq(spark)
        assert dq.unique(keys=["geoid"], check_name="nk", raise_on_fail=False) is False

    def test_query_uses_catalog_qualified_table(self):
        spark = _FakeSpark(0, 10)
        _table_dq(spark).unique(keys=["geoid"], check_name="nk")
        assert all("ecdh_dev.weather_processed" in q for q in spark.queries)


@pytest.mark.unit
class TestTableDQNotNull:
    def test_records_nullability(self):
        spark = _FakeSpark(0, 50)
        dq = _table_dq(spark)
        assert dq.not_null(columns=["geoid"], check_name="geoid_present") is True
        assert dq.recorder._buffer[0]["category"] == "nullability"

    def test_nulls_raise(self):
        spark = _FakeSpark(3, 50)
        dq = _table_dq(spark)
        with pytest.raises(ValueError):
            dq.not_null(columns=["geoid"], check_name="geoid_present")
        assert dq.recorder._buffer[0]["failing_row_count"] == 3


@pytest.mark.unit
class TestTableDQFk:
    def test_records_referential_and_passes(self):
        spark = _FakeSpark(0, 80)  # orphans, total
        dq = _table_dq(spark)
        ok = dq.fk(
            key="geoid",
            parent_table="ecdh_model_dev.geography.us_county",
            parent_key="geoid",
            parent_where="vintage = 2020",
            check_name="geoid_fk",
        )
        assert ok is True
        assert dq.recorder._buffer[0]["category"] == "referential"
        assert any("vintage = 2020" in q for q in spark.queries)

    def test_orphans_raise(self):
        spark = _FakeSpark(4, 80)
        dq = _table_dq(spark)
        with pytest.raises(ValueError):
            dq.fk(key="g", parent_table="p", parent_key="g", check_name="fk")
        assert dq.recorder._buffer[0]["failing_row_count"] == 4


@pytest.mark.unit
class TestTableDQCardinality:
    def test_warn_out_of_range_does_not_raise(self):
        spark = _FakeSpark(5)  # total below min
        dq = _table_dq(spark)
        assert dq.cardinality(check_name="rowcount", min_rows=10) is False
        row = dq.recorder._buffer[0]
        assert row["category"] == "cardinality"
        assert row["severity"] == "warn"
        assert row["passed"] is False

    def test_within_range_passes(self):
        spark = _FakeSpark(50)
        dq = _table_dq(spark)
        assert dq.cardinality(check_name="rc", min_rows=10, max_rows=100) is True

    def test_blocking_severity_raises_when_out_of_range(self):
        spark = _FakeSpark(5)
        dq = _table_dq(spark)
        with pytest.raises(ValueError):
            dq.cardinality(
                check_name="rc", min_rows=10, severity=DQSeverity.FAIL, raise_on_fail=True
            )


@pytest.mark.unit
class TestTableDQRowcountEquals:
    def test_parity_passes(self):
        spark = _FakeSpark(100, 100)  # this, other
        dq = _table_dq(spark, where="year = 2020")
        assert dq.rowcount_equals(other_table="ecdh_dev.weather_raw.x", check_name="parity") is True
        # the same where applies to both sides
        assert all("year = 2020" in q for q in spark.queries)

    def test_mismatch_raises_with_abs_diff(self):
        spark = _FakeSpark(100, 97)
        dq = _table_dq(spark)
        with pytest.raises(ValueError):
            dq.rowcount_equals(other_table="other", check_name="parity")
        assert dq.recorder._buffer[0]["failing_row_count"] == 3
