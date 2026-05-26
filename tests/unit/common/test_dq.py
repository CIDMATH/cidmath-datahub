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
            table_name="geography.state",
            check_name="state_geoid_unique",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=True,
            total_row_count=51,
            failing_row_count=0,
        )
        assert rec.buffered == 1
        row = rec._buffer[0]
        assert row["table_name"] == "geography.state"
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
