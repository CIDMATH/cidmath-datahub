"""Persist DQ check outcomes to ``_ops.dq_results`` (ADR 0009).

Inline DQ checks in pipeline code (uniqueness, FK integrity, business-rule
asserts) historically just raised on failure and were never recorded. This
module fixes that: a single :class:`DQRecorder` per pipeline run buffers
check outcomes and writes them to ``_ops.dq_results`` at flush/exit time,
producing the audit trail the discovery view and operational dashboards
rely on.

The recorder is intentionally silent about raising: callers decide whether
to fail the pipeline based on severity and their own logic. This keeps the
recorder a pure persistence concern and lets callers compose pass/fail
semantics however they like (raise immediately, collect-and-report,
warn-only, etc.).

Usage::

    from cidmath_datahub.common.dq import DQRecorder, new_run_id
    from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity

    with DQRecorder(spark, catalog, new_run_id(), "build_geography") as recorder:
        passed = len(duplicates) == 0
        recorder.record(
            table_name="geography.state",
            check_name="state_geoid_uniqueness_2020",
            category=DQCategory.UNIQUENESS,
            severity=DQSeverity.FAIL,
            passed=passed,
            failing_row_count=len(duplicates),
            total_row_count=len(rows),
            details={"sample_duplicates": duplicates[:5]} if duplicates else None,
        )
        if not passed:
            raise ValueError(f"Duplicate state geoids: {duplicates[:5]}")
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)

DQ_RESULTS_TABLE = "_ops.dq_results"


def _dq_results_spark_schema() -> Any:
    """Return the Spark schema for ``_ops.dq_results``.

    Imported lazily so this module can be imported (and unit-tested) without
    pyspark installed; the schema is only needed when flushing to Delta.
    Mirrors ``setup_ops_tables.py`` exactly.
    """
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("pipeline_reference", T.StringType(), False),
            T.StructField("table_name", T.StringType(), False),
            T.StructField("check_name", T.StringType(), False),
            T.StructField("category", T.StringType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("passed", T.BooleanType(), False),
            T.StructField("failing_row_count", T.LongType(), True),
            T.StructField("total_row_count", T.LongType(), True),
            T.StructField("failure_rate", T.DoubleType(), True),
            T.StructField("details", T.StringType(), True),
            T.StructField("checked_at", T.TimestampType(), False),
        ]
    )


def new_run_id() -> str:
    """Return a fresh UUID4 string suitable as a ``run_id`` value.

    Callers that have access to the Databricks job-run id (via
    ``dbutils.notebook.entry_point.getDbutils().notebook().getContext()``)
    may prefer to pass that instead so DQ rows can be linked back to the
    UI; this helper is for pipelines that don't have that context.
    """
    return str(uuid.uuid4())


class DQRecorder:
    """Buffers DQ check outcomes and writes them to ``_ops.dq_results``.

    Holds the per-run context (Spark session, catalog, run id, pipeline
    reference) so individual ``record()`` calls only need to specify the
    per-check fields. Buffering avoids one Delta write per check, which
    would be wasteful for a pipeline with many small checks.

    The recorder is a context manager: ``__exit__`` flushes the buffer
    even when the wrapped block raised, so failed checks are still
    persisted before the pipeline dies.

    Args:
        spark: The active Spark session.
        catalog: Target catalog (e.g., ``"ecdh_model_dev"``). The recorder
            writes to ``{catalog}.{DQ_RESULTS_TABLE}``.
        run_id: Unique identifier for this pipeline run; groups all checks
            from the same invocation.
        pipeline_reference: Name of the pipeline (e.g., ``"build_geography"``)
            for join-back to ``_ops.dataset_engineering``.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        run_id: str,
        pipeline_reference: str,
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._run_id = run_id
        self._pipeline_reference = pipeline_reference
        self._buffer: list[dict[str, Any]] = []

    @property
    def run_id(self) -> str:
        """The run id this recorder is tagging all checks with."""
        return self._run_id

    @property
    def buffered(self) -> int:
        """Number of results currently buffered (not yet flushed)."""
        return len(self._buffer)

    def record(
        self,
        *,
        table_name: str,
        check_name: str,
        category: DQCategory | str,
        severity: DQSeverity | str,
        passed: bool,
        failing_row_count: int | None = None,
        total_row_count: int | None = None,
        details: dict[str, Any] | str | None = None,
    ) -> None:
        """Buffer one DQ result; persisted on the next ``flush()``.

        ``failure_rate`` is derived from ``failing_row_count / total_row_count``
        when both are present and the total is positive; otherwise it is
        left null. ``details`` is JSON-encoded if a dict; passed through
        if already a string.

        Args:
            table_name: Schema-qualified table the check ran against
                (e.g., ``"geography.state"``).
            check_name: Stable identifier for the check itself.
            category: Member of :class:`DQCategory` (or its string value).
            severity: Member of :class:`DQSeverity` (or its string value).
            passed: ``True`` if the check passed, ``False`` if it failed.
            failing_row_count: Optional count of rows that failed the check.
            total_row_count: Optional count of rows the check inspected.
            details: Optional dict or pre-encoded string for the ``details``
                column. Dicts are JSON-encoded.

        Raises:
            ValueError: If ``category`` or ``severity`` is a string that
                isn't a recognized vocabulary value.
        """
        cat = category.value if isinstance(category, DQCategory) else category
        sev = severity.value if isinstance(severity, DQSeverity) else severity
        if cat not in {c.value for c in DQCategory}:
            raise ValueError(f"Unknown DQ category: {cat!r}")
        if sev not in {s.value for s in DQSeverity}:
            raise ValueError(f"Unknown DQ severity: {sev!r}")

        rate: float | None = None
        if failing_row_count is not None and total_row_count is not None and total_row_count > 0:
            rate = failing_row_count / total_row_count

        det: str | None
        if isinstance(details, dict):
            det = json.dumps(details, default=str, sort_keys=True)
        else:
            det = details

        self._buffer.append(
            {
                "run_id": self._run_id,
                "pipeline_reference": self._pipeline_reference,
                "table_name": table_name,
                "check_name": check_name,
                "category": cat,
                "severity": sev,
                "passed": passed,
                "failing_row_count": failing_row_count,
                "total_row_count": total_row_count,
                "failure_rate": rate,
                "details": det,
                "checked_at": datetime.now(tz=UTC),
            }
        )
        log.info(
            "DQ check recorded",
            extra={
                "table_name": table_name,
                "check_name": check_name,
                "category": cat,
                "severity": sev,
                "passed": passed,
                "failing_row_count": failing_row_count,
                "total_row_count": total_row_count,
                "run_id": self._run_id,
            },
        )

    def flush(self) -> int:
        """Append buffered results to ``{catalog}._ops.dq_results``.

        Returns the number of rows written. A no-op (returns 0) if the
        buffer is empty. Safe to call repeatedly; the buffer is cleared
        after a successful write.
        """
        if not self._buffer:
            return 0
        df = self._spark.createDataFrame(self._buffer, schema=_dq_results_spark_schema())
        df.write.mode("append").saveAsTable(f"{self._catalog}.{DQ_RESULTS_TABLE}")
        n = len(self._buffer)
        self._buffer = []
        log.info(
            "DQ results flushed",
            extra={
                "row_count": n,
                "table": f"{self._catalog}.{DQ_RESULTS_TABLE}",
                "run_id": self._run_id,
            },
        )
        return n

    def __enter__(self) -> DQRecorder:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Always try to flush, even on exception — failed checks are the
        # most important rows to persist. Swallow flush errors so we don't
        # mask the original exception.
        try:
            self.flush()
        except Exception:
            log.exception("DQ flush failed; original exception (if any) takes precedence")
