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
            table_name="geography.us_state",
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
from collections.abc import Sequence
from dataclasses import dataclass
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
                (e.g., ``"geography.us_state"``).
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


# ---------------------------------------------------------------------------
# Reusable DQ checks (ADR 0029)
# ---------------------------------------------------------------------------
# Query-based checks that recur in nearly every build -- uniqueness, null,
# foreign-key integrity, cardinality, rowcount parity -- were re-implemented by
# hand (fresh SQL + record() + raise) in each entrypoint. These helpers single-
# source the standard query + record + raise so an entrypoint *declares* its
# checks. The SQL builders are pure (unit-tested); ``TableDQ`` binds the per-
# table context and runs them. Bespoke checks (coverage against an in-memory
# set, density/completeness) stay inline -- these cover the common cases only.

_BLOCKING_SEVERITIES = frozenset({DQSeverity.FAIL, DQSeverity.QUARANTINE})


def _where_suffix(where: str | None) -> str:
    return f" WHERE {where}" if where else ""


def count_sql(table: str, where: str | None = None) -> str:
    """Row count over ``table`` (optionally filtered by ``where``)."""
    return f"SELECT COUNT(*) AS n FROM {table}{_where_suffix(where)}"


def duplicate_count_sql(table: str, keys: Sequence[str], where: str | None = None) -> str:
    """Count of key-groups that violate uniqueness on ``keys``."""
    cols = ", ".join(keys)
    return (
        f"SELECT COUNT(*) AS n FROM ("
        f"SELECT {cols} FROM {table}{_where_suffix(where)} "
        f"GROUP BY {cols} HAVING COUNT(*) > 1)"
    )


def null_count_sql(table: str, columns: Sequence[str], where: str | None = None) -> str:
    """Count of rows where any of ``columns`` is NULL (within ``where``)."""
    null_pred = " OR ".join(f"{c} IS NULL" for c in columns)
    clause = f" WHERE ({where}) AND ({null_pred})" if where else f" WHERE {null_pred}"
    return f"SELECT COUNT(*) AS n FROM {table}{clause}"


def orphan_count_sql(
    table: str,
    key: str,
    parent_table: str,
    parent_key: str,
    parent_where: str | None = None,
    where: str | None = None,
) -> str:
    """Count of distinct ``key`` values in ``table`` with no match in the parent."""
    return (
        f"SELECT COUNT(*) AS n FROM "
        f"(SELECT DISTINCT {key} FROM {table}{_where_suffix(where)}) c "
        f"LEFT ANTI JOIN "
        f"(SELECT {parent_key} FROM {parent_table}{_where_suffix(parent_where)}) p "
        f"ON c.{key} = p.{parent_key}"
    )


@dataclass(frozen=True)
class TableDQ:
    """Run the recurring DQ checks against one table and record them (ADR 0029).

    Binds the per-table context once: the DQ recorder, the Spark session, the
    fully-qualified table to query, the (schema-qualified) name to record under
    in ``_ops.dq_results`` (the ADR 0019 discovery join expects schema.table,
    not catalog-qualified), and an optional row filter applied to every check.
    Each method runs its query, records the outcome, and -- for a blocking
    severity -- raises ``ValueError`` on failure unless ``raise_on_fail=False``.
    Returns ``True`` if the check passed.
    """

    recorder: DQRecorder
    spark: SparkSession
    query_table: str  # catalog.schema.table -- used in the SQL
    record_table: str  # schema.table -- recorded in _ops.dq_results
    where: str | None = None

    def _scalar(self, sql: str) -> int:
        return int(self.spark.sql(sql).collect()[0]["n"])

    def _emit(
        self,
        *,
        check_name: str,
        category: DQCategory,
        severity: DQSeverity,
        passed: bool,
        failing_row_count: int,
        total_row_count: int,
        details: dict[str, Any],
        raise_on_fail: bool,
    ) -> bool:
        self.recorder.record(
            table_name=self.record_table,
            check_name=check_name,
            category=category,
            severity=severity,
            passed=passed,
            failing_row_count=failing_row_count,
            total_row_count=total_row_count,
            details=details if not passed else None,
        )
        if raise_on_fail and not passed and severity in _BLOCKING_SEVERITIES:
            raise ValueError(f"DQ check {check_name!r} failed on {self.record_table}: {details}")
        return passed

    def unique(
        self,
        *,
        keys: Sequence[str],
        check_name: str,
        severity: DQSeverity = DQSeverity.FAIL,
        raise_on_fail: bool = True,
    ) -> bool:
        """Natural-key uniqueness over ``keys``."""
        dups = self._scalar(duplicate_count_sql(self.query_table, keys, self.where))
        total = self._scalar(count_sql(self.query_table, self.where))
        return self._emit(
            check_name=check_name,
            category=DQCategory.UNIQUENESS,
            severity=severity,
            passed=dups == 0,
            failing_row_count=dups,
            total_row_count=total,
            details={"keys": list(keys), "duplicate_key_groups": dups},
            raise_on_fail=raise_on_fail,
        )

    def not_null(
        self,
        *,
        columns: Sequence[str],
        check_name: str,
        severity: DQSeverity = DQSeverity.FAIL,
        raise_on_fail: bool = True,
    ) -> bool:
        """No NULLs in ``columns``."""
        nulls = self._scalar(null_count_sql(self.query_table, columns, self.where))
        total = self._scalar(count_sql(self.query_table, self.where))
        return self._emit(
            check_name=check_name,
            category=DQCategory.NULLABILITY,
            severity=severity,
            passed=nulls == 0,
            failing_row_count=nulls,
            total_row_count=total,
            details={"columns": list(columns), "null_rows": nulls},
            raise_on_fail=raise_on_fail,
        )

    def fk(
        self,
        *,
        key: str,
        parent_table: str,
        parent_key: str,
        check_name: str,
        parent_where: str | None = None,
        severity: DQSeverity = DQSeverity.FAIL,
        raise_on_fail: bool = True,
    ) -> bool:
        """Every ``key`` value resolves to a row in ``parent_table.parent_key``."""
        orphans = self._scalar(
            orphan_count_sql(
                self.query_table, key, parent_table, parent_key, parent_where, self.where
            )
        )
        total = self._scalar(count_sql(self.query_table, self.where))
        return self._emit(
            check_name=check_name,
            category=DQCategory.REFERENTIAL,
            severity=severity,
            passed=orphans == 0,
            failing_row_count=orphans,
            total_row_count=total,
            details={"key": key, "parent": parent_table, "orphan_key_values": orphans},
            raise_on_fail=raise_on_fail,
        )

    def cardinality(
        self,
        *,
        check_name: str,
        min_rows: int | None = None,
        max_rows: int | None = None,
        severity: DQSeverity = DQSeverity.WARN,
        raise_on_fail: bool = False,
    ) -> bool:
        """Row count within ``[min_rows, max_rows]`` (either bound optional)."""
        total = self._scalar(count_sql(self.query_table, self.where))
        passed = (min_rows is None or total >= min_rows) and (max_rows is None or total <= max_rows)
        return self._emit(
            check_name=check_name,
            category=DQCategory.CARDINALITY,
            severity=severity,
            passed=passed,
            failing_row_count=0 if passed else total,
            total_row_count=total,
            details={"expected_min": min_rows, "expected_max": max_rows, "actual": total},
            raise_on_fail=raise_on_fail,
        )

    def rowcount_equals(
        self,
        *,
        other_table: str,
        check_name: str,
        category: DQCategory = DQCategory.REFERENTIAL,
        severity: DQSeverity = DQSeverity.FAIL,
        raise_on_fail: bool = True,
    ) -> bool:
        """This table's row count (under ``where``) equals ``other_table``'s."""
        this_n = self._scalar(count_sql(self.query_table, self.where))
        other_n = self._scalar(count_sql(other_table, self.where))
        return self._emit(
            check_name=check_name,
            category=category,
            severity=severity,
            passed=this_n == other_n,
            failing_row_count=abs(this_n - other_n),
            total_row_count=this_n,
            details={"this_rows": this_n, "other_rows": other_n, "other_table": other_table},
            raise_on_fail=raise_on_fail,
        )
