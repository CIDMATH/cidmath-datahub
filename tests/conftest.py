"""Shared pytest fixtures.

The `spark` fixture provides a local SparkSession for `data` tests (ADR 0011).
Unit tests don't need it; they should be marked with `@pytest.mark.unit` and
will run without Spark even being importable.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Module-scoped local SparkSession for DataFrame tests.

    Configured for fast test startup (small driver memory, no Hive). Tests
    share this session within a module; teardown happens at session end.
    """
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession  # noqa: WPS433

    session = (
        SparkSession.builder.appName("cidmath-datahub-tests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session

    session.stop()
