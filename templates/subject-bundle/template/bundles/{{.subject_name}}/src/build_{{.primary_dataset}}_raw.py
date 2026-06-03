"""Land {{.provider_code}} {{.primary_dataset}} into {{.subject_name}}_raw.{{.provider_code}}_{{.primary_dataset}}.

Thin IO + Spark entrypoint (ADR 0011): parse/transform logic lives in the
unit-tested ``cidmath_datahub.{{.subject_name}}.{{.primary_dataset}}`` module.
Orchestration uses the shared ``run_build`` seam (ADR 0027): the canonical
``ensure -> [DQ context: work] -> register -> grant`` lifecycle.

Raw is engineer-tier staging (ADR 0018) and is *not* catalogued, so
``register=None`` (the explicit, logged opt-out). Fill the TODO hooks; read the
source's documentation before inferring its format (docs-first, CLAUDE.md).

Usage:
    build_{{.primary_dataset}}_raw.py --catalog ecdh_dev
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from cidmath_datahub.common import grants
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build

log = get_logger(__name__)

SCHEMA = "{{.subject_name}}_raw"
TABLE = "{{.provider_code}}_{{.primary_dataset}}"
FULL_TABLE_REL = f"{SCHEMA}.{TABLE}"
PIPELINE_REF = "bundles/{{.subject_name}}/src/build_{{.primary_dataset}}_raw.py"


def _ensure_table(spark: SparkSession, catalog: str) -> None:
    """Create the schema and raw table if absent (idempotent DDL)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")
    # TODO({{.subject_name}}): CREATE TABLE IF NOT EXISTS {catalog}.{FULL_TABLE_REL} (...) USING delta
    raise NotImplementedError("define the raw table DDL")


def _work(ctx: BuildContext, catalog: str) -> None:
    """Extract from source, parse, write, and record DQ via ``ctx.recorder``.

    Raise on a blocking DQ failure; the recorder still flushes (run_build).
    """
    log.info("TODO implement raw ingest", extra={"run_id": ctx.run_id, "catalog": catalog})
    # TODO({{.subject_name}}): fetch source -> parse via
    # cidmath_datahub.{{.subject_name}}.{{.primary_dataset}} -> write to
    # {catalog}.{FULL_TABLE_REL} -> record DQ (uniqueness FAIL, etc.) via ctx.recorder.
    raise NotImplementedError("implement extract / parse / write / DQ")


def run(catalog: str, data_engineers_group: str) -> None:
    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=lambda spark: _ensure_table(spark, catalog),
        work=lambda ctx: _work(ctx, catalog),
        register=None,  # raw is engineer-tier staging, not catalogued (ADR 0018)
        grant=lambda spark: grants.grant_schema_engineer(
            spark, catalog, SCHEMA, data_engineers_group
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Source-aligned catalog (ecdh_<env>).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    args = parser.parse_args()
    run(args.catalog, args.data_engineers_group)


if __name__ == "__main__":
    main()
