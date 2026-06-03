"""Conform {{.subject_name}}_raw.{{.provider_code}}_{{.primary_dataset}} into the processed layer.

Reads the raw landing, conforms it to the shared reference dimensions (geography,
time) in the integrated catalog, and writes
``{{.subject_name}}_processed.{{.provider_code}}_{{.primary_dataset}}`` with update
semantics ``{{.update_semantics}}`` (ADR 0007). Thin entrypoint (ADR 0011) over the
``run_build`` seam (ADR 0027); logic in
``cidmath_datahub.{{.subject_name}}.{{.primary_dataset}}``.

Usage:
    build_{{.primary_dataset}}_processed.py --catalog ecdh_dev --model-catalog ecdh_model_dev
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from cidmath_datahub.common import grants
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build

log = get_logger(__name__)

SOURCE_SCHEMA = "{{.subject_name}}_raw"
SCHEMA = "{{.subject_name}}_processed"
TABLE = "{{.provider_code}}_{{.primary_dataset}}"
FULL_TABLE_REL = f"{SCHEMA}.{TABLE}"
PIPELINE_REF = "bundles/{{.subject_name}}/src/build_{{.primary_dataset}}_processed.py"


def _ensure_table(spark: SparkSession, catalog: str) -> None:
    """Create the schema and processed table if absent (idempotent DDL)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")
    # TODO({{.subject_name}}): CREATE TABLE IF NOT EXISTS {catalog}.{FULL_TABLE_REL} (...) USING delta
    raise NotImplementedError("define the processed table DDL")


def _work(ctx: BuildContext, catalog: str, model_catalog: str) -> None:
    """Conform raw -> processed; record blocking DQ (FK integrity, uniqueness) via ctx.recorder."""
    log.info(
        "TODO implement conformance",
        extra={"run_id": ctx.run_id, "catalog": catalog, "model_catalog": model_catalog},
    )
    # TODO({{.subject_name}}): read {catalog}.{SOURCE_SCHEMA}.{TABLE} -> conform to
    # {model_catalog}.geography / .time -> write {catalog}.{FULL_TABLE_REL} ({{.update_semantics}})
    # -> record blocking FK + uniqueness DQ via ctx.recorder.
    raise NotImplementedError("implement conformance / write / DQ")


def _register(spark: SparkSession, catalog: str) -> None:
    """Register _ops catalog metadata (ADR 0008) via cidmath_datahub.common.registration."""
    # TODO({{.subject_name}}): registration.register_dataset(spark, catalog,
    #   DatasetCatalogEntry(..., derived_from=[f"{catalog}.{SOURCE_SCHEMA}.{TABLE}"]),
    #   DatasetEngineeringEntry(update_semantics="{{.update_semantics}}", ...)).
    raise NotImplementedError("register the processed table")


def run(catalog: str, model_catalog: str, data_engineers_group: str) -> None:
    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=lambda spark: _ensure_table(spark, catalog),
        work=lambda ctx: _work(ctx, catalog, model_catalog),
        register=lambda spark: _register(spark, catalog),
        grant=lambda spark: grants.grant_schema_engineer(
            spark, catalog, SCHEMA, data_engineers_group
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Source-aligned catalog (ecdh_<env>).")
    parser.add_argument(
        "--model-catalog", required=True, help="Integrated catalog holding geography/time."
    )
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    args = parser.parse_args()
    run(args.catalog, args.model_catalog, args.data_engineers_group)


if __name__ == "__main__":
    main()
