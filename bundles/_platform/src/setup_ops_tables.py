"""Create or update _ops tables in a target catalog.

Run as a one-shot job from the platform bundle. Idempotent — uses CREATE
TABLE IF NOT EXISTS so re-running is safe.

The schemas defined here are the authoritative versions referenced by:
  - ADR 0008 (dataset_catalog, dataset_engineering)
  - ADR 0009 (dq_results)
  - ADR 0005 (taxonomy_* reference tables)
  - ADR 0006 (provider_codes)

When these schemas evolve, update both this file and the corresponding ADR.

Usage:
    spark-submit setup_ops_tables.py --catalog ecdh_dev --scope source
    spark-submit setup_ops_tables.py --catalog ecdh_model_dev --scope model
"""

from __future__ import annotations

import argparse
from typing import Literal

from pyspark.sql import SparkSession

from cidmath_datahub.common.logging import get_logger

log = get_logger(__name__)


# --- Universal tables (created in both source and model catalogs) ---

DATASET_CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.dataset_catalog (
    full_table_name STRING NOT NULL COMMENT 'Three-level UC name. Primary key.',
    subject STRING NOT NULL COMMENT 'Schema subject (e.g., wastewater).',
    layer STRING NOT NULL COMMENT 'raw / processed / analysis / model.',
    source_provider_code STRING COMMENT 'Provider code from ADR 0006.',
    source_dataset_name STRING COMMENT 'Provider''s own name for the dataset.',
    description STRING NOT NULL COMMENT 'Plain-English description.',
    public_health_relevance STRING COMMENT 'Why this data matters for ID modeling/analytics.',
    known_limitations STRING COMMENT 'Caveats, biases, sampling issues.',
    derived_from ARRAY<STRING> COMMENT 'Source tables for derived/aggregated data.',
    preprocessing_notes STRING,
    data_suppression_notes STRING COMMENT 'Censoring rules from source.',
    missingness_notes STRING,
    temporal_resolution STRING COMMENT 'daily, weekly, annual, etc.',
    temporal_coverage_start DATE,
    temporal_coverage_end DATE COMMENT 'NULL = ongoing.',
    spatial_resolution STRING COMMENT 'state, county, zcta, etc.',
    spatial_coverage STRING,
    demographic_resolution STRING,
    demographic_coverage STRING,
    refresh_cadence STRING,
    reporting_lag STRING COMMENT 'Typical delay between event and availability.',
    revision_cadence STRING COMMENT 'How often upstream revises past data.',
    source_url STRING,
    source_documentation_url STRING,
    source_data_dictionary_url STRING,
    external_maintainer_name STRING,
    external_maintainer_email STRING,
    license STRING,
    dua_required BOOLEAN,
    dua_reference STRING,
    access_tier STRING COMMENT 'public / restricted / commercial.',
    is_hosted BOOLEAN NOT NULL COMMENT 'TRUE if materialized; FALSE if catalogued only.',
    owner STRING NOT NULL COMMENT 'Internal owner (person or team).',
    last_validated DATE,
    domain_metadata_misc MAP<STRING, STRING> COMMENT 'Long-tail fields not promoted to extension.'
)
USING DELTA
CLUSTER BY (subject, layer)
COMMENT 'Universal provenance metadata for every catalogued dataset. ADR 0008.';
"""

DATASET_ENGINEERING_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.dataset_engineering (
    full_table_name STRING NOT NULL COMMENT 'FK to dataset_catalog.',
    update_semantics STRING NOT NULL COMMENT 'Controlled vocabulary per ADR 0007.',
    materialization_type STRING NOT NULL COMMENT 'table / view / materialized_view / streaming_table.',
    partition_columns ARRAY<STRING>,
    cluster_columns ARRAY<STRING>,
    history_table STRING COMMENT 'Companion history table name if SCD2_side.',
    pipeline_reference STRING NOT NULL COMMENT 'Bundle path of owning pipeline.',
    pipeline_run_id_last STRING,
    last_refresh_at TIMESTAMP,
    ingestion_watermark STRING,
    schema_version INT NOT NULL DEFAULT 1,
    dq_status_last STRING COMMENT 'passed / warned / failed / unknown.',
    dq_results_run_id STRING,
    freshness_sla_hours INT COMMENT 'Max hours since last refresh before alerting.',
    freshness_check_paused BOOLEAN DEFAULT FALSE
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Engineering state per materialized table. ADR 0008.';
"""

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.dq_results (
    run_id STRING NOT NULL,
    pipeline_reference STRING NOT NULL,
    table_name STRING NOT NULL,
    check_name STRING NOT NULL,
    category STRING NOT NULL COMMENT 'schema/nullability/uniqueness/range/cardinality/referential/freshness/business_rule.',
    severity STRING NOT NULL COMMENT 'Controlled vocabulary: info/warn/quarantine/fail. ADR 0009.',
    passed BOOLEAN NOT NULL,
    failing_row_count BIGINT,
    total_row_count BIGINT,
    failure_rate DOUBLE,
    details STRING COMMENT 'Check-specific JSON or free-text.',
    checked_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (table_name, check_name, checked_at)
COMMENT 'DQ check execution log. ADR 0009.';
"""

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.pipeline_runs (
    run_id STRING NOT NULL,
    pipeline_reference STRING NOT NULL,
    pipeline_type STRING NOT NULL COMMENT 'job / dlt_pipeline.',
    target_env STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status STRING NOT NULL COMMENT 'running / succeeded / failed / cancelled.',
    triggered_by STRING COMMENT 'human user / service principal / schedule.',
    tables_written ARRAY<STRING>,
    row_count_total BIGINT,
    error_details STRING
)
USING DELTA
CLUSTER BY (pipeline_reference, started_at)
COMMENT 'Pipeline run history. Populated by pipelines via the cidmath_datahub.common.pipeline_runs helper.';
"""

# --- Taxonomy reference tables (controlled vocabularies for UC tags, ADR 0005) ---

TAXONOMY_DOMAIN_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.taxonomy_domain (
    value STRING NOT NULL,
    label STRING NOT NULL,
    description STRING,
    parent_value STRING,
    added_at DATE NOT NULL DEFAULT CURRENT_DATE()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Controlled vocabulary for the domain:* UC tag namespace. ADR 0005.';
"""

TAXONOMY_PATHOGEN_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.taxonomy_pathogen (
    value STRING NOT NULL,
    label STRING NOT NULL,
    description STRING,
    added_at DATE NOT NULL DEFAULT CURRENT_DATE()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Controlled vocabulary for the pathogen:* UC tag namespace. ADR 0005.';
"""

TAXONOMY_SURVEILLANCE_CATEGORY_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.taxonomy_surveillance_category (
    value STRING NOT NULL,
    label STRING NOT NULL,
    description STRING,
    sort_order INT,
    added_at DATE NOT NULL DEFAULT CURRENT_DATE()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Controlled vocabulary for the surveillance_category:* UC tag namespace. Per Delphi EpiPortal taxonomy. ADR 0005.';
"""

# --- Provider codes (operational reference, ADR 0006) ---

PROVIDER_CODES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.provider_codes (
    code STRING NOT NULL COMMENT 'Short code used in raw/processed table names.',
    provider_name STRING NOT NULL COMMENT 'Full provider name.',
    provider_type STRING COMMENT 'government / state_dph / academic / commercial / international / internal.',
    notes STRING,
    added_at DATE NOT NULL DEFAULT CURRENT_DATE()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Provider code registry. Documented-only per ADR 0006 (not CI-enforced).';
"""

# --- Composite view (ADR 0008) ---
# Created in both catalogs; LEFT JOINs domain extensions as they materialize.
# Initial version joins only the universal tables; updated when extensions land.

DATASET_CATALOG_FULL_VIEW_DDL = """
CREATE OR REPLACE VIEW {catalog}._ops.dataset_catalog_full AS
SELECT
    c.*,
    e.update_semantics,
    e.materialization_type,
    e.last_refresh_at,
    e.dq_status_last,
    e.freshness_sla_hours,
    e.pipeline_reference AS pipeline_reference_engineering
FROM {catalog}._ops.dataset_catalog c
LEFT JOIN {catalog}._ops.dataset_engineering e
    ON c.full_table_name = e.full_table_name;
"""


UNIVERSAL_TABLES = [
    ("dataset_catalog", DATASET_CATALOG_DDL),
    ("dataset_engineering", DATASET_ENGINEERING_DDL),
    ("dq_results", DQ_RESULTS_DDL),
    ("pipeline_runs", PIPELINE_RUNS_DDL),
    ("taxonomy_domain", TAXONOMY_DOMAIN_DDL),
    ("taxonomy_pathogen", TAXONOMY_PATHOGEN_DDL),
    ("taxonomy_surveillance_category", TAXONOMY_SURVEILLANCE_CATEGORY_DDL),
    ("provider_codes", PROVIDER_CODES_DDL),
]

VIEWS = [
    ("dataset_catalog_full", DATASET_CATALOG_FULL_VIEW_DDL),
]

# --- Grants on _ops schema ---
# Engineer-tier access to the _ops schema. Applied here rather than via a
# DAB `grants` resource because some DAB CLI versions don't yet support the
# grants resource type.

GRANTS_DDL = [
    "GRANT USE SCHEMA ON SCHEMA {catalog}._ops TO `{group}`",
    "GRANT SELECT ON SCHEMA {catalog}._ops TO `{group}`",
    "GRANT MODIFY ON SCHEMA {catalog}._ops TO `{group}`",
    "GRANT CREATE TABLE ON SCHEMA {catalog}._ops TO `{group}`",
]


def run(
    catalog: str,
    scope: Literal["source", "model"],
    data_engineers_group: str,
) -> None:
    """Create or update _ops tables in the given catalog, then apply grants.

    Args:
        catalog: Catalog name (e.g., "ecdh_dev").
        scope: "source" or "model". Currently both create the same set of
            tables, but reserved for future scope-specific tables (e.g., the
            model catalog may want an `entity_resolution_log` that the source
            catalog doesn't need).
        data_engineers_group: Name of the workspace group to grant engineer-
            tier access on `_ops` (typically `ecdh-data-engineers`).
    """
    spark = SparkSession.builder.getOrCreate()

    # Ensure the _ops schema exists. DAB's `schemas` resource type isn't
    # reliably supported across CLI versions, so we create idempotently here.
    log.info("Ensuring _ops schema exists", extra={"catalog": catalog})
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}._ops "
        f"COMMENT 'Operational metadata: dataset catalog, engineering state, "
        f"DQ results, taxonomy reference tables. Owned by _platform bundle. "
        f"See docs/adr/0008-catalog-metadata-schema-design.md.'"
    )

    log.info("Creating _ops tables", extra={"catalog": catalog, "scope": scope})
    for name, ddl in UNIVERSAL_TABLES:
        full = f"{catalog}._ops.{name}"
        log.info("Creating table", extra={"table": full})
        spark.sql(ddl.format(catalog=catalog))

    for name, ddl in VIEWS:
        full = f"{catalog}._ops.{name}"
        log.info("Creating view", extra={"view": full})
        spark.sql(ddl.format(catalog=catalog))

    log.info(
        "Applying grants on _ops schema",
        extra={"catalog": catalog, "group": data_engineers_group},
    )
    for stmt in GRANTS_DDL:
        spark.sql(stmt.format(catalog=catalog, group=data_engineers_group))

    log.info(
        "Completed _ops setup",
        extra={
            "catalog": catalog,
            "scope": scope,
            "tables": len(UNIVERSAL_TABLES),
            "grants": len(GRANTS_DDL),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--scope", choices=["source", "model"], required=True)
    parser.add_argument(
        "--data-engineers-group",
        default="ecdh-data-engineers",
        help="Workspace group to grant engineer-tier access on _ops.",
    )
    args = parser.parse_args()
    run(args.catalog, args.scope, args.data_engineers_group)


if __name__ == "__main__":
    main()
