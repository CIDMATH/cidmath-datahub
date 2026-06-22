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

from cidmath_datahub.common import grants
from cidmath_datahub.common.logging import get_logger

log = get_logger(__name__)


# --- Universal tables (created in both source and model catalogs) ---

DATASET_CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}._ops.dataset_catalog (
    full_table_name STRING NOT NULL COMMENT 'Three-level UC name. Primary key.',
    subject STRING NOT NULL COMMENT 'Schema subject (e.g., wastewater).',
    layer STRING NOT NULL COMMENT 'raw / processed / analysis / model.',
    source_provider_code STRING COMMENT 'Vendor/distributor code (ADR 0006), e.g. ipums_nhgis, cmu_delphi.',
    source_origin_code STRING COMMENT 'Originating authority (ADR 0006), e.g. census, cdc; distinct from provider.',
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

# dq_status_last is DERIVED from _ops.dq_results (latest result per check, rolled
# up per table) rather than read from the dataset_engineering column of the same
# name — no pipeline writes that column, so it was perpetually null. failed if any
# fail/quarantine check failed; warned if any warn check failed; passed otherwise;
# unknown (COALESCE) when a table has no DQ results. dq_results.table_name is
# schema-qualified ("geography.country"), so it joins to full_table_name via the
# catalog prefix.
DATASET_CATALOG_FULL_VIEW_DDL = """
CREATE OR REPLACE VIEW {catalog}._ops.dataset_catalog_full AS
SELECT
    c.*,
    e.update_semantics,
    e.materialization_type,
    e.last_refresh_at,
    COALESCE(dq.dq_status_last, 'unknown') AS dq_status_last,
    e.freshness_sla_hours,
    e.pipeline_reference AS pipeline_reference_engineering
FROM {catalog}._ops.dataset_catalog c
LEFT JOIN {catalog}._ops.dataset_engineering e
    ON c.full_table_name = e.full_table_name
LEFT JOIN (
    SELECT table_name,
        CASE
            WHEN MAX(CASE WHEN severity IN ('fail', 'quarantine') AND NOT passed THEN 1 ELSE 0 END) = 1 THEN 'failed'
            WHEN MAX(CASE WHEN severity = 'warn' AND NOT passed THEN 1 ELSE 0 END) = 1 THEN 'warned'
            ELSE 'passed'
        END AS dq_status_last
    FROM (
        SELECT table_name, severity, passed,
            ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY checked_at DESC) AS rn
        FROM {catalog}._ops.dq_results
    )
    WHERE rn = 1
    GROUP BY table_name
) dq ON c.full_table_name = '{catalog}.' || dq.table_name;
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

# --- Analyst-facing discovery surface (ADR 0019) ---
# The dataset catalog lives in `_ops`, which is engineer-only. To let analysts
# (and other reader-tier groups) browse "what data exists" without granting any
# access to `_ops`, we expose a curated view in a separate, reader-readable
# `discovery` schema.
#
# This works via Unity Catalog's view ownership chain: the deploy SP owns both
# the `_ops` base tables (it created them) and this view (it creates it here),
# so a principal querying the view only needs SELECT on the view itself — UC
# does not require them to hold any privilege on the underlying `_ops` tables.
#
# Columns are deliberately curated for a data consumer: what the dataset is,
# why it matters, its coverage/cadence, how to access it (license, DUA, access
# tier), and freshness. Internal plumbing (pipeline refs, watermarks, the
# free-form metadata map, maintainer contact details) is intentionally omitted.

DISCOVERY_SCHEMA = "discovery"

DISCOVERY_DATASETS_VIEW_DDL = """
CREATE OR REPLACE VIEW {catalog}.discovery.datasets AS
SELECT
    c.full_table_name,
    c.subject,
    c.layer,
    c.description,
    c.public_health_relevance,
    c.known_limitations,
    c.missingness_notes,
    c.data_suppression_notes,
    c.temporal_resolution,
    c.temporal_coverage_start,
    c.temporal_coverage_end,
    c.spatial_resolution,
    c.spatial_coverage,
    c.demographic_resolution,
    c.demographic_coverage,
    c.refresh_cadence,
    c.reporting_lag,
    c.revision_cadence,
    c.source_url,
    c.source_documentation_url,
    c.source_data_dictionary_url,
    c.license,
    c.dua_required,
    c.dua_reference,
    c.access_tier,
    c.is_hosted,
    c.owner,
    c.last_validated,
    e.update_semantics,
    e.last_refresh_at,
    COALESCE(dq.dq_status_last, 'unknown') AS dq_status_last
FROM {catalog}._ops.dataset_catalog c
LEFT JOIN {catalog}._ops.dataset_engineering e
    ON c.full_table_name = e.full_table_name
LEFT JOIN (
    SELECT table_name,
        CASE
            WHEN MAX(CASE WHEN severity IN ('fail', 'quarantine') AND NOT passed THEN 1 ELSE 0 END) = 1 THEN 'failed'
            WHEN MAX(CASE WHEN severity = 'warn' AND NOT passed THEN 1 ELSE 0 END) = 1 THEN 'warned'
            ELSE 'passed'
        END AS dq_status_last
    FROM (
        SELECT table_name, severity, passed,
            ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY checked_at DESC) AS rn
        FROM {catalog}._ops.dq_results
    )
    WHERE rn = 1
    GROUP BY table_name
) dq ON c.full_table_name = '{catalog}.' || dq.table_name;
"""

# --- Grants ---
# Grants are applied here (SQL DDL via the cidmath_datahub.common.grants
# helpers) rather than via a DAB `grants` resource because some DAB CLI
# versions don't yet support the grants resource type (ADR 0017).
#
# This job applies only SCHEMA-level grants, which the deploy SP can make
# because it owns the schemas it created (ADR 0018):
#   - Engineers (ecdh-data-engineers): engineer-tier (USE SCHEMA, SELECT,
#     MODIFY, CREATE TABLE) on _ops; reader-tier on discovery.
#   - Analysts (ecdh-analysts): reader-tier on discovery only. Deliberately
#     NOT granted anything on _ops (it is internal/engineer-only).
#
# CATALOG-level USE CATALOG for both groups is granted separately by an admin
# in scripts/setup/grant_catalog_permissions.sql, because granting on a catalog
# requires MANAGE/ownership that the deploy SP does not have.


# Idempotent additive column migrations for _ops tables. CREATE TABLE IF NOT EXISTS
# does NOT add columns to a table that already exists, so a column added to a DDL
# above must also be back-filled here for catalogs created before the change. Each
# entry is `table -> {column: "TYPE [COMMENT '...'] [AFTER other]"}`; applied only
# when the column is absent, so it is safe to re-run.
_ADDED_COLUMNS: dict[str, dict[str, str]] = {
    "dataset_catalog": {
        "source_origin_code": (
            "STRING COMMENT 'Originating authority (ADR 0006), e.g. census, cdc; "
            "distinct from provider.' AFTER source_provider_code"
        ),
    },
}


def _apply_added_columns(spark: SparkSession, catalog: str) -> None:
    """Add any missing additive columns to existing _ops tables (idempotent)."""
    for table, columns in _ADDED_COLUMNS.items():
        full = f"{catalog}._ops.{table}"
        existing = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {full}").collect()}
        for column, spec in columns.items():
            if column not in existing:
                log.info("Adding missing column", extra={"table": full, "column": column})
                spark.sql(f"ALTER TABLE {full} ADD COLUMNS ({column} {spec})")


def run(
    catalog: str,
    scope: Literal["source", "model"],
    data_engineers_group: str,
    analysts_group: str,
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
        analysts_group: Name of the reader-tier workspace group to grant
            USE CATALOG on the catalog (typically `ecdh-analysts`). Receives
            no grant on `_ops`. See ADR 0018.
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

    # Back-fill additive columns on tables that pre-date a DDL change (idempotent).
    _apply_added_columns(spark, catalog)

    for name, ddl in VIEWS:
        full = f"{catalog}._ops.{name}"
        log.info("Creating view", extra={"view": full})
        spark.sql(ddl.format(catalog=catalog))

    # Analyst-facing discovery surface (ADR 0019): a curated view over the
    # dataset catalog, in a reader-readable schema separate from _ops.
    log.info("Ensuring discovery schema + view", extra={"catalog": catalog})
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{DISCOVERY_SCHEMA} "
        f"COMMENT 'Analyst-facing data discovery surface. Curated views over "
        f"operational metadata; readable by reader-tier groups without _ops "
        f"access. Owned by _platform bundle. See ADR 0019.'"
    )
    spark.sql(DISCOVERY_DATASETS_VIEW_DDL.format(catalog=catalog))
    spark.sql(
        f"COMMENT ON VIEW {catalog}.{DISCOVERY_SCHEMA}.datasets IS "
        f"'Browse available datasets: what each is, why it matters, its coverage "
        f"and cadence, how to access it, and freshness. Curated from the dataset "
        f"catalog. ADR 0019.'"
    )

    log.info(
        "Applying _ops + discovery schema grants",
        extra={
            "catalog": catalog,
            "engineers_group": data_engineers_group,
            "analysts_group": analysts_group,
        },
    )
    # NOTE: catalog-level USE CATALOG grants are deliberately NOT applied here.
    # Granting a privilege ON a catalog requires MANAGE/ownership on the catalog,
    # which the deploy SP does not have (it only holds USE CATALOG + CREATE
    # SCHEMA). So USE CATALOG for the engineer and analyst groups is a one-time
    # admin step in scripts/setup/grant_catalog_permissions.sql. The SP owns the
    # schemas it creates, so the schema-level grants below succeed. (ADR 0017/0018.)
    #
    # Engineer-tier full access on the _ops schema. Analysts get nothing on
    # _ops by design (ADR 0018).
    grants.grant_schema_engineer(spark, catalog, "_ops", data_engineers_group)
    # Reader-tier on the discovery schema for both groups. Analysts read the
    # curated catalog here in lieu of _ops; the view's ownership chain exposes
    # the underlying _ops rows without granting _ops access (ADR 0019).
    grants.grant_schema_reader(spark, catalog, DISCOVERY_SCHEMA, data_engineers_group)
    grants.grant_schema_reader(spark, catalog, DISCOVERY_SCHEMA, analysts_group)

    # --- Verify the access model (deploy-time gate; ADR 0018) ---
    # Read the grants back and assert they match the intended tiers. A mismatch
    # raises and fails this job, which fails the deploy. The negative assertion
    # (analysts have NO access to _ops) is the security-critical one — it cannot
    # be confirmed by the apply step alone, only by reading grants back.
    log.info("Verifying access model", extra={"catalog": catalog})
    grants.verify_schema_no_access(spark, catalog, "_ops", analysts_group)
    grants.verify_schema_engineer(spark, catalog, "_ops", data_engineers_group)
    grants.verify_schema_reader(spark, catalog, DISCOVERY_SCHEMA, analysts_group)
    grants.verify_schema_reader(spark, catalog, DISCOVERY_SCHEMA, data_engineers_group)
    log.info("Access model verified", extra={"catalog": catalog})

    log.info(
        "Completed _ops setup",
        extra={
            "catalog": catalog,
            "scope": scope,
            "tables": len(UNIVERSAL_TABLES),
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
    parser.add_argument(
        "--analysts-group",
        default="ecdh-analysts",
        help="Reader-tier workspace group to grant USE CATALOG (no _ops access).",
    )
    args = parser.parse_args()
    run(args.catalog, args.scope, args.data_engineers_group, args.analysts_group)


if __name__ == "__main__":
    main()
