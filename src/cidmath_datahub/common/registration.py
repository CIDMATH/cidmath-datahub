"""Register a materialized table in the ``_ops`` catalog metadata (ADR 0008).

Every materialized analysis/reference table records two rows: one in
``_ops.dataset_catalog`` (universal provenance — source, license, DUA, access
tier, public-health relevance) and one in ``_ops.dataset_engineering``
(engineering state — update semantics, materialization type, cluster columns,
pipeline reference). The MERGE that writes them is identical across every build
job, so it lived as ~80 copy-pasted lines in each (and a column-count mismatch
in one copy caused a real bug). This module is the single implementation.

Two notes on the MERGE shape that the copies all had to get right and which a
shared helper now guarantees:
  - ``_ops.dataset_catalog`` has more columns than the source, so
    ``MERGE ... UPDATE SET *`` fails — the UPDATE/INSERT clauses must list
    columns explicitly.
  - ``last_validated`` (catalog) and ``last_refresh_at`` (engineering) are set
    by the MERGE itself (``CURRENT_DATE()`` / ``CURRENT_TIMESTAMP()``), not
    carried on the entry objects.

Spark types are imported lazily so the wheel imports (and the dataclasses unit-
test) without pyspark; the dataclasses validate controlled-vocabulary values at
construction (belt-and-suspenders alongside the CI convention scan, ADR 0016).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.vocabularies import (
    MATERIALIZATION_TYPE_VALUES,
    UPDATE_SEMANTICS_VALUES,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


@dataclass(frozen=True)
class DatasetCatalogEntry:
    """One ``_ops.dataset_catalog`` row (universal provenance, ADR 0008)."""

    full_table_name: str
    subject: str
    layer: str
    description: str
    public_health_relevance: str
    spatial_resolution: str
    spatial_coverage: str
    source_provider_code: str
    source_url: str
    source_documentation_url: str
    license: str
    dua_required: bool
    dua_reference: str
    access_tier: str
    external_maintainer_name: str
    is_hosted: bool
    owner: str = "cidmath-data-team"


@dataclass(frozen=True)
class DatasetEngineeringEntry:
    """One ``_ops.dataset_engineering`` row (engineering state, ADR 0008).

    Validates ``update_semantics`` and ``materialization_type`` against the
    controlled vocabularies at construction (ADR 0007/0008), so a typo fails
    here rather than producing an out-of-vocabulary metadata row.
    """

    full_table_name: str
    update_semantics: str
    materialization_type: str
    cluster_columns: list[str] | None
    pipeline_reference: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.update_semantics not in UPDATE_SEMANTICS_VALUES:
            raise ValueError(
                f"update_semantics {self.update_semantics!r} not in "
                f"{sorted(UPDATE_SEMANTICS_VALUES)}"
            )
        if self.materialization_type not in MATERIALIZATION_TYPE_VALUES:
            raise ValueError(
                f"materialization_type {self.materialization_type!r} not in "
                f"{sorted(MATERIALIZATION_TYPE_VALUES)}"
            )


def _safe_view_name(prefix: str, full_table_name: str) -> str:
    """A temp-view name unique per table (a run may register several tables)."""
    sanitized = full_table_name.replace(".", "_").replace("-", "_")
    return f"{prefix}_{sanitized}"


def register_dataset(
    spark: SparkSession,
    catalog: str,
    catalog_entry: DatasetCatalogEntry,
    engineering_entry: DatasetEngineeringEntry,
) -> None:
    """MERGE the catalog + engineering rows for one table into ``{catalog}._ops``.

    Idempotent: re-running updates the existing rows (and refreshes
    ``last_validated`` / ``last_refresh_at``) rather than duplicating them.
    """
    from pyspark.sql import types as T

    full = catalog_entry.full_table_name

    cat_schema = T.StructType(
        [
            T.StructField("full_table_name", T.StringType()),
            T.StructField("subject", T.StringType()),
            T.StructField("layer", T.StringType()),
            T.StructField("description", T.StringType()),
            T.StructField("public_health_relevance", T.StringType()),
            T.StructField("spatial_resolution", T.StringType()),
            T.StructField("spatial_coverage", T.StringType()),
            T.StructField("source_provider_code", T.StringType()),
            T.StructField("source_url", T.StringType()),
            T.StructField("source_documentation_url", T.StringType()),
            T.StructField("license", T.StringType()),
            T.StructField("dua_required", T.BooleanType()),
            T.StructField("dua_reference", T.StringType()),
            T.StructField("access_tier", T.StringType()),
            T.StructField("external_maintainer_name", T.StringType()),
            T.StructField("is_hosted", T.BooleanType()),
            T.StructField("owner", T.StringType()),
        ]
    )
    cat_row: list[tuple[Any, ...]] = [
        (
            catalog_entry.full_table_name,
            catalog_entry.subject,
            catalog_entry.layer,
            catalog_entry.description,
            catalog_entry.public_health_relevance,
            catalog_entry.spatial_resolution,
            catalog_entry.spatial_coverage,
            catalog_entry.source_provider_code,
            catalog_entry.source_url,
            catalog_entry.source_documentation_url,
            catalog_entry.license,
            catalog_entry.dua_required,
            catalog_entry.dua_reference,
            catalog_entry.access_tier,
            catalog_entry.external_maintainer_name,
            catalog_entry.is_hosted,
            catalog_entry.owner,
        )
    ]
    cat_view = _safe_view_name("_tmp_reg_cat", full)
    spark.createDataFrame(cat_row, cat_schema).createOrReplaceTempView(cat_view)
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_catalog AS t
        USING {cat_view} AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            subject = s.subject, layer = s.layer, description = s.description,
            public_health_relevance = s.public_health_relevance,
            spatial_resolution = s.spatial_resolution, spatial_coverage = s.spatial_coverage,
            source_provider_code = s.source_provider_code, source_url = s.source_url,
            source_documentation_url = s.source_documentation_url, license = s.license,
            dua_required = s.dua_required, dua_reference = s.dua_reference,
            access_tier = s.access_tier, external_maintainer_name = s.external_maintainer_name,
            is_hosted = s.is_hosted, owner = s.owner, last_validated = CURRENT_DATE()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, subject, layer, description, public_health_relevance,
             spatial_resolution, spatial_coverage, source_provider_code, source_url,
             source_documentation_url, license, dua_required, dua_reference, access_tier,
             external_maintainer_name, is_hosted, owner, last_validated)
            VALUES
            (s.full_table_name, s.subject, s.layer, s.description, s.public_health_relevance,
             s.spatial_resolution, s.spatial_coverage, s.source_provider_code, s.source_url,
             s.source_documentation_url, s.license, s.dua_required, s.dua_reference, s.access_tier,
             s.external_maintainer_name, s.is_hosted, s.owner, CURRENT_DATE())
        """
    )

    eng_schema = T.StructType(
        [
            T.StructField("full_table_name", T.StringType()),
            T.StructField("update_semantics", T.StringType()),
            T.StructField("materialization_type", T.StringType()),
            T.StructField("cluster_columns", T.ArrayType(T.StringType())),
            T.StructField("pipeline_reference", T.StringType()),
            T.StructField("schema_version", T.IntegerType()),
        ]
    )
    eng_row: list[tuple[Any, ...]] = [
        (
            engineering_entry.full_table_name,
            engineering_entry.update_semantics,
            engineering_entry.materialization_type,
            engineering_entry.cluster_columns,
            engineering_entry.pipeline_reference,
            engineering_entry.schema_version,
        )
    ]
    eng_view = _safe_view_name("_tmp_reg_eng", full)
    spark.createDataFrame(eng_row, eng_schema).createOrReplaceTempView(eng_view)
    spark.sql(
        f"""
        MERGE INTO {catalog}._ops.dataset_engineering AS t
        USING {eng_view} AS s
        ON t.full_table_name = s.full_table_name
        WHEN MATCHED THEN UPDATE SET
            update_semantics = s.update_semantics,
            materialization_type = s.materialization_type,
            cluster_columns = s.cluster_columns,
            pipeline_reference = s.pipeline_reference,
            last_refresh_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (full_table_name, update_semantics, materialization_type, cluster_columns,
             pipeline_reference, schema_version, last_refresh_at)
            VALUES
            (s.full_table_name, s.update_semantics, s.materialization_type, s.cluster_columns,
             s.pipeline_reference, s.schema_version, CURRENT_TIMESTAMP())
        """
    )
    log.info("Registered dataset metadata", extra={"table": full})
