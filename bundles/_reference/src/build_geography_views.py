"""Build the geography hierarchical-filter views (ADR 0028).

Creates ``geography.us_county_enriched`` and ``geography.us_tract_enriched`` --
convenience views that denormalize stable parent *display* attributes (state
name / USPS / HHS region; county name) onto the child levels, so analysts can
filter by the human-readable parent ("counties in Georgia", "tracts in Fulton
County") without hand-writing the hierarchy joins. The parent geoids already
live on the child tables; these views add the labels, leaving the canonical
entity tables normalized (ADR 0028).

View SQL is single-sourced (and unit-tested) in
``cidmath_datahub.reference.geography.us_enriched_view_definitions``. This is a
thin entrypoint (ADR 0011) over the shared ``run_build`` seam (ADR 0027) --
ensure -> [DQ: work] -> register -> grant. Deploy order: after build_geography
(the us_state/us_county/us_tract entity tables must exist).

Usage:
    build_geography_views.py --catalog ecdh_model_dev \\
        --data-engineers-group ecdh-data-engineers --analysts-group ecdh-analysts
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import DQCategory, DQSeverity
from cidmath_datahub.reference import geography as geo

log = get_logger(__name__)

SCHEMA = "geography"
PIPELINE_REF = "bundles/_reference/src/build_geography_views.py"

# IPUMS NHGIS provenance (mirrors build_geography.py so the views' catalog rows
# match their base tables').
NHGIS_SOURCE_URL = "https://www.nhgis.org/"
NHGIS_DOC_URL = "https://www.nhgis.org/documentation"
NHGIS_LICENSE = (
    "IPUMS NHGIS terms of use: citation and attribution required; "
    "redistribution restricted (permission requested)."
)
NHGIS_DUA_REFERENCE = "IPUMS NHGIS citation required; see https://www.nhgis.org/ for terms."
NHGIS_MAINTAINER = "IPUMS NHGIS, University of Minnesota"

# enriched view (short name) -> the base table whose row count it must equal
# (INNER join to parents must not drop a child; FK integrity guarantees it).
_VIEW_BASE = {"us_county_enriched": "us_county", "us_tract_enriched": "us_tract"}


def _ensure(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}")


def _work(ctx: BuildContext, catalog: str) -> None:
    """Create the views, then assert each one's rowcount equals its base table's."""
    spark = ctx.spark
    g = f"{catalog}.{SCHEMA}"
    for fq_name, sql in geo.us_enriched_view_definitions(catalog).items():
        spark.sql(sql)
        log.info("Created view", extra={"view": fq_name})

    failures: list[tuple[str, int, int]] = []
    for short, base in _VIEW_BASE.items():
        view_n = spark.sql(f"SELECT COUNT(*) AS n FROM {g}.{short}").collect()[0]["n"]
        base_n = spark.sql(f"SELECT COUNT(*) AS n FROM {g}.{base}").collect()[0]["n"]
        ok = view_n == base_n
        ctx.recorder.record(
            table_name=f"{SCHEMA}.{short}",
            check_name=f"{short}_parent_join_completeness",
            category=DQCategory.REFERENTIAL,
            severity=DQSeverity.FAIL,
            passed=ok,
            failing_row_count=abs(int(base_n) - int(view_n)),
            total_row_count=int(base_n),
            details={"view_rows": int(view_n), "base_rows": int(base_n), "base": base}
            if not ok
            else None,
        )
        if not ok:
            failures.append((short, int(view_n), int(base_n)))
    if failures:
        raise ValueError(f"Enriched view rowcount != base (orphan parent join): {failures}")


def _register(spark: SparkSession, catalog: str) -> None:
    g = f"{catalog}.{SCHEMA}"
    specs = {
        "us_county_enriched": {
            "description": (
                "us_county enriched with state name / USPS / HHS region for hierarchical "
                "filtering (select counties in a state by name). View over us_county + "
                "us_state, vintage-keyed. ADR 0028."
            ),
            "spatial_resolution": "us_county",
            "derived_from": [f"{g}.us_county", f"{g}.us_state"],
        },
        "us_tract_enriched": {
            "description": (
                "us_tract enriched with county name + state name / USPS / HHS region for "
                "hierarchical filtering (select tracts in a county by name). View over "
                "us_tract + us_county + us_state, vintage-keyed. ADR 0028."
            ),
            "spatial_resolution": "us_tract",
            "derived_from": [f"{g}.us_tract", f"{g}.us_county", f"{g}.us_state"],
        },
    }
    for short, s in specs.items():
        full = f"{g}.{short}"
        registration.register_dataset(
            spark,
            catalog,
            registration.DatasetCatalogEntry(
                full_table_name=full,
                subject="geography",
                layer="reference",
                description=s["description"],
                public_health_relevance=(
                    "Convenience surface for selecting child geographies by their parent "
                    "(state/county) without hierarchy joins -- a common analyst/dashboard need."
                ),
                spatial_resolution=s["spatial_resolution"],
                spatial_coverage="United States",
                source_provider_code="ipums_nhgis",
                source_url=NHGIS_SOURCE_URL,
                source_documentation_url=NHGIS_DOC_URL,
                license=NHGIS_LICENSE,
                dua_required=True,
                dua_reference=NHGIS_DUA_REFERENCE,
                access_tier="restricted",
                external_maintainer_name=NHGIS_MAINTAINER,
                is_hosted=False,  # view, not materialized
                derived_from=s["derived_from"],
            ),
            registration.DatasetEngineeringEntry(
                full_table_name=full,
                update_semantics="full_refresh",  # a view is recomputed on every read
                materialization_type="view",
                cluster_columns=None,
                pipeline_reference=PIPELINE_REF,
            ),
        )


def run(catalog: str, data_engineers_group: str, analysts_group: str) -> None:
    def _grant(spark: SparkSession) -> None:
        # Schema-level reader grants already cover new views; re-assert for both
        # reader-tier groups (idempotent) so the views are explicitly readable.
        grants.grant_schema_reader(spark, catalog, SCHEMA, data_engineers_group)
        grants.grant_schema_reader(spark, catalog, SCHEMA, analysts_group)

    run_build(
        catalog=catalog,
        pipeline_reference=PIPELINE_REF,
        ensure=lambda spark: _ensure(spark, catalog),
        work=lambda ctx: _work(ctx, catalog),
        register=lambda spark: _register(spark, catalog),
        grant=_grant,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Integrated catalog (ecdh_model_<env>).")
    parser.add_argument("--data-engineers-group", default="ecdh-data-engineers")
    parser.add_argument("--analysts-group", default="ecdh-analysts")
    args = parser.parse_args()
    run(args.catalog, args.data_engineers_group, args.analysts_group)


if __name__ == "__main__":
    main()
