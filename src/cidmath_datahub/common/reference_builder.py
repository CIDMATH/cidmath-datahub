"""Shared reference-table builder (ADR 0036).

One path for every reference subject (ADR 0037 decision 6):

    raw (source catalog) → [processed (source catalog)] → promote canonical (model)

realized as a **two-phase composition** of the ``run_build`` lifecycle seam (ADR
0027), one phase per catalog:

  - **Phase A — source catalog:** ensure staging tables → write raw (+ processed,
    enriched from same-catalog parents) per vintage → validate the staging (DQ
    recorded in the *source* catalog's ``_ops``; **gates the promote**) → register
    the raw/processed rows (source ``_ops``) → grant engineer-tier on the source
    staging schemas.
  - **Phase B — model catalog:** ensure the canonical table → promote per vintage →
    optionally validate the canonical → register the canonical (model ``_ops``) →
    grant reader-tier on the model schema.

The promote gate falls out of the sequencing: Phase A runs the staging validation
inside its ``run_build`` DQ context, so a blocking ``TableDQ`` failure raises out of
Phase A and Phase B never runs — the model-catalog canonical never lands data the
staging failed, at any scale (ADR 0037 decision 8). ``run_build`` itself is unchanged
(one catalog per call).

The caller supplies the *variable* parts via a :class:`ReferenceTableSpec` (config /
composition, not inheritance); the builder owns the *invariant* skeleton: the atomic
per-vintage write (ADR 0034), the two-phase orchestration, per-layer ``_ops``
registration (ADR 0008), and grant + verify (ADR 0018).

Conventions baked in so adopters can't drift:
  - ``update_semantics="vintage_snapshot"`` + per-vintage atomic ``replaceWhere``;
    vintages immutable, a revision is a new vintage key (ADR 0034).
  - source-catalog tables carry the source token (``us_census_state``); the promoted
    canonical stays source-agnostic (``us_state``) — ADR 0006 refinement.
  - enrichment joins run in ``processed`` against **same-source-catalog** tables;
    the builder never reads the model catalog during a build (ADR 0037 decision 1 /
    7 bound c). Cross-level subjects are migrated parents-first by the caller.
  - every materialized layer (raw / processed / canonical) is registered in the
    ``_ops`` of the catalog it lives in, distinguished by ``layer``; grants — not
    (non-)registration — keep raw/processed engineer-only.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from cidmath_datahub.common import grants, registration
from cidmath_datahub.common.dq import TableDQ
from cidmath_datahub.common.logging import get_logger
from cidmath_datahub.common.pipeline import BuildContext, run_build
from cidmath_datahub.common.vocabularies import is_valid_update_semantics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cidmath_datahub.common.registration import DatasetCatalogEntry

log = get_logger(__name__)

# Hook type aliases.
EnsureFn = Callable[["SparkSession"], None]
PerVintageFrameFn = Callable[[BuildContext, int], "DataFrame"]
ValidateFn = Callable[[BuildContext, str], None]


@dataclass(frozen=True, kw_only=True)
class ReferenceTableSpec:
    """Everything the builder needs to take ONE reference table through the path.

    A multi-level subject (geography: state → county → tract → block-group → block)
    is a *sequence* of specs built **parents-first** by the caller — each parent's
    ``processed`` table must exist before a child's ``process`` joins it same-catalog
    (ADR 0037 decision 7 bound c). The builder builds one spec.

    Naming (ADR 0003 / 0006 refinement): ``source_table`` is the source-tokened name
    used for both the ``_raw`` and ``_processed`` tables; ``canonical_table`` is the
    source-agnostic name promoted to the model catalog.
    """

    # --- identity / placement ---
    subject: str  # schema name, e.g. "geography"
    source_table: str  # source-tokened, e.g. "us_census_state"
    canonical_table: str  # source-agnostic, e.g. "us_state"
    source_catalog: str  # e.g. "ecdh_dev"
    model_catalog: str  # e.g. "ecdh_model_dev"
    pipeline_reference: str  # for _ops + DQ join-back

    # --- governance (ADR 0018) ---
    reader_groups: Sequence[str]  # reader-tier on the model schema (engineers + analysts)
    engineer_group: str  # engineer-tier on the source staging schemas

    # --- provenance (ADR 0008); builder sets full_table_name + layer + derived_from per table ---
    catalog_entry: DatasetCatalogEntry

    # --- phase hooks ---
    ensure_staging: EnsureFn  # idempotent DDL for source-catalog raw (+ processed)
    acquire_raw: PerVintageFrameFn  # fetch OR generate; the raw frame for one vintage
    validate_staging: ValidateFn  # declare TableDQ over staging; raise = gate the promote
    ensure_canonical: EnsureFn  # idempotent DDL for the model-catalog canonical
    promote: PerVintageFrameFn  # staging → canonical projection for one vintage
    process: PerVintageFrameFn | None = None  # REQUIRED iff has_processed_stage
    validate_canonical: ValidateFn | None = None  # optional post-promote checks

    # --- update / partitioning (ADR 0034) ---
    vintage_column: str = "vintage"  # the replaceWhere key; integer-valued
    update_semantics: str = "vintage_snapshot"
    staging_cluster_columns: Sequence[str] | None = None
    canonical_cluster_columns: Sequence[str] | None = None

    # --- complexity (ADR 0037 decision 2) ---
    has_processed_stage: bool = False

    def __post_init__(self) -> None:
        if not is_valid_update_semantics(self.update_semantics):
            raise ValueError(f"update_semantics {self.update_semantics!r} not in the vocabulary")
        if self.has_processed_stage and self.process is None:
            raise ValueError(
                f"{self.subject}.{self.source_table}: complex spec needs a `process` hook"
            )
        if not self.has_processed_stage and self.process is not None:
            raise ValueError(
                f"{self.subject}.{self.source_table}: `process` given but has_processed_stage=False"
            )

    # --- derived names ---
    @property
    def raw_schema(self) -> str:
        return f"{self.subject}_raw"

    @property
    def processed_schema(self) -> str:
        return f"{self.subject}_processed"

    @property
    def raw_fqn(self) -> str:
        return f"{self.source_catalog}.{self.raw_schema}.{self.source_table}"

    @property
    def processed_fqn(self) -> str:
        return f"{self.source_catalog}.{self.processed_schema}.{self.source_table}"

    @property
    def canonical_fqn(self) -> str:
        return f"{self.model_catalog}.{self.subject}.{self.canonical_table}"

    @property
    def staging_fqn(self) -> str:
        """The table DQ validates and the promote reads — processed if complex, else raw."""
        return self.processed_fqn if self.has_processed_stage else self.raw_fqn


def build_reference_table(
    spec: ReferenceTableSpec,
    *,
    vintages: Sequence[int],
    spark: SparkSession | None = None,
) -> tuple[str, str]:
    """Build one reference table through the two-phase path; return ``(phase_a, phase_b)`` run ids.

    Phase A (source catalog) and Phase B (model catalog) are each a ``run_build``
    invocation. Phase A's staging validation gates Phase B: if it raises, Phase B
    never runs. ``spark`` is resolved once and shared across both phases.
    """
    if spark is None:
        from pyspark.sql import SparkSession as _SparkSession

        spark = _SparkSession.builder.getOrCreate()

    # ---- Phase A: source-catalog staging -------------------------------------
    def _work_staging(ctx: BuildContext) -> None:
        for v in vintages:
            _write_vintage(
                ctx.spark, spec.raw_fqn, spec.acquire_raw(ctx, v), spec.vintage_column, v
            )
            if spec.has_processed_stage:
                assert spec.process is not None  # guaranteed by __post_init__
                _write_vintage(
                    ctx.spark, spec.processed_fqn, spec.process(ctx, v), spec.vintage_column, v
                )
        # Validate the staging and GATE the promote (ADR 0037 decision 8): a blocking
        # TableDQ failure raises here, so Phase B below is never reached.
        spec.validate_staging(ctx, spec.staging_fqn)

    def _register_staging(spark: SparkSession) -> None:
        registration.register_dataset(
            spark,
            spec.source_catalog,
            _layer_catalog_entry(spec, spec.raw_fqn, layer="raw", derived_from=None),
            _layer_engineering_entry(spec, spec.raw_fqn, spec.staging_cluster_columns),
        )
        if spec.has_processed_stage:
            registration.register_dataset(
                spark,
                spec.source_catalog,
                _layer_catalog_entry(
                    spec, spec.processed_fqn, layer="processed", derived_from=[spec.raw_fqn]
                ),
                _layer_engineering_entry(spec, spec.processed_fqn, spec.staging_cluster_columns),
            )

    def _grant_staging(spark: SparkSession) -> None:
        schemas = [spec.raw_schema] + ([spec.processed_schema] if spec.has_processed_stage else [])
        for schema in schemas:
            grants.grant_schema_engineer(spark, spec.source_catalog, schema, spec.engineer_group)
            grants.verify_schema_engineer(spark, spec.source_catalog, schema, spec.engineer_group)

    phase_a = run_build(
        catalog=spec.source_catalog,
        pipeline_reference=f"{spec.pipeline_reference}#staging",
        ensure=spec.ensure_staging,
        work=_work_staging,
        register=_register_staging,
        grant=_grant_staging,
        spark=spark,
    )

    # ---- Phase B: model-catalog promote (reached only if Phase A did not raise) ----
    def _work_promote(ctx: BuildContext) -> None:
        for v in vintages:
            _write_vintage(
                ctx.spark, spec.canonical_fqn, spec.promote(ctx, v), spec.vintage_column, v
            )
        if spec.validate_canonical is not None:
            spec.validate_canonical(ctx, spec.canonical_fqn)

    def _register_canonical(spark: SparkSession) -> None:
        derived = [spec.processed_fqn] if spec.has_processed_stage else [spec.raw_fqn]
        registration.register_dataset(
            spark,
            spec.model_catalog,
            _layer_catalog_entry(spec, spec.canonical_fqn, layer="reference", derived_from=derived),
            _layer_engineering_entry(spec, spec.canonical_fqn, spec.canonical_cluster_columns),
        )

    def _grant_canonical(spark: SparkSession) -> None:
        for group in spec.reader_groups:
            grants.grant_schema_reader(spark, spec.model_catalog, spec.subject, group)
            grants.verify_schema_reader(spark, spec.model_catalog, spec.subject, group)

    phase_b = run_build(
        catalog=spec.model_catalog,
        pipeline_reference=spec.pipeline_reference,
        ensure=spec.ensure_canonical,
        work=_work_promote,
        register=_register_canonical,
        grant=_grant_canonical,
        spark=spark,
    )

    log.info(
        "reference table built",
        extra={"table": spec.canonical_fqn, "phase_a_run": phase_a, "phase_b_run": phase_b},
    )
    return phase_a, phase_b


def _layer_catalog_entry(
    spec: ReferenceTableSpec,
    full_table_name: str,
    *,
    layer: str,
    derived_from: list[str] | None,
) -> DatasetCatalogEntry:
    """Clone the spec's base provenance entry for one layer (ADR 0008).

    The base ``catalog_entry`` carries the shared provenance (source URL, license,
    provider/origin codes, access tier, …); the builder overrides only the
    per-layer fields. ``layer`` ∈ raw / processed / reference.
    """
    return replace(
        spec.catalog_entry,
        full_table_name=full_table_name,
        layer=layer,
        derived_from=derived_from,
    )


def _layer_engineering_entry(
    spec: ReferenceTableSpec,
    full_table_name: str,
    cluster_columns: Sequence[str] | None,
) -> registration.DatasetEngineeringEntry:
    """Engineering row for one layer — all formulaic from the spec (ADR 0008)."""
    return registration.DatasetEngineeringEntry(
        full_table_name=full_table_name,
        update_semantics=spec.update_semantics,
        materialization_type="table",
        cluster_columns=list(cluster_columns) if cluster_columns else None,
        pipeline_reference=spec.pipeline_reference,
    )


def _write_vintage(
    spark: SparkSession,
    full_table_name: str,
    df: DataFrame,
    vintage_column: str,
    vintage: int,
) -> None:
    """Atomically replace exactly the rows for ``vintage`` (ADR 0034 vintage_snapshot).

    Delta ``replaceWhere`` makes the per-vintage swap atomic and leaves every other
    vintage untouched — replacing the non-atomic DELETE+append the old builds used.
    The caller's frame must contain only rows for ``vintage`` (the predicate scopes
    the overwrite to that one vintage). The target table is created on first write if
    absent, but callers should ``ensure`` it first so schema/clustering are explicit.
    Per-vintage writes are also the chunking boundary for large grains (block ~8M
    rows/vintage, ADR 0020) — each vintage is one Spark job, nothing accumulates on
    the driver. ``vintage`` is integer-valued; a subject needing a composite or
    string vintage key would extend the predicate here.
    """
    predicate = f"{vintage_column} = {int(vintage)}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(full_table_name)
    )
    log.info(
        "vintage written",
        extra={"table": full_table_name, "vintage": vintage, "predicate": predicate},
    )


def make_staging_dq(
    ctx: BuildContext,
    staging_fqn: str,
    *,
    record_table: str,
    where: str | None = None,
) -> TableDQ:
    """A ``TableDQ`` bound to the staging table, for use inside a ``validate_staging`` hook.

    ``record_table`` is the schema.table the result is recorded under in
    ``_ops.dq_results`` (the ADR 0019 discovery join expects schema.table, not the
    catalog-qualified name). ``where`` scopes a check to one vintage (e.g. for
    FK-within-vintage checks).
    """
    return TableDQ(
        recorder=ctx.recorder,
        spark=ctx.spark,
        query_table=staging_fqn,
        record_table=record_table,
        where=where,
    )
