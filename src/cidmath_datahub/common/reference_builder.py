"""Shared reference-table builder (ADR 0036).

One path for every reference subject (ADR 0037 decision 6), modelled as a
**subject pipeline**:

    raw landings (1:1 source copies, source catalog)
        → [processed: derive / split / join, source catalog]
        → canonical promotes (model catalog)

A build can have **multiple** raw landings and **multiple** canonical outputs:
raw stays a faithful 1:1 copy of each source *file* (a TIGER shapefile, a CenPop
file), and any splitting (attributes vs geometry) or joining (CenPop centroids,
parent labels) happens in ``processed``. A simple subject degenerates to one
landing + one output with no processed stage.

Realized as a **two-phase composition** of the ``run_build`` lifecycle seam
(ADR 0027), one phase per catalog:

  - **Phase A — source catalog:** ensure staging → land each raw (1:1) per vintage
    → run each output's ``process`` (per vintage) → validate the staging (DQ in the
    *source* ``_ops``; **gates the promote**) → register raw + processed rows →
    grant engineer-tier on the source staging schemas.
  - **Phase B — model catalog:** ensure canonical tables → promote each output per
    vintage → optionally validate → register canonical rows (model ``_ops``) →
    grant reader-tier on the model schema.

The promote gate falls out of sequencing: Phase A runs the staging validation
inside its ``run_build`` DQ context, so a blocking ``TableDQ`` failure raises out
of Phase A and Phase B never runs (ADR 0037 decision 8). ``run_build`` itself is
unchanged (one catalog per call).

Conventions baked in so adopters can't drift:
  - ``update_semantics="vintage_snapshot"`` + per-vintage atomic ``replaceWhere``;
    vintages immutable (ADR 0034).
  - source-catalog tables carry the source token (``us_census_state``); promoted
    canonicals stay source-agnostic (``us_state``) — ADR 0006 refinement.
  - raw is a strict 1:1 copy of the source file; derivation lives in ``processed``,
    which joins only **same-source-catalog** tables — the builder never reads the
    model catalog during a build (ADR 0037 decision 1 / 7 bound c). Cross-level
    subjects are ordered parents-first by the caller.
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
class RawLanding:
    """One source file copied 1:1 into ``<source_catalog>.<subject>_raw``.

    ``acquire(ctx, vintage)`` reads the source file (fetch or generate) and returns
    a DataFrame for that vintage — a faithful copy, carrying the vintage column. No
    derivation here; splitting/joining happens in an output's ``process``.
    """

    table: str  # source-tokened name, e.g. "us_census_state", "us_census_state_cenpop"
    acquire: PerVintageFrameFn
    description: str | None = None  # _ops row (layer=raw); falls back to the build's base entry


@dataclass(frozen=True, kw_only=True)
class CanonicalOutput:
    """One promoted canonical table, optionally via a processed derivation.

    ``reads`` names the raw landing table(s) this output derives from (lineage, and
    — when there is no processed stage — the table the ``promote`` reads). If
    ``process`` is given, the builder writes ``processed_table`` from it and the
    promote/validation target is that processed table; otherwise the output is
    simple (``promote`` reads its single raw landing directly).
    """

    canonical_table: str  # source-agnostic, e.g. "us_state", "us_state_boundary"
    reads: Sequence[str]  # raw landing table name(s) consumed
    promote: PerVintageFrameFn
    validate_staging: ValidateFn
    process: PerVintageFrameFn | None = None
    processed_table: str | None = None  # required iff process is given
    validate_canonical: ValidateFn | None = None
    staging_cluster_columns: Sequence[str] | None = None
    canonical_cluster_columns: Sequence[str] | None = None
    description: str | None = None
    public_health_relevance: str | None = None

    def __post_init__(self) -> None:
        if self.process is not None and not self.processed_table:
            raise ValueError(f"{self.canonical_table}: `process` given but no `processed_table`")
        if self.process is None and len(self.reads) != 1:
            raise ValueError(
                f"{self.canonical_table}: a no-processed output must read exactly one raw landing"
            )


@dataclass(frozen=True, kw_only=True)
class ReferenceBuildSpec:
    """A whole reference build: raw landings → [processed] → canonical outputs.

    Multi-level subjects (geography: state → county → … → block) are a *sequence*
    of these specs built **parents-first** by the caller, so a child's ``process``
    can join an already-built parent's processed table same-catalog (ADR 0037
    decision 7 bound c).
    """

    subject: str  # schema name, e.g. "geography"
    source_catalog: str  # e.g. "ecdh_dev"
    model_catalog: str  # e.g. "ecdh_model_dev"
    pipeline_reference: str

    reader_groups: Sequence[str]  # reader-tier on the model schema
    engineer_group: str  # engineer-tier on the source staging schemas

    base_catalog_entry: DatasetCatalogEntry  # shared provenance; builder sets per-table fields
    raw_landings: Sequence[RawLanding]
    outputs: Sequence[CanonicalOutput]

    ensure_staging: EnsureFn  # idempotent DDL for raw (+ processed) tables
    ensure_canonical: EnsureFn  # idempotent DDL for the canonical tables

    vintage_column: str = "vintage"
    update_semantics: str = "vintage_snapshot"

    def __post_init__(self) -> None:
        if not is_valid_update_semantics(self.update_semantics):
            raise ValueError(f"update_semantics {self.update_semantics!r} not in the vocabulary")
        if not self.raw_landings:
            raise ValueError(f"{self.subject}: a build needs at least one raw landing")
        if not self.outputs:
            raise ValueError(f"{self.subject}: a build needs at least one canonical output")
        landing_names = {landing.table for landing in self.raw_landings}
        for out in self.outputs:
            unknown = set(out.reads) - landing_names
            if unknown:
                raise ValueError(f"{out.canonical_table}: reads unknown raw landing(s) {unknown}")

    # --- derived names ---
    @property
    def raw_schema(self) -> str:
        return f"{self.subject}_raw"

    @property
    def processed_schema(self) -> str:
        return f"{self.subject}_processed"

    @property
    def has_processed(self) -> bool:
        return any(out.process is not None for out in self.outputs)

    def raw_fqn(self, table: str) -> str:
        return f"{self.source_catalog}.{self.raw_schema}.{table}"

    def processed_fqn(self, out: CanonicalOutput) -> str:
        return f"{self.source_catalog}.{self.processed_schema}.{out.processed_table}"

    def canonical_fqn(self, out: CanonicalOutput) -> str:
        return f"{self.model_catalog}.{self.subject}.{out.canonical_table}"

    def staging_fqn(self, out: CanonicalOutput) -> str:
        """What DQ validates and the promote reads — processed if derived, else the raw."""
        return self.processed_fqn(out) if out.process is not None else self.raw_fqn(out.reads[0])


def build_reference(
    spec: ReferenceBuildSpec,
    *,
    vintages: Sequence[int],
    spark: SparkSession | None = None,
) -> tuple[str, str]:
    """Run a reference build through the two-phase path; return ``(phase_a, phase_b)`` run ids.

    Phase A (source catalog) and Phase B (model catalog) are each a ``run_build``
    invocation; Phase A's staging validation gates Phase B. ``spark`` is resolved
    once and shared across both phases.
    """
    if spark is None:
        from pyspark.sql import SparkSession as _SparkSession

        spark = _SparkSession.builder.getOrCreate()

    # ---- Phase A: source-catalog staging -------------------------------------
    def _work_staging(ctx: BuildContext) -> None:
        for v in vintages:
            for landing in spec.raw_landings:
                _write_vintage(
                    ctx.spark,
                    spec.raw_fqn(landing.table),
                    landing.acquire(ctx, v),
                    spec.vintage_column,
                    v,
                )
            for out in spec.outputs:
                if out.process is not None:
                    _write_vintage(
                        ctx.spark,
                        spec.processed_fqn(out),
                        out.process(ctx, v),
                        spec.vintage_column,
                        v,
                    )
        # Validate each output's staging and GATE the promote (ADR 0037 decision 8):
        # a blocking TableDQ failure raises here, so Phase B below never runs.
        for out in spec.outputs:
            out.validate_staging(ctx, spec.staging_fqn(out))

    def _register_staging(spark: SparkSession) -> None:
        for landing in spec.raw_landings:
            registration.register_dataset(
                spark,
                spec.source_catalog,
                _layer_catalog_entry(
                    spec,
                    spec.raw_fqn(landing.table),
                    layer="raw",
                    derived_from=None,
                    description=landing.description,
                ),
                _layer_engineering_entry(spec, spec.raw_fqn(landing.table), None),
            )
        for out in spec.outputs:
            if out.process is not None:
                registration.register_dataset(
                    spark,
                    spec.source_catalog,
                    _layer_catalog_entry(
                        spec,
                        spec.processed_fqn(out),
                        layer="processed",
                        derived_from=[spec.raw_fqn(t) for t in out.reads],
                        description=out.description,
                    ),
                    _layer_engineering_entry(
                        spec, spec.processed_fqn(out), out.staging_cluster_columns
                    ),
                )

    def _grant_staging(spark: SparkSession) -> None:
        schemas = [spec.raw_schema] + ([spec.processed_schema] if spec.has_processed else [])
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
            for out in spec.outputs:
                _write_vintage(
                    ctx.spark, spec.canonical_fqn(out), out.promote(ctx, v), spec.vintage_column, v
                )
        for out in spec.outputs:
            if out.validate_canonical is not None:
                out.validate_canonical(ctx, spec.canonical_fqn(out))

    def _register_canonical(spark: SparkSession) -> None:
        for out in spec.outputs:
            lineage = (
                [spec.processed_fqn(out)]
                if out.process is not None
                else [spec.raw_fqn(out.reads[0])]
            )
            registration.register_dataset(
                spark,
                spec.model_catalog,
                _layer_catalog_entry(
                    spec,
                    spec.canonical_fqn(out),
                    layer="reference",
                    derived_from=lineage,
                    description=out.description,
                    public_health_relevance=out.public_health_relevance,
                ),
                _layer_engineering_entry(
                    spec, spec.canonical_fqn(out), out.canonical_cluster_columns
                ),
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
        "reference build complete",
        extra={
            "subject": spec.subject,
            "outputs": [out.canonical_table for out in spec.outputs],
            "phase_a_run": phase_a,
            "phase_b_run": phase_b,
        },
    )
    return phase_a, phase_b


def _layer_catalog_entry(
    spec: ReferenceBuildSpec,
    full_table_name: str,
    *,
    layer: str,
    derived_from: list[str] | None,
    description: str | None = None,
    public_health_relevance: str | None = None,
) -> DatasetCatalogEntry:
    """Clone the build's base provenance entry for one layer/table (ADR 0008).

    The base entry carries shared provenance (source URL, license, provider/origin
    codes, access tier, …); only the per-table fields are overridden. ``layer`` ∈
    raw / processed / reference.
    """
    overrides: dict[str, object] = {
        "full_table_name": full_table_name,
        "layer": layer,
        "derived_from": derived_from,
    }
    if description is not None:
        overrides["description"] = description
    if public_health_relevance is not None:
        overrides["public_health_relevance"] = public_health_relevance
    return replace(spec.base_catalog_entry, **overrides)


def _layer_engineering_entry(
    spec: ReferenceBuildSpec,
    full_table_name: str,
    cluster_columns: Sequence[str] | None,
) -> registration.DatasetEngineeringEntry:
    """Engineering row for one table — all formulaic from the spec (ADR 0008)."""
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
    vintage untouched. The caller's frame must contain only rows for ``vintage``.
    The target table is created on first write if absent, but callers should
    ``ensure`` it first so schema/clustering are explicit. Per-vintage writes are
    also the chunking boundary for large grains (block ~8M rows/vintage, ADR 0020) —
    each vintage is one Spark job, nothing accumulates on the driver. ``vintage`` is
    integer-valued; a composite/string vintage key would extend the predicate here.
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
    """A ``TableDQ`` bound to a staging table, for use inside a ``validate_staging`` hook.

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
