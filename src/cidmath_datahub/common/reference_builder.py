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

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
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
FetchToVolumeFn = Callable[[int, str], None]  # (vintage, volume_dir) -> writes payload files
ReadFromVolumeFn = Callable[[BuildContext, int, str], "DataFrame"]  # (ctx, vintage, volume_dir)


class LandingRetention(StrEnum):
    """How a raw source payload is retained in the landing Volume (ADR 0039).

    Mirrors the table's update_semantics, chosen from the same source-behavior call.
    """

    # one payload per vintage, immutable; fetch once, skip if already present
    PER_VINTAGE_IMMUTABLE = "per_vintage_immutable"
    # timestamped snapshot each run, never overwrite a date (revise-in-place; ADR 0032)
    SNAPSHOT_PER_RUN = "snapshot_per_run"
    # one payload per extraction batch/window
    PER_BATCH = "per_batch"
    # no Volume — generated reference (no extraction), or a not-yet-migrated direct acquire
    NONE = "none"


@dataclass(frozen=True, kw_only=True)
class RawLanding:
    """One source payload landed 1:1 into ``<source_catalog>.<subject>_raw`` (ADR 0039).

    Two shapes:
      - **Volume-backed** (``landing_retention != NONE``): ``fetch_to_volume(vintage, dir)``
        writes the verbatim extracted payload (file, or an API/query response) into a
        landing Volume; ``read_from_volume(ctx, vintage, dir)`` reads it into the 1:1 raw
        DataFrame. The builder fetches once for immutable vintages (skip-if-present) and a
        fresh snapshot per run otherwise.
      - **Direct** (``landing_retention == NONE``): ``acquire(ctx, vintage)`` returns the raw
        DataFrame with no Volume — for purely generated reference (no extraction) or a
        not-yet-migrated source.

    Either way the raw table is a faithful 1:1 copy; derivation happens in ``process``.
    """

    table: str  # source-tokened name, e.g. "us_census_state", "us_census_state_cenpop"
    landing_retention: LandingRetention = LandingRetention.NONE
    acquire: PerVintageFrameFn | None = None
    fetch_to_volume: FetchToVolumeFn | None = None
    read_from_volume: ReadFromVolumeFn | None = None
    description: str | None = None  # _ops row (layer=raw); falls back to the build's base entry

    def __post_init__(self) -> None:
        if self.landing_retention == LandingRetention.NONE:
            if self.acquire is None:
                raise ValueError(f"{self.table}: landing_retention=none needs an `acquire` hook")
            if self.fetch_to_volume is not None or self.read_from_volume is not None:
                raise ValueError(
                    f"{self.table}: a direct (`acquire`) landing must not set Volume hooks"
                )
        else:
            if self.fetch_to_volume is None or self.read_from_volume is None:
                raise ValueError(
                    f"{self.table}: Volume landing needs `fetch_to_volume` + `read_from_volume`"
                )
            if self.acquire is not None:
                raise ValueError(f"{self.table}: a Volume-backed landing must not set `acquire`")

    @property
    def is_volume_backed(self) -> bool:
        return self.landing_retention != LandingRetention.NONE


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

    run_date = datetime.now(tz=UTC).date().isoformat()

    # ---- Phase 0: land verbatim source payloads in the Volume (ADR 0039) ------
    # Fetch each Volume-backed landing's payload into the landing Volume before any
    # parsing; immutable vintages are skipped if already present (zero re-fetch).
    _ensure_and_fetch_volume(spec, spark, vintages, run_date)

    # ---- Phase A: source-catalog staging -------------------------------------
    def _work_staging(ctx: BuildContext) -> None:
        for v in vintages:
            for landing in spec.raw_landings:
                if landing.is_volume_backed:
                    vdir = _landing_volume_dir(spec, landing, v, run_date)
                    df = landing.read_from_volume(ctx, v, vdir)
                else:
                    df = landing.acquire(ctx, v)
                _write_vintage(ctx.spark, spec.raw_fqn(landing.table), df, spec.vintage_column, v)
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


def _landing_volume(spec: ReferenceBuildSpec) -> str:
    """The fully-qualified landing Volume for a build's raw payloads (ADR 0039)."""
    return f"{spec.source_catalog}.{spec.raw_schema}._landing"


def _landing_volume_dir(
    spec: ReferenceBuildSpec, landing: RawLanding, vintage: int, run_date: str
) -> str:
    """Filesystem path (under the landing Volume) for one landing's payload (ADR 0039)."""
    root = f"/Volumes/{spec.source_catalog}/{spec.raw_schema}/_landing/{landing.table}"
    if landing.landing_retention == LandingRetention.PER_VINTAGE_IMMUTABLE:
        return f"{root}/vintage={int(vintage)}"
    if landing.landing_retention == LandingRetention.SNAPSHOT_PER_RUN:
        return f"{root}/snapshot_date={run_date}"
    # PER_BATCH: use the vintage as the batch-window key for now (not yet exercised).
    return f"{root}/batch={int(vintage)}"


def _volume_dir_has_content(path: str) -> bool:
    """True if the Volume dir exists and is non-empty (driver FUSE; safe-False on error)."""
    try:
        return os.path.isdir(path) and bool(os.listdir(path))
    except OSError:
        return False


def _ensure_and_fetch_volume(
    spec: ReferenceBuildSpec,
    spark: SparkSession,
    vintages: Sequence[int],
    run_date: str,
) -> None:
    """Phase 0: create the landing Volume and fetch each payload into it (ADR 0039).

    Fetch is per ``(landing, vintage)``. ``PER_VINTAGE_IMMUTABLE`` skips a fetch only when
    *that landing's* payload for *that vintage* is already present — so re-running the same
    landings+vintages does zero fetches, while a new vintage fetches only the missing combos.
    ``SNAPSHOT_PER_RUN`` and ``PER_BATCH`` fetch every run. No-op when the build has no
    Volume-backed landings.
    """
    volume_landings = [landing for landing in spec.raw_landings if landing.is_volume_backed]
    if not volume_landings:
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {spec.source_catalog}.{spec.raw_schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {_landing_volume(spec)} "
        f"COMMENT 'Verbatim raw source payloads — engineer-only landing zone. ADR 0039.'"
    )
    # READ/WRITE VOLUME is a volume-scoped privilege, separate from the schema grants —
    # the build reads + writes the landing files, so grant the engineer principal on it.
    grants.grant_volume_engineer(
        spark, spec.source_catalog, spec.raw_schema, "_landing", spec.engineer_group
    )
    for landing in volume_landings:
        for v in vintages:
            vdir = _landing_volume_dir(spec, landing, v, run_date)
            if landing.landing_retention == LandingRetention.PER_VINTAGE_IMMUTABLE and (
                _volume_dir_has_content(vdir)
            ):
                log.info("landing payload present; skipping fetch", extra={"dir": vdir})
                continue
            os.makedirs(vdir, exist_ok=True)
            assert landing.fetch_to_volume is not None  # guaranteed for volume-backed landings
            landing.fetch_to_volume(v, vdir)
            log.info(
                "fetched landing payload",
                extra={"table": landing.table, "vintage": v, "dir": vdir},
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
