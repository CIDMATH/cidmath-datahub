"""Canonical orchestration seam for table-build entrypoints (ADR 0027).

Every build entrypoint — raw ingest, processed conformance, reference build —
shares the same *outer* lifecycle: ensure the target exists, run the work inside
a DQ-recording context (so checks persist even when the build fails), then — only
if the work succeeded — register catalog metadata and apply grants. Historically
each entrypoint hand-rolled that skeleton, which drifted: phase ordering, a
forgotten registration or grant, or DQ not flushed on the failure path.

``run_build`` owns the invariant lifecycle; the caller supplies the *variable*
parts as hooks:

  - ``ensure(spark)``   — create schema/table (idempotent DDL).
  - ``work(ctx)``       — extract, transform, write, and record DQ via
                          ``ctx.recorder``; raise to fail the build (DQ is still
                          flushed). The validate-then-write (in-memory DQ, typical
                          of reference builds) vs write-then-query-validate
                          (query-based DQ, typical of source conformance) choice
                          lives *inside* this hook — the seam stays agnostic.
  - ``register(spark)`` — register ``_ops`` metadata (ADR 0008). Pass ``None`` to
                          explicitly opt out for engineer-only staging layers
                          (e.g. raw) that aren't catalogued; the skip is logged.
  - ``grant(spark)``    — apply schema grants (ADR 0018).

Canonical phase order: ``ensure -> [DQ context: work] -> register -> grant``.
The DQ buffer is flushed on *both* the success and failure paths (DQRecorder is a
context manager); ``register`` and ``grant`` run *only* if ``work`` succeeded, so
a failed build never publishes metadata or opens grants. This is a thin seam, not
a framework: it does not constrain the transform, the DQ strategy, the table
shape, or the update semantics — only the lifecycle around them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from cidmath_datahub.common.dq import DQRecorder, new_run_id
from cidmath_datahub.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


@dataclass(frozen=True)
class BuildContext:
    """What a ``work`` hook receives: the session, the run's DQ recorder, run id."""

    spark: SparkSession
    recorder: DQRecorder
    run_id: str


def run_build(
    *,
    catalog: str,
    pipeline_reference: str,
    ensure: Callable[[SparkSession], None],
    work: Callable[[BuildContext], None],
    grant: Callable[[SparkSession], None],
    register: Callable[[SparkSession], None] | None = None,
    spark: SparkSession | None = None,
) -> str:
    """Run a table build through the canonical phase sequence; return the run id.

    See the module docstring for the contract. ``register=None`` is the explicit
    opt-out for engineer-only staging layers (raw) that aren't catalogued — the
    skip is logged so it reads as a deliberate choice, not an omission. ``spark``
    is injectable for tests; in production it defaults to the active session.
    """
    if spark is None:
        from pyspark.sql import SparkSession as _SparkSession

        spark = _SparkSession.builder.getOrCreate()

    run_id = new_run_id()
    log.info(
        "build starting",
        extra={"pipeline_reference": pipeline_reference, "catalog": catalog, "run_id": run_id},
    )

    ensure(spark)

    with DQRecorder(spark, catalog, run_id, pipeline_reference) as recorder:
        work(BuildContext(spark=spark, recorder=recorder, run_id=run_id))
    # Reached only if work() did not raise; DQ already flushed by DQRecorder.__exit__.

    if register is not None:
        register(spark)
    else:
        log.info(
            "registration skipped (engineer-only staging layer)",
            extra={"pipeline_reference": pipeline_reference, "run_id": run_id},
        )

    grant(spark)

    log.info(
        "build complete",
        extra={"pipeline_reference": pipeline_reference, "catalog": catalog, "run_id": run_id},
    )
    return run_id
