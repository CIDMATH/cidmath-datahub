"""Tests for cidmath_datahub.common.pipeline.run_build (ADR 0027).

These exercise the orchestration lifecycle, not Spark: a stand-in ``spark`` is
passed through to the hooks, and because ``work`` records nothing, the
DQRecorder buffer is empty so ``flush()`` is a no-op (no Spark API is touched).
"""

from __future__ import annotations

import pytest

from cidmath_datahub.common import pipeline as pl


class _Spark:
    """Inert stand-in; run_build only passes it to hooks and the (no-op) recorder."""


@pytest.mark.unit
class TestRunBuild:
    def test_phase_order_happy_path(self):
        calls = []
        sp = _Spark()
        run_id = pl.run_build(
            catalog="ecdh_dev",
            pipeline_reference="test_build",
            ensure=lambda s: calls.append(("ensure", s is sp)),
            work=lambda ctx: calls.append(("work", ctx.spark is sp, bool(ctx.run_id))),
            register=lambda s: calls.append(("register", s is sp)),
            grant=lambda s: calls.append(("grant", s is sp)),
            spark=sp,
        )
        assert [c[0] for c in calls] == ["ensure", "work", "register", "grant"]
        assert all(c[1] for c in calls)  # the same spark reached every hook
        assert isinstance(run_id, str) and run_id

    def test_register_none_is_skipped_but_grant_still_runs(self):
        calls = []
        pl.run_build(
            catalog="ecdh_dev",
            pipeline_reference="raw_staging",
            ensure=lambda s: calls.append("ensure"),
            work=lambda ctx: calls.append("work"),
            register=None,
            grant=lambda s: calls.append("grant"),
            spark=_Spark(),
        )
        assert calls == ["ensure", "work", "grant"]  # registration deliberately skipped

    def test_work_failure_skips_register_and_grant(self):
        calls = []

        def boom(ctx):
            calls.append("work")
            raise ValueError("blocking DQ failed")

        with pytest.raises(ValueError, match="blocking DQ failed"):
            pl.run_build(
                catalog="ecdh_dev",
                pipeline_reference="test_build",
                ensure=lambda s: calls.append("ensure"),
                work=boom,
                register=lambda s: calls.append("register"),
                grant=lambda s: calls.append("grant"),
                spark=_Spark(),
            )
        # A failed build must not publish metadata or open grants.
        assert calls == ["ensure", "work"]

    def test_build_context_carries_recorder_and_run_id(self):
        seen = {}
        pl.run_build(
            catalog="ecdh_dev",
            pipeline_reference="test_build",
            ensure=lambda s: None,
            work=lambda ctx: seen.update(run_id=ctx.run_id, has_recorder=ctx.recorder is not None),
            grant=lambda s: None,
            spark=_Spark(),
        )
        assert seen["has_recorder"] is True
        assert isinstance(seen["run_id"], str) and seen["run_id"]
