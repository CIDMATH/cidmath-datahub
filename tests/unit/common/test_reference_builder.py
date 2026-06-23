"""Tests for cidmath_datahub.common.reference_builder (ADR 0036).

Covers the pure surface — RawLanding / CanonicalOutput / ReferenceBuildSpec
validation, the derived table-name properties, and per-layer entry cloning. The
two-phase build flow needs Spark and is exercised by the build jobs.
"""

from __future__ import annotations

import os

import pytest

from cidmath_datahub.common import reference_builder as rb
from cidmath_datahub.common import registration as reg


def _base_catalog_entry(**overrides):
    defaults = dict(
        full_table_name="placeholder",  # builder overrides per layer/table
        subject="geography",
        layer="reference",
        description="US states, vintaged.",
        public_health_relevance="Canonical state spatial unit.",
        spatial_resolution="us_state",
        spatial_coverage="United States",
        source_provider_code="ipums_nhgis",
        source_origin_code="census",
        source_url="https://www.nhgis.org/",
        source_documentation_url="https://www.nhgis.org/documentation",
        license="NHGIS",
        dua_required=True,
        dua_reference="cite NHGIS",
        access_tier="restricted",
        external_maintainer_name="IPUMS NHGIS",
        is_hosted=True,
    )
    defaults.update(overrides)
    return reg.DatasetCatalogEntry(**defaults)


def _landing(**overrides):
    defaults = dict(table="us_census_state", acquire=lambda _ctx, _v: None)
    defaults.update(overrides)
    return rb.RawLanding(**defaults)


def _volume_landing(**overrides):
    defaults = dict(
        table="us_census_state",
        landing_retention=rb.LandingRetention.PER_VINTAGE_IMMUTABLE,
        fetch_to_volume=lambda _v, _d: None,
        read_from_volume=lambda _ctx, _v, _d: None,
    )
    defaults.update(overrides)
    return rb.RawLanding(**defaults)


def _output(**overrides):
    defaults = dict(
        canonical_table="us_state",
        reads=("us_census_state",),
        promote=lambda _ctx, _v: None,
        validate_staging=lambda _ctx, _t: None,
    )
    defaults.update(overrides)
    return rb.CanonicalOutput(**defaults)


def _processed_output(**overrides):
    defaults = dict(
        canonical_table="us_state",
        reads=("us_census_state", "us_census_state_cenpop"),
        promote=lambda _ctx, _v: None,
        validate_staging=lambda _ctx, _t: None,
        process=lambda _ctx, _v: None,
        processed_table="us_census_state",
    )
    defaults.update(overrides)
    return rb.CanonicalOutput(**defaults)


def _spec(**overrides):
    defaults = dict(
        subject="geography",
        source_catalog="ecdh_dev",
        model_catalog="ecdh_model_dev",
        pipeline_reference="bundles/_reference/src/build_geography.py",
        reader_groups=("ecdh-data-engineers", "ecdh-analysts"),
        engineer_group="ecdh-data-engineers",
        base_catalog_entry=_base_catalog_entry(),
        raw_landings=[_landing()],
        outputs=[_output()],
        ensure_staging=lambda _spark: None,
        ensure_canonical=lambda _spark: None,
    )
    defaults.update(overrides)
    return rb.ReferenceBuildSpec(**defaults)


@pytest.mark.unit
class TestCanonicalOutputValidation:
    def test_process_requires_processed_table(self):
        with pytest.raises(ValueError, match="no `processed_table`"):
            _output(process=lambda _ctx, _v: None)

    def test_no_process_must_read_exactly_one_raw(self):
        with pytest.raises(ValueError, match="exactly one raw landing"):
            _output(reads=("a", "b"))

    def test_simple_output_valid(self):
        assert _output().process is None

    def test_processed_output_valid(self):
        assert _processed_output().processed_table == "us_census_state"


@pytest.mark.unit
class TestReferenceBuildSpecValidation:
    def test_defaults_to_vintage_snapshot(self):
        assert _spec().update_semantics == "vintage_snapshot"

    def test_bad_update_semantics_rejected(self):
        with pytest.raises(ValueError, match="update_semantics"):
            _spec(update_semantics="vintage-snapshot")

    def test_requires_a_raw_landing(self):
        with pytest.raises(ValueError, match="at least one raw landing"):
            _spec(raw_landings=[])

    def test_requires_an_output(self):
        with pytest.raises(ValueError, match="at least one canonical output"):
            _spec(outputs=[])

    def test_output_reading_unknown_landing_rejected(self):
        with pytest.raises(ValueError, match="unknown raw landing"):
            _spec(outputs=[_output(reads=("does_not_exist",))])


@pytest.mark.unit
class TestDerivedNames:
    def test_raw_and_canonical_fqns(self):
        s = _spec()
        assert s.raw_fqn("us_census_state") == "ecdh_dev.geography_raw.us_census_state"
        assert s.canonical_fqn(s.outputs[0]) == "ecdh_model_dev.geography.us_state"

    def test_simple_build_has_no_processed(self):
        s = _spec()
        assert s.has_processed is False
        assert s.staging_fqn(s.outputs[0]) == s.raw_fqn("us_census_state")

    def test_processed_build_stages_on_processed(self):
        s = _spec(
            raw_landings=[_landing(), _landing(table="us_census_state_cenpop")],
            outputs=[_processed_output()],
        )
        out = s.outputs[0]
        assert s.has_processed is True
        assert s.processed_fqn(out) == "ecdh_dev.geography_processed.us_census_state"
        assert s.staging_fqn(out) == s.processed_fqn(out)


@pytest.mark.unit
class TestLayerEntryCloning:
    def test_raw_entry_overrides_keep_provenance(self):
        s = _spec()
        raw = rb._layer_catalog_entry(
            s,
            s.raw_fqn("us_census_state"),
            layer="raw",
            derived_from=None,
            description="TIGER/NHGIS state shapefile, as-is.",
        )
        assert raw.full_table_name == "ecdh_dev.geography_raw.us_census_state"
        assert raw.layer == "raw"
        assert raw.derived_from is None
        assert raw.description == "TIGER/NHGIS state shapefile, as-is."
        # provenance preserved, incl. the origin/provider distinction
        assert raw.source_provider_code == "ipums_nhgis"
        assert raw.source_origin_code == "census"

    def test_processed_entry_records_lineage(self):
        s = _spec(
            raw_landings=[_landing(), _landing(table="us_census_state_cenpop")],
            outputs=[_processed_output()],
        )
        out = s.outputs[0]
        proc = rb._layer_catalog_entry(
            s,
            s.processed_fqn(out),
            layer="processed",
            derived_from=[s.raw_fqn(t) for t in out.reads],
        )
        assert proc.layer == "processed"
        assert proc.derived_from == [
            "ecdh_dev.geography_raw.us_census_state",
            "ecdh_dev.geography_raw.us_census_state_cenpop",
        ]

    def test_engineering_entry_is_formulaic(self):
        s = _spec()
        eng = rb._layer_engineering_entry(s, s.canonical_fqn(s.outputs[0]), ["vintage"])
        assert eng.update_semantics == "vintage_snapshot"
        assert eng.materialization_type == "table"
        assert eng.cluster_columns == ["vintage"]
        assert eng.pipeline_reference == s.pipeline_reference

    def test_engineering_entry_cluster_columns_optional(self):
        assert rb._layer_engineering_entry(_spec(), "t", None).cluster_columns is None


@pytest.mark.unit
class TestRawLandingValidation:
    def test_direct_landing_is_not_volume_backed(self):
        assert _landing().is_volume_backed is False

    def test_direct_landing_needs_acquire(self):
        with pytest.raises(ValueError, match="needs an `acquire` hook"):
            rb.RawLanding(table="t")

    def test_direct_landing_rejects_volume_hooks(self):
        with pytest.raises(ValueError, match="must not set Volume hooks"):
            rb.RawLanding(
                table="t", acquire=lambda _c, _v: None, fetch_to_volume=lambda _v, _d: None
            )

    def test_volume_landing_is_volume_backed(self):
        assert _volume_landing().is_volume_backed is True

    def test_volume_landing_needs_both_hooks(self):
        with pytest.raises(ValueError, match="needs `fetch_to_volume`"):
            rb.RawLanding(
                table="t",
                landing_retention=rb.LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=lambda _v, _d: None,
            )

    def test_volume_landing_rejects_acquire(self):
        with pytest.raises(ValueError, match="must not set `acquire`"):
            rb.RawLanding(
                table="t",
                landing_retention=rb.LandingRetention.PER_VINTAGE_IMMUTABLE,
                fetch_to_volume=lambda _v, _d: None,
                read_from_volume=lambda _c, _v, _d: None,
                acquire=lambda _c, _v: None,
            )


@pytest.mark.unit
class TestVolumePaths:
    def test_landing_volume_name(self):
        assert rb._landing_volume(_spec()) == "ecdh_dev.geography_raw._landing"

    def test_per_vintage_immutable_dir(self):
        lnd = _volume_landing()
        s = _spec(raw_landings=[lnd], outputs=[_output()])
        got = rb._landing_volume_dir(s, lnd, 2020, "2026-06-22")
        want = "/Volumes/ecdh_dev/geography_raw/_landing/us_census_state/vintage=2020"
        assert got == want

    def test_snapshot_per_run_dir(self):
        lnd = _volume_landing(landing_retention=rb.LandingRetention.SNAPSHOT_PER_RUN)
        s = _spec(raw_landings=[lnd], outputs=[_output()])
        got = rb._landing_volume_dir(s, lnd, 2020, "2026-06-22")
        want = "/Volumes/ecdh_dev/geography_raw/_landing/us_census_state/snapshot_date=2026-06-22"
        assert got == want


@pytest.mark.unit
class TestVolumeCompletion:
    def test_nonempty_dir_without_marker_is_incomplete(self, tmp_path):
        d = tmp_path / "vintage=2020"
        d.mkdir()
        (d / "partial.zip").write_text("x")  # non-empty, but no completion marker
        assert rb._volume_dir_is_complete(str(d)) is False

    def test_marker_makes_dir_complete(self, tmp_path):
        d = tmp_path / "vintage=2020"
        d.mkdir()
        rb._mark_fetch_complete(str(d))
        assert rb._volume_dir_is_complete(str(d)) is True

    def test_missing_dir_is_incomplete(self, tmp_path):
        assert rb._volume_dir_is_complete(str(tmp_path / "nope")) is False

    def test_reset_clears_partial_leftovers(self, tmp_path):
        d = tmp_path / "vintage=2020"
        d.mkdir()
        (d / "partial.zip").write_text("x")
        rb._reset_volume_dir(str(d))
        assert d.is_dir() and not os.listdir(d)
