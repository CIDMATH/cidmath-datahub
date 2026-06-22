"""Tests for cidmath_datahub.common.reference_builder (ADR 0036).

Covers the pure surface — ReferenceTableSpec validation, the derived table-name
properties, and the per-layer entry cloning. The two-phase build flow needs Spark
and is exercised by the build jobs; the point of the shared builder is one path to
get right instead of a hand-rolled skeleton per subject.
"""

from __future__ import annotations

import pytest

from cidmath_datahub.common import reference_builder as rb
from cidmath_datahub.common import registration as reg


def _base_catalog_entry(**overrides):
    defaults = dict(
        full_table_name="placeholder",  # builder overrides per layer
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


def _spec(**overrides):
    defaults = dict(
        subject="geography",
        source_table="us_census_state",
        canonical_table="us_state",
        source_catalog="ecdh_dev",
        model_catalog="ecdh_model_dev",
        pipeline_reference="bundles/_reference/src/build_geography.py",
        reader_groups=("ecdh-data-engineers", "ecdh-analysts"),
        engineer_group="ecdh-data-engineers",
        catalog_entry=_base_catalog_entry(),
        ensure_staging=lambda _spark: None,
        acquire_raw=lambda _ctx, _v: None,
        validate_staging=lambda _ctx, _t: None,
        ensure_canonical=lambda _spark: None,
        promote=lambda _ctx, _v: None,
    )
    defaults.update(overrides)
    return rb.ReferenceTableSpec(**defaults)


@pytest.mark.unit
class TestReferenceTableSpecValidation:
    def test_defaults_to_vintage_snapshot(self):
        assert _spec().update_semantics == "vintage_snapshot"

    def test_bad_update_semantics_rejected(self):
        with pytest.raises(ValueError, match="update_semantics"):
            _spec(update_semantics="vintage-snapshot")

    def test_complex_requires_process_hook(self):
        with pytest.raises(ValueError, match="needs a `process` hook"):
            _spec(has_processed_stage=True)

    def test_simple_rejects_process_hook(self):
        with pytest.raises(ValueError, match="has_processed_stage=False"):
            _spec(process=lambda _ctx, _v: None)

    def test_complex_with_process_is_valid(self):
        assert _spec(has_processed_stage=True, process=lambda _ctx, _v: None).has_processed_stage


@pytest.mark.unit
class TestDerivedNames:
    def test_source_and_model_fqns(self):
        s = _spec()
        assert s.raw_fqn == "ecdh_dev.geography_raw.us_census_state"
        assert s.processed_fqn == "ecdh_dev.geography_processed.us_census_state"
        assert s.canonical_fqn == "ecdh_model_dev.geography.us_state"

    def test_staging_is_raw_when_simple(self):
        assert _spec().staging_fqn == _spec().raw_fqn

    def test_staging_is_processed_when_complex(self):
        s = _spec(has_processed_stage=True, process=lambda _ctx, _v: None)
        assert s.staging_fqn == s.processed_fqn


@pytest.mark.unit
class TestLayerEntryCloning:
    def test_catalog_entry_overrides_per_layer_keeps_provenance(self):
        s = _spec()
        raw = rb._layer_catalog_entry(s, s.raw_fqn, layer="raw", derived_from=None)
        assert raw.full_table_name == "ecdh_dev.geography_raw.us_census_state"
        assert raw.layer == "raw"
        assert raw.derived_from is None
        # provenance preserved, incl. the origin/provider distinction
        assert raw.source_provider_code == "ipums_nhgis"
        assert raw.source_origin_code == "census"

    def test_processed_records_lineage_to_raw(self):
        s = _spec(has_processed_stage=True, process=lambda _ctx, _v: None)
        proc = rb._layer_catalog_entry(
            s, s.processed_fqn, layer="processed", derived_from=[s.raw_fqn]
        )
        assert proc.layer == "processed"
        assert proc.derived_from == [s.raw_fqn]

    def test_engineering_entry_is_formulaic(self):
        s = _spec()
        eng = rb._layer_engineering_entry(s, s.canonical_fqn, ["vintage"])
        assert eng.update_semantics == "vintage_snapshot"
        assert eng.materialization_type == "table"
        assert eng.cluster_columns == ["vintage"]
        assert eng.pipeline_reference == s.pipeline_reference

    def test_engineering_entry_cluster_columns_optional(self):
        assert rb._layer_engineering_entry(_spec(), "t", None).cluster_columns is None
