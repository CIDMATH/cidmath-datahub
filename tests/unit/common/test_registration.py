"""Tests for cidmath_datahub.common.registration (ADR 0008).

Covers the pure parts — the dataclasses' controlled-vocabulary validation and
the temp-view-name sanitizer. The MERGE itself needs Spark and is exercised by
the build jobs; the point of the shared helper is that there is now one
implementation to get right instead of six copies.
"""

from __future__ import annotations

from datetime import date

import pytest

from cidmath_datahub.common import registration as reg


def _catalog_entry(**overrides):
    defaults = dict(
        full_table_name="ecdh_model_dev.geography.country",
        subject="geography",
        layer="reference",
        description="d",
        public_health_relevance="p",
        spatial_resolution="country",
        spatial_coverage="global",
        source_provider_code="gadm",
        source_url="https://gadm.org/",
        source_documentation_url="https://gadm.org/metadata.html",
        license="GADM academic non-commercial",
        dua_required=True,
        dua_reference="cite GADM",
        access_tier="restricted",
        external_maintainer_name="GADM, UC Davis",
        is_hosted=True,
    )
    defaults.update(overrides)
    return reg.DatasetCatalogEntry(**defaults)


def _eng_entry(**overrides):
    defaults = dict(
        full_table_name="ecdh_model_dev.geography.country",
        update_semantics="full_refresh",
        materialization_type="table",
        cluster_columns=["country_alpha3"],
        pipeline_reference="bundles/_reference/src/build_geography_country.py",
    )
    defaults.update(overrides)
    return reg.DatasetEngineeringEntry(**defaults)


@pytest.mark.unit
class TestDatasetEngineeringEntry:
    def test_valid(self):
        assert _eng_entry().schema_version == 1

    def test_bad_update_semantics_rejected(self):
        with pytest.raises(ValueError, match="update_semantics"):
            _eng_entry(update_semantics="full-refresh")

    def test_bad_materialization_type_rejected(self):
        with pytest.raises(ValueError, match="materialization_type"):
            _eng_entry(materialization_type="tabel")

    def test_cluster_columns_optional(self):
        assert _eng_entry(cluster_columns=None).cluster_columns is None

    def test_other_valid_semantics_accepted(self):
        assert _eng_entry(update_semantics="snapshot_replace", materialization_type="view")


@pytest.mark.unit
class TestDatasetCatalogEntry:
    def test_owner_defaults(self):
        assert _catalog_entry().owner == "cidmath-data-team"

    def test_owner_overridable(self):
        assert _catalog_entry(owner="someone-else").owner == "someone-else"

    def test_temporal_and_doc_fields_default_none(self):
        # Spatial-only reference tables (geography) leave these unset (ADR 0025).
        e = _catalog_entry()
        assert e.source_data_dictionary_url is None
        assert e.temporal_coverage_start is None
        assert e.temporal_coverage_end is None
        assert e.temporal_resolution is None
        assert e.known_limitations is None

    def test_temporal_and_doc_fields_populated(self):
        e = _catalog_entry(
            source_data_dictionary_url="https://example.org/readme.txt",
            temporal_coverage_start=date(2024, 1, 1),
            temporal_coverage_end=date(2026, 12, 31),
            temporal_resolution="daily",
            known_limitations="CONUS-only",
        )
        assert e.source_data_dictionary_url == "https://example.org/readme.txt"
        assert e.temporal_coverage_start == date(2024, 1, 1)
        assert e.temporal_coverage_end == date(2026, 12, 31)
        assert e.temporal_resolution == "daily"
        assert e.known_limitations == "CONUS-only"


@pytest.mark.unit
class TestSafeViewName:
    def test_sanitizes_dots_and_dashes(self):
        assert (
            reg._safe_view_name("_tmp_reg_cat", "ecdh_model_dev.geography.country")
            == "_tmp_reg_cat_ecdh_model_dev_geography_country"
        )

    def test_unique_per_table(self):
        a = reg._safe_view_name("_tmp_reg_cat", "c.geography.country")
        b = reg._safe_view_name("_tmp_reg_cat", "c.geography.country_subdivision")
        assert a != b
