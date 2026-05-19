"""Unit tests for `cidmath_datahub.common.naming`."""

from __future__ import annotations

import pytest

from cidmath_datahub.common.naming import Layer, full_table_name, schema_for


@pytest.mark.unit
class TestSchemaFor:
    """Covers the ADR 0001 layering convention: raw/processed get suffixes,
    analysis uses the bare subject name."""

    def test_raw_layer_appends_suffix(self):
        assert schema_for("wastewater", Layer.RAW) == "wastewater_raw"

    def test_processed_layer_appends_suffix(self):
        assert schema_for("wastewater", Layer.PROCESSED) == "wastewater_processed"

    def test_analysis_layer_uses_bare_subject(self):
        assert schema_for("wastewater", Layer.ANALYSIS) == "wastewater"

    def test_multi_word_subject(self):
        assert schema_for("public_health_dept", Layer.RAW) == "public_health_dept_raw"
        assert schema_for("public_health_dept", Layer.ANALYSIS) == "public_health_dept"

    @pytest.mark.parametrize(
        "bad_subject",
        ["", "1wastewater", "waste water", "waste-water"],
    )
    def test_rejects_invalid_subject(self, bad_subject):
        with pytest.raises(ValueError):
            schema_for(bad_subject, Layer.RAW)


@pytest.mark.unit
class TestFullTableName:
    def test_assembles_three_level_name(self):
        assert (
            full_table_name("ecdh_dev", "wastewater_raw", "cdc_nwss")
            == "ecdh_dev.wastewater_raw.cdc_nwss"
        )

    def test_rejects_empty_component(self):
        with pytest.raises(ValueError):
            full_table_name("", "wastewater_raw", "cdc_nwss")
        with pytest.raises(ValueError):
            full_table_name("ecdh_dev", "", "cdc_nwss")
        with pytest.raises(ValueError):
            full_table_name("ecdh_dev", "wastewater_raw", "")
