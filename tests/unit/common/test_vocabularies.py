"""Unit tests for `cidmath_datahub.common.vocabularies`."""

from __future__ import annotations

import pytest

from cidmath_datahub.common import vocabularies as vocab


@pytest.mark.unit
class TestUpdateSemantics:
    def test_known_values_are_valid(self):
        assert vocab.is_valid_update_semantics("merge_upsert")
        assert vocab.is_valid_update_semantics("append_only")
        assert vocab.is_valid_update_semantics("merge_scd2_side")

    def test_unknown_value_is_invalid(self):
        assert not vocab.is_valid_update_semantics("upsert")
        assert not vocab.is_valid_update_semantics("")

    def test_enum_and_value_set_agree(self):
        assert {s.value for s in vocab.UpdateSemantics} == vocab.UPDATE_SEMANTICS_VALUES


@pytest.mark.unit
class TestDQSeverity:
    def test_known_values_are_valid(self):
        for s in ("info", "warn", "quarantine", "fail"):
            assert vocab.is_valid_dq_severity(s)

    def test_typo_is_invalid(self):
        # The exact mistake ADR 0010 warns about: `failed` instead of `fail`.
        assert not vocab.is_valid_dq_severity("failed")

    def test_enum_and_value_set_agree(self):
        assert {s.value for s in vocab.DQSeverity} == vocab.DQ_SEVERITY_VALUES


@pytest.mark.unit
class TestTagNamespaces:
    def test_known_namespaces_valid(self):
        assert vocab.is_valid_tag_namespace("domain")
        assert vocab.is_valid_tag_namespace("pathogen")
        assert vocab.is_valid_tag_namespace("surveillance_category")

    def test_unknown_namespace_invalid(self):
        assert not vocab.is_valid_tag_namespace("kimball_role")  # dropped in ADR 0015
        assert not vocab.is_valid_tag_namespace("random")

    def test_parse_tag_splits_namespace_and_value(self):
        assert vocab.parse_tag("domain:wastewater_surveillance") == (
            "domain",
            "wastewater_surveillance",
        )

    @pytest.mark.parametrize("bad", ["no_colon", ":empty_namespace", "empty_value:"])
    def test_parse_tag_rejects_malformed(self, bad):
        with pytest.raises(ValueError):
            vocab.parse_tag(bad)
