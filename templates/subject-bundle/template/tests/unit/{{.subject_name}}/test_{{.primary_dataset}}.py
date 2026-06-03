"""Tests for cidmath_datahub.{{.subject_name}}.{{.primary_dataset}}."""

from __future__ import annotations

import pytest

from cidmath_datahub.{{.subject_name}} import {{.primary_dataset}} as mod


@pytest.mark.unit
def test_module_importable():
    # TODO({{.subject_name}}): replace with real tests of parse_records / conform logic.
    assert mod is not None
