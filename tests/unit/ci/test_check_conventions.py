"""Tests for the CI convention checks (ADR 0016 / scripts/ci/check_conventions.py).

``scripts/ci`` is not an importable package, so the module is loaded by path.
The whole module is skipped where ``cidmath_datahub.common.vocabularies`` can't
import (it uses ``enum.StrEnum``, Python 3.11+); CI runs on 3.11 where it
executes.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "check_conventions.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_conventions", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


try:
    cc = _load()
except Exception as exc:  # pragma: no cover - environment guard (py<3.11)
    pytest.skip(f"check_conventions unavailable: {exc}", allow_module_level=True)


@pytest.mark.unit
class TestScanSourceForVocabErrors:
    def test_clean_source_passes(self):
        src = (
            "recorder.record(severity=DQSeverity.FAIL, category=DQCategory.UNIQUENESS)\n"
            'eng_row = [(full, "full_refresh", "table")]\n'
        )
        assert cc.scan_source_for_vocab_errors(src, "f.py") == []

    def test_bad_enum_member_flagged(self):
        errs = cc.scan_source_for_vocab_errors("x = DQSeverity.FAILED\n", "f.py")
        assert any("FAILED" in e for e in errs)

    def test_bad_category_member_flagged(self):
        errs = cc.scan_source_for_vocab_errors("c = DQCategory.UNIQ\n", "f.py")
        assert any("DQCategory.UNIQ" in e for e in errs)

    def test_bad_update_semantics_kwarg_flagged(self):
        errs = cc.scan_source_for_vocab_errors('f(update_semantics="full-refresh")\n', "f.py")
        assert any("full-refresh" in e for e in errs)

    def test_bad_dict_value_flagged(self):
        errs = cc.scan_source_for_vocab_errors('d = {"materialization_type": "tabel"}\n', "f.py")
        assert any("tabel" in e for e in errs)

    def test_valid_dict_value_passes(self):
        src = 'd = {"update_semantics": "full_refresh"}\n'
        assert cc.scan_source_for_vocab_errors(src, "f.py") == []

    def test_unrelated_attribute_ignored(self):
        assert cc.scan_source_for_vocab_errors("y = os.path.join('a', 'b')\n", "f.py") == []

    def test_syntax_error_reported_not_raised(self):
        errs = cc.scan_source_for_vocab_errors("def (:\n", "bad.py")
        assert errs and "could not parse" in errs[0]


@pytest.mark.unit
class TestVocabularyIntegrity:
    def test_passes(self):
        assert cc.check_vocabulary_integrity() == []


@pytest.mark.unit
class TestRepoControlledVocabularyClean:
    def test_real_pipelines_use_valid_vocabulary(self):
        # End-to-end: scans the actual src/ + bundles/ tree. Guards against a
        # typo'd DQSeverity/DQCategory member or update_semantics string in the
        # geography (or future) build scripts.
        assert cc.check_controlled_vocabulary_usage() == []
