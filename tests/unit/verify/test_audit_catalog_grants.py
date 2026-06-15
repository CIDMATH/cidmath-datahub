"""Tests for the catalog-grant drift checker (ADR 0033).

``scripts/verify`` is not an importable package, so the module is loaded by path
(same pattern as tests/unit/ci/test_check_conventions.py). Only the pure parse +
diff logic is exercised here; the SDK/workspace IO is not.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "verify" / "audit_catalog_grants.py"
_DECLARED_SQL = _REPO / "scripts" / "setup" / "grant_catalog_permissions.sql"


def _load():
    spec = importlib.util.spec_from_file_location("audit_catalog_grants", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    # Register before exec so the @dataclass can resolve its own module namespace
    # under `from __future__ import annotations` (loaded-by-path modules otherwise
    # aren't in sys.modules and dataclasses' type check fails).
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


acg = _load()

SAMPLE_SQL = """\
-- header comment
GRANT USE CATALOG ON CATALOG ecdh_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;
GRANT CREATE SCHEMA ON CATALOG ecdh_dev TO `a55b6164-c0eb-42cf-a438-7de33c150f4a`;
GRANT USE CATALOG ON CATALOG ecdh_dev TO `ecdh-data-engineers`;
-- GRANT USE CATALOG ON CATALOG ecdh_prod TO `ecdh-analysts`;  -- deliberately disabled
SHOW GRANTS `ecdh-analysts` ON CATALOG ecdh_dev;
"""


@pytest.mark.unit
class TestNormalizePrivilege:
    def test_underscore_and_case(self):
        assert acg.normalize_privilege("use_catalog") == "USE CATALOG"
        assert acg.normalize_privilege("  CREATE   SCHEMA ") == "CREATE SCHEMA"


@pytest.mark.unit
class TestParseDeclared:
    def test_parses_active_grants(self):
        declared = acg.parse_declared_catalog_grants(SAMPLE_SQL)
        assert declared[("ecdh_dev", "a55b6164-c0eb-42cf-a438-7de33c150f4a")] == {
            "USE CATALOG",
            "CREATE SCHEMA",
        }
        assert declared[("ecdh_dev", "ecdh-data-engineers")] == {"USE CATALOG"}

    def test_excludes_commented_grants(self):
        declared = acg.parse_declared_catalog_grants(SAMPLE_SQL)
        assert ("ecdh_prod", "ecdh-analysts") not in declared

    def test_ignores_show_grants_and_comments(self):
        # Only GRANT ... ON CATALOG lines become entries.
        declared = acg.parse_declared_catalog_grants(SAMPLE_SQL)
        assert all(isinstance(k, tuple) and len(k) == 2 for k in declared)
        assert len(declared) == 2

    def test_real_source_file_parses_expected_grants(self):
        declared = acg.parse_declared_catalog_grants(_DECLARED_SQL.read_text(encoding="utf-8"))
        # Engineers traverse both dev catalogs.
        assert "USE CATALOG" in declared[("ecdh_dev", "ecdh-data-engineers")]
        assert "USE CATALOG" in declared[("ecdh_model_dev", "ecdh-data-engineers")]
        # Analyst prod access is commented out -> must not be declared.
        assert ("ecdh_prod", "ecdh-analysts") not in declared
        assert ("ecdh_model_prod", "ecdh-analysts") not in declared


@pytest.mark.unit
class TestDiff:
    def test_no_drift(self):
        declared = {("c", "p"): {"USE CATALOG"}}
        actual = {("c", "p"): {"USE CATALOG"}}
        assert acg.diff_catalog_grants(declared, actual) == []

    def test_missing_grant_flagged(self):
        declared = {("c", "p"): {"USE CATALOG", "CREATE SCHEMA"}}
        actual = {("c", "p"): {"USE CATALOG"}}
        drifts = acg.diff_catalog_grants(declared, actual)
        assert len(drifts) == 1
        assert drifts[0].missing == frozenset({"CREATE SCHEMA"})
        assert drifts[0].extra == frozenset()

    def test_extra_grant_flagged_by_default(self):
        declared = {("c", "p"): {"USE CATALOG"}}
        actual = {("c", "p"): {"USE CATALOG", "MODIFY"}}
        drifts = acg.diff_catalog_grants(declared, actual)
        assert drifts[0].extra == frozenset({"MODIFY"})

    def test_extra_suppressed_when_flag_off(self):
        declared = {("c", "p"): {"USE CATALOG"}}
        actual = {("c", "p"): {"USE CATALOG", "MODIFY"}}
        assert acg.diff_catalog_grants(declared, actual, flag_extra=False) == []

    def test_unmanaged_principals_ignored(self):
        # A principal we don't declare is never audited, even if it holds grants.
        declared = {("c", "p"): {"USE CATALOG"}}
        actual = {("c", "p"): {"USE CATALOG"}, ("c", "someone_else"): {"MODIFY"}}
        assert acg.diff_catalog_grants(declared, actual) == []
