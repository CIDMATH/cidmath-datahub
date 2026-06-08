# CIDMATH Data Hub — task runner. Encodes the exact commands CI runs so local
# checks and CI stay in lockstep (lint scope, test tiers, bundle validate).
#
# Install just:  winget install Casey.Just (Windows) | brew install just (macOS)
#                or see https://just.systems
# Usage:         `just` lists recipes; `just check` is the full pre-PR gate.
#
# Recipes invoke only ruff / pytest / python / databricks (no shell-specific
# operators), so they run identically under PowerShell and sh.

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# List available recipes.
default:
    @just --list

# Lint + format-check (CI scope: src + tests; bundles/ excluded by design).
lint:
    ruff check src tests
    ruff format --check src tests

# Auto-format and apply safe lint fixes (src + tests).
fmt:
    ruff format src tests
    ruff check --fix src tests

# F821 + compile guard for the thin bundle entrypoints (mirrors CI).
lint-bundles:
    ruff check --select F821 bundles
    python -m compileall -q bundles

# Fast unit tests (no Spark).
test:
    pytest tests/unit -q

# Unit + local-Spark data tests, with coverage (what CI runs).
test-all:
    pytest tests/unit tests/data --cov

# Validate one bundle's config against dev, e.g. `just validate _reference`.
validate bundle:
    python scripts/ci/validate_all_bundles.py {{bundle}}

# Validate every bundle (each bundles/* with a databricks.yml); needs auth.
validate-all:
    python scripts/ci/validate_all_bundles.py

# Full pre-PR gate: lint + bundle guard + tests (CI parity, minus bundle validate).
check: lint lint-bundles test-all
    @echo "Local CI checks passed. Run 'just validate-all' for bundle config (needs auth)."
