"""CI convention checks (ADR 0016).

ADR 0016 defines four CI-enforced rules:
  1. `update_semantics` values are in the controlled vocabulary.
  2. DQ severity (and category) values are in the controlled vocabulary.
  3. UC tag values are in their namespace's controlled vocabulary.
  4. `_ops.dataset_catalog` row presence for new analysis-layer tables.

**What runs today.** The geography reference pipelines
(`bundles/_reference/src/build_geography*.py`) declare controlled-vocabulary
values through the `cidmath_datahub.common.vocabularies` enums —
`DQSeverity.FAIL`, `DQCategory.UNIQUENESS`, `update_semantics="full_refresh"`,
`materialization_type="table"`. `check_controlled_vocabulary_usage` walks
`src/` and `bundles/` and statically validates those: every `DQSeverity.X` /
`DQCategory.X` / `UpdateSemantics.X` / `MaterializationType.X` member access
must be a real member, and every string literal declared under a
controlled-vocabulary keyword/dict key (`severity`, `category`,
`update_semantics`, `materialization_type`) must be in the vocabulary. This
turns a class of typo (`DQSeverity.FAILED`, `update_semantics="full-refresh"`)
into a CI failure instead of a runtime failure 40 minutes into a Databricks
job (rules 1 and 2).

**What is still deferred (honestly).** Rule 3 (UC tag values) has nothing to
scan yet — no tags are applied in code. Rule 4 (`_ops.dataset_catalog` row
presence) requires a live workspace query (the CI service principal reading
`_ops`), which is not wired into the CI runner; until it is, catalog-row
presence is enforced by review, not automatically. Both are clearly-marked
stubs below rather than silent no-ops.

Run locally:  python scripts/ci/check_conventions.py
Exit code 0 = all checks pass; non-zero = a violation was found.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from cidmath_datahub.common import vocabularies as vocab

REPO_ROOT = Path(__file__).resolve().parents[2]
SCAN_ROOTS = ("src", "bundles")

# Enum class name -> {valid member NAMES} and {valid string VALUES}. Member
# access (DQSeverity.FAIL) is checked against names; declared string literals
# (update_semantics="full_refresh") against values.
_ENUM_MEMBER_NAMES: dict[str, set[str]] = {
    "DQSeverity": {m.name for m in vocab.DQSeverity},
    "DQCategory": {m.name for m in vocab.DQCategory},
    "UpdateSemantics": {m.name for m in vocab.UpdateSemantics},
    "MaterializationType": {m.name for m in vocab.MaterializationType},
}

# Keyword-argument / dict-literal keys that carry a controlled-vocabulary
# string, mapped to the set of valid values. A *string literal* under one of
# these keys is validated; non-literal values (enum members, variables) are
# left to the member-access check or to runtime.
_DECLARED_STRING_KEYS: dict[str, frozenset[str]] = {
    "severity": vocab.DQ_SEVERITY_VALUES,
    "category": vocab.DQ_CATEGORY_VALUES,
    "update_semantics": vocab.UPDATE_SEMANTICS_VALUES,
    "materialization_type": vocab.MATERIALIZATION_TYPE_VALUES,
}


def check_vocabulary_integrity() -> list[str]:
    """Verify the vocabularies module is internally consistent.

    Returns a list of human-readable error strings (empty == all good).
    """
    errors: list[str] = []

    pairs = [
        ("UpdateSemantics", {s.value for s in vocab.UpdateSemantics}, vocab.UPDATE_SEMANTICS_VALUES),
        ("DQSeverity", {s.value for s in vocab.DQSeverity}, vocab.DQ_SEVERITY_VALUES),
        (
            "MaterializationType",
            {s.value for s in vocab.MaterializationType},
            vocab.MATERIALIZATION_TYPE_VALUES,
        ),
        ("DQCategory", {s.value for s in vocab.DQCategory}, vocab.DQ_CATEGORY_VALUES),
    ]
    for name, enum_values, mirror in pairs:
        if enum_values != mirror:
            errors.append(
                f"{name}: enum values {sorted(enum_values)} do not match "
                f"value-set mirror {sorted(mirror)}"
            )

    for ns in vocab.TAG_NAMESPACES:
        if not ns or ns != ns.lower() or "-" in ns or " " in ns:
            errors.append(f"Tag namespace '{ns}' is not lowercase snake_case")

    if not vocab.is_valid_update_semantics("merge_upsert"):
        errors.append("is_valid_update_semantics rejected a known-good value")
    if vocab.is_valid_dq_severity("failed"):
        errors.append("is_valid_dq_severity accepted 'failed' (should be 'fail')")

    return errors


def scan_source_for_vocab_errors(source: str, filename: str) -> list[str]:
    """Validate controlled-vocabulary usage in one Python source string.

    Pure (no filesystem) so it is directly unit-testable. Flags:
      - ``<Enum>.<MEMBER>`` access where ``<MEMBER>`` is not a real member of
        a controlled-vocabulary enum.
      - string literals declared under a controlled-vocabulary keyword arg or
        dict key whose value is not in the vocabulary.
    """
    errors: list[str] = []
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as e:
        return [f"{filename}: could not parse ({e})"]

    for node in ast.walk(tree):
        # Enum member access: DQSeverity.FAIL, DQCategory.UNIQUENESS, ...
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            enum_name = node.value.id
            valid = _ENUM_MEMBER_NAMES.get(enum_name)
            if valid is not None and node.attr not in valid:
                errors.append(
                    f"{filename}:{node.lineno}: {enum_name}.{node.attr} is not a "
                    f"member of {enum_name} (valid: {sorted(valid)})"
                )

        # Keyword args: severity="fail", update_semantics="full_refresh", ...
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg in _DECLARED_STRING_KEYS and _is_str_const(kw.value):
                    value = kw.value.value
                    valid_values = _DECLARED_STRING_KEYS[kw.arg]
                    if value not in valid_values:
                        errors.append(
                            f"{filename}:{kw.value.lineno}: {kw.arg}={value!r} is not "
                            f"in the controlled vocabulary {sorted(valid_values)}"
                        )

        # Dict literals: {"update_semantics": "full_refresh", ...}
        if isinstance(node, ast.Dict):
            for key, val in zip(node.keys, node.values, strict=False):
                if (
                    isinstance(key, ast.Constant)
                    and key.value in _DECLARED_STRING_KEYS
                    and _is_str_const(val)
                ):
                    valid_values = _DECLARED_STRING_KEYS[key.value]
                    if val.value not in valid_values:
                        errors.append(
                            f"{filename}:{val.lineno}: {key.value}={val.value!r} is not "
                            f"in the controlled vocabulary {sorted(valid_values)}"
                        )

    return errors


def _is_str_const(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def check_controlled_vocabulary_usage() -> list[str]:
    """Rules 1 & 2: validate controlled-vocabulary usage across src/ and bundles/.

    Walks every ``.py`` under the scan roots and runs
    :func:`scan_source_for_vocab_errors`. This is the active enforcement for
    the geography pipelines' DQ severity/category and update-semantics
    declarations.
    """
    errors: list[str] = []
    for root in SCAN_ROOTS:
        base = REPO_ROOT / root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(REPO_ROOT).as_posix()
            errors.extend(scan_source_for_vocab_errors(path.read_text(encoding="utf-8"), rel))
    return errors


def check_tag_values() -> list[str]:
    """Rule 3: every UC tag uses a known namespace and an in-vocabulary value.

    DEFERRED — no tags are applied in code yet, so there is nothing to scan.
    Activates when the first `ALTER TABLE ... SET TAGS` / tag-application
    pattern lands: parse the namespace via `vocab.is_valid_tag_namespace` and
    the value against the `_ops.taxonomy_*` tables.
    """
    return []


def check_dataset_catalog_presence() -> list[str]:
    """Rule 4: every new analysis-layer table has an `_ops.dataset_catalog` row.

    DEFERRED — requires a live workspace query (the CI service principal
    reading `_ops.dataset_catalog`), which is not wired into the CI runner.
    Until it is, catalog-row presence for new analysis-layer tables is
    enforced by PR review, not automatically. (The geography reference builds
    do register their rows via `_register_dataset`; this check would confirm
    that against the live catalog.)
    """
    return []


def main() -> int:
    all_errors: list[str] = []
    all_errors += check_vocabulary_integrity()
    all_errors += check_controlled_vocabulary_usage()
    all_errors += check_tag_values()
    all_errors += check_dataset_catalog_presence()

    if all_errors:
        print("Convention checks FAILED:")
        for err in all_errors:
            print(f"  - {err}")
        return 1

    print("Convention checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
