"""CI convention checks (ADR 0016).

ADR 0016 defines four CI-enforced rules:
  1. `update_semantics` values are in the controlled vocabulary.
  2. DQ severity values are in the controlled vocabulary.
  3. UC tag values are in their namespace's controlled vocabulary.
  4. `_ops.dataset_catalog` row presence for new analysis-layer tables.

Rules 1-3 validate that values appearing in pipeline declarations are members
of the controlled vocabularies defined in `cidmath_datahub.common.vocabularies`.
Rule 4 validates catalog row presence against the live `_ops.dataset_catalog`.

**Current state:** no data pipelines exist yet, so there are no declarations to
scan. What this script enforces *today* is the integrity of the vocabularies
module itself — that the enums and their value-set mirrors agree, and that the
helper functions behave. This catches the failure mode where someone edits the
vocabularies inconsistently and silently breaks downstream validation.

The per-rule scanning logic (parsing bundle resources and pipeline source for
declared values, querying `_ops.dataset_catalog`) is added alongside the first
data pipeline, when the declaration patterns are concrete. Each rule below has
a clearly-marked extension point.

Run locally:  python scripts/ci/check_conventions.py
Exit code 0 = all checks pass; non-zero = a violation was found.
"""

from __future__ import annotations

import sys

from cidmath_datahub.common import vocabularies as vocab


def check_vocabulary_integrity() -> list[str]:
    """Verify the vocabularies module is internally consistent.

    Returns a list of human-readable error strings (empty == all good).
    """
    errors: list[str] = []

    # Each StrEnum's value set must match its frozenset mirror exactly.
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

    # Tag namespaces must be non-empty, lowercase, snake_case-compatible.
    for ns in vocab.TAG_NAMESPACES:
        if not ns or ns != ns.lower() or "-" in ns or " " in ns:
            errors.append(f"Tag namespace '{ns}' is not lowercase snake_case")

    # Helper functions behave as expected on a couple of anchors.
    if not vocab.is_valid_update_semantics("merge_upsert"):
        errors.append("is_valid_update_semantics rejected a known-good value")
    if vocab.is_valid_dq_severity("failed"):
        errors.append("is_valid_dq_severity accepted 'failed' (should be 'fail')")

    return errors


# --- Per-rule extension points (ADR 0016). Activated with the first pipeline. ---


def check_update_semantics_declarations() -> list[str]:
    """Rule 1: every declared `update_semantics` is in the vocabulary.

    Extension point: once pipelines populate `_ops.dataset_engineering` (or
    declare semantics in bundle resources), scan those declarations and validate
    each against `vocab.is_valid_update_semantics`. No declarations exist yet.
    """
    return []


def check_dq_severity_declarations() -> list[str]:
    """Rule 2: every DQ severity used is in the vocabulary.

    Extension point: scan pipeline source / DQ helper calls for severity
    arguments; validate against `vocab.is_valid_dq_severity`. None exist yet.
    """
    return []


def check_tag_values() -> list[str]:
    """Rule 3: every UC tag uses a known namespace and a value in that
    namespace's `_ops.taxonomy_*` table.

    Extension point: scan tag applications in bundle resources / pipeline code;
    validate namespace via `vocab.is_valid_tag_namespace` and value against the
    taxonomy tables. No tags applied yet.
    """
    return []


def check_dataset_catalog_presence() -> list[str]:
    """Rule 4: every new analysis-layer table has an `_ops.dataset_catalog` row.

    Extension point: diff analysis-layer tables declared in bundle resources
    against `_ops.dataset_catalog`; flag any missing rows. Requires workspace
    access in CI (the SP can query `_ops`). No analysis-layer tables yet.
    """
    return []


def main() -> int:
    all_errors: list[str] = []
    all_errors += check_vocabulary_integrity()
    all_errors += check_update_semantics_declarations()
    all_errors += check_dq_severity_declarations()
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
