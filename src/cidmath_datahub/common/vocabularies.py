"""Controlled vocabularies — single source of truth.

These enums and sets are the canonical definitions of the controlled
vocabularies the data hub enforces. They are referenced by:

- Pipeline code, when declaring `update_semantics`, DQ severity, etc.
- CI convention checks (ADR 0016), which validate that declared values are
  members of the vocabulary.
- The `_ops.taxonomy_*` reference tables, which can be seeded from these.

When a vocabulary changes, change it here and the change propagates to both
the runtime code and the CI checks. See:
  - ADR 0007 (update semantics)
  - ADR 0009 (DQ severity)
  - ADR 0008 (materialization types)
  - ADR 0005 (tag namespaces)
"""

from __future__ import annotations

from enum import StrEnum


class UpdateSemantics(StrEnum):
    """How a materialized table is updated. ADR 0007."""

    APPEND_ONLY = "append_only"
    SNAPSHOT_REPLACE = "snapshot_replace"
    # Retain every stamped vintage; each run atomically replaces only the vintage(s) it rebuilt
    # via Delta `replaceWhere`; vintages are immutable (revisions = a new vintage key). ADR 0034.
    VINTAGE_SNAPSHOT = "vintage_snapshot"
    MERGE_UPSERT = "merge_upsert"
    MERGE_SCD2 = "merge_scd2"
    MERGE_SCD2_SIDE = "merge_scd2_side"
    INCREMENTAL_COMPUTE = "incremental_compute"
    FULL_REFRESH = "full_refresh"


class DQSeverity(StrEnum):
    """Severity of a data quality check outcome. ADR 0009."""

    INFO = "info"
    WARN = "warn"
    QUARANTINE = "quarantine"
    FAIL = "fail"


class MaterializationType(StrEnum):
    """How a dataset is physically materialized. ADR 0008."""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"


class DQCategory(StrEnum):
    """Category of a data quality check. ADR 0009."""

    SCHEMA = "schema"
    NULLABILITY = "nullability"
    UNIQUENESS = "uniqueness"
    RANGE = "range"
    CARDINALITY = "cardinality"
    REFERENTIAL = "referential"
    FRESHNESS = "freshness"
    BUSINESS_RULE = "business_rule"


# UC tag namespaces (ADR 0005). The prefix before the colon in a tag like
# `domain:wastewater_surveillance`. Values within each namespace are governed
# by the corresponding `_ops.taxonomy_*` reference table, not enumerated here
# (they grow over time).
TAG_NAMESPACES: frozenset[str] = frozenset(
    {
        "domain",
        "data_type",
        "pathogen",
        "surveillance_category",
        "spatial_resolution",
        "temporal_resolution",
        "access_tier",
    }
)


# Convenience membership sets (string values) for CI checks that work with
# raw strings parsed from config rather than enum instances.
UPDATE_SEMANTICS_VALUES: frozenset[str] = frozenset(s.value for s in UpdateSemantics)
DQ_SEVERITY_VALUES: frozenset[str] = frozenset(s.value for s in DQSeverity)
MATERIALIZATION_TYPE_VALUES: frozenset[str] = frozenset(s.value for s in MaterializationType)
DQ_CATEGORY_VALUES: frozenset[str] = frozenset(s.value for s in DQCategory)


def is_valid_update_semantics(value: str) -> bool:
    """Return True if ``value`` is a recognized update-semantics string."""
    return value in UPDATE_SEMANTICS_VALUES


def is_valid_dq_severity(value: str) -> bool:
    """Return True if ``value`` is a recognized DQ severity string."""
    return value in DQ_SEVERITY_VALUES


def is_valid_tag_namespace(namespace: str) -> bool:
    """Return True if ``namespace`` is a recognized UC tag namespace."""
    return namespace in TAG_NAMESPACES


def parse_tag(tag: str) -> tuple[str, str]:
    """Split a ``namespace:value`` tag into its parts.

    Args:
        tag: A tag string like ``"domain:wastewater_surveillance"``.

    Returns:
        A ``(namespace, value)`` tuple.

    Raises:
        ValueError: If the tag is not in ``namespace:value`` form.
    """
    if ":" not in tag:
        raise ValueError(f"Tag '{tag}' is not in 'namespace:value' form")
    namespace, _, value = tag.partition(":")
    if not namespace or not value:
        raise ValueError(f"Tag '{tag}' has an empty namespace or value")
    return namespace, value
