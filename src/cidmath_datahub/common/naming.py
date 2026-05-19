"""Canonical schema and table name builders.

Source-aligned schema naming follows ADR 0001 (raw / processed / analysis,
with the analysis layer using the bare subject name — no `_analysis` suffix).

Integrated catalog table naming follows ADR 0015 (reference tables unsuffixed;
derived analytical content uses Kimball `_fact`/`_dim`/`_bridge` suffixes).

Pipelines should always use these helpers rather than hardcoding strings:

    from cidmath_datahub.common.naming import Layer, schema_for

    schema_for("wastewater", Layer.RAW)        # -> "wastewater_raw"
    schema_for("wastewater", Layer.PROCESSED)  # -> "wastewater_processed"
    schema_for("wastewater", Layer.ANALYSIS)   # -> "wastewater"
"""

from __future__ import annotations

from enum import StrEnum


class Layer(StrEnum):
    """Source-aligned data layers (ADR 0001)."""

    RAW = "raw"
    PROCESSED = "processed"
    ANALYSIS = "analysis"


def schema_for(subject: str, layer: Layer) -> str:
    """Return the Unity Catalog schema name for a subject + layer combination.

    Implements the bare-subject convention from ADR 0001: analysis-layer
    schemas drop the suffix; raw and processed layers use `<subject>_<layer>`.

    Args:
        subject: Subject area identifier in snake_case (e.g., ``"wastewater"``).
        layer: One of ``Layer.RAW``, ``Layer.PROCESSED``, ``Layer.ANALYSIS``.

    Returns:
        The schema name string (e.g., ``"wastewater_raw"``, ``"wastewater"``).

    Raises:
        ValueError: If ``subject`` is empty or not snake_case-compatible.

    Examples:
        >>> schema_for("wastewater", Layer.RAW)
        'wastewater_raw'
        >>> schema_for("wastewater", Layer.PROCESSED)
        'wastewater_processed'
        >>> schema_for("wastewater", Layer.ANALYSIS)
        'wastewater'
    """
    _validate_identifier(subject, kind="subject")

    if layer is Layer.ANALYSIS:
        return subject
    return f"{subject}_{layer.value}"


def full_table_name(catalog: str, schema: str, table: str) -> str:
    """Return the three-level Unity Catalog table name.

    Args:
        catalog: Catalog name (e.g., ``"ecdh_dev"``).
        schema: Schema name (e.g., ``"wastewater_raw"``).
        table: Table name (e.g., ``"cdc_nwss"``).

    Returns:
        ``"<catalog>.<schema>.<table>"``.
    """
    for name, kind in [(catalog, "catalog"), (schema, "schema"), (table, "table")]:
        _validate_identifier(name, kind=kind)
    return f"{catalog}.{schema}.{table}"


def _validate_identifier(value: str, *, kind: str) -> None:
    """Lightweight sanity check on a UC identifier.

    Not a full validation against Databricks' reserved-word list; that lives
    in the CI check. This just catches the obvious mistakes (empty, leading
    digit, hyphens, whitespace) at runtime so we fail fast in pipelines.
    """
    if not value:
        raise ValueError(f"{kind} name must not be empty")
    if value[0].isdigit():
        raise ValueError(f"{kind} name '{value}' must not start with a digit")
    if any(ch.isspace() for ch in value):
        raise ValueError(f"{kind} name '{value}' must not contain whitespace")
    if "-" in value:
        raise ValueError(f"{kind} name '{value}' must use snake_case, not kebab-case")
