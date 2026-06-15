"""Unity Catalog grant helpers encoding the access-tier model (ADR 0001, 0018).

Two privilege tiers:

- **Engineer** (`ecdh-data-engineers`): full access to a schema — USE SCHEMA,
  SELECT, MODIFY, CREATE TABLE. Applied to the schemas engineers manage
  directly: raw / processed / analysis schemas and `_ops`.
- **Reader** (`ecdh-analysts` and other end-user groups): read-only — USE
  SCHEMA, SELECT. Applied to analysis-layer schemas for end-user groups.

Reference schemas (`time`, `geography`, ...) are a special case: they are
canonical, pipeline-owned, and never hand-edited, so *both* the engineer and
analyst groups receive only the reader tier on them. The owning bundle's
deploy service principal retains full control as the schema owner. See ADR 0018.

`USE CATALOG` is required to traverse into any schema and is granted at the
catalog level to whichever groups need to reach content inside. Catalog-level
grants are applied by an admin (scripts/setup/grant_catalog_permissions.sql),
not by the deploy jobs: granting on a catalog requires MANAGE/ownership that the
deploy service principal does not have. The deploy jobs apply only schema-level
grants, on schemas the SP owns.

The statement-builder functions are pure (return SQL strings) so they're unit-
testable without a Spark session. The `grant_*` convenience functions execute
the statements via `spark.sql`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Privilege sets per tier.
ENGINEER_SCHEMA_PRIVILEGES: tuple[str, ...] = (
    "USE SCHEMA",
    "SELECT",
    "MODIFY",
    "CREATE TABLE",
)
READER_SCHEMA_PRIVILEGES: tuple[str, ...] = (
    "USE SCHEMA",
    "SELECT",
)


def _grant_stmt(securable_type: str, securable_name: str, privilege: str, principal: str) -> str:
    """Build a single GRANT statement.

    Principals (groups, users, service principal application_ids) are quoted
    with backticks, matching Unity Catalog SQL.
    """
    return f"GRANT {privilege} ON {securable_type} {securable_name} TO `{principal}`"


def catalog_usage_statements(catalog: str, principal: str) -> list[str]:
    """Statements granting catalog traversal (USE CATALOG) to a principal."""
    return [_grant_stmt("CATALOG", catalog, "USE CATALOG", principal)]


def schema_grant_statements(
    catalog: str,
    schema: str,
    principal: str,
    privileges: Sequence[str],
) -> list[str]:
    """Statements granting ``privileges`` on ``catalog.schema`` to a principal."""
    name = f"{catalog}.{schema}"
    return [_grant_stmt("SCHEMA", name, p, principal) for p in privileges]


def engineer_schema_statements(catalog: str, schema: str, principal: str) -> list[str]:
    """Engineer-tier grant statements for a schema."""
    return schema_grant_statements(catalog, schema, principal, ENGINEER_SCHEMA_PRIVILEGES)


def reader_schema_statements(catalog: str, schema: str, principal: str) -> list[str]:
    """Reader-tier (analyst / end-user) grant statements for a schema."""
    return schema_grant_statements(catalog, schema, principal, READER_SCHEMA_PRIVILEGES)


def volume_read_statements(catalog: str, schema: str, volume: str, principal: str) -> list[str]:
    """Statements granting read access to a UC Volume's files.

    ``READ VOLUME`` is a volume-scoped privilege distinct from a schema's
    ``SELECT`` (which covers tables/views only), so a reader who can query a
    schema's tables still cannot list/open a Volume's files without it. Used for
    raw source-snapshot Volumes (ADR 0032). Reading the files also needs ``USE
    SCHEMA`` on the parent schema, which the schema reader/engineer grants cover.
    """
    name = f"{catalog}.{schema}.{volume}"
    return [_grant_stmt("VOLUME", name, "READ VOLUME", principal)]


# --- Execution convenience wrappers ---


def apply(spark: SparkSession, statements: Iterable[str]) -> None:
    """Execute a sequence of GRANT statements."""
    for stmt in statements:
        spark.sql(stmt)


def grant_catalog_usage(spark: SparkSession, catalog: str, principal: str) -> None:
    """Grant USE CATALOG on ``catalog`` to ``principal``."""
    apply(spark, catalog_usage_statements(catalog, principal))


def grant_schema_engineer(spark: SparkSession, catalog: str, schema: str, principal: str) -> None:
    """Grant engineer-tier access on ``catalog.schema`` to ``principal``."""
    apply(spark, engineer_schema_statements(catalog, schema, principal))


def grant_schema_reader(spark: SparkSession, catalog: str, schema: str, principal: str) -> None:
    """Grant reader-tier access on ``catalog.schema`` to ``principal``."""
    apply(spark, reader_schema_statements(catalog, schema, principal))


def grant_volume_reader(
    spark: SparkSession, catalog: str, schema: str, volume: str, principal: str
) -> None:
    """Grant READ VOLUME on ``catalog.schema.volume`` to ``principal`` (ADR 0032)."""
    apply(spark, volume_read_statements(catalog, schema, volume, principal))


# --- Verification (read-back of applied grants) ---
#
# After applying grants, deploy jobs read them back with SHOW GRANTS and assert
# the privilege set matches the intended tier. This turns the access model into
# a deploy-time gate: a drifted, missing, or over-broad grant (e.g., an analyst
# accidentally holding access to _ops) raises and fails the job — and therefore
# the deploy. Verification runs as the deploy SP, which owns the schemas it
# created, so it can SHOW GRANTS on them.
#
# Catalog-level grants (USE CATALOG) are intentionally NOT verified this way:
# the SP may not own the catalog, so it can't always SHOW GRANTS at the catalog
# level. Those grants are self-checking at apply time — a GRANT to a missing
# group errors immediately.


class GrantVerificationError(AssertionError):
    """Raised when applied grants don't match the intended access tier."""


def _normalize_privilege(privilege: str) -> str:
    """Normalize a privilege name for comparison (space/underscore-insensitive).

    SHOW GRANTS may report privileges with underscores (``USE_SCHEMA``) or
    spaces (``USE SCHEMA``) depending on runtime version; normalize to spaces.
    """
    return privilege.strip().upper().replace("_", " ")


def _parse_grant_rows(rows: Iterable[dict], principal: str) -> set[str]:
    """Extract the privileges granted to ``principal`` from SHOW GRANTS rows.

    Pure function (no Spark) so it is unit-testable. ``rows`` are dict-like
    (e.g., ``Row.asDict()``). Column names differ across runtimes, so the
    principal and action columns are matched case-insensitively.
    """
    privileges: set[str] = set()
    for row in rows:
        lowered = {str(k).lower(): v for k, v in row.items()}
        row_principal = lowered.get("principal")
        action = lowered.get("actiontype") or lowered.get("action_type") or lowered.get("action")
        if row_principal == principal and action:
            privileges.add(_normalize_privilege(str(action)))
    return privileges


def fetch_privileges(
    spark: SparkSession, securable_type: str, securable_name: str, principal: str
) -> set[str]:
    """Return the privileges currently granted to ``principal`` on a securable."""
    rows = spark.sql(f"SHOW GRANTS `{principal}` ON {securable_type} {securable_name}").collect()
    return _parse_grant_rows([r.asDict() for r in rows], principal)


def verify_privileges(
    spark: SparkSession,
    securable_type: str,
    securable_name: str,
    principal: str,
    *,
    expected: Sequence[str],
    exact: bool = True,
) -> None:
    """Assert ``principal`` holds the expected privileges on a securable.

    With ``exact=True`` (default) the granted set must equal ``expected`` —
    catching both missing privileges and accidental over-granting. With
    ``exact=False`` the granted set must merely include ``expected``.

    Raises:
        GrantVerificationError: if the granted set doesn't match.
    """
    actual = fetch_privileges(spark, securable_type, securable_name, principal)
    want = {_normalize_privilege(p) for p in expected}
    missing = want - actual
    extra = actual - want
    if missing or (exact and extra):
        detail = ""
        if missing:
            detail += f"; missing {sorted(missing)}"
        if exact and extra:
            detail += f"; unexpected {sorted(extra)}"
        raise GrantVerificationError(
            f"grant mismatch for `{principal}` on {securable_type} {securable_name}: "
            f"expected {sorted(want)}{' exactly' if exact else ' (subset)'}, "
            f"got {sorted(actual)}{detail}"
        )


def verify_no_privileges(
    spark: SparkSession, securable_type: str, securable_name: str, principal: str
) -> None:
    """Assert ``principal`` holds NO privileges on a securable (the negative test)."""
    actual = fetch_privileges(spark, securable_type, securable_name, principal)
    if actual:
        raise GrantVerificationError(
            f"`{principal}` should have no access to {securable_type} {securable_name} "
            f"but holds {sorted(actual)}"
        )


def verify_schema_reader(
    spark: SparkSession, catalog: str, schema: str, principal: str, *, exact: bool = True
) -> None:
    """Assert ``principal`` holds reader-tier (and, if ``exact``, only that) on a schema."""
    verify_privileges(
        spark,
        "SCHEMA",
        f"{catalog}.{schema}",
        principal,
        expected=READER_SCHEMA_PRIVILEGES,
        exact=exact,
    )


def verify_schema_engineer(
    spark: SparkSession, catalog: str, schema: str, principal: str, *, exact: bool = True
) -> None:
    """Assert ``principal`` holds engineer-tier (and, if ``exact``, only that) on a schema."""
    verify_privileges(
        spark,
        "SCHEMA",
        f"{catalog}.{schema}",
        principal,
        expected=ENGINEER_SCHEMA_PRIVILEGES,
        exact=exact,
    )


def verify_schema_no_access(spark: SparkSession, catalog: str, schema: str, principal: str) -> None:
    """Assert ``principal`` holds no privileges on ``catalog.schema``."""
    verify_no_privileges(spark, "SCHEMA", f"{catalog}.{schema}", principal)
