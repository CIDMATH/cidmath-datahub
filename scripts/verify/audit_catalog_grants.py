"""Drift check for catalog-level grants (ADR 0033; complements ADR 0012/0018).

Catalog-level grants are *not* applied by the bundle deploy jobs — the deploy
service principals lack ``MANAGE`` on the catalog (ADR 0012/0018), so they can't
grant at catalog scope. Those grants are declared in
``scripts/setup/grant_catalog_permissions.sql`` and applied by an account
admin / catalog owner. That file is the source of truth; this script makes it a
*checkable* one: it parses the declared ``GRANT ... ON CATALOG ...`` statements
and compares them against what Unity Catalog actually reports via ``SHOW
GRANTS``, failing on any drift (a declared grant that's missing, or — for a
declared principal — a catalog privilege it holds but the file doesn't declare).

This keeps catalog grants "code-based" in the way that matters (declarative,
reviewed, drift-detected) without handing the deploy SP catalog ``MANAGE`` — the
trade-off ADR 0033 deliberately avoids.

The parse + diff functions are pure (no Spark/SDK) so they unit-test offline
(ADR 0011); only :func:`fetch_actual_grants` and :func:`main` touch a workspace.

Identity: this must run as a principal that can ``SHOW GRANTS`` on the catalog —
a catalog owner, metastore admin, or the same governance identity that applies
``grant_catalog_permissions.sql``. The deploy SP generally cannot (no MANAGE),
which is the point. Uses the Databricks SDK default credential chain and a SQL
warehouse (``--warehouse-id`` / ``DATABRICKS_WAREHOUSE_ID``).

Usage:
    python scripts/verify/audit_catalog_grants.py --warehouse-id 0123... \\
        --sql-file scripts/setup/grant_catalog_permissions.sql --catalogs ecdh_dev ecdh_model_dev
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Pure logic: parse the declared grants and diff against actual (unit-tested).
# ---------------------------------------------------------------------------

#: A single-line ``GRANT <privilege> ON CATALOG <catalog> TO `<principal>```.
#: Commented lines (``-- ...``) are excluded by the caller before matching, so a
#: commented-out grant (e.g. the deliberately-disabled analyst prod access) is
#: correctly treated as *not declared*.
_GRANT_RE = re.compile(
    r"^GRANT\s+(?P<priv>.+?)\s+ON\s+CATALOG\s+(?P<catalog>[A-Za-z0-9_]+)\s+TO\s+`(?P<principal>[^`]+)`",
    re.IGNORECASE,
)


def normalize_privilege(privilege: str) -> str:
    """Normalize a privilege for comparison (space/underscore/case-insensitive).

    ``SHOW GRANTS`` may report ``USE_CATALOG`` while the SQL says ``USE CATALOG``;
    both normalize to ``"USE CATALOG"``.
    """
    return " ".join(privilege.strip().upper().replace("_", " ").split())


def parse_declared_catalog_grants(sql_text: str) -> dict[tuple[str, str], set[str]]:
    """Parse declared catalog GRANTs into ``{(catalog, principal): {privilege}}``.

    Only ``GRANT ... ON CATALOG ...`` statements are considered (schema-level and
    other statements are ignored). Comment lines are skipped, so commented-out
    grants are excluded.

    Args:
        sql_text: Contents of ``grant_catalog_permissions.sql``.

    Returns:
        Mapping of ``(catalog, principal)`` to the set of normalized privileges
        declared for it.
    """
    declared: dict[tuple[str, str], set[str]] = {}
    for raw_line in sql_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        match = _GRANT_RE.match(line)
        if not match:
            continue
        key = (match.group("catalog"), match.group("principal"))
        declared.setdefault(key, set()).add(normalize_privilege(match.group("priv")))
    return declared


@dataclass(frozen=True)
class GrantDrift:
    """A mismatch between declared and actual catalog grants for one principal."""

    catalog: str
    principal: str
    missing: frozenset[str]  # declared but not present
    extra: frozenset[str]  # present but not declared (over-grant)


def diff_catalog_grants(
    declared: dict[tuple[str, str], set[str]],
    actual: dict[tuple[str, str], set[str]],
    *,
    flag_extra: bool = True,
) -> list[GrantDrift]:
    """Diff declared vs. actual grants for the *declared* (catalog, principal) pairs.

    Only pairs we declare are audited — privileges held by principals we don't
    manage in the file are out of scope and never flagged. For each declared
    pair, ``missing`` is declared-minus-actual and (when ``flag_extra``)
    ``extra`` is actual-minus-declared (catalog-level over-granting).

    Returns:
        One :class:`GrantDrift` per pair that differs, sorted by catalog then
        principal. Empty list means no drift.
    """
    drifts: list[GrantDrift] = []
    for key in sorted(declared):
        want = declared[key]
        have = actual.get(key, set())
        missing = want - have
        extra = (have - want) if flag_extra else set()
        if missing or extra:
            drifts.append(
                GrantDrift(
                    catalog=key[0],
                    principal=key[1],
                    missing=frozenset(missing),
                    extra=frozenset(extra),
                )
            )
    return drifts


# ---------------------------------------------------------------------------
# IO: read actual grants from the workspace (mirrors verify_analyst_access.py).
# ---------------------------------------------------------------------------

_RUNNING_STATES = {"PENDING", "RUNNING"}
_POLL_TIMEOUT_SECONDS = 90


def _state_of(resp) -> str:
    if resp.status and resp.status.state:
        return resp.status.state.value
    return "UNKNOWN"


def _rows(resp) -> list[list[str]]:
    """Return the result rows of a finished statement as lists of strings."""
    if not resp.result or not resp.result.data_array:
        return []
    return resp.result.data_array


def _column_index(resp, *names: str) -> int:
    """Find a result column index by any of ``names`` (case-insensitive)."""
    cols = resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else []
    wanted = {n.lower() for n in names}
    for col in cols:
        if (col.name or "").lower() in wanted:
            return col.position
    raise KeyError(f"none of columns {names} found in SHOW GRANTS result")


def fetch_actual_grants(
    w, warehouse_id: str, pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], set[str]]:
    """Run ``SHOW GRANTS`` per (catalog, principal) and return actual privileges.

    Args:
        w: A ``databricks.sdk.WorkspaceClient``.
        warehouse_id: SQL warehouse to execute against.
        pairs: The ``(catalog, principal)`` pairs to query (the declared ones).

    Returns:
        Mapping of each pair to its set of normalized catalog privileges.
    """
    actual: dict[tuple[str, str], set[str]] = {}
    for catalog, principal in pairs:
        resp = w.statement_execution.execute_statement(
            statement=f"SHOW GRANTS `{principal}` ON CATALOG {catalog}",
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        deadline = time.time() + _POLL_TIMEOUT_SECONDS
        while _state_of(resp) in _RUNNING_STATES and time.time() < deadline:
            time.sleep(2)
            resp = w.statement_execution.get_statement(resp.statement_id)
        if _state_of(resp) != "SUCCEEDED":
            raise RuntimeError(
                f"SHOW GRANTS failed for `{principal}` ON CATALOG {catalog}: {_state_of(resp)}"
            )
        principal_idx = _column_index(resp, "Principal", "principal")
        action_idx = _column_index(resp, "ActionType", "action_type", "action")
        privs = {
            normalize_privilege(row[action_idx])
            for row in _rows(resp)
            if (row[principal_idx] or "") == principal
        }
        actual[(catalog, principal)] = privs
    return actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse id (or set DATABRICKS_WAREHOUSE_ID).",
    )
    parser.add_argument(
        "--sql-file",
        default="scripts/setup/grant_catalog_permissions.sql",
        help="Declared-grants source of truth (default: scripts/setup/grant_catalog_permissions.sql).",
    )
    parser.add_argument(
        "--catalogs",
        nargs="*",
        default=None,
        help="Limit the audit to these catalogs (default: all catalogs in the SQL file).",
    )
    parser.add_argument(
        "--no-flag-extra",
        action="store_true",
        help="Only fail on missing grants, not on extra (over-granted) catalog privileges.",
    )
    args = parser.parse_args()

    if not args.warehouse_id:
        print("ERROR: --warehouse-id is required (or DATABRICKS_WAREHOUSE_ID).", file=sys.stderr)
        return 2

    declared = parse_declared_catalog_grants(Path(args.sql_file).read_text(encoding="utf-8"))
    if args.catalogs:
        wanted = set(args.catalogs)
        declared = {k: v for k, v in declared.items() if k[0] in wanted}
    if not declared:
        print("No declared catalog grants matched; nothing to audit.", file=sys.stderr)
        return 2

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    print(f"Auditing {len(declared)} declared (catalog, principal) grant set(s) from {args.sql_file}\n")
    actual = fetch_actual_grants(w, args.warehouse_id, sorted(declared))
    drifts = diff_catalog_grants(declared, actual, flag_extra=not args.no_flag_extra)

    for key in sorted(declared):
        catalog, principal = key
        status = "OK"
        drift = next((d for d in drifts if (d.catalog, d.principal) == key), None)
        if drift:
            parts = []
            if drift.missing:
                parts.append(f"MISSING {sorted(drift.missing)}")
            if drift.extra:
                parts.append(f"EXTRA {sorted(drift.extra)}")
            status = "; ".join(parts)
        print(f"[{'DRIFT' if drift else 'ok':<5}] {catalog} / {principal}: {status}")

    print()
    if drifts:
        print(f"{len(drifts)} catalog grant(s) drifted from {args.sql_file}.")
        print("Apply the file (as a catalog owner) or update it to match intent.")
        return 1
    print("Catalog grants match the declared source of truth.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
