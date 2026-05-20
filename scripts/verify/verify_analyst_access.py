"""Live verification of the analyst access boundary (ADR 0018, ADR 0019).

Run this AS a principal that is a member of ``ecdh-analysts`` ONLY (a dedicated
test service principal is the easiest such identity — see the README). It
confirms, by actually issuing queries, that a reader-tier identity:

  - CAN read the discovery surface (``discovery.datasets``) and the ``time``
    reference tables, and
  - CANNOT read the internal ``_ops`` schema.

It changes nothing — it only runs ``SELECT count(*)`` against each target and
classifies the result as allowed (succeeded) or blocked (errored). A correct
configuration yields all checks PASS.

Why a separate principal: Unity Catalog privileges are additive, so if you run
this as yourself while you are in ``ecdh-data-engineers``, the engineer grants
mask the analyst restriction and the ``_ops`` checks would (correctly) show you
*can* read it — which tells you nothing about the analyst experience. The whole
point is to test with an identity that has only the reader-tier grants.

Auth: uses the Databricks SDK default credential chain. Point it at the analyst
principal with either a config profile (``DATABRICKS_CONFIG_PROFILE``) or the
OAuth machine-to-machine env vars for that service principal:
``DATABRICKS_HOST`` / ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``.

A SQL warehouse is required to execute the statements; pass ``--warehouse-id``
or set ``DATABRICKS_WAREHOUSE_ID``. The warehouse only needs to exist and be
runnable by the analyst principal (CAN_USE); the analyst does not need any
extra catalog grant to *use* the warehouse itself.

Usage:
    python scripts/verify/verify_analyst_access.py \\
        --warehouse-id 0123456789abcdef --catalog ecdh_model_dev
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

_RUNNING_STATES = {"PENDING", "RUNNING"}
_POLL_TIMEOUT_SECONDS = 90


@dataclass
class Check:
    """A single access probe and whether the analyst should be allowed to run it."""

    name: str
    sql: str
    expect_allowed: bool


def build_checks(catalog: str) -> list[Check]:
    """The access boundary, expressed as queries the analyst should/shouldn't run."""
    return [
        # Should succeed — reader-tier grants on these schemas (ADR 0018/0019).
        Check(
            "read discovery.datasets",
            f"SELECT count(*) FROM {catalog}.discovery.datasets",
            True,
        ),
        Check(
            "read time.calendar_date",
            f"SELECT count(*) FROM {catalog}.time.calendar_date",
            True,
        ),
        Check(
            "read time.epi_week",
            f"SELECT count(*) FROM {catalog}.time.epi_week",
            True,
        ),
        # Should be blocked — analysts hold no grant on _ops.
        Check(
            "blocked from _ops.dataset_catalog",
            f"SELECT count(*) FROM {catalog}._ops.dataset_catalog",
            False,
        ),
        Check(
            "blocked from _ops.dq_results",
            f"SELECT count(*) FROM {catalog}._ops.dq_results",
            False,
        ),
    ]


def _state_of(resp) -> str:
    if resp.status and resp.status.state:
        return resp.status.state.value
    return "UNKNOWN"


def _error_detail(resp) -> str:
    if resp.status and resp.status.error:
        err = resp.status.error
        code = str(err.error_code) if err.error_code else ""
        msg = err.message or ""
        return f"{code}: {msg}".strip(": ").strip()
    return f"state={_state_of(resp)}"


def execute(w: WorkspaceClient, warehouse_id: str, catalog: str, sql: str) -> tuple[bool, str]:
    """Run a statement; return (succeeded, detail).

    ``succeeded`` is True only if the statement reached SUCCEEDED. Any error
    (permission denied, name resolution failure because the schema is invisible,
    etc.) returns False with the error text — for the "blocked" checks, any
    non-success is the expected outcome.
    """
    resp = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, catalog=catalog, wait_timeout="30s"
    )
    deadline = time.time() + _POLL_TIMEOUT_SECONDS
    while _state_of(resp) in _RUNNING_STATES and time.time() < deadline:
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if _state_of(resp) == "SUCCEEDED":
        return True, "query succeeded"
    return False, _error_detail(resp)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse id (or set DATABRICKS_WAREHOUSE_ID).",
    )
    parser.add_argument(
        "--catalog",
        default="ecdh_model_dev",
        help="Catalog to probe (default: ecdh_model_dev, where time + discovery live).",
    )
    args = parser.parse_args()

    if not args.warehouse_id:
        print("ERROR: --warehouse-id is required (or DATABRICKS_WAREHOUSE_ID).", file=sys.stderr)
        return 2

    w = WorkspaceClient()
    auth_type = w.config.auth_type
    me = w.current_user.me()
    identity = me.user_name or me.display_name
    print(f"Running as: {identity}  (auth_type={auth_type})")

    # Guard against the most common false result: if OAuth M2M didn't take, the
    # SDK silently falls back to your own profile, so you end up testing as an
    # engineer (who legitimately reads _ops) instead of the analyst SP — every
    # "blocked" check then wrongly "passes as allowed". Refuse to continue.
    if auth_type != "oauth-m2m":
        print(
            "\nERROR: not authenticated via OAuth M2M (auth_type="
            f"{auth_type!r}). This script must run AS the analyst service "
            "principal, or the results are meaningless. Set DATABRICKS_AUTH_TYPE="
            "oauth-m2m plus the SP's DATABRICKS_CLIENT_ID (application_id UUID) "
            "and DATABRICKS_CLIENT_SECRET, then re-run.",
            file=sys.stderr,
        )
        return 2

    print(f"Catalog under test: {args.catalog}")
    print(f"Warehouse: {args.warehouse_id}\n")

    results: list[tuple[Check, bool, bool, str]] = []
    for check in build_checks(args.catalog):
        succeeded, detail = execute(w, args.warehouse_id, args.catalog, check.sql)
        passed = succeeded if check.expect_allowed else not succeeded
        results.append((check, succeeded, passed, detail))

    name_w = max(len(c.name) for c, *_ in results)
    print(f"{'CHECK':<{name_w}}  {'EXPECT':<8}  {'RESULT':<9}  {'PASS?':<5}  DETAIL")
    print("-" * (name_w + 40))
    for check, succeeded, passed, detail in results:
        expect = "allow" if check.expect_allowed else "block"
        result = "allowed" if succeeded else "blocked"
        mark = "PASS" if passed else "FAIL"
        print(f"{check.name:<{name_w}}  {expect:<8}  {result:<9}  {mark:<5}  {detail}")

    failures = [r for r in results if not r[2]]
    print()
    if failures:
        print(f"{len(failures)} check(s) FAILED — the analyst boundary is not as intended.")
        return 1
    print("All checks PASSED — analyst can read discovery + time, and is blocked from _ops.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
