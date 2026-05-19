"""Create an OIDC federation policy on a Databricks service principal.

Cross-platform alternative to create_federation_policies.sh. Works on Windows
PowerShell, cmd, Bash, zsh — anywhere Python and databricks-sdk run. Avoids
shell-quoting pitfalls with JSON arguments.

Run once per SP (dev and prod). Idempotent — checks for an existing matching
policy on the SP before creating a new one.

Reference: https://docs.databricks.com/aws/en/dev-tools/auth/provider-github

Usage:
    python scripts/setup/create_federation_policies.py \\
        --env dev \\
        --sp-id 75962650827339

    python scripts/setup/create_federation_policies.py \\
        --env prod \\
        --sp-id <prod-sp-numeric-id>

Prerequisites:
    - Authenticated to the Databricks account (not just a workspace):
        databricks auth login --host https://accounts.cloud.databricks.com \\
            --account-id 020f2275-adfe-44a9-99fa-e65e9369cea9
    - The SP exists (created by create_service_principals.py)
    - `pip install databricks-sdk>=0.30`
    - Account-admin privileges
"""

from __future__ import annotations

import argparse
import sys

from databricks.sdk import AccountClient
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service import oauth2

# --- Configuration ---
# Hardcoded for the CIDMATH Data Hub. Edit if forking this scaffold.
ACCOUNT_ID = "020f2275-adfe-44a9-99fa-e65e9369cea9"
GITHUB_ORG = "CIDMATH"
GITHUB_REPO = "cidmath-datahub"
ISSUER = "https://token.actions.githubusercontent.com"


def build_subject(env: str) -> str:
    """Return the GitHub OIDC subject claim string for an environment."""
    return f"repo:{GITHUB_ORG}/{GITHUB_REPO}:environment:{env}"


def find_existing_policy(
    account: AccountClient,
    sp_id: int,
    subject: str,
) -> oauth2.FederationPolicy | None:
    """Return an existing policy on the SP with the matching subject, or None.

    The Databricks API allows multiple policies per SP. We treat a policy with
    the same subject as 'already configured' for idempotency.

    Note: the list endpoint returns 404 (NotFound) when the SP has zero
    policies — a Databricks API quirk. We catch and treat as "no policies".
    """
    try:
        policies = list(
            account.service_principal_federation_policy.list(
                service_principal_id=sp_id,
            )
        )
    except NotFound:
        return None

    for policy in policies:
        oidc = policy.oidc_policy
        if oidc and oidc.subject == subject and oidc.issuer == ISSUER:
            return policy
    return None


def create_policy(account: AccountClient, sp_id: int, env: str) -> None:
    subject = build_subject(env)

    print(f"Creating federation policy for SP {sp_id} (environment: {env})")
    print(f"  Issuer:   {ISSUER}")
    print(f"  Subject:  {subject}")
    print(f"  Audience: {ACCOUNT_ID}")
    print()

    existing = find_existing_policy(account, sp_id, subject)
    if existing is not None:
        print(
            f"  exists  policy_id={existing.uid}  "
            f"matches subject and issuer — skipping create."
        )
        return

    policy = oauth2.FederationPolicy(
        oidc_policy=oauth2.OidcFederationPolicy(
            issuer=ISSUER,
            audiences=[ACCOUNT_ID],
            subject=subject,
        ),
    )

    created = account.service_principal_federation_policy.create(
        service_principal_id=sp_id,
        policy=policy,
    )
    print(f"  created policy_id={created.uid}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        required=True,
        help="Environment the SP authenticates for (dev or prod).",
    )
    parser.add_argument(
        "--sp-id",
        type=int,
        required=True,
        help=(
            "Service principal numeric id (NOT the application_id UUID). "
            "Get this from create_service_principals.py output."
        ),
    )
    parser.add_argument(
        "--account-id",
        default=ACCOUNT_ID,
        help=f"Databricks account ID (default: {ACCOUNT_ID}).",
    )
    args = parser.parse_args()

    account = AccountClient(account_id=args.account_id)
    create_policy(account, args.sp_id, args.env)

    print()
    print("Done. Verify with:")
    print(f"  databricks account service-principal-federation-policy list {args.sp_id}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
