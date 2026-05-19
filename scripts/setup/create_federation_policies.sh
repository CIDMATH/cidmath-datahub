#!/usr/bin/env bash
#
# Create an OIDC federation policy on a Databricks service principal so that
# GitHub Actions can authenticate to it without long-lived secrets.
#
# Run twice — once for the dev SP, once for the prod SP. The script takes the
# environment name and the SP's *numeric* id (not its application_id) as
# arguments. See the docs/operations.md and scripts/setup/README.md for the
# full bootstrap sequence.
#
# Reference: https://docs.databricks.com/aws/en/dev-tools/auth/provider-github
#
# Usage:
#   bash scripts/setup/create_federation_policies.sh <env> <sp-numeric-id>
#
# Example:
#   bash scripts/setup/create_federation_policies.sh dev 1234567890123456
#   bash scripts/setup/create_federation_policies.sh prod 6543210987654321
#
# Prerequisites:
#   - databricks CLI installed and authenticated to the Databricks ACCOUNT
#     (not a workspace): `databricks auth login --host https://accounts.cloud.databricks.com`
#   - The SP exists (created by create_service_principals.py)
#   - You have account-admin privileges (federation policy creation requires this)

set -euo pipefail

# --- Configuration ---
# These are hardcoded for the CIDMATH Data Hub. Edit if forking this scaffold
# for a different project.
ACCOUNT_ID="020f2275-adfe-44a9-99fa-e65e9369cea9"
GITHUB_ORG="CIDMATH"
GITHUB_REPO="cidmath-datahub"

# --- Argument parsing ---
if [[ $# -lt 2 ]]; then
    cat <<EOF
Usage: $0 <env> <sp-numeric-id>

Arguments:
  env             dev or prod
  sp-numeric-id   The 'id' (not 'application_id') of the service principal,
                  as printed by scripts/setup/create_service_principals.py.

Examples:
  $0 dev 1234567890123456
  $0 prod 6543210987654321
EOF
    exit 1
fi

ENV="$1"
SP_ID="$2"

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
    echo "ERROR: env must be 'dev' or 'prod' (got: $ENV)" >&2
    exit 1
fi

if [[ ! "$SP_ID" =~ ^[0-9]+$ ]]; then
    echo "ERROR: sp-numeric-id must be all digits (got: $SP_ID)" >&2
    echo "       Use the 'id' field from create_service_principals.py output," >&2
    echo "       not the 'application_id' UUID." >&2
    exit 1
fi

SUBJECT="repo:${GITHUB_ORG}/${GITHUB_REPO}:environment:${ENV}"

echo "Creating federation policy for SP ${SP_ID} (environment: ${ENV})"
echo "  Issuer:   https://token.actions.githubusercontent.com"
echo "  Subject:  ${SUBJECT}"
echo "  Audience: ${ACCOUNT_ID}"
echo

databricks account service-principal-federation-policy create "${SP_ID}" --json "$(cat <<EOF
{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["${ACCOUNT_ID}"],
    "subject": "${SUBJECT}"
  }
}
EOF
)"

echo
echo "Done. Federation policy created for the ${ENV} SP."
echo
echo "If the command failed with a duplicate-policy error, an existing policy"
echo "is already attached. List policies for this SP with:"
echo
echo "  databricks account service-principal-federation-policy list ${SP_ID}"
echo
echo "And delete the existing one if you need to recreate:"
echo
echo "  databricks account service-principal-federation-policy delete ${SP_ID} <policy-id>"
