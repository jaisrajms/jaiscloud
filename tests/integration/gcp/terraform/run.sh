#!/usr/bin/env bash
#
# run.sh — opt-in Terraform / OpenTofu compatibility suite for jaiscloud-gcp.
#
# Drives the hashicorp/google provider against a running jaiscloud-gcp and
# asserts the resulting resources through the emulator's REST API. Only services
# jaiscloud-gcp implements are exercised (see README.md). The suite runs
# init -> validate -> plan -> apply -> spot checks -> destroy, and always
# destroys what it created (trap on exit).
#
# Usage:
#   tests/integration/gcp/terraform/run.sh [ENDPOINT] [PROJECT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#     PROJECT   project id (default: test-project)
#
# Env:
#   TF_BIN    terraform binary to use (default: terraform; set TF_BIN=tofu for
#             the OpenTofu variant)
#
# Prefer the Makefile targets, which start/stop the emulator for you:
#   make test-gcp-terraform
#   make test-gcp-opentofu

set -euo pipefail

ENDPOINT="${1:-http://localhost:8080}"
PROJECT="${2:-test-project}"
TF="${TF_BIN:-terraform}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="us-central1"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  exit 0
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

PASS=0
FAIL=0

# check NAME URL SUBSTRING [SUBSTRING...] — GET the URL and require every
# substring to be present. Records a failure (does not abort) so one run
# reports every divergence.
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# check_post NAME URL SUBSTRING [SUBSTRING...] — POST an empty JSON body (the
# project IAM read is a POST custom method) and require every substring.
check_post() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

tf() { "$TF" -chdir="$DIR" "$@"; }

DESTROYED=0
destroy() {
  [[ "$DESTROYED" == "1" ]] && return 0
  DESTROYED=1
  tf destroy -input=false -auto-approve -no-color \
    -var="endpoint=$ENDPOINT" -var="project=$PROJECT" >/dev/null 2>&1 || true
}
trap destroy EXIT INT TERM

echo "== $TF: init / validate / plan / apply (endpoint=$ENDPOINT project=$PROJECT)"
tf init -input=false -no-color >/dev/null
tf validate -no-color >/dev/null
tf plan -input=false -no-color \
  -var="endpoint=$ENDPOINT" -var="project=$PROJECT" >/dev/null
tf apply -input=false -auto-approve -no-color \
  -var="endpoint=$ENDPOINT" -var="project=$PROJECT" >/dev/null

out() { tf output -raw "$1" 2>/dev/null || true; }
SA_EMAIL="$(out service_account_email)"
SQL_INSTANCE="$(out sql_instance_name)"
SQL_DB="$(out sql_database_name)"
SQL_USER="$(out sql_user_name)"
ENABLED_SERVICE="$(out enabled_service)"

echo "== spot checks"
check "GCS bucket created" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket" \
  '"name":"jaiscloud-tf-bucket"'
check "GCS bucket labels persist" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket" \
  '"env":"compat-test"'
check "GCS object uploaded" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket/o/README.txt" \
  '"name":"README.txt"'
check "GCS object content-type" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket/o/README.txt" \
  '"contentType":"text/plain"'
check "IAM service account created" \
  "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$SA_EMAIL" 'jaiscloud-tf-sa'
check "Secret Manager secret created" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret" \
  'jaiscloud-tf-secret' '"automatic"'
check "Secret Manager version ENABLED" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret/versions/1" '"state":"ENABLED"'
check "KMS key ring created" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring" \
  'jaiscloud-tf-keyring'
check "KMS crypto key created" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring/cryptoKeys/jaiscloud-tf-key" \
  '"purpose":"ENCRYPT_DECRYPT"'
check "Pub/Sub topic created" \
  "$ENDPOINT/v1/projects/$PROJECT/topics/jaiscloud-tf-topic" 'jaiscloud-tf-topic'
check "Pub/Sub subscription created" \
  "$ENDPOINT/v1/projects/$PROJECT/subscriptions/jaiscloud-tf-sub" 'jaiscloud-tf-sub'
# Both topic grants must survive the provider's get/merge/set IAM flow.
check "Pub/Sub topic IAM keeps both grants" \
  "$ENDPOINT/v1/projects/$PROJECT/topics/jaiscloud-tf-topic:getIamPolicy" \
  'roles/pubsub.publisher' 'roles/pubsub.viewer' 'jaiscloud-tf-sa@' \
  'user:compat-viewer@example.com'
check "Pub/Sub subscription IAM grant" \
  "$ENDPOINT/v1/projects/$PROJECT/subscriptions/jaiscloud-tf-sub:getIamPolicy" \
  'roles/pubsub.subscriber'
check "Secret Manager IAM accessor grant" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret:getIamPolicy" \
  'roles/secretmanager.secretAccessor'
check "CRM project lookup reports ACTIVE" \
  "$ENDPOINT/v1/projects/$PROJECT" '"lifecycleState":"ACTIVE"'
check_post "Project IAM grant present" \
  "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy" \
  'roles/pubsub.publisher' 'jaiscloud-tf-sa@'
check "Service Usage service enabled" \
  "$ENDPOINT/v1/projects/$PROJECT/services/$ENABLED_SERVICE" '"state":"ENABLED"'
check "Service Usage enabled list includes service" \
  "$ENDPOINT/v1/projects/$PROJECT/services?filter=state:ENABLED" "$ENABLED_SERVICE"
check "Cloud SQL instance created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE" \
  '"kind":"sql#instance"' '"state":"RUNNABLE"'
check "Cloud SQL database created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE/databases/$SQL_DB" \
  '"kind":"sql#database"'
check "Cloud SQL user created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE/users/$SQL_USER" \
  '"kind":"sql#user"'

echo "== destroy"
destroy

echo "== summary: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
