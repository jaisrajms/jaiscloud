#!/usr/bin/env bash
#
# Composition pattern 00 — project bootstrap (TF4-1).
#
# What: google_project (with a billing association) + four google_project_service
#       APIs + a service account + a project IAM grant + a GCS bucket + a
#       Pub/Sub topic/subscription + a Secret Manager secret/version + a Cloud
#       KMS key ring/key, all in one root module.
#
# Contract (patterns/lib.sh): init -> apply -> REST read-back asserts ->
# plan No changes -> destroy -> post-destroy asserts.
#
# The project id is unique per run: a destroyed project stays DELETE_REQUESTED
# in the emulator (real GCP reserves ids too), so a fixed id would make a
# second run against the same emulator fail on create. Override it with
# JC_TF_BOOTSTRAP_PROJECT=<id> when you need a stable name.
#
# Usage:
#   tests/integration/gcp/terraform/patterns/00-bootstrap/run.sh [ENDPOINT] [PROJECT]
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-patterns
#   make test-gcp-terraform-pattern PATTERN=00-bootstrap

set -euo pipefail

PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_NAME="00-bootstrap"
# shellcheck source=../lib.sh
source "$PATTERN_DIR/../lib.sh"

# A run-unique, grammar-valid project id (6-30 chars, starts with a letter).
BOOTSTRAP_PROJECT="${JC_TF_BOOTSTRAP_PROJECT:-jc-tf-boot-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')}"
BILLING_ACCOUNT="0X0X0X-0X0X0X-0X0X0X"
TF_VARS+=(-var "bootstrap_project=$BOOTSTRAP_PROJECT" -var "billing_account=$BILLING_ACCOUNT")

init_phase
apply_phase "apply (project + billing + services + IAM + storage + pubsub + secret + KMS)"
assert_no_api_errors

echo "== read-back (project $BOOTSTRAP_PROJECT)"
PROJECT_ID="$(tf_output project_id)"
PROJECT_NUMBER="$(tf_output project_number)"
BILLING_OUT="$(tf_output billing_account)"
SA_EMAIL="$(tf_output service_account_email)"
BUCKET="$(tf_output bucket_name)"
BUCKET_SELF_LINK="$(tf_output bucket_self_link)"
TOPIC_ID="$(tf_output topic_id)"
SUBSCRIPTION_ID="$(tf_output subscription_id)"
SECRET_ID="$(tf_output secret_id)"
SECRET_VERSION="$(tf_output secret_version)"
KEYRING_ID="$(tf_output key_ring_id)"
CRYPTO_KEY_ID="$(tf_output crypto_key_id)"

if [[ "$PROJECT_ID" == "$BOOTSTRAP_PROJECT" ]]; then
  pass "output project_id is the id Terraform created"
else
  fail "output project_id = '$PROJECT_ID', want '$BOOTSTRAP_PROJECT'"
fi

# ── Cloud Resource Manager + Cloud Billing ───────────────────────────────────
check "the created project is ACTIVE with its display name and labels" \
  "$ENDPOINT/v1/projects/$PROJECT_ID" \
  "\"projectId\":\"$PROJECT_ID\"" '"lifecycleState":"ACTIVE"' \
  '"name":"jaiscloud tf bootstrap"' '"env":"compat-test"'
check "the project lookup carries the Terraform-computed number" \
  "$ENDPOINT/v1/projects/$PROJECT_ID" "\"projectNumber\":\"$PROJECT_NUMBER\""
check "the billing association is stored for the created project" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/billingInfo" \
  "\"name\":\"projects/$PROJECT_ID/billingInfo\"" \
  "\"billingAccountName\":\"billingAccounts/$BILLING_ACCOUNT\"" \
  '"billingEnabled":true'
if [[ "$BILLING_OUT" == "$BILLING_ACCOUNT" ]]; then
  pass "google_project.billing_account round-trips as the bare id"
else
  fail "google_project.billing_account = '$BILLING_OUT', want '$BILLING_ACCOUNT'"
fi

# ── Service Usage: four APIs enabled in one apply ────────────────────────────
for api in storage.googleapis.com pubsub.googleapis.com \
  secretmanager.googleapis.com cloudkms.googleapis.com; do
  check "Service Usage enabled $api" \
    "$ENDPOINT/v1/projects/$PROJECT_ID/services/$api" \
    "\"name\":\"projects/$PROJECT_ID/services/$api\"" '"state":"ENABLED"'
done
check "the enabled-services list carries every API in the stack" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/services?filter=state:ENABLED" \
  storage.googleapis.com pubsub.googleapis.com \
  secretmanager.googleapis.com cloudkms.googleapis.com

# ── IAM: the service account and its project grant ───────────────────────────
check "the service account exists in the created project" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/serviceAccounts/$SA_EMAIL" \
  "\"email\":\"$SA_EMAIL\"" 'jc-tf-boot-sa' '"disabled":false'
check_post "the project IAM policy carries the service account grant" \
  "$ENDPOINT/v1/projects/$PROJECT_ID:getIamPolicy" \
  'roles/pubsub.publisher' "serviceAccount:$SA_EMAIL"

# ── Cloud Storage: the bucket in the created project ────────────────────────
check "the GCS bucket exists with its labels and location" \
  "$ENDPOINT/storage/v1/b/$BUCKET" \
  "\"name\":\"$BUCKET\"" '"location":"US"' '"env":"compat-test"'
check "the bucket self_link Terraform computed matches the API" \
  "$ENDPOINT/storage/v1/b/$BUCKET" "\"selfLink\":\"$BUCKET_SELF_LINK\""

# ── Pub/Sub: the topic and the subscription on it ───────────────────────────
check "the Pub/Sub topic exists in the created project" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/topics/jc-tf-boot-topic" \
  "\"name\":\"$TOPIC_ID\"" '"env":"compat-test"'
check "the subscription references the created topic" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/subscriptions/jc-tf-boot-sub" \
  "\"name\":\"$SUBSCRIPTION_ID\"" "\"topic\":\"$TOPIC_ID\"" '"ackDeadlineSeconds":20'

# ── Secret Manager: the secret and its version ──────────────────────────────
SECRET_B64="$(printf '%s' 'jaiscloud-tf-bootstrap-secret' | base64 | tr -d '\n')"
check "the secret exists with automatic replication" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/secrets/jc-tf-boot-secret" \
  "\"name\":\"$SECRET_ID\"" '"automatic"' '"env":"compat-test"'
check "the secret version is ENABLED" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/secrets/jc-tf-boot-secret/versions/$SECRET_VERSION" \
  '"state":"ENABLED"'
check "the secret version payload is readable" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/secrets/jc-tf-boot-secret/versions/$SECRET_VERSION:access" \
  "\"data\":\"$SECRET_B64\""

# ── Cloud KMS: the key ring and key ─────────────────────────────────────────
check "the KMS key ring exists in the created project" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/locations/$REGION/keyRings/jc-tf-boot-ring" \
  "\"name\":\"$KEYRING_ID\""
check "the KMS crypto key is ENCRYPT_DECRYPT" \
  "$ENDPOINT/v1/$CRYPTO_KEY_ID" \
  "\"name\":\"$CRYPTO_KEY_ID\"" '"purpose":"ENCRYPT_DECRYPT"'

plan_no_changes "re-plan of the composed stack reports no changes"

destroy_phase

echo "== post-destroy (the stack is empty; the project is DELETE_REQUESTED)"
check_status "the bucket is gone" "$ENDPOINT/storage/v1/b/$BUCKET" 404
check_status "the topic is gone" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/topics/jc-tf-boot-topic" 404
check_status "the subscription is gone" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/subscriptions/jc-tf-boot-sub" 404
check_status "the secret is gone" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/secrets/jc-tf-boot-secret" 404
check_status "the service account is gone" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/serviceAccounts/$SA_EMAIL" 404

# disable_on_destroy runs services.disable before the project is deleted.
for api in storage.googleapis.com pubsub.googleapis.com \
  secretmanager.googleapis.com cloudkms.googleapis.com; do
  check "Service Usage disabled $api on destroy" \
    "$ENDPOINT/v1/projects/$PROJECT_ID/services/$api" \
    "\"name\":\"projects/$PROJECT_ID/services/$api\"" '"state":"DISABLED"'
done

# The emulator (like real GCP) moves a deleted project to DELETE_REQUESTED and
# reserves the id; it never removes the row.
check "the project is DELETE_REQUESTED after destroy" \
  "$ENDPOINT/v1/projects/$PROJECT_ID" '"lifecycleState":"DELETE_REQUESTED"'

# Recorded provider/emulator boundary, not a stack defect: hashicorp/google
# never calls the documented keyRings.delete (state-only) or cryptoKeys.delete,
# and projects.locations.keyRings.delete is TF5-4 (501 in the emulator), so the
# KMS objects survive a destroy that removed everything else. The lifecycle kms
# case gates the same boundary in detail.
check_status "the KMS key ring survives destroy (the provider never calls delete)" \
  "$ENDPOINT/v1/projects/$PROJECT_ID/locations/$REGION/keyRings/jc-tf-boot-ring" 200
check_status "the KMS crypto key survives destroy (versions only are scheduled)" \
  "$ENDPOINT/v1/$CRYPTO_KEY_ID" 200

summary
