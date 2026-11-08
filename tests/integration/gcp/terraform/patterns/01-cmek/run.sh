#!/usr/bin/env bash
#
# Composition pattern 01 — CMEK everywhere (TF4-1).
#
# What: Cloud KMS key rings/keys (a regional pair for storage + BigQuery and a
#       global key for Secret Manager's automatic replication), a GCS bucket
#       with `encryption.default_kms_key_name`, a BigQuery dataset with
#       `default_encryption_configuration` and a Secret Manager secret whose
#       replication carries the key.
#
# Contract (patterns/lib.sh): init -> apply -> REST read-back asserts ->
# plan No changes -> rotate the referenced key -> apply -> asserts ->
# plan No changes -> destroy -> post-destroy asserts.
#
# The rotation phase re-applies the same stack with `active_key = "rotated"`:
# both the bucket and the dataset must switch to the second key in place and
# re-plan clean. The secret's replication key is not rotated because the
# provider models `replication` as ForceNew (a change would replace the
# secret); its PATCH path is covered by the service/REST tests.
#
# Usage:
#   tests/integration/gcp/terraform/patterns/01-cmek/run.sh [ENDPOINT] [PROJECT]
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-patterns
#   make test-gcp-terraform-pattern PATTERN=01-cmek

set -euo pipefail

PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_NAME="01-cmek"
# shellcheck source=../lib.sh
source "$PATTERN_DIR/../lib.sh"

# Per-run suffix: the provider never deletes a key ring (TF5-4), so the rings
# survive destroy and a fixed name would collide on the next run against the
# same emulator.
SUFFIX="${JC_TF_CMEK_SUFFIX:-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')}"
TF_VARS+=(-var "cmek_suffix=$SUFFIX")

# active_key goes in its own auto tfvars so the rotation phase can rewrite it
# without passing the same -var twice.
active_key_file="$WORK/active.auto.tfvars.json"
write_active_key() {
  printf '{"active_key":"%s"}\n' "$1" >"$active_key_file"
}

write_active_key primary
init_phase
apply_phase "apply (KMS + bucket CMEK + BigQuery CMEK + Secret Manager CMEK)"
assert_no_api_errors

echo "== read-back (primary key)"
BUCKET="$(tf_output bucket_name)"
DATASET="$(tf_output dataset_id)"
SECRET_ID="$(tf_output secret_id)"
SECRET_VERSION="$(tf_output secret_version)"
PRIMARY_KEY="$(tf_output primary_key_id)"
ROTATED_KEY="$(tf_output rotated_key_id)"
GLOBAL_KEY="$(tf_output global_key_id)"
DATA_KEY="$(tf_output data_key)"

if [[ "$DATA_KEY" == "$PRIMARY_KEY" ]]; then
  pass "the stack references the primary key"
else
  fail "data_key = '$DATA_KEY', want the primary key '$PRIMARY_KEY'"
fi

check "the primary crypto key is ENCRYPT_DECRYPT" \
  "$ENDPOINT/v1/$PRIMARY_KEY" "\"name\":\"$PRIMARY_KEY\"" '"purpose":"ENCRYPT_DECRYPT"'
check "the rotation crypto key is ENCRYPT_DECRYPT" \
  "$ENDPOINT/v1/$ROTATED_KEY" "\"name\":\"$ROTATED_KEY\"" '"purpose":"ENCRYPT_DECRYPT"'
check "the global crypto key is ENCRYPT_DECRYPT" \
  "$ENDPOINT/v1/$GLOBAL_KEY" "\"name\":\"$GLOBAL_KEY\"" '"purpose":"ENCRYPT_DECRYPT"'

check "the GCS bucket echoes the default KMS key" \
  "$ENDPOINT/storage/v1/b/$BUCKET" \
  "\"defaultKmsKeyName\":\"$PRIMARY_KEY\"" '"env":"compat-test"'
check "the BigQuery dataset echoes the default encryption key" \
  "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$DATASET" \
  '"defaultEncryptionConfiguration"' "\"kmsKeyName\":\"$PRIMARY_KEY\"" '"env":"compat-test"'
check "the Secret Manager secret echoes the replication CMEK key" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jc-tf-cmek-secret-$SUFFIX" \
  "\"name\":\"$SECRET_ID\"" '"automatic"' \
  '"customerManagedEncryption"' "\"kmsKeyName\":\"$GLOBAL_KEY\""
check "the secret version is ENABLED" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jc-tf-cmek-secret-$SUFFIX/versions/$SECRET_VERSION" \
  '"state":"ENABLED"'

plan_no_changes "re-plan of the CMEK stack reports no changes"

# Capture the storage object identity and the dataset creation time so the
# rotation phase can prove the key change was *in place* (a replacement would
# mint a new bucket/dataset instead).
bucket_json() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/storage/v1/b/$BUCKET" 2>/dev/null || true
}
dataset_json() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$DATASET" 2>/dev/null || true
}
json_field() { sed -n "s/.*\"$2\":\"\([^\"]*\)\".*/\1/p" <<<"$1" | head -1; }

BUCKET_CREATED="$(json_field "$(bucket_json)" timeCreated)"
BUCKET_METAGEN="$(json_field "$(bucket_json)" metageneration)"
DATASET_CREATED="$(json_field "$(dataset_json)" creationTime)"

# ── Rotate the referenced key on the resources that update it in place ───────
echo "== rotate: bucket and dataset switch to the second regional key"
write_active_key rotated
apply_phase "apply (active_key = rotated)"
assert_no_api_errors

DATA_KEY="$(tf_output data_key)"
if [[ "$DATA_KEY" == "$ROTATED_KEY" ]]; then
  pass "the stack now references the rotated key"
else
  fail "data_key = '$DATA_KEY', want the rotated key '$ROTATED_KEY'"
fi
check "the GCS bucket echoes the rotated key" \
  "$ENDPOINT/storage/v1/b/$BUCKET" "\"defaultKmsKeyName\":\"$ROTATED_KEY\""
check "the BigQuery dataset echoes the rotated key" \
  "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$DATASET" \
  "\"kmsKeyName\":\"$ROTATED_KEY\""
check "the Secret Manager secret kept its global key" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jc-tf-cmek-secret-$SUFFIX" \
  "\"kmsKeyName\":\"$GLOBAL_KEY\""

# The rotation must not replace the data resources: the bucket keeps its
# creation time and increments its metageneration, and the dataset keeps its
# creationTime.
BUCKET_CREATED_AFTER="$(json_field "$(bucket_json)" timeCreated)"
BUCKET_METAGEN_AFTER="$(json_field "$(bucket_json)" metageneration)"
if [[ -n "$BUCKET_CREATED" && "$BUCKET_CREATED_AFTER" == "$BUCKET_CREATED" &&
  "$BUCKET_METAGEN" =~ ^[0-9]+$ && "$BUCKET_METAGEN_AFTER" =~ ^[0-9]+$ &&
  "$BUCKET_METAGEN_AFTER" -gt "$BUCKET_METAGEN" ]]; then
  pass "the bucket key rotation was in place (timeCreated kept, metageneration $BUCKET_METAGEN -> $BUCKET_METAGEN_AFTER)"
else
  fail "the bucket was replaced by the key rotation (timeCreated '$BUCKET_CREATED' -> '$BUCKET_CREATED_AFTER', metageneration '$BUCKET_METAGEN' -> '$BUCKET_METAGEN_AFTER')"
fi
DATASET_CREATED_AFTER="$(json_field "$(dataset_json)" creationTime)"
if [[ -n "$DATASET_CREATED" && "$DATASET_CREATED_AFTER" == "$DATASET_CREATED" ]]; then
  pass "the dataset key rotation was in place (creationTime kept)"
else
  fail "the dataset was replaced by the key rotation (creationTime '$DATASET_CREATED' -> '$DATASET_CREATED_AFTER')"
fi

plan_no_changes "re-plan after the key rotation reports no changes"

destroy_phase

echo "== post-destroy (data resources gone; key rings are provider artifacts)"
check_status "the bucket is gone" "$ENDPOINT/storage/v1/b/$BUCKET" 404
check_status "the BigQuery dataset is gone" \
  "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$DATASET" 404
check_status "the secret is gone" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jc-tf-cmek-secret-$SUFFIX" 404

# Recorded provider/emulator boundary (TF5-3/TF5-4): hashicorp/google never
# calls the documented keyRings.delete (state-only) or cryptoKeys.delete, and
# projects.locations.keyRings.delete is a 501 in the emulator, so the KMS
# objects outlive the destroy that removed every data resource.
check_status "the primary key ring survives destroy (the provider never calls delete)" \
  "$ENDPOINT/v1/$PRIMARY_KEY" 200
check_status "the global key ring survives destroy" "$ENDPOINT/v1/$GLOBAL_KEY" 200

summary
