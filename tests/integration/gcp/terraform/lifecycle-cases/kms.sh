# shellcheck shell=bash
#
# Case: kms — the lifecycle paths a Terraform user performs on a key ring and a
# crypto key, plus the two upstream constraints the Cloud KMS API imposes.
#
#   apply -> import (key ring + crypto key, clean plan) -> refresh-only
#   -> out-of-band label drift -> -replace classified as unsupported upstream
#     (the API cannot delete a key; re-create is 409, so the gate proves the
#     failure, the re-import and the rotation-restoring apply)
#   -> negative paths -> destroy -> the key ring and key survive (KMS deletes
#     neither; the provider only destroys versions)
#
# Two constraints are recorded, not worked around:
#   - The provider never deletes a key ring or crypto key: the key-ring delete is
#     state-only (no API call) and the crypto-key delete destroys every version
#     (scheduling them DESTROY_SCHEDULED). The Cloud KMS Discovery does define
#     both delete methods (the emulator serves them), but the provider does not
#     call them, so `terraform apply -replace` on a crypto key fails 409 on the
#     re-create. That is the behaviour a real-GCP user sees with this provider
#     (TF5-3); the gate asserts the failure, re-imports the key and re-enables
#     its rotation schedule.
#   - `projects.locations.keyRings.delete` and `cryptoKeys.delete` are served
#     over REST and enforce the documented child preconditions: a key with a live
#     version cannot be deleted and neither can a ring that still holds a key
#     (400 FAILED_PRECONDITION). The gate asserts both, then deletes an empty
#     ring and checks it is gone (TF5-4). Key-ring deletion is REST-only: the
#     Cloud KMS proto declares no DeleteKeyRing RPC.
#
# The empty-labels refresh artifact is the same provider normalisation as the
# storage and pubsub cases.

KEYRING="lc-kms-ring"
KEY="lc-kms-key"
KEY_ID="projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys/$KEY"

PROVIDER_HCL='provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  add_terraform_attribution_label = false

  # The versioned API base URL: one trailing slash.
  kms_custom_endpoint = "${var.endpoint}/v1/"
}'

# write_main <labels-hcl> generates the root module.
write_main() {
  local labels="${1:-}"
  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
  }
}

variable "endpoint" {
  type    = string
  default = "$ENDPOINT"
}

variable "project" {
  type    = string
  default = "$PROJECT"
}

$PROVIDER_HCL

resource "google_kms_key_ring" "ring" {
  name     = "$KEYRING"
  location = "global"
}

resource "google_kms_crypto_key" "key" {
  name     = "$KEY"
  key_ring = google_kms_key_ring.ring.id
  purpose  = "ENCRYPT_DECRYPT"

  rotation_period = "7776000s"

  version_template {
    algorithm        = "GOOGLE_SYMMETRIC_ENCRYPTION"
    protection_level = "SOFTWARE"
  }
  $labels
}
EOF
}

case_main() {
  write_main
  init_phase
  plan_phase "plan (key ring + crypto key)"
  apply_phase
  assert_no_api_errors "$LAST_APPLY_LOG" "apply"

  echo "== spot checks (create)"
  check "key ring read-back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING" \
    "\"name\":\"projects/$PROJECT/locations/global/keyRings/$KEYRING\""
  check "crypto key read-back" "$ENDPOINT/v1/$KEY_ID" \
    '"purpose":"ENCRYPT_DECRYPT"' \
    '"rotationPeriod":"7776000s"' \
    '"versionTemplate":{' \
    '"algorithm":"GOOGLE_SYMMETRIC_ENCRYPTION"' \
    '"protectionLevel":"SOFTWARE"' \
    '"state":"ENABLED"'
  plan_no_changes "re-plan after create reports no changes"

  echo "== refresh"
  refresh_phase labels-artifact "refresh-only plan reports no drift (empty-labels normalization classified)"

  echo "== import"
  import_phase google_kms_key_ring.ring \
    "projects/$PROJECT/locations/global/keyRings/$KEYRING"
  import_phase google_kms_crypto_key.key "$KEY_ID"

  echo "== out-of-band drift"
  write_main 'labels = { env = "lifecycle" }'
  apply_phase "apply (add crypto key labels)"
  plan_no_changes "re-plan after adding labels reports no changes"
  out_of_band "PATCH the crypto key's labels outside Terraform" PATCH \
    "$ENDPOINT/v1/$KEY_ID?updateMask=labels" \
    '{"labels":{"env":"drifted","drift":"yes"}}'
  plan_expect_diff "plan shows the managed out-of-band label" '"drifted"'
  apply_phase "apply (converge the drift)"
  plan_no_changes "re-plan after drift convergence reports no changes"
  check "crypto key labels converged to configuration" \
    "$ENDPOINT/v1/$KEY_ID" '"env":"lifecycle"'
  check "the unmanaged label key is preserved (SetLabels filters it)" \
    "$ENDPOINT/v1/$KEY_ID" '"drift":"yes"'

  echo "== replace is unsupported for a crypto key (upstream constraint)"
  # The provider's delete destroys versions and drops the key from state; it
  # never deletes the key. Re-creating the same name is a 409, so `-replace`
  # cannot work upstream — the gate proves the surfaced failure and the recovery.
  apply_fails_phase "crypto key -replace surfaces the 409" "409" \
    "$WORK/kms-replace.log" -replace=google_kms_crypto_key.key
  reimport_phase google_kms_crypto_key.key "$KEY_ID"
  # The failed delete disabled rotation; re-imported state needs the configured
  # schedule re-applied before the plan is clean.
  plan_phase "plan after the failed replace shows the disabled rotation"
  apply_phase "apply (restore the rotation schedule)"
  plan_no_changes "re-plan after restoring rotation reports no changes"
  check "rotation schedule restored" "$ENDPOINT/v1/$KEY_ID" '"rotationPeriod":"7776000s"'

  echo "== negative paths"
  expect_http_error "duplicate key ring create" POST \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings?keyRingId=$KEYRING" \
    '{}' 409
  expect_http_error "duplicate crypto key create" POST \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys?cryptoKeyId=$KEY" \
    '{"purpose":"ENCRYPT_DECRYPT"}' 409
  expect_http_error "invalid version template algorithm" POST \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys?cryptoKeyId=lc-kms-bad" \
    '{"purpose":"ENCRYPT_DECRYPT","versionTemplate":{"algorithm":"BOGUS"}}' 400
  expect_http_error "unsupported update mask" PATCH \
    "$ENDPOINT/v1/$KEY_ID?updateMask=purpose" '{"purpose":"MAC"}' 400
  # cryptoKeys.delete is served and enforces the documented precondition: a key
  # with a live version cannot be deleted (real KMS: FAILED_PRECONDITION).
  curl -s -o /dev/null -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' -d '{"purpose":"ENCRYPT_DECRYPT"}' \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys?cryptoKeyId=lc-kms-delprobe" 2>/dev/null || true
  expect_http_error "crypto key delete with a live version" DELETE \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys/lc-kms-delprobe" \
    '' 400
  curl -s -o /dev/null -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' -d '{}' \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings?keyRingId=lc-kms-dup" 2>/dev/null || true
  provider_error_create "duplicate key ring create surfaces through the provider" \
    "$PROVIDER_HCL" \
    'resource "google_kms_key_ring" "dup" {
  name     = "lc-kms-dup"
  location = "global"
}' \
    "409"

  destroy_phase
  # The provider does not call the documented delete methods, so the objects it
  # "destroyed" are still there: the key ring is state-only and a crypto key can
  # only have its versions scheduled for destruction.
  check_status "the key ring survives destroy (the provider never calls delete)" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING" 200
  check_status "the crypto key survives destroy" "$ENDPOINT/v1/$KEY_ID" 200
  check "the destroyed version is DESTROY_SCHEDULED" \
    "$ENDPOINT/v1/$KEY_ID/cryptoKeyVersions" '"state":"DESTROY_SCHEDULED"'
  # Cloud KMS documents projects.locations.keyRings.delete (an LRO) and the
  # emulator serves it: an empty ring is deleted (a follow-up GET is 404), while
  # a ring that still holds a crypto key is 400 FAILED_PRECONDITION — the same
  # child-precondition shape cryptoKeys.delete enforces. `KEYRING` holds
  # `$KEY` and the `lc-kms-delprobe` key here, so it is the negative case; the
  # empty ring is a separate name.
  expect_http_error "key ring delete with a crypto key in the ring" DELETE \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING" '' 400
  curl -s -o /dev/null -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' -d '{}' \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings?keyRingId=lc-kms-empty" 2>/dev/null || true
  local del_body
  del_body="$(curl -s -X DELETE -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/lc-kms-empty" 2>/dev/null || true)"
  if [[ "$del_body" == *'"done":true'* ]]; then
    pass "key ring delete returns a done Operation"
  else
    fail "key ring delete did not return a done Operation (body: ${del_body:0:240})"
  fi
  check_status "the deleted empty key ring is gone" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/lc-kms-empty" 404
}
