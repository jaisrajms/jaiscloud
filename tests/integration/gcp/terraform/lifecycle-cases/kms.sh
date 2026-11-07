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
#   - A Cloud KMS key ring and crypto key cannot be deleted. The provider's
#     delete removes the key ring from state without calling the API and destroys
#     every crypto-key version (scheduling them DESTROY_SCHEDULED); the objects
#     remain readable. `terraform apply -replace` therefore cannot recreate a
#     key under the same name: it fails 409 on the create. That is real GCP
#     behaviour, so the replace gate is an expected-failure gate (TF5-3), and
#     recoveries re-import the key and re-enable its rotation schedule.
#   - A DELETE on a key ring has no API method (real KMS has no delete route).
#     The emulator answers 501; the provider never sends it. Recorded as TF5-4.
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
  # Cloud KMS deletes neither object: the key ring has no delete method and a
  # crypto key can only have its versions scheduled for destruction.
  check_status "the key ring survives destroy (no API delete method)" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING" 200
  check_status "the crypto key survives destroy" "$ENDPOINT/v1/$KEY_ID" 200
  check "the destroyed version is DESTROY_SCHEDULED" \
    "$ENDPOINT/v1/$KEY_ID/cryptoKeyVersions" '"state":"DESTROY_SCHEDULED"'
  # Cloud KMS has no key-ring delete method at all; the provider never issues
  # one. The emulator answers 501 UNIMPLEMENTED. Recorded (TF5-4) rather than
  # gated, because any 4xx/5xx is defensible for a method real GCP does not
  # define and no consumer depends on it.
  local del_code
  del_code="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING" 2>/dev/null || true)"
  note "key-ring DELETE returns HTTP $del_code (real KMS has no delete method; provider never sends it)"
}
