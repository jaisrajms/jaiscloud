# shellcheck shell=bash
#
# Case: terraform-google-kms
#
# The pinned consumer is terraform-google-modules/terraform-google-kms
# (v4.2.1, commit ef6e0eda7032). It creates a key ring plus one
# `google_kms_crypto_key` per entry in `keys`, and it always emits the key's
# `version_template { algorithm = ..., protection_level = ... }` block.
#
# Before TGM6 the emulator's crypto-key update allowlisted only labels,
# rotation_period and next_rotation_time, so changing `key_algorithm` sent
#
#   PATCH /v1/projects/<p>/locations/global/keyRings/jc-keyring/cryptoKeys/jc-mac?updateMask=versionTemplate.algorithm
#
# and got
#
#   400 invalidArgument: unsupported update_mask path: versionTemplate.algorithm
#
# so the module could not change a key's algorithm in place.
#
# The gate proves the lifecycle converges:
#
#   create  (MAC key, HMAC_SHA256, labels) -> assert GET ->
#           No changes
#   mutate  (key_algorithm HMAC_SHA512) -> apply -> no 400 -> assert GET ->
#           No changes; a version created afterwards carries HMAC_SHA512
#   clear   (key_algorithm back to HMAC_SHA256) -> apply -> No changes
#   destroy
#
# Module note: the module's `key_rotation_period` default is "7776000s", but
# CryptoKey.rotation_period is "Keys with purpose ENCRYPT_DECRYPT support
# automatic rotation. For other keys, this field must be omitted." (Discovery),
# and real GCP rejects a schedule on a MAC key. The case therefore passes
# `key_rotation_period = null`; the rotation mask path keeps its own coverage in
# the service/transport tests.
#
# Provider note: `kms_custom_endpoint` is the versioned API base URL (the
# provider appends the resource path to it), so it carries exactly one trailing
# slash: "${endpoint}/v1/".
#
# Every apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-kms"
MODULE_COMMIT="ef6e0eda7032"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-kms"

KEYRING="jc-keyring"
KEY="jc-mac"

# write_module generates the root module for one gate phase. $1 is the HCL
# literal for `key_algorithm`, $2 the HCL literal for `labels`.
write_module() {
  local algorithm="$1" labels="$2"

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

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

provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false

  # The versioned API base URL: one trailing slash.
  kms_custom_endpoint = "\${var.endpoint}/v1/"
}

module "kms" {
  source = "$MODULE_DIR"

  project_id = var.project
  keyring    = "$KEYRING"
  location   = "global"
  keys       = ["$KEY"]

  purpose         = "MAC"
  key_algorithm   = $algorithm
  key_protection_level = "SOFTWARE"

  # Use the module's ephemeral key (prevent_destroy = false) so destroy works.
  prevent_destroy = false

  # A MAC key cannot carry a rotation schedule (Discovery: only
  # ENCRYPT_DECRYPT keys rotate), and the module defaults it to 7776000s.
  key_rotation_period = null

  labels = $labels
}

output "key_id" {
  value = module.kms.keys
}
EOF
}

key_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/global/keyRings/$KEYRING/cryptoKeys/$KEY"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_key asserts a crypto-key GET carries each expected substring.
check_key() {
  local name="$1"
  shift
  local body want
  body="$(body_of "$(key_url)")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $(key_url))"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      fail "$name (missing: $want)"
      return 0
    fi
  done
  pass "$name"
}

# create_version POSTs an empty CreateCryptoKeyVersion request and asserts the
# new version's algorithm — the proof that the update changed the template for
# *future* versions while leaving existing ones alone.
check_new_version_algorithm() {
  local want="$1" body
  body="$(curl -sf -X POST \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{}' "$(key_url)/cryptoKeyVersions" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "create crypto key version (no response)"
    return 0
  fi
  if [[ "$body" != *"\"algorithm\":\"$want\""* ]]; then
    fail "new version algorithm = $want (got: $body)"
    return 0
  fi
  pass "a version created after the update uses $want"
}

case_main() {
  write_module '"HMAC_SHA256"' '{ team = "platform" }'
  init_phase
  plan_phase "plan (key ring + MAC crypto key)"
  apply_phase "apply (key ring + MAC crypto key, HMAC_SHA256)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_key "version template algorithm and labels read back" \
    '"purpose":"MAC"' \
    '"versionTemplate":{' '"algorithm":"HMAC_SHA256"' \
    '"protectionLevel":"SOFTWARE"' \
    '"team":"platform"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  write_module '"HMAC_SHA512"' '{ team = "platform" }'
  apply_phase "apply (key_algorithm changed to HMAC_SHA512)"
  assert_no_api_errors
  echo "== spot checks (mutate)"
  check_key "changed version template algorithm read back" \
    '"algorithm":"HMAC_SHA512"' \
    '"team":"platform"'
  plan_no_changes "re-plan after the algorithm change reports no changes"
  check_new_version_algorithm "HMAC_SHA512"

  write_module '"HMAC_SHA256"' '{ team = "platform" }'
  apply_phase "apply (key_algorithm changed back to HMAC_SHA256)"
  assert_no_api_errors
  echo "== spot checks (clear)"
  check_key "restored version template algorithm read back" \
    '"algorithm":"HMAC_SHA256"' \
    '"team":"platform"'
  plan_no_changes "re-plan after restoring the algorithm reports no changes"

  destroy_phase
}
