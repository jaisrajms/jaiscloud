# shellcheck shell=bash
#
# Case: terraform-google-firestore (TGM11, TGM12)
#
# The pinned consumer is GoogleCloudPlatform/terraform-google-firestore
# (commit 50d170b). Its root module creates one `google_firestore_database` and
# emits the CMEK block only when `kms_key_name != null`:
#
#   dynamic "cmek_config" {
#     for_each = var.kms_key_name != null ? [var.kms_key_name] : []
#     content { kms_key_name = cmek_config.value }
#   }
#
# Before TGM11 the emulator's Database did not model `cmek_config` at all, so a
# create succeeded silently and the provider's Read lost the block
# (`d.Set("cmek_config", flatten(res["cmekConfig"]))` → nil when absent). Since
# both `cmek_config` and its `kms_key_name` are Optional/Required + ForceNew, the
# next plan always forced a replacement:
#
#   # module.firestore.google_firestore_database.firestore_database must be replaced
#   + cmek_config { # forces replacement
#       + active_key_version = (known after apply)
#       + kms_key_name = "projects/test-project/locations/us-central1/keyRings/jc-ring/cryptoKeys/jc-key" # forces replacement
#     }
#   Plan: 1 to add, 0 to change, 1 to destroy.
#
# The gate proves the lifecycle converges, and that no block is fabricated:
#
#   CMEK apply -> GET carries cmekConfig.kmsKeyName -> No changes
#   mutate a non-ForceNew field (PITR disabled) -> apply -> GET -> No changes
#   a second module instance with kms_key_name = null -> GET has no cmekConfig
#   destroy
#
# Before TGM12 the emulator answered 409 ALREADY_EXISTS for any
# `POST …/databases?databaseId=(default)`, even in a project with no database,
# so the Firebase shape (`google_firestore_database { name = "(default)" }`)
# could not be created at all. Real GCP creates the implicit default database
# when the project has none and records the location, which is the phase added
# below:
#
#   (default) apply in its own workdir/state -> GET carries locationId ->
#   No changes -> destroy
#
# It runs FIRST and in its own state because a read of `(default)` (a LIST or a
# GET, including one in the CMEK phases) materialises it; a materialised default
# is a live record, so a later create would be ALREADY_EXISTS. The location is
# us-central1 and is echoed verbatim, and no location is invented for a default
# that only a read created.
#
# The key is created the way a real config creates one (`google_kms_key_ring` +
# `google_kms_crypto_key`, location us-central1) and its `.id` is passed as the
# module's `kms_key_name`, so the request carries a real KMS resource name.
#
# Module note: the module defaults `delete_protection_state` to
# DELETE_PROTECTION_ENABLED; the case disables it because real GCP (and this
# emulator) refuses to delete a protected database, so destroy would fail. Its
# `deletion_policy` default is already DELETE.
#
# Provider note: `firestore_custom_endpoint` and `kms_custom_endpoint` are the
# versioned API base URLs (the provider appends the resource path to them), so
# they carry exactly one trailing slash: "${endpoint}/v1/".
#
# Every apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-firestore"
MODULE_COMMIT="50d170b9ebfc"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-firestore"

DB="jc-cmek"
PLAIN_DB="jc-plain"
KEYRING="jc-firestore-ring"
KEY="jc-firestore-key"

KEY_NAME="projects/$PROJECT/locations/us-central1/keyRings/$KEYRING/cryptoKeys/$KEY"

# write_module generates the root module for one gate phase. $1 is the HCL
# literal for the CMEK module's `point_in_time_recovery_enablement`.
write_module() {
  local pitr="$1"

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

  # Versioned API base URLs: exactly one trailing slash each.
  firestore_custom_endpoint = "\${var.endpoint}/v1/"
  kms_custom_endpoint       = "\${var.endpoint}/v1/"
}

resource "google_kms_key_ring" "ring" {
  project  = var.project
  name     = "$KEYRING"
  location = "us-central1"
}

resource "google_kms_crypto_key" "key" {
  name     = "$KEY"
  key_ring = google_kms_key_ring.ring.id
  purpose  = "ENCRYPT_DECRYPT"
}

# The module emits cmek_config only when kms_key_name is non-null, so this
# instance is the CMEK path.
module "cmek" {
  source = "$MODULE_DIR"

  project_id  = var.project
  database_id = "$DB"
  location    = "us-central1"

  kms_key_name = google_kms_crypto_key.key.id

  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"

  point_in_time_recovery_enablement = $pitr
}

# kms_key_name defaults to null: the module emits no cmek_config block, and the
# emulator must not invent one.
module "plain" {
  source = "$MODULE_DIR"

  project_id  = var.project
  database_id = "$PLAIN_DB"
  location    = "nam5"

  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
}
EOF
}

# write_default_module generates the root module for the TGM12 phase: one module
# instance with database_id = "(default)" (the Firebase shape), no CMEK key, in
# this phase's own working directory.
write_default_module() {
  cat >"$DEFAULT_WORK/main.tf" <<EOF
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

  # Versioned API base URL: exactly one trailing slash.
  firestore_custom_endpoint = "\${var.endpoint}/v1/"
}

module "firestore" {
  source = "$MODULE_DIR"

  project_id  = var.project
  database_id = "(default)"
  location    = "us-central1"

  kms_key_name = null

  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
}
EOF
}

db_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/databases/$1"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_db asserts a database GET carries each expected substring.
check_db() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(body_of "$url")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
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

# check_db_absent asserts a database GET does not carry a substring. cmekConfig's
# presence is the CMEK enablement signal (database.proto), so an absent block is
# the only correct answer for a Google-managed database.
check_db_absent() {
  local name="$1" url="$2" unwanted="$3" body
  body="$(body_of "$url")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  if [[ "$body" == *"$unwanted"* ]]; then
    fail "$name (unexpected: $unwanted in $body)"
    return 0
  fi
  pass "$name"
}

# default_phase drives the Firebase shape — database_id = "(default)" — in its
# own working directory and state. It runs before any other phase because a read
# of `(default)` materialises it, and a materialised default is live, so a later
# create would be ALREADY_EXISTS.
default_phase() {
  local saved="$WORK"
  WORK="$DEFAULT_WORK"
  mkdir -p "$DEFAULT_WORK"
  write_default_module

  init_phase
  plan_phase "plan (implicit (default) database)"
  apply_phase "apply (implicit (default) database)"
  assert_no_api_errors

  echo "== spot checks ((default))"
  check_db "(default) GET carries the created locationId" "$(db_url "(default)")" \
    '"locationId":"us-central1"'

  echo "== $TF: second plan ((default) permadiff check)"
  plan_no_changes "re-plan reports no changes ((default) database)"

  # Destroy is this phase's own, in its own state directory: the driver's
  # destroy_phase (and its DESTROYED latch) belongs to $WORK.
  echo "== destroy ((default))"
  if "$TF" -chdir="$DEFAULT_WORK" destroy -input=false -auto-approve -no-color >"$DEFAULT_WORK/destroy.log" 2>&1; then
    pass "destroy ((default) database)"
  else
    cat "$DEFAULT_WORK/destroy.log"
    fail "destroy ((default) database) (see the log above)"
  fi

  WORK="$saved"
}

case_main() {
  # The (default) phase goes first, in $WORK/default: its own working directory
  # and state, so no earlier read of this project's default has materialised it.
  DEFAULT_WORK="$WORK/default"
  default_phase

  write_module '"POINT_IN_TIME_RECOVERY_ENABLED"'
  init_phase
  plan_phase "plan (CMEK database + Google-managed database)"
  apply_phase "apply (CMEK database + Google-managed database)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_db "CMEK database echoes its cmekConfig.kmsKeyName" "$(db_url "$DB")" \
    '"cmekConfig":{"kmsKeyName":"'"$KEY_NAME"'"}'
  check_db_absent "Google-managed database reports no cmekConfig" "$(db_url "$PLAIN_DB")" '"cmekConfig"'

  echo "== $TF: second plan (CMEK permadiff check)"
  plan_no_changes "re-plan reports no changes (cmek_config is not a forced replacement)"

  write_module '"POINT_IN_TIME_RECOVERY_DISABLED"'
  apply_phase "apply (point_in_time_recovery_enablement disabled)"
  assert_no_api_errors
  echo "== spot checks (mutate)"
  check_db "non-ForceNew PITR mutate applied" "$(db_url "$DB")" \
    '"pointInTimeRecoveryEnablement":"POINT_IN_TIME_RECOVERY_DISABLED"' \
    '"cmekConfig":{"kmsKeyName":"'"$KEY_NAME"'"}'
  plan_no_changes "re-plan after the PITR change reports no changes"

  destroy_phase
}
