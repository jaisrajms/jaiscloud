# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-secret-manager (TF3-3)
#
# The pinned consumer is the CFT Secret Manager blueprint (v0.10.0, commit
# 77fca80ccd6a). Its root module creates the served secret surface:
#
#   google_secret_manager_secret           -> secrets.create / .get / .patch
#   google_secret_manager_secret_version   -> secrets.versions.add / .access /
#                                             .destroy, plus the version list
#   google_secret_manager_secret_iam_binding
#                                          -> secrets.getIamPolicy /
#                                             .setIamPolicy (authoritative)
#
# and it emits `replication`, `labels`, `topics`, `rotation` and
# `version_destroy_ttl`. The gate proves that lifecycle converges:
#
#   apply (2 secrets: automatic + CMEK + rotation + topic, and user-managed
#          replicas with a per-replica CMEK key; one version each; one
#          authoritative IAM binding carrying two members)
#     -> REST asserts (replication arm and keys, labels, topics, rotation,
#        version payload, IAM members)
#   plan -detailed-exitcode -> No changes
#   mutate (labels, rotation period, the automatic secret's payload, the IAM
#           member list) -> apply -> REST asserts -> No changes
#   destroy -> secret GET and version access are 404
#
# Pruned inputs, so nothing here reaches an unserved resource (documented, not
# silent):
#
#   * `add_kms_permissions` / `add_pubsub_permissions` stay empty: both drive
#     `google_project_service_identity` (google-beta), which needs the unserved
#     Service Usage `generateServiceIdentity` surface. The CMEK keys are passed
#     directly instead, which is the part the secret API validates.
#   * `version_destroy_ttl` stays null (the delayed-destroy surface is covered by
#     the service/transport tests); `topics` is configured only on the rotating
#     secret, because the API requires a topic for a rotation schedule.
#   * Regional secrets (`google_secret_manager_regional_secret*`) are not served
#     (TF1-4, `fix/gcp-secretmanager-regional`); the module only creates global
#     `projects.secrets` resources.
#
# Provider note: `secret_manager_custom_endpoint`, `kms_custom_endpoint` and
# `pubsub_custom_endpoint` are the versioned API base URLs, so each carries one
# trailing slash. Every apply log must be free of 400/501 and
# `unsupported update_mask`.

MODULE_REPO="terraform-google-secret-manager"
MODULE_COMMIT="77fca80ccd6a"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-secret-manager"

SECRET_AUTO="jc-tf-sm-auto"
SECRET_USER="jc-tf-sm-user"

# write_module generates the root module for one gate phase.
#   $1 automatic-secret labels (HCL map literal)
#   $2 automatic-secret rotation period (HCL string literal)
#   $3 automatic-secret payload (HCL string literal)
#   $4 IAM binding members (HCL list literal)
write_module() {
  local labels="$1" period="$2" payload="$3" members="$4"

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
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
  project               = var.project
  region                = "us-central1"
  user_project_override = false

  secret_manager_custom_endpoint = "\${var.endpoint}/v1/"
  kms_custom_endpoint            = "\${var.endpoint}/v1/"
  pubsub_custom_endpoint         = "\${var.endpoint}/v1/"
  iam_custom_endpoint            = "\${var.endpoint}/"
  # google_service_account resolves through the IAM Admin client, whose beta
  # endpoint the provider uses; the storage case records the same requirement.
  iam_beta_custom_endpoint = "\${var.endpoint}/v1/"
}

# The CMEK keys the secrets name. Automatic replication only accepts a key in
# \`global\`; a user-managed replica accepts a key in its own location.
resource "google_kms_key_ring" "global" {
  name     = "jc-tf-sm-global"
  location = "global"
  project  = var.project
}

resource "google_kms_crypto_key" "global" {
  name     = "jc-tf-sm-global-key"
  key_ring = google_kms_key_ring.global.id
  purpose  = "ENCRYPT_DECRYPT"
}

resource "google_kms_key_ring" "regional" {
  name     = "jc-tf-sm-regional"
  location = "us-central1"
  project  = var.project
}

resource "google_kms_crypto_key" "regional" {
  name     = "jc-tf-sm-regional-key"
  key_ring = google_kms_key_ring.regional.id
  purpose  = "ENCRYPT_DECRYPT"
}

# A rotation schedule requires a Pub/Sub topic. The emulator stores the topic
# and publishes the test notifications to it; nothing consumes them.
resource "google_pubsub_topic" "rotation" {
  name    = "jc-tf-sm-rotation"
  project = var.project
}

resource "google_service_account" "reader" {
  account_id   = "jc-tf-sm-reader"
  display_name = "jaiscloud terraform secret manager reader"
  project      = var.project
}

module "secret_manager" {
  source = "$MODULE_DIR"

  project_id = var.project

  secrets = [
    {
      name               = "$SECRET_AUTO"
      secret_data        = $payload
      next_rotation_time = "2035-01-01T00:00:00Z"
      rotation_period    = $period
    },
    {
      name        = "$SECRET_USER"
      secret_data = "user-managed-payload"
    },
  ]

  automatic_replication = {
    "$SECRET_AUTO" = { kms_key_name = google_kms_crypto_key.global.id }
  }

  user_managed_replication = {
    "$SECRET_USER" = [
      { location = "us-central1", kms_key_name = google_kms_crypto_key.regional.id },
      { location = "us-east1", kms_key_name = null },
    ]
  }

  topics = {
    "$SECRET_AUTO" = [{ name = google_pubsub_topic.rotation.id }]
  }

  labels = {
    "$SECRET_AUTO" = $labels
  }

  secret_accessors_list = $members
}
EOF
}

secret_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/secrets/$1"
}

# post_body POSTs an empty JSON body (the IAM read custom methods use POST) and
# returns the response body.
post_body() {
  curl -sf -X POST -H "Content-Type: application/json" -d '{}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_body asserts a GET carries each expected substring. Unlike the driver's
# check() it also fails when the endpoint answers nothing, so a typo in the URL
# cannot pass vacuously.
check_body() {
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

# check_post_body is check_body for the IAM custom methods.
check_post_body() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(post_body "$url")"
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

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

case_main() {
  local payload_v1="c2VjcmV0LXBheWxvYWQtdjE=" # base64("secret-payload-v1")
  local payload_v2="c2VjcmV0LXBheWxvYWQtdjI=" # base64("secret-payload-v2")
  local reader="serviceAccount:jc-tf-sm-reader@$PROJECT.iam.gserviceaccount.com"
  local auditor="serviceAccount:jc-tf-sm-auditor@$PROJECT.iam.gserviceaccount.com"

  write_module '{ env = "compat-test" }' '"86400s"' '"secret-payload-v1"' \
    "[\"$reader\"]"
  init_phase
  plan_phase "plan (module secrets + versions + IAM)"
  apply_phase "apply (module secrets + versions + IAM)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "automatic secret echoes its reference, labels and replication arm" "$(secret_url "$SECRET_AUTO")" \
    "\"name\":\"projects/$PROJECT/secrets/$SECRET_AUTO\"" \
    '"env":"compat-test"' \
    '"replication"' '"automatic"'
  check_body "automatic secret echoes the global CMEK key" "$(secret_url "$SECRET_AUTO")" \
    "jc-tf-sm-global-key"
  check_body "automatic secret echoes the rotation schedule and its topic" "$(secret_url "$SECRET_AUTO")" \
    '"nextRotationTime":"2035-01-01T00:00:00Z"' \
    '"rotationPeriod":"86400s"' \
    "/topics/jc-tf-sm-rotation"
  check_body "user-managed secret echoes both replica locations" "$(secret_url "$SECRET_USER")" \
    '"userManaged"' '"location":"us-central1"' '"location":"us-east1"'
  check_body "user-managed secret echoes the regional CMEK key" "$(secret_url "$SECRET_USER")" \
    '"customerManagedEncryption"' "jc-tf-sm-regional-key"
  check_body "automatic secret version 1 carries the payload" \
    "$(secret_url "$SECRET_AUTO")/versions/latest:access" "\"data\":\"$payload_v1\""
  check_body "user-managed secret version 1 carries the payload" \
    "$(secret_url "$SECRET_USER")/versions/latest:access" \
    '"data":"dXNlci1tYW5hZ2VkLXBheWxvYWQ="' # base64("user-managed-payload")
  check_post_body "automatic secret IAM binding carries the reader" \
    "$(secret_url "$SECRET_AUTO"):getIamPolicy" \
    '"bindings"' '"role":"roles/secretmanager.secretAccessor"' "$reader"
  check_post_body "user-managed secret IAM binding carries the reader" \
    "$(secret_url "$SECRET_USER"):getIamPolicy" \
    '"role":"roles/secretmanager.secretAccessor"' "$reader"

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: labels, rotation period, the automatic secret's payload
  # (a version replacement) and a second authoritative IAM member.
  write_module '{ env = "compat-test", tier = "gold" }' '"172800s"' '"secret-payload-v2"' \
    "[\"$reader\", \"$auditor\"]"
  apply_phase "apply (labels + rotation period + payload + IAM member)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "automatic secret echoes the mutated labels" "$(secret_url "$SECRET_AUTO")" \
    '"env":"compat-test"' '"tier":"gold"'
  check_body "automatic secret echoes the mutated rotation period" "$(secret_url "$SECRET_AUTO")" \
    '"rotationPeriod":"172800s"'
  check_body "automatic secret latest version carries the new payload" \
    "$(secret_url "$SECRET_AUTO")/versions/latest:access" "\"data\":\"$payload_v2\""
  check_post_body "automatic secret IAM binding carries both members" \
    "$(secret_url "$SECRET_AUTO"):getIamPolicy" "$reader" "$auditor"

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(secret_url "$SECRET_AUTO")")"
  if [[ "$status" == "404" ]]; then
    pass "automatic secret GET after destroy is 404"
  else
    fail "automatic secret GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$(secret_url "$SECRET_USER")/versions/latest:access")"
  if [[ "$status" == "404" ]]; then
    pass "user-managed secret version access after destroy is 404"
  else
    fail "user-managed secret version access after destroy = HTTP $status, want 404"
  fi
}
