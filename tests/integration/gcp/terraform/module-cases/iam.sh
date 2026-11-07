# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-iam (TF3-5)
#
# The pinned consumer is the CFT IAM module (commit 9b9eacfa2b4f). The repo is a
# collection of scope submodules, so the case drives three of them on one
# project:
#
#   modules/projects_iam            -> google_project_iam_member
#   modules/storage_buckets_iam     -> google_storage_bucket_iam_member
#   modules/pubsub_topics_iam       -> google_pubsub_topic_iam_member
#
# all in additive mode (one `*_iam_member` per role/member pair). Every member
# is an independent get-etag / mutate / set-etag round trip, so this is the
# emulator's OCC stress path: the case runs every phase with `-parallelism=10`
# and asserts that a role-set change adds the new members without dropping the
# surviving ones ("no lost updates").
#
# The repo's submodules reference `../helper`, and a local-path module source
# makes Terraform treat the submodule directory as the package root, so the
# relative helper path "escapes the module package". The case therefore pins
# the source as `git::file://<checkout>//modules/<name>?ref=<HEAD>`, which is
# still offline (the repo is already checked out) but presents the whole repo
# as the module package, exactly as the registry does.
#
# Scope: project, bucket and topic IAM only. Folder/organization bindings and
# the module's audit-config submodule are out of scope (the emulator is
# project-scoped, wire-contract §8); the module's `member_iam` and
# `service_accounts_iam` submodules are not needed because the root module
# creates the service accounts it binds.
#
# Provider note: `iam_custom_endpoint` is the bare origin (the IAM client
# appends v1/), `iam_beta_custom_endpoint` is the v1/ base used by
# `google_service_account`, `resource_manager_custom_endpoint` is the Resource
# Manager v1 client project IAM uses, and the storage/pubsub endpoints are the
# versioned bases. The case pins google = 7.46.2. Every apply log must be free
# of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-iam"
MODULE_COMMIT="9b9eacfa2b4f"
# The repo root is a collection of submodules with no main.tf; point the driver
# at one of them so its main.tf check passes, and derive the repo root in
# write_module for the three git::file sources.
MODULE_SUBDIR="modules/projects_iam"

CAPTURE_RESOURCES="google_service_account,google_project_iam_member,google_storage_bucket_iam_member,google_pubsub_topic_iam_member"

BUCKET="jc-tf-iam-bucket"
TOPIC="jc-tf-iam-topic"
ACCOUNTS=("jc-tf-iam-sa-one" "jc-tf-iam-sa-two" "jc-tf-iam-sa-three")

# The OCC path is the point of the case: run every phase with the provider's
# concurrency turned up so members overlap their get/set policy round trips.
export TF_CLI_ARGS_apply="-parallelism=10"
export TF_CLI_ARGS_plan="-parallelism=10"
export TF_CLI_ARGS_destroy="-parallelism=10"

# write_module generates the root module for one gate phase.
#   $1 project bindings (HCL map literal)
#   $2 bucket bindings (HCL map literal)
#   $3 topic bindings (HCL map literal)
write_module() {
  local project_bindings="$1" bucket_bindings="$2" topic_bindings="$3"
  # The three submodules live in one git repo and reference ../helper; see the
  # header for why the source is a git::file URL over the existing checkout.
  local ref repo
  repo="$(cd "$MODULE_DIR/../.." && pwd)"
  ref="$(git -C "$repo" rev-parse HEAD)"

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
  project               = var.project
  region                = "us-central1"
  user_project_override = false

  iam_custom_endpoint              = "\${var.endpoint}/"
  iam_beta_custom_endpoint         = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "\${var.endpoint}/v1/"
  storage_custom_endpoint          = "\${var.endpoint}/storage/v1/"
  pubsub_custom_endpoint           = "\${var.endpoint}/v1/"
}

resource "google_service_account" "accounts" {
  for_each     = toset(["one", "two", "three"])
  account_id   = "jc-tf-iam-sa-\${each.value}"
  display_name = "jaiscloud terraform iam \${each.value}"
  project      = var.project
}

resource "google_storage_bucket" "bucket" {
  name                        = "$BUCKET"
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_pubsub_topic" "topic" {
  name = "$TOPIC"
}

module "project_iam" {
  source = "git::file://$repo//modules/projects_iam?ref=$ref"

  projects = [var.project]
  mode     = "additive"
  bindings = $project_bindings
}

module "bucket_iam" {
  source = "git::file://$repo//modules/storage_buckets_iam?ref=$ref"

  storage_buckets = [google_storage_bucket.bucket.name]
  mode            = "additive"
  bindings        = $bucket_bindings
}

module "topic_iam" {
  source = "git::file://$repo//modules/pubsub_topics_iam?ref=$ref"

  project       = var.project
  pubsub_topics = [google_pubsub_topic.topic.name]
  mode          = "additive"
  bindings      = $topic_bindings
}
EOF
}

member() {
  echo "\"serviceAccount:$1@$PROJECT.iam.gserviceaccount.com\""
}

account_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$1@$PROJECT.iam.gserviceaccount.com"
}

project_policy_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy"
}

topic_policy_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC:getIamPolicy"
}

bucket_policy_url() {
  echo "$ENDPOINT/storage/v1/b/$BUCKET/iam"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

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

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# iam_body returns the policy for a project/topic (POST :getIamPolicy) or a
# bucket (GET .../iam).
iam_body() {
  local kind="$1"
  case "$kind" in
    project) curl -sf -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -H 'Content-Type: application/json' -d '{}' "$(project_policy_url)" 2>/dev/null || true ;;
    topic) curl -sf -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -H 'Content-Type: application/json' -d '{}' "$(topic_policy_url)" 2>/dev/null || true ;;
    bucket) body_of "$(bucket_policy_url)" ;;
  esac
}

# binding_for prints the binding object for one role, so a member is never
# counted under a different role. The emulator renders each binding as
# {"members":[...],"role":"..."}.
binding_for() {
  local role="$1" body="$2" seg
  printf '%s\n' "$body" | sed 's/},{/}\n{/g' | while IFS= read -r seg; do
    if [[ "$seg" == *"\"role\":\"$role\""* ]]; then
      printf '%s\n' "$seg"
    fi
  done
}

# check_policy asserts the named accounts are members of the role at one scope.
#   $1 label, $2 scope (project|bucket|topic), $3 role, $4.. account suffixes
check_policy() {
  local label="$1" scope="$2" role="$3"
  shift 3
  local body binding account ok=1
  body="$(iam_body "$scope")"
  if [[ -z "$body" ]]; then
    fail "$label (no $scope policy)"
    return 0
  fi
  binding="$(binding_for "$role" "$body")"
  if [[ -z "$binding" ]]; then
    fail "$label ($scope role $role has no binding)"
    return 0
  fi
  for account in "$@"; do
    if [[ "$binding" != *"serviceAccount:jc-tf-iam-sa-$account@$PROJECT.iam.gserviceaccount.com"* ]]; then
      ok=0
    fi
  done
  if [[ "$ok" == "1" ]]; then
    pass "$label"
  else
    fail "$label (missing one of: $*) under $scope $role)"
  fi
}

# check_policy_absent asserts a role has no binding at the scope.
check_policy_absent() {
  local label="$1" scope="$2" role="$3"
  local body
  body="$(iam_body "$scope")"
  if [[ -n "$(binding_for "$role" "$body")" ]]; then
    fail "$label ($scope role $role still present)"
  else
    pass "$label"
  fi
}

case_main() {
  local account

  write_module \
    '{"roles/viewer" = ['"$(member jc-tf-iam-sa-one)"'], "roles/logging.viewer" = ['"$(member jc-tf-iam-sa-two)"']}' \
    '{"roles/storage.objectViewer" = ['"$(member jc-tf-iam-sa-one), $(member jc-tf-iam-sa-two)"'], "roles/storage.admin" = ['"$(member jc-tf-iam-sa-three)"']}' \
    '{"roles/pubsub.publisher" = ['"$(member jc-tf-iam-sa-one), $(member jc-tf-iam-sa-three)"'], "roles/pubsub.viewer" = ['"$(member jc-tf-iam-sa-two)"']}'

  init_phase
  plan_phase "plan (project + bucket + topic IAM modules)"
  apply_phase "apply (project + bucket + topic IAM modules)"
  assert_no_api_errors

  echo "== spot checks (create)"
  for account in one two three; do
    check_body "service account jc-tf-iam-sa-$account reads back" "$(account_url "jc-tf-iam-sa-$account")" \
      "\"email\":\"jc-tf-iam-sa-$account@$PROJECT.iam.gserviceaccount.com\""
  done
  check_policy "project roles/viewer holds its member" project "roles/viewer" one
  check_policy "project roles/logging.viewer holds its member" project "roles/logging.viewer" two
  check_policy "bucket roles/storage.objectViewer holds both members" bucket "roles/storage.objectViewer" one two
  check_policy "bucket roles/storage.admin holds its member" bucket "roles/storage.admin" three
  check_policy "topic roles/pubsub.publisher holds both members" topic "roles/pubsub.publisher" one three
  check_policy "topic roles/pubsub.viewer holds its member" topic "roles/pubsub.viewer" two

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # Role-set change under -parallelism=10: the surviving roles must keep their
  # members while the new members are all added (the OCC no-lost-update check).
  write_module \
    '{"roles/viewer" = ['"$(member jc-tf-iam-sa-one), $(member jc-tf-iam-sa-two), $(member jc-tf-iam-sa-three)"'], "roles/iam.securityReviewer" = ['"$(member jc-tf-iam-sa-three)"']}' \
    '{"roles/storage.objectViewer" = ['"$(member jc-tf-iam-sa-one), $(member jc-tf-iam-sa-two), $(member jc-tf-iam-sa-three)"']}' \
    '{"roles/pubsub.publisher" = ['"$(member jc-tf-iam-sa-one), $(member jc-tf-iam-sa-two), $(member jc-tf-iam-sa-three)"']}'

  apply_phase "apply (role-set change under parallelism 10)"
  assert_no_api_errors

  echo "== spot checks (role set change, no lost updates)"
  check_policy "project roles/viewer kept every member and gained the third" project "roles/viewer" one two three
  check_policy "project roles/iam.securityReviewer holds its member" project "roles/iam.securityReviewer" three
  check_policy "bucket roles/storage.objectViewer kept both members and gained the third" bucket "roles/storage.objectViewer" one two three
  check_policy "topic roles/pubsub.publisher kept both members and gained the third" topic "roles/pubsub.publisher" one two three
  check_policy_absent "the replaced project role is gone" project "roles/logging.viewer"
  check_policy_absent "the replaced bucket role is gone" bucket "roles/storage.admin"
  check_policy_absent "the replaced topic role is gone" topic "roles/pubsub.viewer"

  echo "== $TF: second plan after the role change (permadiff check)"
  plan_no_changes "re-plan after the role change reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(bucket_policy_url)")"
  if [[ "$status" == "404" ]]; then
    pass "bucket GET after destroy is 404"
  else
    fail "bucket GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC")"
  if [[ "$status" == "404" ]]; then
    pass "topic GET after destroy is 404"
  else
    fail "topic GET after destroy = HTTP $status, want 404"
  fi
  for account in one two three; do
    status="$(status_of "$(account_url "jc-tf-iam-sa-$account")")"
    if [[ "$status" == "404" ]]; then
      pass "service account jc-tf-iam-sa-$account GET after destroy is 404"
    else
      fail "service account jc-tf-iam-sa-$account GET after destroy = HTTP $status, want 404"
    fi
  done
}
