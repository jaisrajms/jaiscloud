# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-service-accounts (TF3-5)
#
# The pinned consumer is the CFT service-accounts module (commit 570caafff32d).
# It creates N service accounts and a project binding for every
# (account, role) pair:
#
#   google_service_account        -> serviceAccounts.create / .get / .patch
#   google_project_iam_member     -> projects.getIamPolicy / setIamPolicy
#
# The project-IAM path is the emulator's highest-value read-modify-write
# stress: every member is an independent get-etag / mutate / set-etag round
# trip, so concurrent members must not lose each other's bindings.
#
# The case applies three accounts with two project roles, asserts the accounts
# and the policy, re-plans, then *changes the role set* (the storage role is
# replaced by a Pub/Sub role) and re-plans. The whole case runs with
# `-parallelism=10`, and the final assertion checks that every member of the
# new role set survived (no lost updates under the etag OCC path).
#
# Boundaries, recorded rather than worked around:
#
#  * `project_roles` entries are `"project=>role"` (the module's documented
#    format). A bare `"roles/viewer"` makes the module parse both the project
#    and the role from the same string, so the case always passes the explicit
#    `var.project=>role` form.
#  * `display_name` is set explicitly. The provider's `display_name` is
#    Optional (not Computed); the emulator used to default an omitted
#    `serviceAccount.displayName` to the account id, which made an omitted
#    value permadiff — fixed in TF2-14
#    (`fix/gcp-iam-serviceaccount-display-name`), gated by the fixture's
#    `google_service_account.nodesc` + second plan. The module's own default
#    is a non-empty display name, so this case keeps the module default.
#  * `grant_billing_role` and `grant_xpn_roles` are false: billing and
#    organization IAM are outside the emulated project-scoped surface.
#  * `generate_keys` is false: service-account keys are a separate surface
#    (already covered by the IAM unit/transport tests).
#
# Provider note: `iam_custom_endpoint` is the bare origin (the IAM client
# appends v1/); `google_service_account` resolves through the IAM *beta* client
# as well, so `iam_beta_custom_endpoint` carries the v1/ base. Project IAM uses
# `resource_manager_custom_endpoint` (the Resource Manager v1 client). The case
# pins google = 7.46.2. Every apply log must be free of 400/501 and
# `unsupported update_mask`.

MODULE_REPO="terraform-google-service-accounts"
MODULE_COMMIT="570caafff32d"

CAPTURE_RESOURCES="google_service_account,google_project_iam_member"

ACCOUNTS=("one" "two" "three")
DISPLAY_NAME="jaiscloud terraform service account"

# The IAM/OCC path is the point of the case: run every phase with the provider's
# concurrency turned up so members overlap their get/set policy round trips.
export TF_CLI_ARGS_apply="-parallelism=10"
export TF_CLI_ARGS_plan="-parallelism=10"
export TF_CLI_ARGS_destroy="-parallelism=10"

hcl_strings() {
  local out="" item
  for item in "$@"; do
    out+="\"$item\", "
  done
  printf '[%s]' "${out%, }"
}

# write_module generates the root module for one gate phase.
#   $1 roles list (HCL list literal of "project=>role" strings)
write_module() {
  local roles="$1" names
  names="$(hcl_strings "${ACCOUNTS[@]}")"

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
}

module "service_accounts" {
  source = "$MODULE_DIR"

  project_id = var.project
  prefix     = "jc-tf"

  names         = $names
  project_roles = $roles
  display_name  = "$DISPLAY_NAME"
  description   = "a service account created by the module case"

  grant_billing_role = false
  grant_xpn_roles    = false
  generate_keys      = false
}
EOF
}

account_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/jc-tf-$1@$PROJECT.iam.gserviceaccount.com"
}

policy_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

policy_body() {
  curl -sf -X POST \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{}' "$(policy_url)" 2>/dev/null || true
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

# binding_for prints the policy binding object for one role. The emulator
# renders each binding as {"members":[...],"role":"..."}; splitting on the
# object boundary keeps a member from being counted under the wrong role.
binding_for() {
  local role="$1" body="$2" seg
  printf '%s\n' "$body" | sed 's/},{/}\n{/g' | while IFS= read -r seg; do
    if [[ "$seg" == *"\"role\":\"$role\""* ]]; then
      printf '%s\n' "$seg"
    fi
  done
}

# check_policy asserts that every account is a member of the given role.
check_policy() {
  local label="$1" role="$2"
  shift 2
  local body binding account ok=1
  body="$(policy_body)"
  if [[ -z "$body" ]]; then
    fail "$label (no project policy)"
    return 0
  fi
  binding="$(binding_for "$role" "$body")"
  if [[ -z "$binding" ]]; then
    fail "$label (role $role has no binding)"
    return 0
  fi
  for account in "$@"; do
    if [[ "$binding" != *"serviceAccount:jc-tf-$account@$PROJECT.iam.gserviceaccount.com"* ]]; then
      ok=0
    fi
  done
  if [[ "$ok" == "1" ]]; then
    pass "$label"
  else
    fail "$label (missing one of: $*) under $role)"
  fi
}

# check_policy_absent asserts a role is gone from the project policy (after the
# role-set change).
check_policy_absent() {
  local label="$1" role="$2"
  local body
  body="$(policy_body)"
  if [[ -n "$(binding_for "$role" "$body")" ]]; then
    fail "$label (role $role still present)"
  else
    pass "$label"
  fi
}

case_main() {
  write_module '["${var.project}=>roles/viewer", "${var.project}=>roles/storage.objectViewer"]'
  init_phase
  plan_phase "plan (module service accounts + project roles)"
  apply_phase "apply (module service accounts + project roles)"
  assert_no_api_errors

  echo "== spot checks (create)"
  local account
  for account in "${ACCOUNTS[@]}"; do
    check_body "service account jc-tf-$account reads back" "$(account_url "$account")" \
      "\"email\":\"jc-tf-$account@$PROJECT.iam.gserviceaccount.com\"" \
      "\"displayName\":\"$DISPLAY_NAME\"" \
      '"disabled":false'
  done
  check_policy "every account holds roles/viewer" "roles/viewer" "${ACCOUNTS[@]}"
  check_policy "every account holds roles/storage.objectViewer" "roles/storage.objectViewer" "${ACCOUNTS[@]}"

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # Change the role set: the storage role is replaced by Pub/Sub. The three
  # members must be added under the new role while the surviving viewer members
  # stay put (the concurrency path).
  write_module '["${var.project}=>roles/viewer", "${var.project}=>roles/pubsub.viewer"]'
  apply_phase "apply (changed project role set)"
  assert_no_api_errors

  echo "== spot checks (role set change)"
  check_policy "every account holds the new roles/pubsub.viewer" "roles/pubsub.viewer" "${ACCOUNTS[@]}"
  check_policy "every account still holds roles/viewer" "roles/viewer" "${ACCOUNTS[@]}"
  check_policy_absent "the replaced role is gone" "roles/storage.objectViewer"

  echo "== $TF: second plan after the role change (permadiff check)"
  plan_no_changes "re-plan after the role change reports no changes"

  destroy_phase

  local status
  for account in "${ACCOUNTS[@]}"; do
    status="$(status_of "$(account_url "$account")")"
    if [[ "$status" == "404" ]]; then
      pass "service account jc-tf-$account GET after destroy is 404"
    else
      fail "service account jc-tf-$account GET after destroy = HTTP $status, want 404"
    fi
  done
}
