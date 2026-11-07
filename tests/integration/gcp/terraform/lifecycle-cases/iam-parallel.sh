# shellcheck shell=bash
#
# Case: iam-parallel — the read-modify-write IAM path under concurrency.
#
#   plan -> apply -parallelism=10 (10 accounts, 10 project bindings)
#   -> every member is in the policy -> re-plan clean -> destroy -parallelism=10
#   -> the accounts are 404 and no lc-par- member remains in the policy
#
# Each `google_project_iam_member` is an independent add to the same project
# policy: the provider reads the policy, adds its member and writes it back with
# the etag it read. Ten of them run at once, so a lost update (last-writer-wins
# without OCC) drops a member and the policy assertion fails. The emulator's
# etag/merge path is the subject, not the service-account read.
#
# The role is the same for all ten members: it is the policy object that is
# contended, not the role map.

IDS="lc-par-00 lc-par-01 lc-par-02 lc-par-03 lc-par-04 lc-par-05 lc-par-06 lc-par-07 lc-par-08 lc-par-09"

PROVIDER_HCL='provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false
  add_terraform_attribution_label = false

  iam_custom_endpoint              = "${var.endpoint}/"
  iam_beta_custom_endpoint         = "${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "${var.endpoint}/v1/"
}'

write_main() {
  local ids_hcl
  ids_hcl="$(printf '"%s", ' $IDS)"
  ids_hcl="[${ids_hcl%, }]"
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

locals {
  account_ids = $ids_hcl
}

resource "google_service_account" "sa" {
  for_each = toset(local.account_ids)

  account_id   = each.key
  display_name = "lifecycle parallel \${each.key}"
}

resource "google_project_iam_member" "viewer" {
  for_each = toset(local.account_ids)

  project = var.project
  role    = "roles/viewer"
  member  = "serviceAccount:\${google_service_account.sa[each.key].email}"
}
EOF
}

case_main() {
  write_main
  init_phase
  plan_phase "plan (10 service accounts + 10 bindings)"
  apply_phase "apply (10 accounts + 10 bindings at parallelism 10)" -parallelism=10
  assert_no_api_errors "$LAST_APPLY_LOG" "apply"

  echo "== policy holds every member"
  check_post "project policy carries all 10 members" \
    "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy" \
    'roles/viewer' \
    "lc-par-00@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-01@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-02@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-03@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-04@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-05@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-06@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-07@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-08@$PROJECT.iam.gserviceaccount.com" \
    "lc-par-09@$PROJECT.iam.gserviceaccount.com"
  check "the accounts were created" \
    "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/lc-par-00@$PROJECT.iam.gserviceaccount.com" \
    '"email":"lc-par-00@'"$PROJECT"'.iam.gserviceaccount.com"'

  plan_no_changes "re-plan after the parallel apply reports no changes"

  destroy_phase -parallelism=10
  check_status "destroy leaves the first account 404" \
    "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/lc-par-00@$PROJECT.iam.gserviceaccount.com" 404
  check_status "destroy leaves the last account 404" \
    "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/lc-par-09@$PROJECT.iam.gserviceaccount.com" 404
  check_post_absent "the parallel members are gone from the policy" \
    "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy" 'lc-par-'
}
