# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-cloud-workflows (TF3-3)
#
# The pinned consumer is the CFT Workflows blueprint (v0.2.0, commit
# 4c431381b5d8). Its ROOT module always creates a Cloud Scheduler or Eventarc
# trigger (`workflow_trigger` has no default and a validation that one arm is
# set) and can create a service account through the registry
# `terraform-google-modules/service-accounts` module, so the case drives the
# `modules/simple_workflow` submodule instead — the blueprint's own standalone
# workflow resource, and the only part of the repo that touches the served
# surface. The generated root module supplies the consumer context (its own
# service account and the `roles/workflows.invoker` project grant) that the
# root module would otherwise create through that registry module. The trigger
# paths belong to TF3-4 (Scheduler) and TF4-2 (event-driven chain).
#
# It creates:
#
#   google_workflows_workflow   -> workflows.create / .get / .patch / .delete
#
# and the gate proves the lifecycle converges:
#
#   apply (workflow with description, labels, source, service account)
#     -> REST asserts (name, state, description, labels, source, revision 1,
#        service account) -> plan No changes
#   mutate (source, description, labels) -> apply
#     -> REST asserts (new source/description/labels, revision 2) -> No changes
#   destroy: the workflow through the API, then the service account and IAM
#     grant through Terraform; all are 404
#
# The `modules/simple_workflow` submodule does not expose
# `execution_history_level`, so the generated root module also declares the
# provider's own `google_workflows_workflow` resource with the attribute set
# (BASIC, then DETAILED on the in-place mutate). That is the permadiff
# assertion for the field: apply stores it, the read-back echoes it, the
# post-apply re-plan is clean, and the second apply PATCHes it
# (`updateMask=executionHistoryLevel`). The module workflow, which does not set
# it, must not gain the field on read (an unset value stays unset).
#
# Provider note 1: the module declares `google >= 3.53, < 7.0`, so this case
# pins `= 6.49.3` (the latest 6.x) while every other case pins 7.46.2. The FC6
# capture is therefore scoped to google_workflows_workflow and tolerates an
# attribute the 6.x schema does not declare: the shared mapping carries the
# union of the two provider versions, and re-checking unrelated 7.x-only rows
# under 6.x would fail for reasons this case cannot measure.
#
# Provider note 2: the blueprint's `simple_workflow` submodule does not expose
# `deletion_protection`, whose provider default is `true`, so a Terraform
# destroy is refused by the provider client-side. The case removes the workflow
# from state and issues the DELETE the provider is guarding (the same request
# `terraform destroy` would send), then lets the driver destroy the service
# account and IAM grant. This is an upstream module limitation, recorded as a
# ledger row; it is not an emulator gap.
#
# Boundaries: `crypto_key_name` (CMEK; README-GCP records workflow CMEK as not
# implemented), `call_log_level` and `user_env_vars` are not exposed by the
# submodule and stay unconfigured; `execution_history_level` is not exposed
# either, so a root-level `google_workflows_workflow` covers it. Workflow IAM is
# not a provider resource type, and the emulator serves no workflows
# getIamPolicy/setIamPolicy (README-GCP records workflow IAM as not
# implemented); the `roles/workflows.invoker` grant is project-level IAM, which
# is served.
#
# Provider note 3: `workflows_custom_endpoint`, `resource_manager_custom_endpoint`
# are versioned base URLs (one trailing slash); `iam_custom_endpoint` is the
# bare origin (the IAM client appends v1/). Every apply log must be free of
# 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-workflows"
MODULE_COMMIT="4c431381b5d8"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-cloud-workflows"
MODULE_SUBDIR="modules/simple_workflow"

# The case's module pins google < 7.0, so its provider predates some mapping
# rows; scope the capture and downgrade absent rows to notes (see note 1).
CAPTURE_RESOURCES="google_workflows_workflow"
CAPTURE_ALLOW_MISSING=1

WORKFLOW="jc-tf-workflow"

# write_module generates the root module for one gate phase.
#   $1 description (HCL string literal)
#   $2 labels (HCL map literal)
#   $3 workflow source (HCL string literal)
#   $4 execution history level (HCL string literal)
write_module() {
  local description="$1" labels="$2" source="$3" history_level="$4"

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 6.49.3"
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

  workflows_custom_endpoint        = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint = "\${var.endpoint}/v1/"
  iam_custom_endpoint              = "\${var.endpoint}/"
}

# The consumer context the blueprint's root module creates through the
# registry service-accounts module: the workflow's identity and its
# roles/workflows.invoker grant.
resource "google_service_account" "workflow" {
  account_id   = "jc-tf-workflow"
  display_name = "jaiscloud terraform workflow identity"
  project      = var.project
}

resource "google_project_iam_member" "workflow_invoker" {
  project = var.project
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:\${google_service_account.workflow.email}"
}

module "workflow" {
  source = "$MODULE_DIR"

  project_id            = var.project
  workflow_name         = "$WORKFLOW"
  location              = "us-central1"
  workflow_description  = $description
  workflow_labels       = $labels
  workflow_source       = $source
  service_account_email = google_service_account.workflow.email
}

# The submodule does not expose execution_history_level, so this root-level
# resource pins the provider attribute: it must be stored, echoed on read and
# applied by an in-place update, with no permadiff on the post-apply re-plan.
# deletion_protection is cleared explicitly so terraform destroy deletes it.
resource "google_workflows_workflow" "history" {
  name                    = "jc-tf-workflow-history"
  region                  = "us-central1"
  project                 = var.project
  description             = "jaiscloud execution history level case"
  source_contents         = $source
  execution_history_level = $history_level
  deletion_protection     = false
}
EOF
}

workflow_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/us-central1/workflows/$WORKFLOW"
}

# history_url is the root-level google_workflows_workflow that pins
# execution_history_level.
history_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/us-central1/workflows/jc-tf-workflow-history"
}

workflows_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/us-central1/workflows"
}

service_account_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/jc-tf-workflow@$PROJECT.iam.gserviceaccount.com"
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

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_body_absent asserts the API body carries none of the given substrings:
# an unset optional field must stay unset instead of being invented by the
# emulator (executionHistoryLevel's documented default is UNSPECIFIED).
check_body_absent() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(body_of "$url")"
  if [[ -z "$body" ]]; then
    fail "$name (no response from $url)"
    return 0
  fi
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (unexpected: $want)"
      return 0
    fi
  done
  pass "$name"
}

post_body() {
  curl -sf -X POST -H "Content-Type: application/json" -d '{}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_post_body is check_body for the IAM custom methods (getIamPolicy is a
# POST on the CRM v1 surface).
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

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

case_main() {
  local source_v1='main:\n  steps:\n    - hello:\n        return: \"hello\"\n'
  local source_v2='main:\n  steps:\n    - hello:\n        return: \"goodbye\"\n'

  write_module '"jaiscloud terraform workflow case"' '{ env = "compat-test" }' "\"$source_v1\"" '"EXECUTION_HISTORY_BASIC"'
  init_phase
  plan_phase "plan (module workflow + service account + IAM)"
  apply_phase "apply (module workflow + service account + IAM)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "workflow echoes its reference, description, labels and state" "$(workflow_url)" \
    "\"name\":\"projects/$PROJECT/locations/us-central1/workflows/$WORKFLOW\"" \
    '"state":"ACTIVE"' \
    '"description":"jaiscloud terraform workflow case"' \
    '"env":"compat-test"'
  check_body "workflow echoes its source and revision 1" "$(workflow_url)" \
    '"sourceContents"' 'return: \"hello\"' \
    '"revisionId":"000001-'
  check_body "workflow echoes the service account" "$(workflow_url)" \
    "jc-tf-workflow@$PROJECT.iam.gserviceaccount.com"
  check_body "the bare workflow echoes executionHistoryLevel" "$(history_url)" \
    '"executionHistoryLevel":"EXECUTION_HISTORY_BASIC"'
  check_body "the workflow list echoes executionHistoryLevel" "$(workflows_url)" \
    '"executionHistoryLevel":"EXECUTION_HISTORY_BASIC"'
  check_body_absent "the module workflow omits the unset executionHistoryLevel" "$(workflow_url)" \
    '"executionHistoryLevel"'
  check_body "the workflow identity exists" "$(service_account_url)" \
    '"email":"jc-tf-workflow@'"$PROJECT"'.iam.gserviceaccount.com"'
  check_post_body "the project IAM grant is present" \
    "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy" \
    '"role":"roles/workflows.invoker"' \
    "serviceAccount:jc-tf-workflow@$PROJECT.iam.gserviceaccount.com"

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # An in-place mutate: a new source (bumps the revision), description and a
  # second label. The bare workflow's executionHistoryLevel moves BASIC ->
  # DETAILED, which the provider sends as a PATCH with
  # updateMask=executionHistoryLevel.
  write_module '"jaiscloud terraform workflow case v2"' '{ env = "compat-test", tier = "gold" }' "\"$source_v2\"" '"EXECUTION_HISTORY_DETAILED"'
  apply_phase "apply (source + description + labels + executionHistoryLevel)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "workflow echoes the mutated description and labels" "$(workflow_url)" \
    '"description":"jaiscloud terraform workflow case v2"' \
    '"env":"compat-test"' '"tier":"gold"'
  check_body "workflow echoes the new source and revision 2" "$(workflow_url)" \
    'return: \"goodbye\"' \
    '"revisionId":"000002-'
  check_body "the bare workflow echoes the updated executionHistoryLevel" "$(history_url)" \
    '"executionHistoryLevel":"EXECUTION_HISTORY_DETAILED"'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  # See provider note 2: the module does not expose deletion_protection, so the
  # workflow is removed from state and deleted through the API.
  echo "== destroy (workflow through the API; the module exposes no deletion_protection)"
  tf state rm module.workflow.google_workflows_workflow.simple_workflow
  local status
  status="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$(workflow_url)" 2>/dev/null || true)"
  if [[ "$status" == "200" || "$status" == "204" ]]; then
    pass "workflow DELETE accepted"
  else
    fail "workflow DELETE = HTTP $status, want 200/204"
  fi

  destroy_phase

  status="$(status_of "$(workflow_url)")"
  if [[ "$status" == "404" ]]; then
    pass "workflow GET after destroy is 404"
  else
    fail "workflow GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$(history_url)")"
  if [[ "$status" == "404" ]]; then
    pass "the bare workflow GET after destroy is 404"
  else
    fail "the bare workflow GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$(service_account_url)")"
  if [[ "$status" == "404" ]]; then
    pass "service account GET after destroy is 404"
  else
    fail "service account GET after destroy = HTTP $status, want 404"
  fi
}
