# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-cloud-run (modules/v2) (TF3-1)
#
# The pinned consumer is GoogleCloudPlatform/terraform-google-cloud-run
# (commit 4ce640a), and specifically its `modules/v2` submodule: the repo's
# ROOT module still creates the v1 `google_cloud_run_service`, which the
# emulator does not serve (TF1-6, `no fix`). The v2 submodule creates the
# served surface:
#
#   google_cloud_run_v2_service              (service + revision control plane)
#   google_service_account                   (create_service_account = true)
#   google_cloud_run_v2_service_iam_member   (var.members -> roles/run.invoker)
#
# The gate proves the module's lifecycle converges on that surface:
#
#   apply (service + SA + invoker IAM member) -> REST asserts
#   plan -detailed-exitcode -> No changes
#   mutate a served field (ingress, labels, container env) -> apply
#     -> REST asserts (new values, untouched readiness surface) -> No changes
#   destroy -> service GET is 404
#
# Pruned inputs, so nothing here reaches an unserved resource (documented, not
# silent):
#
#   * `create_service_account = true` (the module default) — the alternative
#     path reads `data.google_compute_default_service_account`, which needs the
#     unserved `compute.projects.get` (TF2-1). The module then creates its own
#     SA, which the emulator serves.
#   * `cloud_run_deletion_protection = false` — the module default (true) is a
#     provider-side destroy guard, so it would block `destroy`; real GCP would
#     refuse too. It is provider-local (never sent as a resource field).
#   * `iap_members` / `verified_domain_name` empty — IAP and v1 domain mappings
#     are not served. `traffic` empty — the emulator stores traffic targets and
#     reflects them in `trafficStatuses`, but does not enforce the split
#     (wire-contract §5); traffic is out of scope for this case.
#   * Jobs, WorkerPools and v1 services are not modelled at all: a request to
#     `/v2/.../jobs` or `/v2/.../workerPools` is not claimed by the emulator
#     (404 `UnknownService`), and v1 domain mappings answer 404 "unsupported
#     operation". They are recorded boundaries, not exercised here.
#
# Provider note: `cloud_run_v2_custom_endpoint` is the versioned API base URL
# (the provider appends the resource path), so it carries exactly one trailing
# slash: "${endpoint}/v2/". The module declares both `google` and `google-beta`
# (it creates the service through `google-beta`), so both providers are pinned
# and both point at the emulator. The service account is created through the
# IAM API, which needs `iam_custom_endpoint` AND `iam_beta_custom_endpoint`:
# with only the former the provider's SA call escapes to real googleapis.
#
# Every apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-run"
MODULE_COMMIT="4ce640a2a519"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-cloud-run"
MODULE_SUBDIR="modules/v2"

SERVICE="jc-tf-run"

# write_module generates the root module for one phase. $1 is the HCL literal
# for `ingress`; $2 is the container env value; $3 is the extra service label
# map (as HCL).
write_module() {
  local ingress="$1" env_value="$2" labels="$3"

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

  # Versioned API base URLs: exactly one trailing slash each.
  cloud_run_v2_custom_endpoint = "$CLOUD_RUN_ENDPOINT"
  iam_custom_endpoint          = "\${var.endpoint}/"
  iam_beta_custom_endpoint     = "\${var.endpoint}/v1/"
}

provider "google-beta" {
  alias                 = "beta"
  project               = var.project
  region                = "us-central1"
  user_project_override = false

  cloud_run_v2_custom_endpoint = "$CLOUD_RUN_ENDPOINT"
  iam_custom_endpoint          = "\${var.endpoint}/"
  iam_beta_custom_endpoint     = "\${var.endpoint}/v1/"
}

module "cloudrun" {
  source = "$MODULE_DIR"

  providers = {
    google-beta = google-beta.beta
  }

  project_id  = var.project
  location    = "us-central1"
  service_name = "$SERVICE"
  description  = "jaiscloud terraform cloud run case"

  # Provider-side destroy guard only; it never reaches the API.
  cloud_run_deletion_protection = false

  # Create the module's own service account: the default-account data source
  # needs the unserved compute.projects.get (TF2-1).
  create_service_account = true

  ingress        = $ingress
  service_labels = $labels

  containers = [{
    container_name  = "app"
    container_image = "us-docker.pkg.dev/cloudrun/container/hello"
    env_vars = {
      GREETING = "$env_value"
    }
    ports = {
      name           = "http1"
      container_port = 8080
    }
  }]

  # The invoker grant exercises the served service IAM surface.
  members = ["allUsers"]
}
EOF
}

service_url() {
  echo "$ENDPOINT/v2/projects/$PROJECT/locations/us-central1/services/$SERVICE"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
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

case_main() {
  CLOUD_RUN_ENDPOINT="$(endpoint_for run)"

  write_module '"INGRESS_TRAFFIC_ALL"' "hello" '{ env = "compat-test" }'
  init_phase
  plan_phase "plan (module service + SA + invoker IAM member)"
  apply_phase "apply (module service + SA + invoker IAM member)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "service echoes its labels, ingress and description" "$(service_url)" \
    "\"name\":\"projects/$PROJECT/locations/us-central1/services/$SERVICE\"" \
    '"ingress":"INGRESS_TRAFFIC_ALL"' \
    '"env":"compat-test"' \
    '"description":"jaiscloud terraform cloud run case"'
  check_body "service template echoes the container image, env and port" "$(service_url)" \
    '"image":"us-docker.pkg.dev/cloudrun/container/hello"' \
    '"name":"GREETING"' '"value":"hello"' \
    '"containerPort":8080'
  check_body "service template echoes the module-created service account" "$(service_url)" \
    '"serviceAccount":"jc-tf-run-us-central1-sa@'"$PROJECT"'.iam.gserviceaccount.com"'
  check_body "service IAM carries the invoker member" \
    "$ENDPOINT/v2/projects/$PROJECT/locations/us-central1/services/$SERVICE:getIamPolicy" \
    'roles/run.invoker' 'allUsers'
  check_body "revisions.list carries the first revision" "$(service_url)/revisions" \
    '"revisions"' "services/$SERVICE/revisions/$SERVICE-00001"

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate of served fields: ingress, a service label and the container
  # env. The template change must mint a second revision and still re-plan clean.
  write_module '"INGRESS_TRAFFIC_INTERNAL_ONLY"' "hello-v2" '{ env = "compat-test", tier = "gold" }'
  apply_phase "apply (ingress + labels + container env change)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "service echoes the mutated ingress and labels" "$(service_url)" \
    '"ingress":"INGRESS_TRAFFIC_INTERNAL_ONLY"' '"tier":"gold"'
  check_body "service template echoes the mutated container env" "$(service_url)" \
    '"name":"GREETING"' '"value":"hello-v2"'
  check_body "template change bumped the revision" "$(service_url)/revisions" \
    "services/$SERVICE/revisions/$SERVICE-00002"

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  # The service and its revisions are gone; a GET must now be 404.
  local status
  status="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$(service_url)" 2>/dev/null || true)"
  if [[ "$status" == "404" ]]; then
    pass "service GET after destroy is 404"
  else
    fail "service GET after destroy = HTTP $status, want 404"
  fi
  status="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$(service_url)/revisions" 2>/dev/null || true)"
  if [[ "$status" == "404" ]]; then
    pass "revisions list after destroy is 404"
  else
    fail "revisions list after destroy = HTTP $status, want 404"
  fi
}
