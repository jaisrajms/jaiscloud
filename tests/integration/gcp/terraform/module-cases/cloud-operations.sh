# shellcheck shell=bash
#
# Case: terraform-google-cloud-operations (modules/simple-uptime-check)
#
# The pinned consumer is
# terraform-google-modules/terraform-google-cloud-operations (module v0.7.0).
# Its `simple-uptime-check` module creates a `google_monitoring_uptime_check_config`
# plus a `google_monitoring_alert_policy`, and it emits the policy's
# `alert_strategy` block whenever `auto_close`, `notification_rate_limit_period`
# or `notification_channel_strategy` is set.
#
# Before TGM5 the emulator's AlertPolicy store had no AlertStrategy/Severity:
# the create dropped `alertStrategy`, so GET omitted it and the module's next
# plan always showed
#
#   + alert_strategy {
#       + auto_close = "18000s"
#       + notification_rate_limit { + period = "300s" }
#     }
#
# and the re-apply that followed PATCHed it with updateMask=alertStrategy and
# got `501 unsupported update_mask path: alertStrategy`. A policy with a
# strategy therefore never converged.
#
# The gate proves the lifecycle converges:
#
#   create  (auto_close 18000s, rate limit 300s, user labels) -> assert GET ->
#           No changes
#   mutate  (auto_close 36000s, rate limit 600s, labels changed) -> apply ->
#           no 501 -> No changes
#   clear   (auto_close null, rate limit null, labels {}) -> apply -> No changes
#
# Provider note: `monitoring_custom_endpoint` is the API *base* URL, not the
# versioned one. The provider appends `v3/` itself, so the endpoint is
# "${endpoint}/" — passing "${endpoint}/v3/" produces /v3/v3/... and a 404.
#
# Every apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-operations"
MODULE_COMMIT="8c3e37951bb4"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-cloud-operations"
# The repo is a collection of modules; the gate drives this one.
MODULE_SUBDIR="modules/simple-uptime-check"

UPTIME_DISPLAY_NAME="jc-uptime"

# write_module generates the root module for one gate phase. The arguments are
# the HCL literals for the module's strategy/labels inputs, so the clear phase
# can pass `null`.
write_module() {
  local auto_close="$1" rate_limit="$2" labels="$3"

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

  # The base URL: the provider appends "v3/" itself.
  monitoring_custom_endpoint = "\${var.endpoint}/"
}

module "uptime" {
  source = "$MODULE_DIR"

  project_id                = var.project
  uptime_check_display_name = "$UPTIME_DISPLAY_NAME"
  protocol                  = "HTTPS"
  period                    = "60s"
  timeout                   = "10s"

  monitored_resource = {
    monitored_resource_type = "uptime_url"
    labels = {
      host       = "example.com"
      project_id = var.project
    }
  }

  auto_close                     = $auto_close
  notification_rate_limit_period = $rate_limit
  alert_policy_user_labels       = $labels
}

output "alert_policy_id" {
  value = module.uptime.alert_policy_id
}

output "uptime_check_id" {
  value = module.uptime.uptime_check_id
}
EOF
}

# read_outputs captures the module's resource ids so the spot checks can GET
# them through the emulator's own REST API.
read_outputs() {
  POLICY_ID="$(tf output -raw alert_policy_id 2>/dev/null || true)"
  UPTIME_ID="$(tf output -raw uptime_check_id 2>/dev/null || true)"
}

body_of() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

# check_url asserts a REST GET body carries each expected substring.
check_url() {
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

# check_url_absent is the negative check: a field the client did not send must
# not be rendered.
check_url_absent() {
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

case_main() {
  write_module '"18000s"' '"300s"' '{ team = "platform" }'
  init_phase
  plan_phase "plan (uptime check + alert policy with a strategy)"
  apply_phase "apply (create uptime check and alert policy with alert_strategy)"
  assert_no_api_errors

  read_outputs
  if [[ -z "$POLICY_ID" || -z "$UPTIME_ID" ]]; then
    fail "module outputs (alert_policy_id=$POLICY_ID uptime_check_id=$UPTIME_ID)"
  else
    echo "== spot checks (create)"
    check_url "uptime check exists" "$ENDPOINT/v3/$UPTIME_ID" \
      '"displayName":"jc-uptime"' '"httpCheck":{'
    check_url "alert strategy and user labels read back" "$ENDPOINT/v3/$POLICY_ID" \
      '"alertStrategy":{' \
      '"autoClose":"18000s"' \
      '"notificationRateLimit":{' '"period":"300s"' \
      '"team":"platform"'
    check_url_absent "no severity was synthesized" "$ENDPOINT/v3/$POLICY_ID" '"severity":'
  fi

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  write_module '"36000s"' '"600s"' '{ team = "sre" }'
  apply_phase "apply (auto_close/rate limit changed, labels changed)"
  assert_no_api_errors
  echo "== spot checks (mutate)"
  check_url "changed strategy, rate limit and labels read back" "$ENDPOINT/v3/$POLICY_ID" \
    '"autoClose":"36000s"' \
    '"period":"600s"' \
    '"team":"sre"'
  plan_no_changes "re-plan after mutation reports no changes"

  write_module 'null' 'null' '{}'
  apply_phase "apply (strategy and labels cleared)"
  assert_no_api_errors
  echo "== spot checks (clear)"
  check_url_absent "cleared alert strategy is no longer rendered" "$ENDPOINT/v3/$POLICY_ID" \
    '"alertStrategy":' '"autoClose":'
  check_url_absent "cleared user labels are no longer rendered" "$ENDPOINT/v3/$POLICY_ID" \
    '"userLabels":' '"team":'
  check_url "the uptime check survived the clear" "$ENDPOINT/v3/$UPTIME_ID" \
    '"displayName":"jc-uptime"'
  plan_no_changes "re-plan after clearing the strategy reports no changes"

  destroy_phase
}
