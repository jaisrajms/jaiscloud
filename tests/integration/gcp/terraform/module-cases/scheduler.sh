# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-cloud-scheduler (TF3-4)
#
# The pinned consumer is the CFT Cloud Scheduler blueprint (v0.1.1, commit
# ece5c3e87412). Its root module creates exactly one served resource:
#
#   google_cloud_scheduler_job   -> jobs.create / .get / .patch / .delete,
#                                   plus the :pause and :resume custom methods
#
# and it exposes `schedule`, `time_zone`, `paused`, `attempt_deadline`,
# `retry_config` and the three mutually-exclusive targets (http, pub/sub,
# App Engine; the module rejects a config that sets zero or two of them).
#
# The case drives the **pub/sub target**. The HTTP target is the other common
# shape, but provider 6.49.3 panics in `flattenCloudSchedulerJobHttpTargetHeaders`
# when the response omits an empty `httpTarget.headers` map (the flatten does an
# unguarded `v.(map[string]interface{})`; 7.x added a nil guard). Real Cloud
# Scheduler answers `headers: {}` for a header-less HTTP target — the provider's
# own acceptance test at 6.49.3 uses exactly that shape — so the emulator's
# omission is a response-shape gap, recorded as TF3-4-1
# (`fix/gcp-scheduler-http-target-headers`). This case avoids the crash until
# that fix lands; the pub/sub target is the module's primary published example.
#
# The gate proves the lifecycle converges without the cron engine firing:
#
#   apply (enabled job, far-future schedule, Pub/Sub target, retry config)
#     -> the provider's create issues POST then :resume on the already-ENABLED
#        job, which real Cloud Scheduler answers 200 and the emulator now does
#        too (TF2-12)
#     -> REST asserts (name, description, schedule, timeZone, state ENABLED,
#        scheduleTime, retryConfig, pubsubTarget topic/data/attributes)
#   plan -detailed-exitcode -> No changes
#   mutate (schedule, description, retryConfig, Pub/Sub payload) -> apply
#     -> REST asserts -> No changes
#   pause (paused = true) -> the provider calls :pause -> REST assert
#     PAUSED -> No changes
#   resume (paused = false) -> the provider calls :resume -> REST assert
#     ENABLED + a fresh scheduleTime -> No changes
#   destroy -> job GET is 404
#
# Why the job is created enabled: that is the default `google_cloud_scheduler_job`
# shape (the module default is `paused = false`), and the provider resolves it by
# POSTing the job and then calling `:resume` on the now-ENABLED job. A 400 there
# made an enabled job uncreatable, so the case created paused until TF2-12
# (`fix/gcp-scheduler-resume-enabled`) made `:pause`/`:resume` idempotent the way
# real Cloud Scheduler is. The schedule ("0 0 1 1 *", then "0 0 1 7 *") is far in
# the future, so an ENABLED job still cannot fire during the gate.
#
# Boundaries, recorded rather than worked around:
#
#  * `app_engine_http_target` is not configured. The emulator stores and
#    validates the App Engine target (relative URI + method) but App Engine is
#    not a served service; the module's App Engine input is left null.
#  * `attempt_deadline` is nulled by the module for a pub/sub target (the
#    module's own conditional); the provider's `EmptyOrDefaultStringSuppress`
#    keeps the 180s server default out of the plan.
#  * The module's default `retry_config = { retry_count = 0 }` is not sent: the
#    provider drops an all-empty block and the emulator does not synthesize the
#    documented RetryConfig defaults, so that input re-plans
#    `+ retry_config { retry_count = 0 }`. The case creates a non-default retry
#    config; default synthesis is TF3-4-2
#    (`fix/gcp-scheduler-retry-config-defaults`).
#  * `paused` is a provider-local attribute: Cloud Scheduler's Job resource has
#    no such field (Discovery), the provider deletes it from every request body
#    and maps it onto the `:pause`/`:resume` methods.
#
# Provider note: the module declares `google >= 3.53, < 7.0` and requires the
# `null` provider (its one_of_required guard is a null_resource with a
# precondition), so this case pins `google = 6.49.3` and `null = 3.3.2` while
# the 7.x cases pin 7.46.2. `cloud_scheduler_custom_endpoint` and
# `pubsub_custom_endpoint` are versioned base URLs (one trailing slash). Every
# apply log must be free of 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-scheduler"
MODULE_COMMIT="ece5c3e87412"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-cloud-scheduler"

# The case's module pins google < 7.0, so its provider predates some mapping
# rows; scope the capture and downgrade absent rows to notes (see note above).
CAPTURE_RESOURCES="google_cloud_scheduler_job"
CAPTURE_ALLOW_MISSING=1

JOB="jc-tf-scheduler"
TOPIC="jc-tf-scheduler-topic"

# write_module generates the root module for one gate phase.
#   $1 description (HCL string literal)
#   $2 schedule (HCL string literal)
#   $3 paused (HCL bool literal: true/false)
#   $4 retry_config (HCL object literal)
#   $5 Pub/Sub payload (HCL string literal, base64)
#   $6 Pub/Sub attributes (HCL map literal)
write_module() {
  local description="$1" schedule="$2" paused="$3" retry="$4" payload="$5" attributes="$6"

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 6.49.3"
    }
    null = {
      source  = "hashicorp/null"
      version = "= 3.3.2"
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

  cloud_scheduler_custom_endpoint = "\${var.endpoint}/v1/"
  pubsub_custom_endpoint          = "\${var.endpoint}/v1/"
}

# The target the job publishes to. The emulator stores the topic name without
# validating it, but a real consumer creates the topic first, so the case does.
resource "google_pubsub_topic" "target" {
  name    = "$TOPIC"
  project = var.project
}

module "scheduler" {
  source = "$MODULE_DIR"

  project_id = var.project
  location   = "us-central1"
  name       = "$JOB"

  description  = $description
  schedule     = $schedule
  time_zone    = "UTC"
  paused       = $paused
  retry_config = $retry

  app_engine_http_target = null
  http_target            = null
  pubsub_target = {
    topic_name = google_pubsub_topic.target.id
    data       = $payload
    attributes = $attributes
  }
}
EOF
}

job_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/us-central1/jobs/$JOB"
}

topic_url() {
  echo "$ENDPOINT/v1/projects/$PROJECT/topics/$TOPIC"
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

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

case_main() {
  local body

  write_module '"jaiscloud terraform scheduler case"' '"0 0 1 1 *"' false \
    '{ retry_count = 1 }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjE="' \
    '{ source = "jaiscloud" }' # base64("scheduler-payload-v1")
  init_phase
  plan_phase "plan (module scheduler job)"
  apply_phase "apply (module scheduler job)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "job echoes its reference, schedule and enabled state" "$(job_url)" \
    "\"name\":\"projects/$PROJECT/locations/us-central1/jobs/$JOB\"" \
    '"description":"jaiscloud terraform scheduler case"' \
    '"schedule":"0 0 1 1 *"' \
    '"timeZone":"UTC"' \
    '"state":"ENABLED"'
  check_body "an enabled job reports its next scheduleTime" "$(job_url)" '"scheduleTime"'
  check_body "job echoes the retry config" "$(job_url)" \
    '"retryConfig":{"retryCount":1}'
  check_body "job echoes the Pub/Sub target" "$(job_url)" \
    '"pubsubTarget"' \
    "/topics/$TOPIC" \
    '"data":"c2NoZWR1bGVyLXBheWxvYWQtdjE="' \
    '"attributes":{"source":"jaiscloud"}'
  check_body "the target topic exists" "$(topic_url)" "\"name\":\"projects/$PROJECT/topics/$TOPIC\""

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: a new schedule, description, retry config and Pub/Sub
  # payload/attributes. The job stays enabled, and the schedule is far in the
  # future, so nothing can fire.
  write_module '"jaiscloud terraform scheduler case v2"' '"0 0 1 7 *"' false \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }' # base64("scheduler-payload-v2")
  apply_phase "apply (schedule + description + retryConfig + Pub/Sub payload)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "job echoes the mutated schedule and description" "$(job_url)" \
    '"description":"jaiscloud terraform scheduler case v2"' \
    '"schedule":"0 0 1 7 *"'
  check_body "job echoes the mutated retry config" "$(job_url)" \
    '"retryCount":3' '"maxDoublings":3' \
    '"minBackoffDuration":"2s"' '"maxBackoffDuration":"20s"'
  check_body "job echoes the mutated Pub/Sub payload and attributes" "$(job_url)" \
    '"data":"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '"tier":"gold"'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  # Pause: the provider calls :pause on the ENABLED job. The schedule is far in
  # the future, so a paused job and an enabled one alike cannot fire during the
  # gate.
  write_module '"jaiscloud terraform scheduler case v3"' '"0 0 1 7 *"' true \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }'
  apply_phase "apply (pause -> :pause)"
  assert_no_api_errors
  check_body "pause leaves the job PAUSED" "$(job_url)" '"state":"PAUSED"'
  plan_no_changes "re-plan after :pause reports no changes"

  # Resume: the provider calls :resume on the PAUSED job.
  write_module '"jaiscloud terraform scheduler case v4"' '"0 0 1 7 *"' false \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }'
  apply_phase "apply (resume -> :resume)"
  assert_no_api_errors
  check_body "resume leaves the job ENABLED" "$(job_url)" '"state":"ENABLED"'
  body="$(body_of "$(job_url)")"
  if [[ "$body" == *'"scheduleTime"'* ]]; then
    pass "a resumed job reports its next scheduleTime"
  else
    fail "a resumed job is missing scheduleTime"
  fi
  plan_no_changes "re-plan after :resume reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(job_url)")"
  if [[ "$status" == "404" ]]; then
    pass "job GET after destroy is 404"
  else
    fail "job GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$(topic_url)")"
  if [[ "$status" == "404" ]]; then
    pass "target topic GET after destroy is 404"
  else
    fail "target topic GET after destroy = HTTP $status, want 404"
  fi
}
