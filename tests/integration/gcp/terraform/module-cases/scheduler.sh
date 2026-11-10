# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-cloud-scheduler (TF3-4)
#
# The pinned consumer is the CFT Cloud Scheduler blueprint (v0.1.1, commit
# ece5c3e87412). Its root module creates exactly one served resource per
# instance:
#
#   google_cloud_scheduler_job   -> jobs.create / .get / .patch / .delete,
#                                   plus the :pause and :resume custom methods
#
# and it exposes `schedule`, `time_zone`, `paused`, `attempt_deadline`,
# `retry_config` and the three mutually-exclusive targets (http, pub/sub,
# App Engine; the module rejects a config that sets zero or two of them).
#
# The case drives **both served target shapes** through two module instances:
#
#   * the HTTP target (header-less, so the server fills the defaults) — the
#     shape provider 6.49.3 crashed on when the response omitted
#     `httpTarget.headers`; real Cloud Scheduler always answers a headers map
#     (User-Agent: Google-Cloud-Scheduler, plus Content-Type:
#     application/octet-stream when the job carries a body), which TF3-4-1
#     (`fix/gcp-scheduler-http-target-headers`) makes the emulator do too.
#   * the Pub/Sub target, the module's primary published example.
#
# The gate proves the lifecycle converges without the cron engine firing:
#
#   apply (two enabled jobs, far-future schedules, the module's default
#   `retry_config = { retry_count = 0 }`)
#     -> the provider's create issues POST then :resume on the already-ENABLED
#        job, which real Cloud Scheduler answers 200 and the emulator now does
#        too (TF2-12)
#     -> the provider drops the all-zero retry_config block from the create
#        body; the emulator synthesizes the documented RetryConfig defaults on
#        the read (retry_count 0, min_backoff_duration 5s, max_backoff_duration
#        1h, max_doublings 5, max_retry_duration 0 = unlimited), so the read
#        fills retry_config instead of leaving it out (TF3-4-2)
#     -> REST asserts (name, description, schedule, timeZone, state ENABLED,
#        scheduleTime, the synthesized retryConfig, the HTTP target with the
#        synthesized headers, the Pub/Sub target topic/data/attributes)
#   plan -detailed-exitcode -> No changes (the default retry_config must not
#     re-plan `+ retry_config { retry_count = 0 }`)
#   mutate (schedule, description, retryConfig, HTTP body, Pub/Sub payload)
#     -> apply -> REST asserts -> No changes
#   pause (paused = true) -> the provider calls :pause -> both jobs PAUSED, with
#     no scheduleTime/lastAttemptTime/status (a paused job reads as never
#     scheduled and never attempted) -> No changes
#   resume (paused = false) -> the provider calls :resume -> both ENABLED and a
#     fresh scheduleTime -> No changes
#   destroy -> both job GETs and the topic GET are 404
#
# Why the jobs are created enabled: that is the default `google_cloud_scheduler_job`
# shape (the module default is `paused = false`), and the provider resolves it by
# POSTing the job and then calling `:resume` on the now-ENABLED job. A 400 there
# made an enabled job uncreatable, so the case created paused until TF2-12
# (`fix/gcp-scheduler-resume-enabled`) made `:pause`/`:resume` idempotent the way
# real Cloud Scheduler is. The schedules ("0 0 1 1 *", then "0 0 1 7 *") are far
# in the future, so an ENABLED job still cannot fire during the gate.
#
# Boundaries, recorded rather than worked around:
#
#  * `app_engine_http_target` is not configured. The emulator stores and
#    validates the App Engine target (relative URI + method) but App Engine is
#    not a served service; the module's App Engine input is left null.
#  * The module's HTTP target input has no `headers` field, which is exactly the
#    header-less shape TF3-4-1 fixes; the case asserts the server-filled map.
#  * `attempt_deadline` is nulled by the module for a pub/sub target (the
#    module's own conditional); the provider's `EmptyOrDefaultStringSuppress`
#    keeps the 180s server default out of the plan. The HTTP instance uses the
#    module default (15s), inside the documented 15s..30m HTTP window.
#  * The module's default `retry_config = { retry_count = 0 }` expands to an
#    all-zero block, which the provider drops from the create body. The
#    emulator synthesizes the documented RetryConfig defaults on the read
#    (TF3-4-2), so the read fills `retry_config` and that input no longer
#    re-plans; the case drives the module default in the first phase (and
#    asserts the synthesized values over REST) before mutating to a non-default
#    config, which must be echoed verbatim.
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

JOB_HTTP="jc-tf-scheduler"
JOB_PUBSUB="jc-tf-scheduler-pubsub"
TOPIC="jc-tf-scheduler-topic"
HTTP_URI="http://example.com/jaiscloud-tf-scheduler"
HTTP_BODY_V1="c2NoZWR1bGVyLWh0dHAtYm9keS12MQ==" # base64("scheduler-http-body-v1")
HTTP_BODY_V2="c2NoZWR1bGVyLWh0dHAtYm9keS12Mg==" # base64("scheduler-http-body-v2")

# write_module generates the root module for one gate phase.
#   $1 description (HCL string literal)
#   $2 schedule (HCL string literal)
#   $3 paused (HCL bool literal: true/false)
#   $4 retry_config (HCL object literal)
#   $5 Pub/Sub payload (HCL string literal, base64)
#   $6 Pub/Sub attributes (HCL map literal)
#   $7 HTTP body (HCL string literal, base64)
write_module() {
  local description="$1" schedule="$2" paused="$3" retry="$4" payload="$5" attributes="$6" http_body="$7"

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

# The target the Pub/Sub job publishes to. The emulator stores the topic name
# without validating it, but a real consumer creates the topic first, so the
# case does.
resource "google_pubsub_topic" "target" {
  name    = "$TOPIC"
  project = var.project
}

# The HTTP target is header-less on purpose: the module's input has no headers
# field, so the server must answer the synthesized defaults (TF3-4-1).
module "scheduler_http" {
  source = "$MODULE_DIR"

  project_id = var.project
  location   = "us-central1"
  name       = "$JOB_HTTP"

  description  = $description
  schedule     = $schedule
  time_zone    = "UTC"
  paused       = $paused
  retry_config = $retry

  app_engine_http_target = null
  pubsub_target          = null
  http_target = {
    uri         = "$HTTP_URI"
    http_method = "POST"
    body        = $http_body
  }
}

module "scheduler_pubsub" {
  source = "$MODULE_DIR"

  project_id = var.project
  location   = "us-central1"
  name       = "$JOB_PUBSUB"

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
  echo "$ENDPOINT/v1/projects/$PROJECT/locations/us-central1/jobs/$1"
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
    '{ retry_count = 0 }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjE="' \
    '{ source = "jaiscloud" }' \
    "\"$HTTP_BODY_V1\"" # base64("scheduler-payload-v1")
  # `{ retry_count = 0 }` is the module's default retry_config: the provider
  # drops the all-zero block from the create body, the emulator synthesizes the
  # documented RetryConfig defaults on the read, and the plan must still be
  # empty (TF3-4-2).
  init_phase
  plan_phase "plan (module scheduler jobs)"
  apply_phase "apply (module scheduler jobs)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "the HTTP job echoes its reference, schedule and enabled state" "$(job_url "$JOB_HTTP")" \
    "\"name\":\"projects/$PROJECT/locations/us-central1/jobs/$JOB_HTTP\"" \
    '"description":"jaiscloud terraform scheduler case"' \
    '"schedule":"0 0 1 1 *"' \
    '"timeZone":"UTC"' \
    '"state":"ENABLED"'
  check_body "an enabled job reports its next scheduleTime" "$(job_url "$JOB_HTTP")" '"scheduleTime"'
  check_body "the HTTP job echoes its target and body" "$(job_url "$JOB_HTTP")" \
    '"httpTarget"' \
    "\"uri\":\"$HTTP_URI\"" \
    '"httpMethod":"POST"' \
    "\"body\":\"$HTTP_BODY_V1\""
  # Real Cloud Scheduler answers a headers map for a header-less HTTP target and
  # fills the delivery defaults; provider 6.49.3 panics if the map is absent and
  # strips these exact values before diffing (TF3-4-1).
  check_body "the HTTP job carries the server-filled target headers" "$(job_url "$JOB_HTTP")" \
    '"headers"' \
    '"User-Agent":"Google-Cloud-Scheduler"' \
    '"Content-Type":"application/octet-stream"'
  check_body "the pub/sub job echoes its target" "$(job_url "$JOB_PUBSUB")" \
    '"pubsubTarget"' \
    "/topics/$TOPIC" \
    '"data":"c2NoZWR1bGVyLXBheWxvYWQtdjE="' \
    '"attributes":{"source":"jaiscloud"}'
  check_body "the target topic exists" "$(topic_url)" "\"name\":\"projects/$PROJECT/topics/$TOPIC\""
  # The module default retry_config is dropped from the create body by the
  # provider; the emulator answers the documented RetryConfig defaults so the
  # provider's read fills retry_config (TF3-4-2). Both jobs go through the same
  # core, so one assertion per target shape is enough, but both are checked to
  # prove neither transport/target path drops the synthesized message.
  check_body "the HTTP job reads back the synthesized retryConfig defaults" "$(job_url "$JOB_HTTP")" \
    '"retryConfig"' \
    '"retryCount":0' \
    '"maxDoublings":5' \
    '"minBackoffDuration":"5s"' \
    '"maxBackoffDuration":"3600s"' \
    '"maxRetryDuration":"0s"'
  check_body "the pub/sub job reads back the synthesized retryConfig defaults" "$(job_url "$JOB_PUBSUB")" \
    '"retryConfig"' \
    '"retryCount":0' \
    '"maxDoublings":5' \
    '"minBackoffDuration":"5s"' \
    '"maxBackoffDuration":"3600s"' \
    '"maxRetryDuration":"0s"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: a new schedule, description, retry config, HTTP body and
  # Pub/Sub payload/attributes. The jobs stay enabled, and the schedules are far
  # in the future, so nothing can fire.
  write_module '"jaiscloud terraform scheduler case v2"' '"0 0 1 7 *"' false \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }' \
    "\"$HTTP_BODY_V2\"" # base64("scheduler-payload-v2")
  apply_phase "apply (schedule + description + retryConfig + targets)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "the HTTP job echoes the mutated schedule and description" "$(job_url "$JOB_HTTP")" \
    '"description":"jaiscloud terraform scheduler case v2"' \
    '"schedule":"0 0 1 7 *"'
  check_body "the HTTP job echoes the mutated body" "$(job_url "$JOB_HTTP")" \
    "\"body\":\"$HTTP_BODY_V2\""
  check_body "the jobs echo the mutated retry config" "$(job_url "$JOB_HTTP")" \
    '"retryCount":3' '"maxDoublings":3' \
    '"minBackoffDuration":"2s"' '"maxBackoffDuration":"20s"'
  check_body "the pub/sub job echoes the mutated payload and attributes" "$(job_url "$JOB_PUBSUB")" \
    '"data":"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '"tier":"gold"'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  # Pause: the provider calls :pause on the ENABLED jobs. The schedules are far
  # in the future, so a paused job and an enabled one alike cannot fire during
  # the gate.
  write_module '"jaiscloud terraform scheduler case v3"' '"0 0 1 7 *"' true \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }' \
    "\"$HTTP_BODY_V2\""
  apply_phase "apply (pause -> :pause)"
  assert_no_api_errors
  check_body "pause leaves the HTTP job PAUSED" "$(job_url "$JOB_HTTP")" '"state":"PAUSED"'
  check_body "pause leaves the pub/sub job PAUSED" "$(job_url "$JOB_PUBSUB")" '"state":"PAUSED"'
  # A paused job answers no run state: real Cloud Scheduler removes the next run
  # and clears the last attempt, so scheduleTime/lastAttemptTime/status are
  # absent (TF2-12-1).
  body="$(body_of "$(job_url "$JOB_HTTP")")"
  if [[ "$body" == *'"scheduleTime"'* || "$body" == *'"lastAttemptTime"'* || "$body" == *'"status"'* ]]; then
    fail "a paused job still answers run state (scheduleTime/lastAttemptTime/status)"
  else
    pass "a paused job answers no scheduleTime/lastAttemptTime/status"
  fi
  plan_no_changes "re-plan after :pause reports no changes"

  # Resume: the provider calls :resume on the PAUSED jobs.
  write_module '"jaiscloud terraform scheduler case v4"' '"0 0 1 7 *"' false \
    '{ retry_count = 3, max_doublings = 3, min_backoff_duration = "2s", max_backoff_duration = "20s" }' \
    '"c2NoZWR1bGVyLXBheWxvYWQtdjI="' \
    '{ source = "jaiscloud", tier = "gold" }' \
    "\"$HTTP_BODY_V2\""
  apply_phase "apply (resume -> :resume)"
  assert_no_api_errors
  check_body "resume leaves the HTTP job ENABLED" "$(job_url "$JOB_HTTP")" '"state":"ENABLED"'
  check_body "resume leaves the pub/sub job ENABLED" "$(job_url "$JOB_PUBSUB")" '"state":"ENABLED"'
  body="$(body_of "$(job_url "$JOB_HTTP")")"
  if [[ "$body" == *'"scheduleTime"'* ]]; then
    pass "a resumed job reports its next scheduleTime"
  else
    fail "a resumed job is missing scheduleTime"
  fi
  plan_no_changes "re-plan after :resume reports no changes"

  destroy_phase

  local status job
  for job in "$JOB_HTTP" "$JOB_PUBSUB"; do
    status="$(status_of "$(job_url "$job")")"
    if [[ "$status" == "404" ]]; then
      pass "job $job GET after destroy is 404"
    else
      fail "job $job GET after destroy = HTTP $status, want 404"
    fi
  done
  status="$(status_of "$(topic_url)")"
  if [[ "$status" == "404" ]]; then
    pass "target topic GET after destroy is 404"
  else
    fail "target topic GET after destroy = HTTP $status, want 404"
  fi
}
