# shellcheck shell=bash
#
# Case: GoogleCloudPlatform/terraform-google-cloud-tasks (TF3-4)
#
# The pinned consumer is the CFT Cloud Tasks blueprint (v0.4.0, commit
# ba7dbc000bfb). Its root module creates one queue and, depending on
# `queue_iam_choice`, one IAM resource on it:
#
#   google_cloud_tasks_queue             -> queues.create / .get / .patch /
#                                           .delete, plus :pause / :resume /
#                                           :purge
#   google_cloud_tasks_queue_iam_member  -> queues.getIamPolicy /
#                                           .setIamPolicy (read-modify-write)
#
# and it emits `rate_limits`, `retry_config`, `stackdriver_logging_config`,
# `app_engine_routing_override` and `http_target`. The gate proves the queue
# lifecycle converges without enqueueing anything, so no dispatch engine runs:
#
#   apply (queue + rate limits + retry config + logging config + one IAM
#          member)
#     -> REST asserts (name, state RUNNING, rateLimits, retryConfig,
#        stackdriverLoggingConfig, the IAM binding, an empty task list)
#   plan -detailed-exitcode -> No changes
#   mutate (a rate limit, a retry limit and the IAM member) -> apply
#     -> REST asserts -> No changes
#   destroy -> queue GET is 404
#
# The empty task list is the "no dispatch errors" assertion: with no task
# created, the per-queue dispatch engine has nothing to deliver and cannot make
# the gate flaky. Task creation/dispatch itself is covered by the service and
# engine tests, not this consumer gate.
#
# Boundaries, recorded rather than worked around:
#
#  * `app_engine_routing_override` and `http_target` are left null. They are
#    queue-level defaults for App Engine/HTTP *tasks*, and the module does not
#    create any task; the emulator stores the fields but the gate has nothing to
#    deliver.
#  * `rate_limits.max_burst_size` is **computed-only** in the provider's 6.x
#    schema (7.x made it Optional+Computed), so the module's optional input must
#    stay null and the provider never plans a diff for it. The create body
#    therefore omits it, and the emulator applies the documented Cloud Tasks
#    default: maxBurstSize 100 when the field is absent (output only, set from
#    maxDispatchesPerSecond), alongside maxConcurrentDispatches 1000 when that is
#    absent too. The spot checks below assert the default on read-back (TF3-4-3).
#
# Provider note: the module declares `google >= 3.53, < 7.0`, so this case pins
# `google = 6.49.3` while the 7.x cases pin 7.46.2. `cloud_tasks_custom_endpoint`
# is a versioned base URL (one trailing slash). Every apply log must be free of
# 400/501 and `unsupported update_mask`.

MODULE_REPO="terraform-google-cloud-tasks"
MODULE_COMMIT="ba7dbc000bfb"
MODULE_GIT="https://github.com/GoogleCloudPlatform/terraform-google-cloud-tasks"

# The case's module pins google < 7.0, so its provider predates some mapping
# rows; scope the capture and downgrade absent rows to notes (see note above).
CAPTURE_RESOURCES="google_cloud_tasks_queue,google_cloud_tasks_queue_iam_member"
CAPTURE_ALLOW_MISSING=1

QUEUE="jc-tf-tasks"
MEMBER_A="serviceAccount:jc-tf-tasks-a@$PROJECT.iam.gserviceaccount.com"
MEMBER_B="serviceAccount:jc-tf-tasks-b@$PROJECT.iam.gserviceaccount.com"

# write_module generates the root module for one gate phase.
#   $1 rate limits (HCL object literal)
#   $2 retry config (HCL object literal)
#   $3 IAM member (HCL string literal)
write_module() {
  local rate_limits="$1" retry="$2" member="$3"

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

  cloud_tasks_custom_endpoint = "\${var.endpoint}/v2/"
}

module "tasks" {
  source = "$MODULE_DIR"

  project_id = var.project
  location   = "us-central1"
  queue_name = "$QUEUE"

  rate_limits  = $rate_limits
  retry_config = $retry

  stackdriver_logging_config = {
    sampling_ratio = 0.5
  }

  queue_iam_choice = "iam_member"
  role             = "roles/cloudtasks.enqueuer"
  member           = $member
}
EOF
}

queue_url() {
  echo "$ENDPOINT/v2/projects/$PROJECT/locations/us-central1/queues/$QUEUE"
}

tasks_url() {
  echo "$(queue_url)/tasks"
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

post_body() {
  curl -sf -X POST -H "Content-Type: application/json" -d '{}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
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

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

case_main() {
  local body

  write_module '{ max_dispatches_per_second = 5, max_concurrent_dispatches = 3 }' \
    '{ max_attempts = 4, min_backoff = "1s", max_backoff = "10s", max_doublings = 3, max_retry_duration = "60s" }' \
    "\"$MEMBER_A\""
  init_phase
  plan_phase "plan (module queue + rate limits + IAM)"
  apply_phase "apply (module queue + rate limits + IAM)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "queue echoes its reference and running state" "$(queue_url)" \
    "\"name\":\"projects/$PROJECT/locations/us-central1/queues/$QUEUE\"" \
    '"state":"RUNNING"'
  check_body "queue echoes its rate limits" "$(queue_url)" \
    '"rateLimits"' \
    '"maxDispatchesPerSecond":5' \
    '"maxBurstSize":100' \
    '"maxConcurrentDispatches":3'
  check_body "queue echoes its retry config" "$(queue_url)" \
    '"retryConfig"' \
    '"maxAttempts":4' \
    '"minBackoff":"1s"' \
    '"maxBackoff":"10s"' \
    '"maxDoublings":3' \
    '"maxRetryDuration":"60s"'
  check_body "queue echoes its stackdriver logging config" "$(queue_url)" \
    '"stackdriverLoggingConfig"' '"samplingRatio":0.5'
  check_post_body "queue IAM carries the enqueuer member" \
    "$(queue_url):getIamPolicy" \
    '"role":"roles/cloudtasks.enqueuer"' "$MEMBER_A"

  body="$(body_of "$(tasks_url)")"
  if [[ -z "$body" ]]; then
    fail "task list (no response from $(tasks_url))"
  elif [[ "$body" == *"/tasks/"* ]]; then
    fail "task list is not empty; the gate must not enqueue a task"
  else
    pass "the queue has no tasks (nothing for the dispatch engine to deliver)"
  fi

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: a rate limit, a retry limit and the IAM member (the
  # provider's read-modify-write replaces one member with the other).
  write_module '{ max_dispatches_per_second = 8, max_concurrent_dispatches = 2 }' \
    '{ max_attempts = 6, min_backoff = "2s", max_backoff = "20s", max_doublings = 4, max_retry_duration = "120s" }' \
    "\"$MEMBER_B\""
  apply_phase "apply (rate limit + retry limit + IAM member)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "queue echoes the mutated rate limits" "$(queue_url)" \
    '"maxDispatchesPerSecond":8' \
    '"maxBurstSize":100' \
    '"maxConcurrentDispatches":2'
  check_body "queue echoes the mutated retry config" "$(queue_url)" \
    '"maxAttempts":6' \
    '"minBackoff":"2s"' \
    '"maxBackoff":"20s"' \
    '"maxDoublings":4' \
    '"maxRetryDuration":"120s"'
  check_post_body "queue IAM carries the new member" \
    "$(queue_url):getIamPolicy" "$MEMBER_B"
  body="$(post_body "$(queue_url):getIamPolicy")"
  if [[ -z "$body" ]]; then
    fail "queue IAM read after the member change (no response)"
  elif [[ "$body" == *"$MEMBER_A"* ]]; then
    fail "the replaced queue IAM member is still present"
  else
    pass "the replaced queue IAM member was removed"
  fi

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(queue_url)")"
  if [[ "$status" == "404" ]]; then
    pass "queue GET after destroy is 404"
  else
    fail "queue GET after destroy = HTTP $status, want 404"
  fi
}
