#!/usr/bin/env bash
#
# Composition pattern 02 — event-driven chain (TF4-2).
#
# What: a Pub/Sub topic fed by a Cloud Scheduler job, a push subscription that
#       delivers to a local HTTP echo sink, a Pub/Sub-triggered Cloud Function on
#       the same topic, a storage-triggered Cloud Function on a GCS bucket and a
#       standalone Eventarc trigger with an httpEndpoint destination on the same
#       bucket — one root module, one apply.
#
# Contract (patterns/lib.sh): init -> apply -> REST read-back asserts ->
# delivery asserts (scheduler run -> push + function; object write -> Eventarc
# + function) -> plan No changes -> destroy -> post-destroy asserts.
#
# Delivery is asserted at the far end, not inferred from the resource existing:
# the pattern starts helpers/echo-sink, points the push endpoint and the Eventarc
# httpEndpoint at it, and inspects the body and CloudEvents headers it received.
# The scheduler job is ENABLED with a far-future cron and fired through
# projects.locations.jobs.run, Cloud Scheduler's documented "force it now" call,
# which real GCP only allows on an ENABLED job; the far-future cron means the run
# never waits for a wall-clock minute and needs no clock control.
#
# Usage:
#   tests/integration/gcp/terraform/patterns/02-events/run.sh [ENDPOINT] [PROJECT]
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-patterns
#   make test-gcp-terraform-pattern PATTERN=02-events

set -euo pipefail

PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_NAME="02-events"
# shellcheck source=../lib.sh
source "$PATTERN_DIR/../lib.sh"

# Run-unique names: the emulator keeps state across patterns in one aggregate
# run, so a fixed name would collide on a second run against the same emulator.
SUFFIX="${JC_TF_EVENTS_SUFFIX:-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')}"
TF_VARS+=(-var "name_suffix=$SUFFIX")

# main.tf uploads function-source.zip from path.module; the driver assembles the
# root module in a work dir, so the archive travels with the generated *.tf.
cp "$PATTERN_DIR/function-source.zip" "$WORK/function-source.zip"

if ! start_echo_sink "$WORK/sink.jsonl"; then
  fail "the local echo sink did not start"
  summary
  exit 1
fi
SINK_LOG="$ECHO_SINK_LOG"
SINK_BODY="$ECHO_SINK_BODY"
TF_VARS+=(-var "sink_url=$ECHO_SINK_URL")

# wait_for_substring NAME URL NEEDLE [TIMEOUT_SECONDS] — poll GET URL until its
# body carries NEEDLE. Used for the asynchronous functions delivery engine and
# the scheduler's lastAttemptTime.
wait_for_substring() {
  local name="$1" url="$2" needle="$3" timeout="${4:-20}" i body
  for ((i = 0; i < timeout * 10; i++)); do
    body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
    if [[ "$body" == *"$needle"* ]]; then
      pass "$name"
      return 0
    fi
    sleep 0.1
  done
  fail "$name (timed out waiting for: $needle)"
  return 0
}

# expect_body NAME FILE WANT... — every WANT must appear in FILE (the raw body
# the echo sink received).
expect_body() {
  local name="$1" file="$2" want body
  shift 2
  body="$(cat "$file" 2>/dev/null || true)"
  if [[ -z "$body" ]]; then
    fail "$name (the sink received no body)"
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

# function_delivery_dump — the emulator's state export. Its functions store
# carries the event-delivery records; a binary built without the console (the
# UI build serves /api/ui/.../deliveries) has no REST or gRPC surface for them,
# so the export is how this pattern proves an event actually invoked a function
# rather than only that the trigger exists.
function_delivery_dump() {
  local out="$WORK/export.tgz"
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/_jaiscloud/export" -o "$out" || return 1
  tar xzOf "$out" envelope.json 2>/dev/null
}

# wait_for_delivery NAME FUNCTION [TIMEOUT_SECONDS] — poll the export until a
# delivery record for FUNCTION reaches "delivered".
wait_for_delivery() {
  local name="$1" fn="$2" timeout="${3:-20}" i dump records
  for ((i = 0; i < timeout * 10; i++)); do
    dump="$(function_delivery_dump || true)"
    if [[ -n "$dump" ]]; then
      # Delivery records are separated by `},{"`; splitting there isolates one
      # record per line so a data string (whose quotes are escaped) cannot make
      # the match span two records.
      records="${dump//'},{"'/$'\n'}"
      matched="$(grep -F "\"functionId\":\"$fn\"" <<<"$records" || true)"
      if [[ "$matched" == *'"status":"delivered"'* ]]; then
        pass "$name"
        return 0
      fi
    fi
    sleep 0.1
  done
  fail "$name (timed out waiting for a delivered delivery of $fn)"
  return 0
}

init_phase
apply_phase "apply (scheduler + topic + push subscription + 2 functions + Eventarc trigger)"
assert_no_api_errors

echo "== read-back (suffix $SUFFIX)"
TOPIC_ID="$(tf_output topic_id)"
SUBSCRIPTION_ID="$(tf_output subscription_id)"
JOB_ID="$(tf_output job_id)"
SCHEDULER_PAYLOAD="$(tf_output scheduler_payload)"
BUCKET="$(tf_output bucket_name)"
SOURCE_OBJECT="$(tf_output source_object)"
TRIGGER_ID="$(tf_output trigger_id)"
SA_EMAIL="$(tf_output service_account_email)"
PUBSUB_FN="$(tf_output pubsub_function)"
STORAGE_FN="$(tf_output storage_function)"

# ── Pub/Sub: the topic and the push arm ──────────────────────────────────────
check "the topic exists with its labels" \
  "$ENDPOINT/v1/$TOPIC_ID" "\"name\":\"$TOPIC_ID\"" '"env":"compat-test"'
check "the push subscription points at the local sink" \
  "$ENDPOINT/v1/$SUBSCRIPTION_ID" \
  "\"name\":\"$SUBSCRIPTION_ID\"" "\"topic\":\"$TOPIC_ID\"" \
  "\"pushEndpoint\":\"$ECHO_SINK_URL/pubsub\"" '"ackDeadlineSeconds":10'

# ── Cloud Scheduler: the producer ────────────────────────────────────────────
JOB_URL="$ENDPOINT/v1/$JOB_ID"
check "the scheduler job targets the topic with the configured payload" \
  "$JOB_URL" "\"name\":\"$JOB_ID\"" '"schedule":"0 0 1 1 *"' '"timeZone":"UTC"' \
  '"state":"ENABLED"' "\"topicName\":\"$TOPIC_ID\"" "\"data\":\"$SCHEDULER_PAYLOAD\"" \
  '"source":"cloud-scheduler"'

# ── Cloud Storage: the event source ──────────────────────────────────────────
check "the bucket exists with its location and labels" \
  "$ENDPOINT/storage/v1/b/$BUCKET" "\"name\":\"$BUCKET\"" '"location":"US"' '"env":"compat-test"'
check "the functions' deployment archive is in the bucket" \
  "$ENDPOINT/storage/v1/b/$BUCKET/o/$SOURCE_OBJECT" \
  "\"name\":\"$SOURCE_OBJECT\"" '"bucket"'

# ── Eventarc: the standalone GCS trigger ─────────────────────────────────────
check "the Eventarc trigger filters on the bucket and the finalize type" \
  "$ENDPOINT/v1/$TRIGGER_ID" \
  "\"name\":\"$TRIGGER_ID\"" '"attribute":"type"' \
  '"value":"google.cloud.storage.object.v1.finalized"' \
  '"attribute":"bucket"' "\"value\":\"$BUCKET\""
check "the Eventarc trigger delivers to the local sink as the created account" \
  "$ENDPOINT/v1/$TRIGGER_ID" \
  '"httpEndpoint":{' '"uri":"'"$ECHO_SINK_URL"'/eventarc' \
  "\"serviceAccount\":\"$SA_EMAIL\"" '"env":"compat-test"'

# ── Cloud Functions: both triggers and their provisioned Eventarc triggers ───
PUBSUB_FN_URL="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$PUBSUB_FN"
STORAGE_FN_URL="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/functions/$STORAGE_FN"
check "the Pub/Sub function is ACTIVE on the topic" \
  "$PUBSUB_FN_URL" "\"name\":\"projects/$PROJECT/locations/$REGION/functions/$PUBSUB_FN\"" \
  '"status":"ACTIVE"' '"runtime":"nodejs20"' \
  '"eventType":"google.pubsub.topic.publish"' "\"resource\":\"$TOPIC_ID\""
check "the storage function is ACTIVE on the bucket" \
  "$STORAGE_FN_URL" "\"name\":\"projects/$PROJECT/locations/$REGION/functions/$STORAGE_FN\"" \
  '"status":"ACTIVE"' \
  '"eventType":"google.storage.object.finalize"' \
  "\"resource\":\"projects/$PROJECT/buckets/$BUCKET\""

# Each function materialized a backing Eventarc trigger named functions-<id>
# (the emulator's deterministic FunctionTriggerID). The v1 read does not expose
# it — `trigger` is a v2 field, matching real GCP — so assert it through the
# Eventarc collection and capture the name for the post-destroy orphan check.
TRIGGERS_URL="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/triggers"
PUBSUB_FN_TRIGGER="projects/$PROJECT/locations/$REGION/triggers/functions-$PUBSUB_FN"
STORAGE_FN_TRIGGER="projects/$PROJECT/locations/$REGION/triggers/functions-$STORAGE_FN"
check "the Pub/Sub function materialized its backing Eventarc trigger" \
  "$TRIGGERS_URL" "\"name\":\"$PUBSUB_FN_TRIGGER\"" \
  "\"cloudFunction\":\"projects/$PROJECT/locations/$REGION/functions/$PUBSUB_FN\""
check "the storage function materialized its backing Eventarc trigger" \
  "$TRIGGERS_URL" "\"name\":\"$STORAGE_FN_TRIGGER\"" \
  "\"cloudFunction\":\"projects/$PROJECT/locations/$REGION/functions/$STORAGE_FN\""

# ── Delivery 1: scheduler -> Pub/Sub -> push sink + function ────────────────
echo "== delivery (scheduler -> topic -> push sink; topic -> function)"

# The job is ENABLED with a far-future cron. jobs.run is Cloud Scheduler's
# documented "force this job now" call and uses the same target dispatcher the
# cron engine does, so the producer->topic->consumer path is exercised without
# waiting for a wall-clock minute. It is also the only state jobs.run accepts
# (real GCP: 400 FAILED_PRECONDITION on a paused job), so a non-200 here is a
# gate failure with the API's own error, not a silent `set -e` abort.
sink_reset "$SINK_LOG"
run_code="$(curl -sS -o "$WORK/jobs-run.json" -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' \
  "$JOB_URL:run" 2>"$WORK/jobs-run.err")" || run_code="000"
if [[ "$run_code" != "200" ]]; then
  cat "$WORK/jobs-run.json" "$WORK/jobs-run.err" 2>/dev/null || true
  fail "jobs.run was refused (HTTP $run_code)"
fi

if wait_for_sink "$SINK_LOG" 1 20; then
  pass "jobs.run published the scheduler payload to the push subscription"
else
  fail "jobs.run delivered nothing to the push sink"
fi
if grep -q '"path":"/pubsub"' "$SINK_LOG" 2>/dev/null; then
  pass "the delivery arrived on the subscription's push endpoint path"
else
  fail "the delivery did not arrive on /pubsub"
fi
expect_body "the push body carries the scheduler payload and attributes" "$SINK_BODY" \
  "\"data\":\"$SCHEDULER_PAYLOAD\"" '"source":"cloud-scheduler"'
expect_body "the push body names the push subscription" "$SINK_BODY" \
  "\"subscription\":\"$SUBSCRIPTION_ID\""

# The engine ran (independent of the transport): lastAttemptTime is recorded.
wait_for_substring "the scheduler recorded the attempt" "$JOB_URL" '"lastAttemptTime"'
wait_for_delivery "the topic message invoked the Pub/Sub function" "$PUBSUB_FN"

# ── Delivery 2: GCS object write -> Eventarc httpEndpoint + function ────────
echo "== delivery (object write -> Eventarc httpEndpoint; object write -> function)"
sink_reset "$SINK_LOG"
OBJECT="eventarc-trigger-${SUFFIX}.txt"
curl -sf -o /dev/null -X POST -H "Content-Type: text/plain" \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  --data-binary "jaiscloud terraform eventarc object" \
  "$ENDPOINT/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$OBJECT"

if wait_for_sink "$SINK_LOG" 1 20; then
  pass "the object write reached the Eventarc httpEndpoint destination"
else
  fail "the object write delivered nothing to the Eventarc sink"
fi
if grep -q '"path":"/eventarc"' "$SINK_LOG" 2>/dev/null; then
  pass "the CloudEvent arrived on the trigger's destination path"
else
  fail "the CloudEvent did not arrive on /eventarc"
fi
if grep -q '"Ce-Type":\["google.cloud.storage.object.v1.finalized"\]' "$SINK_LOG" 2>/dev/null; then
  pass "the CloudEvent type is the finalized filter the trigger declares"
else
  fail "the CloudEvent type header is not the finalized type"
fi
if grep -q "\"Ce-Source\":\\[\"//storage.googleapis.com/projects/_/buckets/$BUCKET\"\\]" "$SINK_LOG" 2>/dev/null; then
  pass "the CloudEvent source names the bucket"
else
  fail "the CloudEvent source does not name the bucket"
fi
expect_body "the CloudEvent body carries the written object's metadata" "$SINK_BODY" \
  "\"name\":\"$OBJECT\"" "\"bucket\":\"$BUCKET\""
wait_for_delivery "the object write invoked the storage function" "$STORAGE_FN"

plan_no_changes "re-plan of the event-driven stack reports no changes"

destroy_phase

echo "== post-destroy (no orphan topic, subscription, job, trigger or function)"
check_status "the topic is gone" "$ENDPOINT/v1/$TOPIC_ID" 404
check_status "the push subscription is gone" "$ENDPOINT/v1/$SUBSCRIPTION_ID" 404
check_status "the scheduler job is gone" "$JOB_URL" 404
check_status "the Eventarc trigger is gone" "$ENDPOINT/v1/$TRIGGER_ID" 404
check_status "the Pub/Sub function is gone" "$PUBSUB_FN_URL" 404
check_status "the storage function is gone" "$STORAGE_FN_URL" 404
check_status "the bucket is gone" "$ENDPOINT/storage/v1/b/$BUCKET" 404
check_status "the Pub/Sub function's backing trigger is gone" \
  "$ENDPOINT/v1/$PUBSUB_FN_TRIGGER" 404
check_status "the storage function's backing trigger is gone" \
  "$ENDPOINT/v1/$STORAGE_FN_TRIGGER" 404

# List-level orphan checks: a deleted name must not linger in the collection.
check_absent "no topic orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/topics" "$TOPIC_ID"
check_absent "no subscription orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/subscriptions" "$SUBSCRIPTION_ID"
check_absent "no scheduler job orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/jobs" "$JOB_ID"
check_absent "no Eventarc trigger orphan remains" \
  "$TRIGGERS_URL" "$TRIGGER_ID"
check_absent "no function-backed trigger orphan remains" \
  "$TRIGGERS_URL" "functions-$PUBSUB_FN"
check_absent "no storage-function backing trigger orphan remains" \
  "$TRIGGERS_URL" "functions-$STORAGE_FN"

summary
