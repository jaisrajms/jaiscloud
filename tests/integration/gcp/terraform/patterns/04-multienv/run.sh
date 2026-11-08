#!/usr/bin/env bash
#
# Composition pattern 04 — multi-environment (TF4-4).
#
# What: the same small stack (a bucket pair, a topic, a secret with two versions
#       and an audit topic in a shared project) applied once per Terraform
#       workspace — `staging` then `prod` — with `for_each`/`count` building the
#       per-environment resources and an aliased provider routing the shared
#       resource to another project.
#
# Contract (patterns/lib.sh): init -> apply (staging) -> asserts -> plan == 0 ->
# apply (prod) -> asserts -> plan == 0 -> destroy staging -> isolation asserts
# (prod still readable, its plan still clean) -> destroy prod -> post-destroy
# asserts.
#
# What it proves that the other patterns do not: workspaces are isolated states
# over one configuration. Destroying `staging` must leave every `prod` resource
# readable and `prod`'s plan clean; the aliased provider must put the audit topic
# in `<project>-shared`, and the server's read-back — not the Terraform output
# alone — is what proves the resource landed there.
#
# Usage:
#   tests/integration/gcp/terraform/patterns/04-multienv/run.sh [ENDPOINT] [PROJECT]
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-pattern PATTERN=04-multienv

set -euo pipefail

PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_NAME="04-multienv"
# shellcheck source=../lib.sh
source "$PATTERN_DIR/../lib.sh"

# Run-unique names: the emulator keeps state across patterns in one aggregate
# run, so a fixed name would collide on a second run against the same emulator.
SUFFIX="${JC_TF_MULTIENV_SUFFIX:-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')}"
TF_VARS+=(-var "name_suffix=$SUFFIX")

# The project the `shared` provider alias targets (main.tf derives it the same
# way). The emulator is project-scoped and does not require the project row to
# exist first, which is what the other patterns' `var.project` relies on too.
SHARED_PROJECT="$PROJECT-shared"

# assert_version_payloads ENV SECRET_ID — both secret versions must hold one of
# the two revisions (in any order; they are independent resources).
assert_version_payloads() {
  local env="$1" secret_id="$2" both p ok=1
  both="$(
    curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$ENDPOINT/v1/$secret_id/versions/1:access" 2>/dev/null || true
    curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$ENDPOINT/v1/$secret_id/versions/2:access" 2>/dev/null || true
  )"
  for p in "$(printf 'env=%s revision=1' "$env" | base64 | tr -d '\n')" \
           "$(printf 'env=%s revision=2' "$env" | base64 | tr -d '\n')"; do
    [[ "$both" == *"\"payload\":{\"data\":\"$p\""* ]] || ok=0
  done
  if [[ "$ok" == "1" ]]; then
    pass "the two $env secret versions carry both revisions"
  else
    fail "the $env secret versions do not carry both revisions"
  fi
}

# assert_env ENV applies the read-backs for the selected workspace and records
# the names the isolation phase checks after the other environment is gone.
assert_env() {
  local env="$1" got_env got_bi got_bo got_topic got_audit got_secret got_versions
  local want_bi="jc-tf-multienv-$env-input$SUFFIX"
  local want_bo="jc-tf-multienv-$env-output$SUFFIX"
  local want_topic="jc-tf-multienv-$env-events$SUFFIX"
  local want_audit="jc-tf-multienv-$env-audit$SUFFIX"
  local want_secret="jc-tf-multienv-$env-config$SUFFIX"
  got_env="$(tf_output env)"
  got_bi="$(tf_output bucket_input)"
  got_bo="$(tf_output bucket_output)"
  got_topic="$(tf_output topic)"
  got_audit="$(tf_output audit_topic)"
  got_secret="$(tf_output secret)"
  got_versions="$(tf_output secret_version_count)"

  [[ "$got_env" == "$env" ]] &&
    pass "the $env workspace's module sees terraform.workspace = $env" ||
    fail "the $env workspace's module sees '$got_env', want '$env'"
  [[ "$got_bi" == "$want_bi" && "$got_bo" == "$want_bo" ]] &&
    pass "the for_each bucket pair is named for $env" ||
    fail "the $env bucket pair is '$got_bi'/'$got_bo', want '$want_bi'/'$want_bo'"
  [[ "$got_versions" == "2" ]] &&
    pass "the count-built secret has two versions in $env" ||
    fail "the $env secret has '$got_versions' versions, want 2"

  # ── the environment's own resources (default provider's project) ──────────
  check "the $env input bucket exists with its env/role labels" \
    "$ENDPOINT/storage/v1/b/$got_bi" "\"name\":\"$got_bi\"" \
    "\"env\":\"$env\"" '"role":"input"'
  check "the $env output bucket exists with its env/role labels" \
    "$ENDPOINT/storage/v1/b/$got_bo" "\"name\":\"$got_bo\"" \
    "\"env\":\"$env\"" '"role":"output"'
  check "the $env topic exists with its env label" \
    "$ENDPOINT/v1/$got_topic" "\"name\":\"projects/$PROJECT/topics/$want_topic\"" \
    "\"env\":\"$env\""
  check "the $env secret exists with its env label" \
    "$ENDPOINT/v1/$got_secret" "\"name\":\"projects/$PROJECT/secrets/$want_secret\"" \
    "\"env\":\"$env\""
  check "both $env secret versions are listed" \
    "$ENDPOINT/v1/$got_secret/versions" \
    "\"name\":\"projects/$PROJECT/secrets/$want_secret/versions/1\"" \
    "\"name\":\"projects/$PROJECT/secrets/$want_secret/versions/2\""
  # The two versions are independent resources, so creation order (and thus
  # which number holds which payload) is not deterministic. Assert both payloads
  # exist across the pair, not that version 2 is revision 2.
  assert_version_payloads "$env" "$got_secret"
  # SecretVersion.get carries no payload — real Secret Manager only populates it
  # in the versions.access response, and the emulator matches. The GET above
  # (versions.list) and this absence check pin that split.
  check_absent "a $env version read omits the payload (access carries it)" \
    "$ENDPOINT/v1/$got_secret/versions/1" '"payload"'

  # ── the shared resource (aliased provider = <project>-shared) ─────────────
  [[ "$got_audit" == "projects/$SHARED_PROJECT/topics/$want_audit" ]] &&
    pass "the audit topic is named in the shared project by the alias" ||
    fail "the audit topic is '$got_audit', want 'projects/$SHARED_PROJECT/topics/$want_audit'"
  check "the $env audit topic is readable in the shared project" \
    "$ENDPOINT/v1/$got_audit" "\"name\":\"projects/$SHARED_PROJECT/topics/$want_audit\"" \
    '"scope":"shared"'
  # The alias must be the reason it landed there: the default project has no
  # topic of that name.
  check_status "the $env audit topic is not in the default project" \
    "$ENDPOINT/v1/projects/$PROJECT/topics/$want_audit" 404

  ENV_BUCKET_IN="$got_bi"
  ENV_BUCKET_OUT="$got_bo"
  ENV_TOPIC="$got_topic"
  ENV_AUDIT="$got_audit"
  ENV_SECRET="$got_secret"
}

init_phase

echo "== apply staging (workspace 1 of 2)"
workspace_select staging
apply_phase "apply (staging)"
assert_no_api_errors
assert_env staging
STAGING_BUCKET_IN="$ENV_BUCKET_IN"
STAGING_BUCKET_OUT="$ENV_BUCKET_OUT"
STAGING_TOPIC="$ENV_TOPIC"
STAGING_AUDIT="$ENV_AUDIT"
STAGING_SECRET="$ENV_SECRET"
plan_no_changes "re-plan of staging reports no changes"

echo "== apply prod (workspace 2 of 2)"
workspace_select prod
apply_phase "apply (prod)"
assert_no_api_errors
assert_env prod
PROD_BUCKET_IN="$ENV_BUCKET_IN"
PROD_BUCKET_OUT="$ENV_BUCKET_OUT"
PROD_TOPIC="$ENV_TOPIC"
PROD_AUDIT="$ENV_AUDIT"
PROD_SECRET="$ENV_SECRET"
plan_no_changes "re-plan of prod reports no changes"

# Both environments are live at the same time, from one configuration.
echo "== both environments co-exist"
[[ "$STAGING_BUCKET_IN" != "$PROD_BUCKET_IN" ]] &&
  pass "the two workspaces materialized differently named stacks" ||
  fail "the two workspaces share a bucket name ('$STAGING_BUCKET_IN')"
check "staging is still readable while prod exists" \
  "$ENDPOINT/storage/v1/b/$STAGING_BUCKET_IN" "\"env\":\"staging\""
check "prod is readable alongside staging" \
  "$ENDPOINT/storage/v1/b/$PROD_BUCKET_IN" "\"env\":\"prod\""

echo "== isolation: destroy staging"
destroy_workspace staging
check_status "staging's input bucket is gone" "$ENDPOINT/storage/v1/b/$STAGING_BUCKET_IN" 404
check_status "staging's output bucket is gone" "$ENDPOINT/storage/v1/b/$STAGING_BUCKET_OUT" 404
check_status "staging's topic is gone" "$ENDPOINT/v1/$STAGING_TOPIC" 404
check_status "staging's audit topic is gone" "$ENDPOINT/v1/$STAGING_AUDIT" 404
check_status "staging's secret is gone" "$ENDPOINT/v1/$STAGING_SECRET" 404

# Destroying one workspace must not touch the other's state or its resources.
echo "== prod survives staging's destroy"
check "prod's input bucket is still readable" \
  "$ENDPOINT/storage/v1/b/$PROD_BUCKET_IN" "\"name\":\"$PROD_BUCKET_IN\"" "\"env\":\"prod\""
check "prod's output bucket is still readable" \
  "$ENDPOINT/storage/v1/b/$PROD_BUCKET_OUT" "\"name\":\"$PROD_BUCKET_OUT\"" "\"env\":\"prod\""
check "prod's topic is still readable" \
  "$ENDPOINT/v1/$PROD_TOPIC" "\"env\":\"prod\""
check "prod's audit topic is still readable in the shared project" \
  "$ENDPOINT/v1/$PROD_AUDIT" "\"env\":\"prod\""
check "prod's secret still lists both versions" \
  "$ENDPOINT/v1/$PROD_SECRET/versions" "$PROD_SECRET/versions/1" "$PROD_SECRET/versions/2"
workspace_select prod
plan_no_changes "re-plan of prod after staging's destroy reports no changes"

echo "== destroy prod"
destroy_workspace prod

echo "== post-destroy (no orphan in either project)"
check_status "prod's input bucket is gone" "$ENDPOINT/storage/v1/b/$PROD_BUCKET_IN" 404
check_status "prod's output bucket is gone" "$ENDPOINT/storage/v1/b/$PROD_BUCKET_OUT" 404
check_status "prod's topic is gone" "$ENDPOINT/v1/$PROD_TOPIC" 404
check_status "prod's audit topic is gone" "$ENDPOINT/v1/$PROD_AUDIT" 404
check_status "prod's secret is gone" "$ENDPOINT/v1/$PROD_SECRET" 404

# List-level orphan checks: a deleted name must not linger in either project's
# collection.
check_absent "no staging topic orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/topics" "$STAGING_TOPIC"
check_absent "no prod topic orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/topics" "$PROD_TOPIC"
check_absent "no staging audit orphan remains" \
  "$ENDPOINT/v1/projects/$SHARED_PROJECT/topics" "$STAGING_AUDIT"
check_absent "no prod audit orphan remains" \
  "$ENDPOINT/v1/projects/$SHARED_PROJECT/topics" "$PROD_AUDIT"
check_absent "no staging secret orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets" "$STAGING_SECRET"
check_absent "no prod secret orphan remains" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets" "$PROD_SECRET"

summary
