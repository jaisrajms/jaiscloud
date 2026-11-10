# shellcheck shell=bash
#
# Case: storage — the lifecycle paths a Terraform user performs on buckets.
#
# The fixture and the terraform-google-cloud-storage module case prove create →
# read-back → update → destroy. This case proves the *state* operations instead:
#
#   apply -> import (clean plan) -> refresh-only -> -replace (new timeCreated)
#   -> ForceNew rename (old bucket 404) -> out-of-band label drift (plan shows
#   exactly the drift, apply converges) -> label import artifact (classified)
#   -> negative paths (409/400 surfaced by the provider, no panic/5xx) -> destroy
#
# Two buckets keep the import gate strict: `strict` has no labels, so the
# provider's import-time label reconstruction cannot mask a real read-shape gap,
# and `drift` carries labels so the out-of-band PATCH is inside Terraform's
# managed fields. `add_terraform_attribution_label = false` removes the
# provider's `goog-terraform-provisioned` label, which is a provider-side
# artifact of every create and would otherwise appear in the import plan.
#
# Classified, not worked around:
#   - google_storage_bucket import with labels: tpgresource.SetLabels filters the
#     API labels against the labels already in state, and an import starts from
#     an empty map, so the next plan reconstructs labels/terraform_labels. The
#     gate accepts exactly those two attributes and nothing else (the
#     `label-artifact` mode); every other attribute must be unchanged.
#   - TF5-1 (fixed): real GCS rejects an invalid storageClass, location or
#     iamConfiguration.publicAccessPrevention on buckets.insert with 400; the
#     emulator now validates all three against their documented sets, so the
#     negative block asserts the 400 plus a valid control.

STRICT_BUCKET="lc-storage-strict"
DRIFT_BUCKET="lc-storage-drift"
RENAMED_BUCKET="lc-storage-renamed"

PROVIDER_HCL='provider "google" {
  project = var.project
  region  = "us-central1"

  user_project_override = false

  # The attribution label is provider-side state, not an emulator field; turning
  # it off keeps the import plan an exact read-shape assertion.
  add_terraform_attribution_label = false

  storage_custom_endpoint = "${var.endpoint}/storage/v1/"
}'

# write_main <strict-bucket-name> generates the root module. The `strict`
# bucket is the import/replace/rename target and deliberately declares no
# labels; `drift` declares a label so an out-of-band PATCH is visible.
write_main() {
  local bucket="$1"
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

resource "google_storage_bucket" "strict" {
  name     = "$bucket"
  location = "US"

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket" "drift" {
  name     = "$DRIFT_BUCKET"
  location = "US"

  labels = {
    env = "lifecycle"
  }
}
EOF
}

# bucket_field prints one string field from a bucket GET.
bucket_field() {
  curl -s -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/storage/v1/b/$1" 2>/dev/null |
    sed -n "s/.*\"$2\":\"\\([^\"]*\\)\".*/\\1/p" | head -1
}

case_main() {
  write_main "$STRICT_BUCKET"
  init_phase
  plan_phase "plan (import bucket + drift bucket)"
  apply_phase
  assert_no_api_errors "$LAST_APPLY_LOG" "apply"

  echo "== spot checks (create)"
  check "strict bucket read-back" "$ENDPOINT/storage/v1/b/$STRICT_BUCKET" \
    '"location":"US"' \
    '"locationType":"multi-region"' \
    '"storageClass":"STANDARD"' \
    '"uniformBucketLevelAccess":{"enabled":true}' \
    '"enabled":true' \
    '"age":30'
  check "drift bucket read-back" "$ENDPOINT/storage/v1/b/$DRIFT_BUCKET" \
    '"env":"lifecycle"'
  plan_no_changes "re-plan after create reports no changes"

  echo "== refresh"
  # google_storage_bucket normalizes an absent labels map to {} on read (the
  # strict bucket has no managed labels), which a refresh-only plan reports;
  # every other attribute must be unchanged. The drift bucket below has labels
  # and is part of the same refresh, so a real read-shape gap still fails.
  refresh_phase labels-artifact "refresh-only plan reports no drift (empty-labels normalization classified)"

  echo "== import"
  # Import feeds the emulator's GET back into state; the clean plan proves every
  # served field (versioning, UBLA, storage class, lifecycle rule, location) is
  # read back in the shape the provider stores.
  import_phase google_storage_bucket.strict "$STRICT_BUCKET"

  echo "== replace (-replace forces destroy+create)"
  local before after
  before="$(bucket_field "$STRICT_BUCKET" timeCreated)"
  replace_phase google_storage_bucket.strict "replace the bucket"
  after="$(bucket_field "$STRICT_BUCKET" timeCreated)"
  if [[ -n "$after" && "$before" != "$after" ]]; then
    pass "replacement minted a new timeCreated ($before -> $after)"
  else
    fail "replacement reused timeCreated=$after (want a new object)"
  fi
  check_status "replaced bucket is readable" "$ENDPOINT/storage/v1/b/$STRICT_BUCKET" 200
  check "replacement reset metageneration" "$ENDPOINT/storage/v1/b/$STRICT_BUCKET" '"metageneration":"1"'

  echo "== ForceNew rename (old object must be gone)"
  write_main "$RENAMED_BUCKET"
  apply_phase "apply (bucket name is ForceNew)"
  check_status "the old bucket is gone" "$ENDPOINT/storage/v1/b/$STRICT_BUCKET" 404
  check_status "the renamed bucket exists" "$ENDPOINT/storage/v1/b/$RENAMED_BUCKET" 200
  plan_no_changes "re-plan after the rename reports no changes"

  echo "== out-of-band drift"
  out_of_band "PATCH the drift bucket's labels outside Terraform" PATCH \
    "$ENDPOINT/storage/v1/b/$DRIFT_BUCKET" \
    '{"labels":{"env":"drifted","drift":"yes"}}'
  # The managed key is detected and converged. An unmanaged extra key is
  # filtered out of the `labels` field by the provider's tpgresource.SetLabels
  # (it only keeps keys already in state), so it is neither planned nor removed;
  # the provider's update sends the refreshed effective map back, which real GCS
  # also merges. That is a provider behaviour, recorded here rather than
  # asserted as drift.
  plan_expect_diff "plan shows the managed out-of-band label" '"drifted"'
  apply_phase "apply (converge the drift)"
  plan_no_changes "re-plan after drift convergence reports no changes"
  check "drift bucket's managed label converged" \
    "$ENDPOINT/storage/v1/b/$DRIFT_BUCKET" '"env":"lifecycle"'
  check "the unmanaged label key is preserved (SetLabels filters it)" \
    "$ENDPOINT/storage/v1/b/$DRIFT_BUCKET" '"drift":"yes"'

  echo "== import with labels (provider artifact, classified)"
  import_phase google_storage_bucket.drift "$DRIFT_BUCKET" label-artifact
  apply_phase "apply (reconstruct the labels after import)"
  plan_no_changes "re-plan after the label reconstruction reports no changes"

  echo "== negative paths"
  expect_http_error "duplicate bucket create" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"'"$RENAMED_BUCKET"'"}' 409
  expect_http_error "invalid bucket name" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"LC-Bad_Name"}' 400
  # TF5-1: real GCS validates the storage class enum, the location and the
  # public-access-prevention mode on create; each invalid value is a 400 and the
  # bucket is not created.
  expect_http_error "invalid storageClass" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"lc-storage-enum-probe","storageClass":"BOGUS"}' 400
  expect_http_error "invalid location" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"lc-storage-loc-probe","location":"NOT-A-LOCATION"}' 400
  expect_http_error "invalid publicAccessPrevention" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"lc-storage-pap-probe","iamConfiguration":{"publicAccessPrevention":"BOGUS"}}' 400
  # The valid control: the documented values still create (HTTP 200) and read
  # back canonicalised, so the validation did not narrow a legal request.
  out_of_band "valid storageClass/location/publicAccessPrevention insert" POST \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" \
    '{"name":"lc-storage-valid-probe","location":"eu","storageClass":"NEARLINE","iamConfiguration":{"publicAccessPrevention":"enforced"}}'
  check "valid insert read-back" "$ENDPOINT/storage/v1/b/lc-storage-valid-probe" \
    '"location":"EU"' '"locationType":"multi-region"' \
    '"storageClass":"NEARLINE"' '"publicAccessPrevention":"enforced"'
  curl -s -o /dev/null -X DELETE -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/storage/v1/b/lc-storage-valid-probe" 2>/dev/null || true

  # The provider-facing half of the duplicate path: pre-create the bucket
  # out-of-band, then let a real `terraform apply` hit the 409 and surface it.
  curl -s -o /dev/null -X POST -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    -H 'Content-Type: application/json' -d '{"name":"lc-storage-dup","location":"US"}' \
    "$ENDPOINT/storage/v1/b?project=$PROJECT" 2>/dev/null || true
  provider_error_create "duplicate bucket create surfaces through the provider" \
    "$PROVIDER_HCL" \
    'resource "google_storage_bucket" "dup" {
  name     = "lc-storage-dup"
  location = "US"
}' \
    "409"
  curl -s -o /dev/null -X DELETE -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/storage/v1/b/lc-storage-dup" 2>/dev/null || true

  destroy_phase
  check_status "destroy leaves the remaining bucket 404" \
    "$ENDPOINT/storage/v1/b/$RENAMED_BUCKET" 404
  check_status "destroy leaves the drift bucket 404" \
    "$ENDPOINT/storage/v1/b/$DRIFT_BUCKET" 404
}
