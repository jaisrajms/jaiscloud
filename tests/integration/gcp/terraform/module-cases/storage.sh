# shellcheck shell=bash
#
# Case: terraform-google-cloud-storage
#
# The pinned consumer is terraform-google-modules/terraform-google-cloud-storage
# (module v12.4.1). It emits `autoclass` and `hierarchical_namespace` on
# *every* bucket (both with enabled=false) and `website`, `logging`,
# `custom_placement_config` and `ip_filter` when the caller configures them.
# Before TGM1 the emulator's bucketMeta did not store any of them, so GET
# omitted them: `autoclass{enabled=true}`, `website`, `logging` and `ip_filter`
# permadiffed, while `hierarchical_namespace` and `custom_placement_config`
# (both ForceNew) planned a bucket replacement.
#
# The gate proves the whole lifecycle converges:
#
#   create (module defaults + labels/UBLA/PAP + website/logging/ip_filter,
#           plus an HNS + custom-placement bucket) -> assert GET -> No changes
#   mutate (autoclass on, labels/website/logging changed) -> apply -> No changes
#   clear  (labels empty, logging removed, autoclass off, ip_filter Disabled)
#          -> apply -> No changes
#
# Two more consumer paths hang off the same root module, present in every phase
# so the second plan must stay clean:
#
#   - `modules/simple_bucket` (TGM7) declares
#     `data "google_storage_project_service_account"` unconditionally, so
#     planning it forces projects.serviceAccount.get; it also creates its own
#     bucket (versioning on by default).
#   - `set_hmac_access` + `hmac_service_accounts` make the module create a
#     `google_storage_hmac_key` (TGM8), whose lifecycle is create -> get (the
#     provider's post-create read) -> deactivate/delete on destroy. The map is
#     `map(string)`, so the key (the service-account email) must be known at
#     plan time: it is hardcoded, not `google_service_account.hmac.email`.
#
# `website` is not removed in the clear phase: hashicorp/google 7.46.2 gives it
# no NullFields clearing path (unlike logging), so removing the block would
# send a PATCH with no website field at all and real GCS would keep the value
# too. That is a provider limitation, not an emulator gap.

MODULE_REPO="terraform-google-cloud-storage"
MODULE_COMMIT="c7effc0be0f4"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-cloud-storage"

# The module appends the bucket name to `prefix`; the variables' per-bucket maps
# are keyed by the unprefixed name.
PFX="jc"
BUCKET="$PFX-module-bucket"
LOG_BUCKET="$PFX-logs"
DUAL="$PFX-dual-region"

# write_module generates the root module for one gate phase:
#   create | mutate | clear
write_module() {
  local phase="$1"
  local labels website logging autoclass ip_filter

  case "$phase" in
    create)
      labels='{
    env   = "tf-module"
    owner = "jaiscloud"
  }'
      website='{ main_page_suffix = "index.html", not_found_page = "404.html" }'
      logging="{
    \"module-bucket\" = { log_bucket = \"$LOG_BUCKET\", log_object_prefix = \"access-logs/\" }
  }"
      autoclass='{ "module-bucket" = false }'
      ip_filter="{
    \"module-bucket\" = {
      mode                           = \"Enabled\"
      allow_cross_org_vpcs           = true
      allow_all_service_agent_access = true
      public_network_source = {
        allowed_ip_cidr_ranges = [\"203.0.113.0/24\"]
      }
      vpc_network_sources = [
        {
          network                = \"projects/PLACEHOLDER/global/networks/default\"
          allowed_ip_cidr_ranges = [\"10.0.0.0/8\"]
        },
      ]
    }
  }"
      ip_filter="${ip_filter/PLACEHOLDER/$PROJECT}"
      ;;
    mutate)
      labels='{
    env = "tf-module-updated"
  }'
      website='{ main_page_suffix = "home.html", not_found_page = "missing.html" }'
      logging="{
    \"module-bucket\" = { log_bucket = \"$LOG_BUCKET\", log_object_prefix = \"logs-v2/\" }
  }"
      autoclass='{ "module-bucket" = true }'
      # unchanged in this phase; a PATCH that does not carry ipFilter must not
      # disturb it.
      ip_filter="{
    \"module-bucket\" = {
      mode                           = \"Enabled\"
      allow_cross_org_vpcs           = true
      allow_all_service_agent_access = true
      public_network_source = {
        allowed_ip_cidr_ranges = [\"203.0.113.0/24\"]
      }
      vpc_network_sources = [
        {
          network                = \"projects/PLACEHOLDER/global/networks/default\"
          allowed_ip_cidr_ranges = [\"10.0.0.0/8\"]
        },
      ]
    }
  }"
      ip_filter="${ip_filter/PLACEHOLDER/$PROJECT}"
      ;;
    clear)
      labels='{}'
      website='{ main_page_suffix = "home.html", not_found_page = "missing.html" }'
      logging='{}'
      autoclass='{ "module-bucket" = false }'
      # The provider's DiffSuppressFunc treats mode="Disabled" as "removed"; it
      # has no NullFields path for ip_filter, so a disabled block is how it
      # clears the filter.
      ip_filter='{
    "module-bucket" = { mode = "Disabled" }
  }'
      ;;
    *)
      echo "ERROR: unknown phase '$phase'" >&2
      exit 2
      ;;
  esac

  cat >"$WORK/main.tf" <<EOF
terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 7.46.2"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
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

  storage_custom_endpoint = "\${var.endpoint}/storage/v1/"
  # The HMAC key's owner is a google_service_account, so the IAM Admin surface
  # must also point at the emulator. The resource is shared with the beta
  # provider, which reads iam_beta_custom_endpoint (same split as run.sh's
  # fixture provider block).
  iam_custom_endpoint      = "\${var.endpoint}/"
  iam_beta_custom_endpoint = "\${var.endpoint}/v1/"
}

module "storage" {
  source = "$MODULE_DIR"

  project_id = var.project
  prefix     = "$PFX"
  location   = "NAM4"

  # module-bucket carries the optional blocks; logs is the logging destination
  # (real GCS requires it in the same project/location); dual-region is created
  # with the two ForceNew blocks set.
  names = ["module-bucket", "logs", "dual-region"]

  labels = $labels

  public_access_prevention = "enforced"

  autoclass        = $autoclass
  website          = $website
  logging          = $logging
  ip_filter        = $ip_filter

  hierarchical_namespace = { "dual-region" = true }
  custom_placement_config = {
    "dual-region" = { data_locations = ["US-CENTRAL1", "US-EAST1"] }
  }

  # TGM8: one google_storage_hmac_key per entry. hmac_service_accounts is
  # map(string), so the email must be a plan-time literal — referencing the
  # computed google_service_account.hmac.email would fail the plan. depends_on
  # orders the service account ahead of the keys it owns.
  set_hmac_access = true
  hmac_service_accounts = {
    "jc-hmac@\${var.project}.iam.gserviceaccount.com" = "ACTIVE"
  }

  depends_on = [google_service_account.hmac]
}

# TGM7: the simple_bucket submodule reads
# data "google_storage_project_service_account" unconditionally, so a plan of
# this module is impossible without projects.serviceAccount.get.
module "simple" {
  source = "$MODULE_DIR/modules/simple_bucket"

  name       = "jc-simple-bucket"
  project_id = var.project
  location   = "US"
}

# TGM8: the HMAC key owner. The emulator does not validate that the account
# exists, but it is declared so the module's depends_on has a real dependency
# and the key is associated with an account the apply actually created.
resource "google_service_account" "hmac" {
  account_id   = "jc-hmac"
  display_name = "jaiscloud terraform hmac key owner"
}

output "bucket" {
  value = module.storage.names
}
EOF
}

# bucket_body fetches a bucket through the emulator's REST API.
bucket_body() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/storage/v1/b/$1" 2>/dev/null || true
}

# check_absent is the negative check: a cleared field must not be rendered.
check_absent() {
  local name="$1" bucket="$2"
  shift 2
  local body want
  body="$(bucket_body "$bucket")"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      fail "$name (unexpected: $want)"
      return 0
    fi
  done
  pass "$name"
}

case_main() {
  write_module create
  init_phase
  plan_phase "plan (bucket with module defaults + optional blocks)"
  apply_phase "apply (create)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check "bucket carries labels, UBLA and PAP" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"env":"tf-module"' '"owner":"jaiscloud"' '"name":"'"$BUCKET"'"' \
    '"uniformBucketLevelAccess":{"enabled":true}' '"publicAccessPrevention":"enforced"'
  # hashicorp/google 7.46.2 drops an `autoclass { enabled = false }` block on
  # create: the block's Optional+Computed terminal_storage_class makes GetOk
  # see the whole block as its zero value. Nothing is sent, so there is nothing
  # to read back — and the emulator must not synthesize it either. The schema's
  # DiffSuppressFunc keeps the plan clean; the mutate phase proves the field
  # round-trips once it is actually sent.
  check_absent "create does not fabricate an unsent autoclass block" "$BUCKET" '"autoclass":'
  check "hierarchicalNamespace { enabled = false } reads back" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"hierarchicalNamespace":{"enabled":false}'
  check "website block reads back" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"website":{"mainPageSuffix":"index.html","notFoundPage":"404.html"}'
  check "logging block reads back" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"logging":{"logBucket":"'"$LOG_BUCKET"'","logObjectPrefix":"access-logs/"}'
  check "ipFilter block reads back" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"mode":"Enabled"' '"allowCrossOrgVpcs":true' '"allowAllServiceAgentAccess":true' \
    '"allowedIpCidrRanges":["203.0.113.0/24"]' \
    '"network":"projects/'"$PROJECT"'/global/networks/default"' \
    '"allowedIpCidrRanges":["10.0.0.0/8"]'
  check "ForceNew blocks read back (no replacement planned)" \
    "$ENDPOINT/storage/v1/b/$DUAL" \
    '"hierarchicalNamespace":{"enabled":true}' \
    '"customPlacementConfig":{' '"US-CENTRAL1"' '"US-EAST1"'
  # TGM7: the simple_bucket module's unconditional data source read.
  check "projects.serviceAccount.get is served (simple_bucket data source)" \
    "$ENDPOINT/storage/v1/projects/$PROJECT/serviceAccount" \
    '"kind":"storage#serviceAccount"' 'gs-project-accounts.iam.gserviceaccount.com'
  check "simple_bucket submodule created its bucket" \
    "$ENDPOINT/storage/v1/b/jc-simple-bucket" '"name":"jc-simple-bucket"'
  # TGM8: the module owns one google_storage_hmac_key for the account above.
  check "HMAC key is ACTIVE for the service account" \
    "$ENDPOINT/storage/v1/projects/$PROJECT/hmacKeys" \
    '"kind":"storage#hmacKeysMetadata"' \
    '"serviceAccountEmail":"jc-hmac@'"$PROJECT"'.iam.gserviceaccount.com"' \
    '"state":"ACTIVE"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  write_module mutate
  apply_phase "apply (mutate autoclass/labels/website/logging)"
  assert_no_api_errors
  echo "== spot checks (mutate)"
  check "autoclass flips to enabled=true" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"autoclass":{"enabled":true}'
  check "labels and website changed" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"env":"tf-module-updated"' \
    '"website":{"mainPageSuffix":"home.html","notFoundPage":"missing.html"}'
  check "logging prefix changed" \
    "$ENDPOINT/storage/v1/b/$BUCKET" \
    '"logging":{"logBucket":"'"$LOG_BUCKET"'","logObjectPrefix":"logs-v2/"}'
  check "untouched ipFilter survived the PATCH" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"mode":"Enabled"' '"allowCrossOrgVpcs":true'
  plan_no_changes "re-plan after mutation reports no changes"

  write_module clear
  apply_phase "apply (clear labels/logging/autoclass/ipFilter)"
  assert_no_api_errors
  echo "== spot checks (clear)"
  check "module keeps its own name label only" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"name":"'"$BUCKET"'"'
  check_absent "caller labels cleared" "$BUCKET" '"env":' '"owner":'
  check "autoclass back to enabled=false" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"autoclass":{"enabled":false}'
  check_absent "logging block cleared" "$BUCKET" '"logging":'
  # The provider expands a Disabled filter to the full block (empty ranges,
  # false flags) rather than sending just the mode, so assert the mode and the
  # absence of the previously allowed range instead of an exact object.
  check "ipFilter disabled" \
    "$ENDPOINT/storage/v1/b/$BUCKET" '"mode":"Disabled"'
  check_absent "ipFilter cleared its allowed ranges" "$BUCKET" '"203.0.113.0/24"' '"10.0.0.0/8"'
  plan_no_changes "re-plan after clear reports no changes"

  destroy_phase
}
