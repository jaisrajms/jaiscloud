# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-bigquery (TF3-2)
#
# The pinned consumer is terraform-google-modules/terraform-google-bigquery
# (commit e11db0d), the CFT BigQuery blueprint. Its root module creates the
# served control plane:
#
#   google_bigquery_dataset   -> datasets.insert / datasets.get / datasets.update
#   google_bigquery_table     -> tables.insert / tables.get / tables.update
#
# and its `access` list drives the dataset ACL through datasets.update (the
# same PUT the provider uses for every other dataset field). The gate proves the
# module's lifecycle converges on that surface:
#
#   apply (dataset + table + ACL) -> REST asserts
#   plan -detailed-exitcode -> No changes
#   mutate (new schema column, dataset/table label, description, ACL entry)
#     -> apply -> REST asserts -> No changes
#   destroy -> dataset and table GET are 404
#
# Pruned inputs, so nothing here reaches an unserved resource (documented, not
# silent):
#
#   * dataset IAM (`google_bigquery_dataset_iam_*` / `google_bigquery_table_iam_*`)
#     is not served: datasets.getIamPolicy/setIamPolicy and
#     tables.getIamPolicy/setIamPolicy are not in the operation registry
#     (TF1-1, scheduled as fix/gcp-bigquery-resource-iam). The case exercises
#     the served ACL path through the module's `access` input instead.
#   * `views`, `materialized_views` and `external_tables` stay empty: they are
#     served `google_bigquery_table` variants, but the minimal case needs only
#     the plain table, and an external table would pull in a GCS source object.
#   * `routines` stays empty (routines.* is served and covered by the fixture's
#     google_bigquery_routine; this case keeps the module on dataset + table).
#   * `encryption_key` stays null (CMEK via KMS is served, but KMS is not the
#     subject of this case); `resource_tags`, `default_partition_expiration_ms`
#     and `storage_billing_model` stay at their defaults.
#   * `deletion_protection` is the module default (false) and
#     `delete_contents_on_destroy = true` so a destroy cannot trip over the
#     table; both are provider-side/query-parameter behaviour.
#
# Provider note: `big_query_custom_endpoint` is the versioned API base URL (the
# provider appends the resource path), so it carries one trailing slash:
# "${endpoint}/bigquery/v2/". Every apply log must be free of 400/501 and
# `unsupported update_mask`.

MODULE_REPO="terraform-google-bigquery"
MODULE_COMMIT="e11db0dcd597"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-bigquery"

DATASET="jc_tf_bq"
TABLE="jc_tf_bq_table"

# write_module generates the root module for one phase.
#   $1 description (HCL string literal)
#   $2 dataset labels (HCL map literal)
#   $3 table labels (HCL map literal)
#   $4 extra schema column (HCL object literal, or empty)
#   $5 access list (HCL list literal)
write_module() {
  local description="$1" dataset_labels="$2" table_labels="$3" extra_column="$4" access="$5"

  local extra=""
  if [[ -n "$extra_column" ]]; then
    extra=",
        $extra_column"
  fi

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
  project               = var.project
  region                = "us-central1"
  user_project_override = false

  big_query_custom_endpoint = "$BIGQUERY_ENDPOINT"
}

module "bigquery" {
  source = "$MODULE_DIR"

  project_id  = var.project
  dataset_id  = "$DATASET"
  description = $description
  location    = "US"

  dataset_labels              = $dataset_labels
  default_table_expiration_ms = 3600000
  delete_contents_on_destroy  = true

  # The served ACL surface: the module converts roles/bigquery.data* to the
  # primitive OWNER/WRITER/READER the API returns.
  access = $access

  tables = [
    {
      table_id = "$TABLE"
      schema = jsonencode([
        { name = "region", type = "STRING", mode = "NULLABLE" },
        { name = "amount", type = "INT64", mode = "NULLABLE" }$extra
      ])
      labels = $table_labels
    },
  ]
}
EOF
}

dataset_url() {
  echo "$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$DATASET"
}

table_url() {
  echo "$(dataset_url)/tables/$TABLE"
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

status_of() {
  curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$1" 2>/dev/null || true
}

case_main() {
  BIGQUERY_ENDPOINT="$(endpoint_for bigquery)"

  write_module '"jaiscloud terraform bigquery case"' \
    '{ env = "compat-test" }' \
    '{ env = "compat-test" }' \
    '' \
    '[{ role = "roles/bigquery.dataOwner", special_group = "projectOwners" }]'
  init_phase
  plan_phase "plan (module dataset + table + ACL)"
  apply_phase "apply (module dataset + table + ACL)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check_body "dataset echoes its reference, location, description and labels" "$(dataset_url)" \
    "\"datasetId\":\"$DATASET\"" \
    '"location":"US"' \
    '"description":"jaiscloud terraform bigquery case"' \
    '"env":"compat-test"' \
    '"goog-terraform-provisioned":"true"'
  check_body "dataset echoes the table expiration and the ACL entry" "$(dataset_url)" \
    '"defaultTableExpirationMs":3600000' \
    '"access"' '"role":"OWNER"' '"specialGroup":"projectOwners"'
  check_body "table echoes its reference, friendly name and labels" "$(table_url)" \
    "\"tableId\":\"$TABLE\"" "\"datasetId\":\"$DATASET\"" \
    "\"friendlyName\":\"$TABLE\"" \
    '"env":"compat-test"'
  check_body "table schema carries the two columns" "$(table_url)" \
    '"name":"region"' '"type":"STRING"' \
    '"name":"amount"' '"type":"INT64"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate of a non-ForceNew field set: a new schema column, dataset
  # and table labels, the dataset description and a second ACL entry.
  write_module '"jaiscloud terraform bigquery case v2"' \
    '{ env = "compat-test", tier = "gold" }' \
    '{ env = "compat-test", tier = "gold" }' \
    '{ name = "category", type = "STRING", mode = "NULLABLE" }' \
    '[{ role = "roles/bigquery.dataOwner", special_group = "projectOwners" }, { role = "roles/bigquery.dataViewer", special_group = "projectReaders" }]'
  apply_phase "apply (schema column + labels + description + ACL)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check_body "dataset echoes the mutated description and labels" "$(dataset_url)" \
    '"description":"jaiscloud terraform bigquery case v2"' \
    '"env":"compat-test"' '"tier":"gold"'
  check_body "dataset ACL carries the added reader entry" "$(dataset_url)" \
    '"role":"READER"' '"specialGroup":"projectReaders"'
  check_body "table echoes the mutated labels" "$(table_url)" \
    '"env":"compat-test"' '"tier":"gold"'
  check_body "table schema carries the added column" "$(table_url)" \
    '"name":"category"' '"type":"STRING"'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  local status
  status="$(status_of "$(dataset_url)")"
  if [[ "$status" == "404" ]]; then
    pass "dataset GET after destroy is 404"
  else
    fail "dataset GET after destroy = HTTP $status, want 404"
  fi
  status="$(status_of "$(table_url)")"
  if [[ "$status" == "404" ]]; then
    pass "table GET after destroy is 404"
  else
    fail "table GET after destroy = HTTP $status, want 404"
  fi
}
