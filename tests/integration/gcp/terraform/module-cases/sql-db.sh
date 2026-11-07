# shellcheck shell=bash
#
# Case: terraform-google-modules/terraform-google-sql-db (TF3-6)
#
# The pinned consumer is the CFT `terraform-google-sql-db` `modules/postgresql`
# submodule at v28.3.0. It creates the served Cloud SQL surface:
#
#   google_sql_database_instance   -> instances.insert / .get / .get (update)
#                                     (sqladmin v1beta4), via google-beta
#   google_sql_database            -> databases.insert / .get
#   google_sql_user                -> users.insert / .get / .list
#
# plus client-side `random_password` and `null_resource` resources. This is the
# *module consumer* gate: the module renders its full `settings` block (tier,
# availability, backup configuration, maintenance window, disk settings, labels)
# and the provider must converge on it, not only on our hand-written fixture.
#
# The module reads `data.google_compute_zones.available` from
# `read_replica.tf`, so the case points the compute endpoint at the emulator
# too (the fixture's own SQL block never needed it).
#
# Plan-only tier: `PLAN_ONLY=1` (or `make test-gcp-terraform-plan-only
# CASE=sql-db`) runs `init -> validate -> plan` and stops. That measures what the
# module would send without applying/destroying; the default run promotes the
# same configuration to the full apply -> assert -> plan No changes -> mutate ->
# plan No changes -> destroy contract, because this module's served subset does
# converge (unlike GKE).
#
# Lifecycle:
#
#   apply -> REST asserts (instance RUNNABLE + tier + labels, database, user)
#         -> plan -detailed-exitcode -> No changes
#   mutate (tier + labels) -> apply -> REST asserts -> plan No changes
#   destroy -> instance GET is 404
#
# Boundaries, recorded rather than worked around:
#
#  * `deletion_protection = false` is deliberate: the module default `true` is a
#    provider-side destroy guard and real Cloud SQL refuses the delete too.
#  * Read replicas, private service access, CMEK/autokey, password-validation
#    policy, insights and PITR stay at their defaults/off; those inputs reach
#    surfaces (dns/peering, KMS key handles, the SQL data plane) that are out of
#    the Terraform family's measured scope. The module's default GCE zone read is
#    satisfied by the emulator's metadata-only compute zones.

MODULE_REPO="terraform-google-sql-db"
MODULE_COMMIT="75b57dc6838e"
MODULE_SUBDIR="modules/postgresql"

CAPTURE_RESOURCES="google_sql_database_instance,google_sql_database,google_sql_user"

INSTANCE="jc-tf-sql"

# write_module generates the root module for one phase.
#   $1 tier (HCL string literal)
#   $2 user_labels (HCL map literal)
write_module() {
  local tier="$1" labels="$2"

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
    random = {
      source  = "hashicorp/random"
      version = "= 3.7.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "= 3.2.4"
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
  project                 = var.project
  region                  = "us-central1"
  user_project_override   = false
  sql_custom_endpoint     = "\${var.endpoint}/sql/v1beta4/"
  compute_custom_endpoint = "\${var.endpoint}/compute/v1/"
}

provider "google-beta" {
  project                 = var.project
  region                  = "us-central1"
  user_project_override   = false
  sql_custom_endpoint     = "\${var.endpoint}/sql/v1beta4/"
  compute_custom_endpoint = "\${var.endpoint}/compute/v1/"
}

module "sql" {
  source = "$MODULE_DIR"

  project_id          = var.project
  name                = "$INSTANCE"
  database_version    = "POSTGRES_18"
  region              = "us-central1"
  tier                = $tier
  deletion_protection = false
  user_labels         = $labels
}
EOF
}

instance_url() {
  echo "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$INSTANCE"
}

database_url() {
  echo "$(instance_url)/databases/default"
}

users_url() {
  echo "$(instance_url)/users"
}

case_main() {
  local status

  write_module '"db-custom-1-3840"' '{ env = "terraform-case" }'
  init_phase

  if [[ "$PLAN_ONLY" == "1" ]]; then
    plan_only_phase "plan (module postgresql instance + database + user)"
    plan_includes "module.sql.google_sql_database_instance.default" "plan creates the module instance"
    plan_includes "module.sql.google_sql_database.default[0]" "plan creates the module database"
    plan_includes "module.sql.google_sql_user.default[0]" "plan creates the module user"
    return 0
  fi

  plan_phase "plan (module postgresql instance + database + user)"
  apply_phase "apply (module postgresql instance + database + user)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check "instance is RUNNABLE with the module's tier and labels" "$(instance_url)" \
    "\"name\":\"$INSTANCE\"" \
    '"state":"RUNNABLE"' \
    '"databaseVersion":"POSTGRES_18"' \
    '"tier":"db-custom-1-3840"' \
    '"userLabels":{"env":"terraform-case"}'
  check "the module's default database exists" "$(database_url)" \
    '"name":"default"' \
    "\"instance\":\"$INSTANCE\""
  check "the module's default user exists" "$(users_url)" \
    '"name":"default"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  # In-place mutate: a larger tier and new labels. Both live in settings, so the
  # provider PATCHes the instance and the module's user/database are untouched.
  write_module '"db-custom-2-7680"' '{ env = "terraform-case-v2", tier = "gold" }'
  apply_phase "apply (tier + labels)"
  assert_no_api_errors

  echo "== spot checks (mutate)"
  check "instance echoes the mutated tier and labels" "$(instance_url)" \
    '"tier":"db-custom-2-7680"' \
    '"env":"terraform-case-v2"' \
    '"tier":"gold"'

  echo "== $TF: second plan after the mutate (permadiff check)"
  plan_no_changes "re-plan after the mutate reports no changes"

  destroy_phase

  status="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$(instance_url)" 2>/dev/null || true)"
  if [[ "$status" == "404" ]]; then
    pass "instance GET after destroy is 404"
  else
    fail "instance GET after destroy = HTTP $status, want 404"
  fi
}
