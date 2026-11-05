# shellcheck shell=bash
#
# Case: terraform-google-memorystore
#
# The pinned consumer is terraform-google-modules/terraform-google-memorystore
# (module v16.4.1). Its `google_redis_instance` always sends
# `auth_enabled`, `connect_mode` (provider default DIRECT_PEERING),
# `transit_encryption_mode` (module default SERVER_AUTHENTICATION) and
# `read_replicas_mode` (module default READ_REPLICAS_DISABLED), and sends
# `maintenance_policy` / `persistence_config` when the caller configures them.
# Before TGM2 the emulator stored none of them, so GET omitted them:
# `transit_encryption_mode` and `connect_mode` (both ForceNew) planned an
# instance *replacement*, `read_replicas_mode` and the two blocks permadiffed,
# and changing AUTH later PATCHed `updateMask=authEnabled` into a
# `400 unsupported updateMask field authEnabled`.
#
# The gate proves the lifecycle converges:
#
#   create (STANDARD_HA + module defaults + labels + maintenance window +
#           RDB persistence) -> assert GET -> No changes
#   mutate (AUTH on, different window/period, changed labels) -> apply ->
#          No changes
#   clear  (AUTH off, maintenance window removed, persistence disabled, labels
#           emptied) -> apply -> No changes
#
# `hashicorp/google` 7.46.2 omits `authEnabled` from the PATCH body when the
# value goes true -> false even though the mask names it, so the emulator must
# treat the mask as authoritative (an absent masked field clears it) — that is
# proto3 update semantics, not an emulator convenience.

MODULE_REPO="terraform-google-memorystore"
MODULE_COMMIT="5415d5ccca28"
MODULE_GIT="https://github.com/terraform-google-modules/terraform-google-memorystore"

INSTANCE="jc-module-redis"
REGION="us-central1"

# write_module generates the root module for one gate phase:
#   create | mutate | clear
write_module() {
  local phase="$1"
  local labels auth maintenance persistence

  case "$phase" in
    create)
      labels='{
    env   = "tf-module"
    owner = "jaiscloud"
  }'
      auth="false"
      maintenance='{
    description = "jaiscloud gate window"
    day         = "MONDAY"
    start_time  = { hours = 1, minutes = 0, seconds = 0, nanos = 0 }
  }'
      persistence='{
    persistence_mode    = "RDB"
    rdb_snapshot_period = "TWENTY_FOUR_HOURS"
  }'
      ;;
    mutate)
      labels='{
    env = "tf-module-updated"
  }'
      auth="true"
      maintenance='{
    description = "jaiscloud gate window v2"
    day         = "WEDNESDAY"
    start_time  = { hours = 3, minutes = 30, seconds = 0, nanos = 0 }
  }'
      persistence='{
    persistence_mode    = "RDB"
    rdb_snapshot_period = "ONE_HOUR"
  }'
      ;;
    clear)
      labels='{}'
      auth="false"
      # maintenance_policy is Optional (not Computed), so removing it plans a
      # change and the provider sends updateMask=maintenancePolicy with no block.
      maintenance="null"
      # persistence_config is Optional+Computed: removing the block keeps the
      # prior state and sends no mask at all, so (as in real GCP, and like the
      # storage case's `website`) the documented way to clear persistence is
      # persistence_mode = "DISABLED".
      persistence='{
    persistence_mode    = "DISABLED"
    rdb_snapshot_period = "TWENTY_FOUR_HOURS"
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
  region  = "$REGION"

  user_project_override = false

  redis_custom_endpoint             = "\${var.endpoint}/v1/"
  service_usage_custom_endpoint     = "\${var.endpoint}/v1/"
  resource_manager_custom_endpoint  = "\${var.endpoint}/v1/"
  cloud_billing_custom_endpoint     = "\${var.endpoint}/v1/"
  iam_custom_endpoint               = "\${var.endpoint}/"
}

# The module creates a nested project-factory project_services module
# (google_project_service for redis.googleapis.com, hence service_usage) and
# depends_on it, so the instance is created after the API is enabled.
module "memorystore" {
  source = "$MODULE_DIR"

  project_id     = var.project
  region         = "$REGION"
  name           = "$INSTANCE"
  tier           = "STANDARD_HA"
  memory_size_gb = 1
  redis_version  = "REDIS_7_0"

  labels = $labels

  auth_enabled            = $auth
  transit_encryption_mode = "SERVER_AUTHENTICATION"
  read_replicas_mode      = "READ_REPLICAS_DISABLED"
  replica_count           = 1

  maintenance_policy = $maintenance
  persistence_config = $persistence
}
EOF
}

# instance_body fetches the instance through the emulator's REST API.
instance_body() {
  curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" 2>/dev/null || true
}

# check_absent is the negative check: a cleared field must not be rendered.
check_absent() {
  local name="$1"
  shift
  local body want
  body="$(instance_body)"
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
  plan_phase "plan (STANDARD_HA instance with module defaults + maintenance/persistence)"
  apply_phase "apply (create)"
  assert_no_api_errors

  echo "== spot checks (create)"
  check "AUTH (false), connect mode and tier read back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"authEnabled":false' '"connectMode":"DIRECT_PEERING"' '"tier":"STANDARD_HA"' \
    '"replicaCount":1'
  # transit_encryption_mode and connect_mode are ForceNew in the provider; the
  # create read-back is what stops the next plan replacing the instance.
  check "ForceNew TLS mode and read-replica mode read back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"transitEncryptionMode":"SERVER_AUTHENTICATION"' \
    '"readReplicasMode":"READ_REPLICAS_DISABLED"'
  check "labels read back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"env":"tf-module"' '"owner":"jaiscloud"'
  check "maintenance policy reads back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"maintenancePolicy":{' '"description":"jaiscloud gate window"' \
    '"day":"MONDAY"' '"startTime":{' '"hours":1'
  check "persistence config reads back" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"persistenceConfig":{' '"persistenceMode":"RDB"' \
    '"rdbSnapshotPeriod":"TWENTY_FOUR_HOURS"'

  echo "== $TF: second plan (permadiff check)"
  plan_no_changes "re-plan reports no changes"

  write_module mutate
  apply_phase "apply (AUTH on, maintenance window + snapshot period + labels changed)"
  assert_no_api_errors
  echo "== spot checks (mutate)"
  check "AUTH flipped to true" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"authEnabled":true'
  check "maintenance window changed" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"day":"WEDNESDAY"' '"hours":3' '"minutes":30' \
    '"description":"jaiscloud gate window v2"'
  check "snapshot period changed" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"rdbSnapshotPeriod":"ONE_HOUR"'
  check "labels changed" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"env":"tf-module-updated"'
  check "unchanged ForceNew modes survived the PATCH" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"transitEncryptionMode":"SERVER_AUTHENTICATION"' \
    '"connectMode":"DIRECT_PEERING"'
  plan_no_changes "re-plan after mutation reports no changes (no replacement)"

  write_module clear
  apply_phase "apply (AUTH off, maintenance window removed, persistence disabled, labels emptied)"
  assert_no_api_errors
  echo "== spot checks (clear)"
  check "AUTH flipped back to false" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"authEnabled":false'
  check_absent "maintenance policy cleared" '"maintenancePolicy":'
  # persistence_config is Optional+Computed in the provider, so the block is
  # disabled rather than removed; a removal would leave it in state (and in
  # real GCP) with no PATCH.
  check "persistence disabled" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"persistenceMode":"DISABLED"'
  check_absent "labels cleared" '"env":' '"owner":'
  check "instance is still READY" \
    "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/instances/$INSTANCE" \
    '"state":"READY"'
  plan_no_changes "re-plan after clear reports no changes"

  destroy_phase
}
