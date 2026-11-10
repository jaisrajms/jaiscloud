#!/usr/bin/env bash
#
# run.sh — opt-in Terraform / OpenTofu compatibility suite for jaiscloud-gcp.
#
# Drives the hashicorp/google provider against a running jaiscloud-gcp and
# asserts the resulting resources through the emulator's REST API. Only services
# jaiscloud-gcp implements are exercised (see README.md). The suite runs
# init -> validate -> plan -> apply -> spot checks -> destroy, and always
# destroys what it created (trap on exit).
#
# Usage:
#   tests/integration/gcp/terraform/run.sh [ENDPOINT] [PROJECT]
#
#     ENDPOINT  base URL of jaiscloud-gcp REST (default: http://localhost:8080)
#     PROJECT   project id (default: test-project)
#
# Env:
#   TF_BIN    terraform binary to use (default: terraform; set TF_BIN=tofu for
#             the OpenTofu variant)
#
# Prefer the Makefile targets, which start/stop the emulator for you:
#   make test-gcp-terraform
#   make test-gcp-opentofu

set -euo pipefail

ENDPOINT="${1:-http://localhost:8080}"
PROJECT="${2:-test-project}"
TF="${TF_BIN:-terraform}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="us-central1"

if ! command -v "$TF" >/dev/null 2>&1; then
  echo "SKIP: '$TF' not installed"
  exit 0
fi

# The emulator ignores auth, but the provider needs some credential present.
export GOOGLE_OAUTH_ACCESS_TOKEN="${GOOGLE_OAUTH_ACCESS_TOKEN:-fake-token-jaiscloud}"
export GOOGLE_CLOUD_PROJECT="$PROJECT"

PASS=0
FAIL=0

# check NAME URL SUBSTRING [SUBSTRING...] — GET the URL and require every
# substring to be present. Records a failure (does not abort) so one run
# reports every divergence.
check() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# check_post NAME URL SUBSTRING [SUBSTRING...] — POST an empty JSON body (the
# project IAM read is a POST custom method) and require every substring.
check_post() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

tf() { "$TF" -chdir="$DIR" "$@"; }

# check_post_body NAME URL BODY SUBSTRING... — POST the given JSON body and
# require every substring (the IAM custom methods take a request body).
check_post_body() {
  local name="$1" url="$2" payload="$3"
  shift 3
  local body want
  body="$(curl -sf -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d "$payload" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" != *"$want"* ]]; then
      printf '  [FAIL] %s (missing: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# check_absent NAME URL SUBSTRING... — GET the URL and require every substring to
# be absent. Used for input-only fields real GCP never echoes (grantees).
check_absent() {
  local name="$1" url="$2"
  shift 2
  local body want
  body="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$url" 2>/dev/null || true)"
  for want in "$@"; do
    if [[ "$body" == *"$want"* ]]; then
      printf '  [FAIL] %s (unexpected: %s)\n' "$name" "$want"
      FAIL=$((FAIL + 1))
      return 0
    fi
  done
  printf '  [PASS] %s\n' "$name"
  PASS=$((PASS + 1))
}

# The GKE surface needs a host whose first DNS label is "container": the
# provider keeps only the scheme+host of a custom endpoint, so it cannot send
# the /container path prefix the emulator also accepts, and the bare
# /v1/projects/{p}/locations/{l}/clusters path belongs to Managed Kafka.
# container.localhost resolves to the loopback address on the supported hosts.
CONTAINER_ENDPOINT="$(printf '%s' "$ENDPOINT" | sed -E 's#^(https?://)([^/]+)#\1container.\2#')/container/"
CONTAINER_HOST="$(printf '%s' "$CONTAINER_ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
if ! getent hosts "$CONTAINER_HOST" >/dev/null 2>&1; then
  echo "ERROR: $CONTAINER_HOST does not resolve — the GKE fixture needs a container.* host alias"
  echo "       (add e.g. '127.0.0.1 container.localhost' to the hosts file, or pass a resolvable ENDPOINT)"
  exit 1
fi

TF_VARS=(-var="endpoint=$ENDPOINT" -var="project=$PROJECT" -var="container_endpoint=$CONTAINER_ENDPOINT")

# The endpoint map is one artifact (terraform-endpoints.yaml): generate the
# fixture's `endpoints` tfvars with tools/tfcorpus so this directory cannot
# drift from the module driver and the corpus tool. The locals in provider.tf
# are only a fallback for an environment without Go.
ROOT="$(cd "$DIR/../../../.." && pwd)"
ENDPOINTS_TFVARS="$(mktemp "${TMPDIR:-/tmp}/jc-tf-endpoints.XXXXXX.json")"
if command -v go >/dev/null 2>&1; then
  if ! (cd "$ROOT" && go run ./tools/tfcorpus -emit-tfvars "$ENDPOINTS_TFVARS" \
      -endpoint "$ENDPOINT" -endpoints "$DIR/terraform-endpoints.yaml"); then
    echo "ERROR: could not generate the endpoint tfvars from terraform-endpoints.yaml"
    exit 1
  fi
  TF_VARS+=(-var-file="$ENDPOINTS_TFVARS")
else
  echo "  note: go not found; using provider.tf's endpoint defaults"
fi

# The Dataproc-on-GKE virtual cluster needs a standard, Workload-Identity-
# enabled GKE cluster as its target; that is google_container_cluster.gke in
# main.tf (the DPG37 gate), so it is created and destroyed with the fixture.
GKE_CLUSTER="jaiscloud-tf-gke"
GKE_PARENT="$ENDPOINT/container/v1/projects/$PROJECT/locations/$REGION/clusters"

DESTROYED=0
destroy() {
  [[ "$DESTROYED" == "1" ]] && return 0
  DESTROYED=1
  tf destroy -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null 2>&1 || true
}
cleanup() {
  destroy
  [[ -n "${ENDPOINTS_TFVARS:-}" ]] && rm -f "$ENDPOINTS_TFVARS"
}
trap 'cleanup' EXIT INT TERM

echo "== $TF: init / validate / plan / apply (endpoint=$ENDPOINT project=$PROJECT)"
tf init -input=false -no-color >/dev/null
tf validate -no-color >/dev/null
tf plan -input=false -no-color "${TF_VARS[@]}" >/dev/null
tf apply -input=false -auto-approve -no-color "${TF_VARS[@]}" >/dev/null

# A second plan must report no changes. This is the permadiff assertion for the
# google_container_cluster read and the computed fields around it
# (loggingService/monitoringService/masterAuth): the emulator's defaults have to
# agree with what the provider stored on apply.
echo "== $TF: second plan (permadiff check)"
if tf plan -input=false -no-color -detailed-exitcode "${TF_VARS[@]}" >/dev/null 2>&1; then
  printf '  [PASS] re-plan reports no changes\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] re-plan reports changes (a permadiff)\n'
  FAIL=$((FAIL + 1))
fi

out() { tf output -raw "$1" 2>/dev/null || true; }
SA_EMAIL="$(out service_account_email)"
SA_NODESC_EMAIL="$(out service_account_nodesc_email)"
SQL_INSTANCE="$(out sql_instance_name)"
SQL_DB="$(out sql_database_name)"
SQL_USER="$(out sql_user_name)"
SQL_CERT_FP="$(out sql_ssl_cert_fingerprint)"
ENABLED_SERVICE="$(out enabled_service)"
MS_SERVICE="$(out metastore_service_name)"
FED_NAME="$(out metastore_federation_name)"
GKE_CLUSTER_DP="$(out dataproc_gke_cluster_name)"
GCE_CLUSTER_DP="$(out dataproc_gce_cluster_name)"
BQ_DATASET="$(out bigquery_dataset_id)"
BQ_TABLE="$(out bigquery_table_id)"
BQ_ROUTINE="$(out bigquery_routine_id)"
BQ_POLICY="$(out bigquery_row_access_policy_id)"
BILLING_ACCOUNT="$(out billing_account)"
PROJECT_BILLING="$(out project_billing_account)"
RUN_SERVICE="$(out run_service_name)"
DNS_ZONE="$(out dns_zone_name)"
RUN_BASE="$ENDPOINT/v2/projects/$PROJECT/locations/$REGION/services/$RUN_SERVICE"

echo "== spot checks"
check "GCS bucket created" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket" \
  '"name":"jaiscloud-tf-bucket"'
check "GCS bucket labels persist" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket" \
  '"env":"compat-test"'
check "GCS object uploaded" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket/o/README.txt" \
  '"name":"README.txt"'
check "GCS object content-type" "$ENDPOINT/storage/v1/b/jaiscloud-tf-bucket/o/README.txt" \
  '"contentType":"text/plain"'
check "IAM service account created" \
  "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$SA_EMAIL" 'jaiscloud-tf-sa'
# TF2-14: an account created without display_name reads back without the field
# (real IAM does not default it to the account id), and the second plan above
# gates the provider permadiff.
check_absent "IAM account without display_name omits it" \
  "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$SA_NODESC_EMAIL" '"displayName"'
check "Secret Manager secret created" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret" \
  'jaiscloud-tf-secret' '"automatic"'
check "Secret Manager version ENABLED" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret/versions/1" '"state":"ENABLED"'
# DC31: the provider computes replication[0].auto[0].customer_managed_encryption[0].kms_key_name
# from the Secret response, so an empty automatic policy makes it read back empty.
# The key is the global ring's: automatic replication only accepts global keys.
check "Secret Manager CMEK key echoed" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret-cmek" \
  '"customerManagedEncryption"' \
  "\"kmsKeyName\":\"projects/$PROJECT/locations/global/keyRings/jaiscloud-tf-keyring-global/cryptoKeys/jaiscloud-tf-key-global\""
# DC45: a user-managed replication policy reads back verbatim (both replica
# locations and the per-replica CMEK key), not rewritten as automatic.
check "Secret Manager user-managed replication echoed" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret-user-managed" \
  '"userManaged"' \
  "\"location\":\"$REGION\"" \
  '"location":"us-east1"' \
  "\"kmsKeyName\":\"projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring/cryptoKeys/jaiscloud-tf-key\""
check "KMS key ring created" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring" \
  'jaiscloud-tf-keyring'
check "KMS crypto key created" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring/cryptoKeys/jaiscloud-tf-key" \
  '"purpose":"ENCRYPT_DECRYPT"'
check "Pub/Sub topic created" \
  "$ENDPOINT/v1/projects/$PROJECT/topics/jaiscloud-tf-topic" 'jaiscloud-tf-topic'
check "Pub/Sub subscription created" \
  "$ENDPOINT/v1/projects/$PROJECT/subscriptions/jaiscloud-tf-sub" 'jaiscloud-tf-sub'
# Both topic grants must survive the provider's get/merge/set IAM flow.
check "Pub/Sub topic IAM keeps both grants" \
  "$ENDPOINT/v1/projects/$PROJECT/topics/jaiscloud-tf-topic:getIamPolicy" \
  'roles/pubsub.publisher' 'roles/pubsub.viewer' 'jaiscloud-tf-sa@' \
  'user:compat-viewer@example.com'
check "Pub/Sub subscription IAM grant" \
  "$ENDPOINT/v1/projects/$PROJECT/subscriptions/jaiscloud-tf-sub:getIamPolicy" \
  'roles/pubsub.subscriber'
check "Secret Manager IAM accessor grant" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets/jaiscloud-tf-secret:getIamPolicy" \
  'roles/secretmanager.secretAccessor'
# A regional secret lives at projects/{p}/locations/{l}/secrets/{s}: no
# replication policy, top-level customerManagedEncryption, and a version CMEK
# status instead of replicationStatus.
REGIONAL_SECRET="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/secrets/jaiscloud-tf-regional-secret"
check "Secret Manager regional secret created" "$REGIONAL_SECRET" \
  "projects/$PROJECT/locations/$REGION/secrets/jaiscloud-tf-regional-secret" \
  '"env":"compat-test"' '"owner":"terraform"'
check "Secret Manager regional CMEK echoed" "$REGIONAL_SECRET" \
  '"customerManagedEncryption"' \
  "\"kmsKeyName\":\"projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring/cryptoKeys/jaiscloud-tf-key\""
check_absent "Secret Manager regional secret has no replication policy" "$REGIONAL_SECRET" '"replication"'
check "Secret Manager regional version ENABLED" "$REGIONAL_SECRET/versions/1" \
  '"state":"ENABLED"' '"customerManagedEncryption"' '"kmsKeyVersionName"'
check_absent "Secret Manager regional version has no replicationStatus" \
  "$REGIONAL_SECRET/versions/1" '"replicationStatus"'
check "Secret Manager regional list contains the secret" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/secrets" 'jaiscloud-tf-regional-secret'
check_absent "Secret Manager project list excludes regional secrets" \
  "$ENDPOINT/v1/projects/$PROJECT/secrets" 'jaiscloud-tf-regional-secret'
check "Secret Manager regional IAM grant" \
  "$REGIONAL_SECRET:getIamPolicy" 'roles/secretmanager.secretAccessor'
check "CRM project lookup reports ACTIVE" \
  "$ENDPOINT/v1/projects/$PROJECT" '"lifecycleState":"ACTIVE"'
# TF1-5: google_compute_project_metadata writes the project's common instance
# metadata through projects.setCommonInstanceMetadata (items + the fingerprint
# projects.get reported) and reads it back. The clean re-plan above proves the
# fingerprint round-trip does not permadiff.
check "Compute project metadata stored" \
  "$ENDPOINT/compute/v1/projects/$PROJECT" \
  '"commonInstanceMetadata"' '"key":"compat-key"' '"value":"compat-value"' '"fingerprint":"'
# TF2-11-4: a disk created with a CMEK key is read back with
# diskEncryptionKey.kmsKeyName — the provider feeds it into the ForceNew
# disk_encryption_key block, and never with raw or RSA-wrapped key material.
# The clean re-plan above is the permadiff assertion.
DISK_CMEK="$ENDPOINT/compute/v1/projects/$PROJECT/zones/$REGION-a/disks/jaiscloud-tf-cmek"
check "Compute disk CMEK key echoed" "$DISK_CMEK" \
  '"diskEncryptionKey"' \
  "\"kmsKeyName\":\"projects/$PROJECT/locations/$REGION/keyRings/jaiscloud-tf-keyring/cryptoKeys/jaiscloud-tf-key\""
check_absent "Compute disk CMEK echo has no key material" "$DISK_CMEK" \
  '"rawKey"' '"rsaEncryptedKey"'
# Cloud Billing: data "google_project" reads projects.getBillingInfo to populate
# billing_account. The emulator models only the association, so the four-field
# default (no account, billing disabled) is what both the wire and the data
# source must report — before CB1 this path was an UnknownService 404.
check "Cloud Billing billingInfo reads the no-account default" \
  "$ENDPOINT/v1/projects/$PROJECT/billingInfo" \
  "\"name\":\"projects/$PROJECT/billingInfo\"" \
  '"billingAccountName":""' '"billingEnabled":false'
if [[ -z "$BILLING_ACCOUNT" ]]; then
  printf '  [PASS] data.google_project billing_account is empty\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] data.google_project billing_account = %q, want empty (no billing catalog)\n' "$BILLING_ACCOUNT"
  FAIL=$((FAIL + 1))
fi
# CB2: resource "google_project" with billing_account writes the association
# (POST billingAccounts/{id}:testIamPermissions, POST projects, PUT
# projects/{p}/billingInfo) and the provider reads it back. The wire stores and
# echoes the full billingAccounts/… resource name, while the provider strips the
# prefix into its `billing_account` attribute — that is what keeps the configured
# bare id and the read-back value equal, so the clean re-plan above proves there
# is no permadiff. Both halves are asserted: the wire body below, the attribute
# after it.
TF_BILLING_PROJECT="jaiscloud-tf-billing"
TF_BILLING_ACCOUNT_NAME="billingAccounts/0X0X0X-0X0X0X-0X0X0X"
check "Cloud Billing billingInfo stores the google_project association" \
  "$ENDPOINT/v1/projects/$TF_BILLING_PROJECT/billingInfo" \
  "\"name\":\"projects/$TF_BILLING_PROJECT/billingInfo\"" \
  "\"billingAccountName\":\"$TF_BILLING_ACCOUNT_NAME\"" \
  '"billingEnabled":true'
if [[ "$PROJECT_BILLING" == "0X0X0X-0X0X0X-0X0X0X" ]]; then
  printf '  [PASS] google_project billing_account round-trips as the bare id %s\n' "$PROJECT_BILLING"
  PASS=$((PASS + 1))
else
  printf '  [FAIL] google_project billing_account = %q, want the bare id 0X0X0X-0X0X0X-0X0X0X\n' "$PROJECT_BILLING"
  FAIL=$((FAIL + 1))
fi
# The billing-account catalog stays empty: no account lifecycle is modelled, so
# the list is the schema-valid empty answer rather than a fabricated account.
check "Cloud Billing billingAccounts list is empty" \
  "$ENDPOINT/v1/billingAccounts" '"billingAccounts":[]'
check_post "Project IAM grant present" \
  "$ENDPOINT/v1/projects/$PROJECT:getIamPolicy" \
  'roles/pubsub.publisher' 'jaiscloud-tf-sa@'
check "Service Usage service enabled" \
  "$ENDPOINT/v1/projects/$PROJECT/services/$ENABLED_SERVICE" '"state":"ENABLED"'
check "Service Usage enabled list includes service" \
  "$ENDPOINT/v1/projects/$PROJECT/services?filter=state:ENABLED" "$ENABLED_SERVICE"
check "Cloud SQL instance created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE" \
  '"kind":"sql#instance"' '"state":"RUNNABLE"'
check "Cloud SQL database created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE/databases/$SQL_DB" \
  '"kind":"sql#database"'
check "Cloud SQL user created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE/users/$SQL_USER" \
  '"kind":"sql#user"'
# google_sql_ssl_cert: the fingerprint Terraform state holds addresses the
# certificate the emulator minted, and the instance carries its server CA (the
# server-ca.pem a real client downloads).
check "Cloud SQL client certificate created" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE/sslCerts/$SQL_CERT_FP" \
  '"kind":"sql#sslCert"' '"commonName":"jaiscloud-tf-client"' "$SQL_CERT_FP"
check "Cloud SQL instance exposes its server CA" \
  "$ENDPOINT/sql/v1beta4/projects/$PROJECT/instances/$SQL_INSTANCE" \
  '"serverCaCert"' 'BEGIN CERTIFICATE'
check "Dataproc Metastore service echoes its requested port" \
  "$ENDPOINT/v1/$MS_SERVICE" '"port":9080' ':9080' '"state":"ACTIVE"'
check "Dataproc Metastore federation created" \
  "$ENDPOINT/v1/$FED_NAME" 'jaiscloud-tf-federation' '"state":"ACTIVE"'
check "Dataproc Metastore database IAM grant" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/services/jaiscloud-tf-metastore/databases/default:getIamPolicy" \
  'roles/metastore.admin' 'jaiscloud-tf-sa@'

# ── Cloud Monitoring: the declarative uptime check ───────────────────────────
# The emulator stores and echoes the configuration (NF28); the probe-derived
# uptimeCheckIps list is the only part that stays unimplemented. `apply` above
# succeeded through create, and the re-plan proves the read did not permadiff.
UPTIME_ID="$(out uptime_check_id)"
check "uptime check configuration created" \
  "$ENDPOINT/v3/projects/$PROJECT/uptimeCheckConfigs/$UPTIME_ID" \
  "\"name\":\"projects/$PROJECT/uptimeCheckConfigs/$UPTIME_ID\"" \
  '"displayName":"jaiscloud-tf-uptime"' \
  '"period":"300s"' '"timeout":"10s"' \
  '"type":"uptime_url"' '"host":"shop.example.com"' \
  '"path":"/healthz"' '"useSsl":true' \
  '"selectedRegions":["USA","EUROPE","ASIA_PACIFIC"]' \
  '"team":"compat-test"'
check "uptime check configuration listed" \
  "$ENDPOINT/v3/projects/$PROJECT/uptimeCheckConfigs" \
  '"uptimeCheckConfigs"' "uptimeCheckConfigs/$UPTIME_ID"

# ── GKE: the cluster the Dataproc-on-GKE prerequisite rides on ────────────────
# DPG37: real GKE always renders these nested messages and the provider
# dereferences four of them unconditionally on read.
check "GKE cluster read renders the nested response messages" \
  "$GKE_PARENT/$GKE_CLUSTER" \
  '"legacyAbac":{"enabled":false}' \
  "\"networkConfig\":{\"network\":\"projects/$PROJECT/global/networks/default\",\"subnetwork\":\"projects/$PROJECT/regions/$REGION/subnetworks/default\"}" \
  '"shieldedNodes":{"enabled":true}' \
  '"defaultMaxPodsConstraint":{"maxPodsPerNode":"110"}'
check "GKE cluster read renders the default logging/monitoring services" \
  "$GKE_PARENT/$GKE_CLUSTER" \
  '"loggingService":"logging.googleapis.com/kubernetes"' \
  '"monitoringService":"monitoring.googleapis.com/kubernetes"'

# ── Dataproc on GKE: the virtual cluster and the pools it reconciled ──────────
DP_GKE="$GKE_CLUSTER_DP"
check "Dataproc-on-GKE virtual cluster created" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$DP_GKE" \
  '"state":"RUNNING"' "jaiscloud-tf-gke" 'jaiscloud-tf-dataproc-pool' 'jaiscloud-tf-dataproc-spark'
# nodePoolConfig is input-only in the real API: the read must never echo it.
DP_GKE_BODY="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$DP_GKE" 2>/dev/null || true)"
if [[ "$DP_GKE_BODY" == *"nodePoolConfig"* ]]; then
  printf '  [FAIL] Dataproc-on-GKE read echoes input-only nodePoolConfig\n'
  FAIL=$((FAIL + 1))
else
  printf '  [PASS] Dataproc-on-GKE read omits input-only nodePoolConfig\n'
  PASS=$((PASS + 1))
fi
GKE_POOLS="$GKE_PARENT/$GKE_CLUSTER/nodePools"
check "Dataproc created its shaped node pool in the GKE API" \
  "$GKE_POOLS/jaiscloud-tf-dataproc-pool" \
  '"machineType":"n1-standard-4"' '"maxNodeCount":3' '"goog-dataproc-cluster-name":"jaiscloud-tf-dataproc-gke"'
check "Dataproc created its named-only node pool with the 1/10 defaults" \
  "$GKE_POOLS/jaiscloud-tf-dataproc-spark" \
  '"minNodeCount":1' '"maxNodeCount":10'

# The plain cluster_config shape: the provider's read-back flattens
# config.software_config without a nil check, so the emulator must carry the
# service defaults real Dataproc reports (DPG38). Apply + refresh above must
# have completed without a panic or a diff for the resource to survive.
check "Compute-Engine cluster reads back the default image version" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$GCE_CLUSTER_DP" \
  '"softwareConfig":{"imageVersion":"2.3.3-debian12"}'
DP_GCE_MACHINE="https://www.googleapis.com/compute/v1/projects/$PROJECT/zones/$REGION-a/machineTypes/n1-standard-4"
check "Compute-Engine cluster reads back the default zone and machine type" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$GCE_CLUSTER_DP" \
  "\"zoneUri\":\"https://www.googleapis.com/compute/v1/projects/$PROJECT/zones/$REGION-a\"" \
  "\"machineTypeUri\":\"$DP_GCE_MACHINE\""
check "Compute-Engine cluster reads back the default instance groups" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$GCE_CLUSTER_DP" \
  '"numInstances":1' '"numInstances":2'

# Resource-level IAM on the cluster (TF1-2): google_dataproc_cluster_iam_member
# drives clusters.getIamPolicy/setIamPolicy, so the stored policy must carry the
# granted role and member. The provider reads the etag back into state, so this
# check also proves the get returns the stored policy verbatim.
check_post "Dataproc cluster IAM policy carries the granted binding" \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$GCE_CLUSTER_DP:getIamPolicy" \
  '"role":"roles/dataproc.viewer"' "\"members\":[\"serviceAccount:$SA_EMAIL\"]" '"etag"'

# ── Cloud Run: the v2 service the module gate also drives ────────────────────
# The raw resource exists for container readiness_probe (the v2 submodule has
# only startup/liveness probes). labels/env/ingress are plain served fields;
# the service account is the fixture's IAM SA, so this also proves the provider
# can point a revision at an existing account.
check "Cloud Run service created" "$RUN_BASE" \
  "\"name\":\"projects/$PROJECT/locations/$REGION/services/$RUN_SERVICE\"" \
  '"ingress":"INGRESS_TRAFFIC_INTERNAL_ONLY"' \
  '"env":"compat-test"' \
  '"image":"us-docker.pkg.dev/cloudrun/container/hello"' \
  '"serviceAccount":"'"$SA_EMAIL"'"'
check "Cloud Run container readiness probe read back" "$RUN_BASE" \
  '"readinessProbe"' '"path":"/ready"' '"periodSeconds":10' '"successThreshold":2'
check "Cloud Run revision created" "$RUN_BASE/revisions" \
  '"revisions"' "services/$RUN_SERVICE/revisions/$RUN_SERVICE-00001"

# An in-place update: changing the variable makes the provider PATCH the config
# with an updateMask. The declaration must change and nothing else.
echo "== $TF: in-place update of the uptime check"
tf apply -input=false -auto-approve -no-color "${TF_VARS[@]}" \
  -var="uptime_display_name=jaiscloud-tf-uptime-v2" >/dev/null
check "uptime check display name updated in place" \
  "$ENDPOINT/v3/projects/$PROJECT/uptimeCheckConfigs/$UPTIME_ID" \
  '"displayName":"jaiscloud-tf-uptime-v2"'
check "uptime check update preserved the unpatched fields" \
  "$ENDPOINT/v3/projects/$PROJECT/uptimeCheckConfigs/$UPTIME_ID" \
  '"path":"/healthz"' '"period":"300s"' '"team":"compat-test"'
if tf plan -input=false -no-color -detailed-exitcode "${TF_VARS[@]}" \
  -var="uptime_display_name=jaiscloud-tf-uptime-v2" >/dev/null 2>&1; then
  printf '  [PASS] re-plan after the uptime check update reports no changes\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] re-plan after the uptime check update reports changes\n'
  FAIL=$((FAIL + 1))
fi

# ── BigQuery: routine + row access policy (NF31) ─────────────────────────────
# routines.* and rowAccessPolicies.* are resource CRUD over the BigQuery store.
# The apply above created them through google_bigquery_routine /
# google_bigquery_row_access_policy; the update below changes the two variables
# so the provider issues routines.update and rowAccessPolicies.update in place.
BQ_BASE="$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets/$BQ_DATASET"
check "BigQuery routine created" \
  "$BQ_BASE/routines/$BQ_ROUTINE" \
  "\"routineReference\":{\"datasetId\":\"$BQ_DATASET\",\"projectId\":\"$PROJECT\",\"routineId\":\"$BQ_ROUTINE\"}" \
  '"routineType":"SCALAR_FUNCTION"' '"language":"SQL"' \
  '"definitionBody":"x * 2"'
check "BigQuery routine listed" \
  "$BQ_BASE/routines" '"routines"' "\"routineId\":\"$BQ_ROUTINE\""
check "BigQuery row access policy created" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY" \
  "\"policyId\":\"$BQ_POLICY\"" '"filterPredicate":"region = \"EU\""'
check "BigQuery row access policy listed" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies" \
  '"rowAccessPolicies"' "\"policyId\":\"$BQ_POLICY\""
# grantees is input-only in the Discovery schema; real GCP never echoes it.
check_absent "BigQuery row access policy does not echo grantees" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY" 'grantees' 'compat@example.com'

# ── BigQuery resource IAM (TF1-1) ────────────────────────────────────────────
# google_bigquery_dataset_iam_member drives the legacy `access` ACL (the
# provider reads it with ?accessPolicyVersion=3 and PATCHes the whole list);
# google_bigquery_table_iam_member drives the tables.* IAM mixin.
check "BigQuery dataset IAM member is in the access ACL" \
  "$BQ_BASE?accessPolicyVersion=3" \
  '"role":"roles/bigquery.dataViewer"' '"userByEmail":"tf-dataset@example.com"'
check_post "BigQuery table IAM policy binding" \
  "$BQ_BASE/tables/$BQ_TABLE:getIamPolicy" \
  '"role":"roles/bigquery.dataViewer"' '"user:tf-table@example.com"' '"version":1'
check_post_body "BigQuery table testIamPermissions grants the request" \
  "$BQ_BASE/tables/$BQ_TABLE:testIamPermissions" \
  '{"permissions":["bigquery.tables.get","bigquery.tables.getData"]}' \
  '"permissions":["bigquery.tables.get","bigquery.tables.getData"]'

# ── Cloud DNS zone IAM (TF1-3) ───────────────────────────────────────────────
# google_dns_managed_zone_iam_member drives the managedZones IAM mixin
# (dns/v1/{+resource}:getIamPolicy). The provider read-modify-writes the policy
# with the etag it read back, so the stored binding plus a stable etag are what
# let the next plan converge; run.sh asserts the binding and the permission echo.
DNS_BASE="$ENDPOINT/dns/v1/projects/$PROJECT/managedZones"
check "Cloud DNS managed zone created" "$DNS_BASE/$DNS_ZONE" \
  "\"name\":\"$DNS_ZONE\"" '"dnsName":"jaiscloud-tf.example.com."' '"visibility":"public"'
check_post "Cloud DNS zone IAM policy carries the granted binding" \
  "$DNS_BASE/$DNS_ZONE:getIamPolicy" \
  '"role":"roles/dns.admin"' '"user:tf-dns@example.com"' '"etag"'
check_post_body "Cloud DNS zone testIamPermissions grants the request" \
  "$DNS_BASE/$DNS_ZONE:testIamPermissions" \
  '{"permissions":["dns.managedZones.get","dns.managedZones.update"]}' \
  '"permissions":["dns.managedZones.get","dns.managedZones.update"]'
# creationTime is stable across an update: capture it before the update.
BQ_ROUTINE_CT="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$BQ_BASE/routines/$BQ_ROUTINE" 2>/dev/null | sed -n 's/.*"creationTime":"\([0-9]*\)".*/\1/p')"
BQ_POLICY_CT="$(curl -sf -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY" 2>/dev/null | sed -n 's/.*"creationTime":"\([^"]*\)".*/\1/p')"

echo "== $TF: in-place update of the BigQuery routine and row access policy"
TF_VARS_BQ=("${TF_VARS[@]}" -var="routine_body=x * 3" \
  -var="row_access_policy_predicate=region = \"US\"")
tf apply -input=false -auto-approve -no-color "${TF_VARS_BQ[@]}" \
  -var="uptime_display_name=jaiscloud-tf-uptime-v2" >/dev/null
check "BigQuery routine definition updated in place" \
  "$BQ_BASE/routines/$BQ_ROUTINE" '"definitionBody":"x * 3"'
check "BigQuery row access policy predicate updated in place" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY" \
  '"filterPredicate":"region = \"US\""'
check "BigQuery routine update preserved creationTime" \
  "$BQ_BASE/routines/$BQ_ROUTINE" "\"creationTime\":\"$BQ_ROUTINE_CT\""
check "BigQuery row access policy update preserved creationTime" \
  "$BQ_BASE/tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY" \
  "\"creationTime\":\"$BQ_POLICY_CT\""
if tf plan -input=false -no-color -detailed-exitcode "${TF_VARS_BQ[@]}" \
  -var="uptime_display_name=jaiscloud-tf-uptime-v2" >/dev/null 2>&1; then
  printf '  [PASS] re-plan after the BigQuery updates reports no changes\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] re-plan after the BigQuery updates reports changes\n'
  FAIL=$((FAIL + 1))
fi

# ── Cloud Run: in-place services.update (new revision) ────────────────────────
# Changing the container env makes the provider PATCH the service; the template
# change must mint revision 00002 and still re-plan clean. The readiness probe
# is not part of the update, so it must survive the PATCH untouched.
echo "== $TF: in-place update of the Cloud Run service env"
TF_VARS_RUN=("${TF_VARS[@]}" -var="uptime_display_name=jaiscloud-tf-uptime-v2" \
  -var="routine_body=x * 3" -var="row_access_policy_predicate=region = \"US\"" \
  -var="run_env_value=compat-v2")
tf apply -input=false -auto-approve -no-color "${TF_VARS_RUN[@]}" >/dev/null
check "Cloud Run container env updated in place" "$RUN_BASE" \
  '"name":"MODE"' '"value":"compat-v2"'
check "Cloud Run update preserved the readiness probe" "$RUN_BASE" \
  '"readinessProbe"' '"path":"/ready"'
check "Cloud Run update minted the next revision" "$RUN_BASE/revisions" \
  "services/$RUN_SERVICE/revisions/$RUN_SERVICE-00002"
if tf plan -input=false -no-color -detailed-exitcode "${TF_VARS_RUN[@]}" >/dev/null 2>&1; then
  printf '  [PASS] re-plan after the Cloud Run update reports no changes\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] re-plan after the Cloud Run update reports changes\n'
  FAIL=$((FAIL + 1))
fi

echo "== destroy (the virtual cluster only, to prove the pools survive it)"
tf destroy -input=false -auto-approve -no-color \
  -target=google_dataproc_cluster.gke "${TF_VARS[@]}" >/dev/null

# Real GCP does not delete a virtual cluster's node pools: the pool the destroy
# removed the virtual cluster for is still readable through the GKE API.
check "Dataproc node pools survive the virtual-cluster delete" \
  "$GKE_POOLS/jaiscloud-tf-dataproc-pool" '"machineType":"n1-standard-4"'

echo "== destroy"
destroy

# Destroy (above) removed the uptime check; a get must now be NotFound (404).
UPTIME_STATUS="$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$ENDPOINT/v3/projects/$PROJECT/uptimeCheckConfigs/$UPTIME_ID" 2>/dev/null || true)"
if [[ "$UPTIME_STATUS" == "404" ]]; then
  printf '  [PASS] uptime check configuration deleted on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] uptime check get after destroy = HTTP %s, want 404\n' "$UPTIME_STATUS"
  FAIL=$((FAIL + 1))
fi

# Destroy removed the Metastore service too — and with it the catalog listener
# its requested port named.
MS_STATUS="$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$ENDPOINT/v1/$MS_SERVICE" 2>/dev/null || true)"
if [[ "$MS_STATUS" == "404" ]]; then
  printf '  [PASS] Dataproc Metastore service deleted on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] Metastore service get after destroy = HTTP %s, want 404\n' "$MS_STATUS"
  FAIL=$((FAIL + 1))
fi

# Destroy removed the routine and row access policy too; both gets are 404.
for pair in "routines/$BQ_ROUTINE" "tables/$BQ_TABLE/rowAccessPolicies/$BQ_POLICY"; do
  BQ_STATUS="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
    "$BQ_BASE/$pair" 2>/dev/null || true)"
  if [[ "$BQ_STATUS" == "404" ]]; then
    printf '  [PASS] %s deleted on destroy\n' "$pair"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s get after destroy = HTTP %s, want 404\n' "$pair" "$BQ_STATUS"
    FAIL=$((FAIL + 1))
  fi
done

# The table IAM policy row is dropped with the table: a get after destroy is
# NotFound (404), not a resurrected policy for a table that no longer exists.
BQ_IAM_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' \
  "$BQ_BASE/tables/$BQ_TABLE:getIamPolicy" 2>/dev/null || true)"
if [[ "$BQ_IAM_STATUS" == "404" ]]; then
  printf '  [PASS] BigQuery table IAM policy dropped on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] BigQuery table getIamPolicy after destroy = HTTP %s, want 404\n' "$BQ_IAM_STATUS"
  FAIL=$((FAIL + 1))
fi

# The Dataproc cluster IAM policy row is dropped with the cluster: a get after
# destroy is NotFound (404), not a resurrected policy for a deleted cluster.
DP_IAM_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' \
  "$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters/$GCE_CLUSTER_DP:getIamPolicy" 2>/dev/null || true)"
if [[ "$DP_IAM_STATUS" == "404" ]]; then
  printf '  [PASS] Dataproc cluster IAM policy dropped on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] Dataproc cluster getIamPolicy after destroy = HTTP %s, want 404\n' "$DP_IAM_STATUS"
  FAIL=$((FAIL + 1))
fi

# The Cloud DNS zone IAM policy row is dropped with the zone: a get after
# destroy is NotFound (404), so re-creating the zone starts from the empty
# default rather than inheriting grants.
DNS_IAM_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" -d '{}' \
  "$DNS_BASE/$DNS_ZONE:getIamPolicy" 2>/dev/null || true)"
if [[ "$DNS_IAM_STATUS" == "404" ]]; then
  printf '  [PASS] Cloud DNS zone IAM policy dropped on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] Cloud DNS zone getIamPolicy after destroy = HTTP %s, want 404\n' "$DNS_IAM_STATUS"
  FAIL=$((FAIL + 1))
fi

# Destroy removed the regional secret; a get is 404 and the project list no
# longer reports it.
REGIONAL_STATUS="$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" \
  "$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/secrets/jaiscloud-tf-regional-secret" 2>/dev/null || true)"
if [[ "$REGIONAL_STATUS" == "404" ]]; then
  printf '  [PASS] Secret Manager regional secret deleted on destroy\n'
  PASS=$((PASS + 1))
else
  printf '  [FAIL] Secret Manager regional secret get after destroy = HTTP %s, want 404\n' "$REGIONAL_STATUS"
  FAIL=$((FAIL + 1))
fi

# Destroy removed the Cloud Run service; the service and its revisions are 404.
for path in "$RUN_BASE" "$RUN_BASE/revisions"; do
  RUN_STATUS="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" "$path" 2>/dev/null || true)"
  if [[ "$RUN_STATUS" == "404" ]]; then
    printf '  [PASS] %s deleted on destroy\n' "${path#"$ENDPOINT"}"
    PASS=$((PASS + 1))
  else
    printf '  [FAIL] %s get after destroy = HTTP %s, want 404\n' "${path#"$ENDPOINT"}" "$RUN_STATUS"
    FAIL=$((FAIL + 1))
  fi
done

echo "== summary: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
