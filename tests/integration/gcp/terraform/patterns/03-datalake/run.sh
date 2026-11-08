#!/usr/bin/env bash
#
# Composition pattern 03 — data lake + GKE/Dataproc (TF4-3).
#
# What: input/output/staging buckets with the job's code + seed data, a BigQuery
#       dataset/table/routine/row-access-policy, a GKE cluster, a
#       Dataproc-on-GKE virtual cluster with two node-pool targets, a Dataproc
#       job on that virtual cluster, and a Dataproc Metastore service +
#       federation with a database-level IAM grant — one root module, one apply.
#
# Contract (patterns/lib.sh): init -> apply -> REST read-back asserts ->
# plan No changes -> (job/virtual-cluster destroy -> pools survive) -> full
# destroy -> post-destroy asserts.
#
# The assertions are the stack, not the apply exit code: the virtual cluster's
# pools must be readable through container.v1, the job must reach DONE, the
# Metastore federation must name the service it was created with, and a second
# plan must be clean. The k3d/Helm/Java half of the customer story is
# demo/dpgke-lifecycle; see README.md here.
#
# Usage:
#   tests/integration/gcp/terraform/patterns/03-datalake/run.sh [ENDPOINT] [PROJECT]
#
# Prefer the Makefile target, which starts/stops the emulator:
#   make test-gcp-terraform-patterns
#   make test-gcp-terraform-pattern PATTERN=03-datalake

set -euo pipefail

PATTERN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATTERN_NAME="03-datalake"
# shellcheck source=../lib.sh
source "$PATTERN_DIR/../lib.sh"

# Run-unique names: the emulator keeps state across patterns in one aggregate
# run, so a fixed name would collide on a second run against the same emulator.
SUFFIX="${JC_TF_DATALAKE_SUFFIX:-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')}"
TF_VARS+=(-var "name_suffix=$SUFFIX")

# GKE is routed by host, not by path: the provider keeps only the scheme+host
# of container_custom_endpoint, so the driver would otherwise reach the
# emulator through its default endpoint's host. lib.sh emits the host-prefixed
# endpoint from terraform-endpoints.yaml; this check turns a missing host alias
# into a clear setup failure instead of a provider 404.
CONTAINER_HOST="$(printf '%s' "$ENDPOINT" | sed -E 's#^https?://##; s#[:/].*##')"
[[ "$CONTAINER_HOST" == container.* ]] || CONTAINER_HOST="container.$CONTAINER_HOST"
if ! getent hosts "$CONTAINER_HOST" >/dev/null 2>&1; then
  echo "ERROR: $CONTAINER_HOST does not resolve — the GKE part of this pattern needs a container.* host alias" >&2
  echo "       (add e.g. '127.0.0.1 container.localhost' to the hosts file, or pass a resolvable ENDPOINT)" >&2
  exit 1
fi

GKE_PARENT="$ENDPOINT/container/v1/projects/$PROJECT/locations/$REGION/clusters"
DP_PARENT="$ENDPOINT/v1/projects/$PROJECT/regions/$REGION/clusters"
MS_PARENT="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/services"
FED_PARENT="$ENDPOINT/v1/projects/$PROJECT/locations/$REGION/federations"
BQ_PARENT="$ENDPOINT/bigquery/v2/projects/$PROJECT/datasets"

init_phase
apply_phase "apply (buckets + BigQuery + GKE + virtual cluster + job + Metastore)"
assert_no_api_errors

echo "== read-back (suffix $SUFFIX)"
IN_BUCKET="$(tf_output input_bucket)"
OUT_BUCKET="$(tf_output output_bucket)"
STAGING_BUCKET="$(tf_output staging_bucket)"
JOB_CODE="$(tf_output job_code_object)"
JOB_INPUT="$(tf_output job_input_object)"
BQ_DATASET="$(tf_output bq_dataset)"
BQ_TABLE="$(tf_output bq_table)"
BQ_ROUTINE="$(tf_output bq_routine)"
BQ_POLICY="$(tf_output bq_row_access_policy)"
GKE_CLUSTER="$(tf_output gke_cluster)"
DP_CLUSTER="$(tf_output dataproc_cluster)"
CONTROLLER_POOL="$(tf_output controller_pool)"
SPARK_POOL="$(tf_output spark_pool)"
DP_NAMESPACE="$(tf_output dataproc_namespace)"
DP_JOB="$(tf_output dataproc_job)"
MS_SERVICE="$(tf_output metastore_service)"
MS_FEDERATION="$(tf_output metastore_federation)"
JOB_SA="$(tf_output job_service_account)"

# ── Cloud Storage: the lake ─────────────────────────────────────────────────
check "the input bucket carries its role label" \
  "$ENDPOINT/storage/v1/b/$IN_BUCKET" "\"name\":\"$IN_BUCKET\"" '"role":"input"' \
  '"uniformBucketLevelAccess":{"enabled":true}'
check "the output bucket carries its role label" \
  "$ENDPOINT/storage/v1/b/$OUT_BUCKET" "\"name\":\"$OUT_BUCKET\"" '"role":"output"' \
  '"uniformBucketLevelAccess":{"enabled":true}'
check "the staging bucket carries its role label" \
  "$ENDPOINT/storage/v1/b/$STAGING_BUCKET" "\"name\":\"$STAGING_BUCKET\"" '"role":"staging"' \
  '"uniformBucketLevelAccess":{"enabled":true}'
# Object names carry a slash; the GCS object GET path encodes it.
JOB_CODE_PATH="${JOB_CODE//\//%2F}"
JOB_INPUT_PATH="${JOB_INPUT//\//%2F}"
check "the job code object exists in the input bucket" \
  "$ENDPOINT/storage/v1/b/$IN_BUCKET/o/$JOB_CODE_PATH" \
  "\"name\":\"$JOB_CODE\"" "\"bucket\":\"$IN_BUCKET\""
check "the seed data object exists in the input bucket" \
  "$ENDPOINT/storage/v1/b/$IN_BUCKET/o/$JOB_INPUT_PATH" \
  "\"name\":\"$JOB_INPUT\"" "\"bucket\":\"$IN_BUCKET\""

# ── BigQuery: the catalog ───────────────────────────────────────────────────
BQ_DATASET_URL="$BQ_PARENT/$BQ_DATASET"
BQ_TABLE_URL="$BQ_DATASET_URL/tables/$BQ_TABLE"
check "the BigQuery dataset reads back its location and description" \
  "$BQ_DATASET_URL" "\"datasetReference\":{" "\"datasetId\":\"$BQ_DATASET\"" \
  '"location":"US"' '"jaiscloud terraform data lake composition (TF4-3)"' \
  '"env":"compat-test"'
check "the BigQuery table carries its two-column schema" \
  "$BQ_TABLE_URL" "\"tableReference\":{" "\"tableId\":\"$BQ_TABLE\"" \
  '"name":"region"' '"name":"amount"' '"type":"STRING"' '"type":"INT64"' \
  "\"id\":\"$PROJECT:$BQ_DATASET.$BQ_TABLE\""
check "the BigQuery routine is a SQL scalar function" \
  "$BQ_DATASET_URL/routines/$BQ_ROUTINE" "\"routineReference\":{" \
  "\"routineId\":\"$BQ_ROUTINE\"" '"routineType":"SCALAR_FUNCTION"' \
  '"language":"SQL"' '"definitionBody":"LOWER(region)"'
check "the row access policy is attached to the table" \
  "$BQ_TABLE_URL/rowAccessPolicies/$BQ_POLICY" "\"policyId\":\"$BQ_POLICY\"" \
  '"filterPredicate":"region = '\''us-central1'\''"'

# ── GKE: the cluster the virtual cluster targets ────────────────────────────
GKE_CLUSTER_URL="$GKE_PARENT/$GKE_CLUSTER"
GKE_POOLS="$GKE_CLUSTER_URL/nodePools"
check "the GKE cluster read renders the nested response messages" \
  "$GKE_CLUSTER_URL" "\"name\":\"$GKE_CLUSTER\"" \
  '"legacyAbac":{"enabled":false}' \
  '"shieldedNodes":{"enabled":true}' \
  '"defaultMaxPodsConstraint":{"maxPodsPerNode":"110"}' \
  "\"workloadIdentityConfig\":{\"workloadPool\":\"$PROJECT.svc.id.goog\"}"

# ── Dataproc: the virtual cluster and the two reconciled pools ──────────────
DP_CLUSTER_URL="$DP_PARENT/$DP_CLUSTER"
check "the virtual cluster is RUNNING and names both pool targets" \
  "$DP_CLUSTER_URL" "\"clusterName\":\"$DP_CLUSTER\"" '"status":{"state":"RUNNING",' \
  "\"gkeClusterTarget\":\"projects/$PROJECT/locations/$REGION/clusters/$GKE_CLUSTER\"" \
  "$CONTROLLER_POOL" "$SPARK_POOL" \
  "\"kubernetesNamespace\":\"$DP_NAMESPACE\"" \
  "\"stagingBucket\":\"$STAGING_BUCKET\""
check "the virtual cluster is attached to the Metastore service" \
  "$DP_CLUSTER_URL" '"auxiliaryServicesConfig":{' '"metastoreConfig":{' \
  "\"dataprocMetastoreService\":\"projects/$PROJECT/locations/$REGION/services/$MS_SERVICE\""
check_absent "the virtual cluster read omits input-only nodePoolConfig" \
  "$DP_CLUSTER_URL" "nodePoolConfig"
check "the controller pool is readable through container.v1" \
  "$GKE_POOLS/$CONTROLLER_POOL" "\"name\":\"$CONTROLLER_POOL\"" \
  '"machineType":"n1-standard-4"' '"minNodeCount":1' '"maxNodeCount":3' \
  "\"goog-dataproc-cluster-name\":\"$DP_CLUSTER\""
check "the spark pool carries the service-created pool defaults" \
  "$GKE_POOLS/$SPARK_POOL" "\"name\":\"$SPARK_POOL\"" \
  '"minNodeCount":1' '"maxNodeCount":10'

# ── Dataproc: the job the provider submitted ────────────────────────────────
JOB_URL="$ENDPOINT/v1/$DP_JOB"
check "the submitted job is DONE on the virtual cluster" \
  "$JOB_URL" "\"clusterName\":\"$DP_CLUSTER\"" '"status":{"state":"DONE",' \
  "\"mainPythonFileUri\":\"gs://$IN_BUCKET/$JOB_CODE\"" \
  "\"args\":[\"gs://$IN_BUCKET/$JOB_INPUT\",\"gs://$OUT_BUCKET/counts\"]" \
  '"env":"compat-test"'

# ── Dataproc Metastore: the catalog and its federation ──────────────────────
MS_URL="$MS_PARENT/$MS_SERVICE"
FED_URL="$FED_PARENT/$MS_FEDERATION"
check "the Metastore service is ACTIVE with its Hive version" \
  "$MS_URL" "\"name\":\"projects/$PROJECT/locations/$REGION/services/$MS_SERVICE\"" \
  '"state":"ACTIVE"' '"tier":"DEVELOPER"' \
  '"hiveMetastoreConfig":{' '"version":"3.1.2"' '"env":"compat-test"'
# backendMetastores is a map keyed by rank in the Discovery document (not a
# repeated list), and BackendMetastore has no rank member: the provider's
# `rank` HCL attribute becomes the map key.
check "the federation names the Metastore service as its backend" \
  "$FED_URL" "\"name\":\"projects/$PROJECT/locations/$REGION/federations/$MS_FEDERATION\"" \
  '"state":"ACTIVE"' '"version":"3.1.2"' \
  "\"backendMetastores\":{\"0\":{" \
  "\"name\":\"projects/$PROJECT/locations/$REGION/services/$MS_SERVICE\"" \
  '"metastoreType":"DATAPROC_METASTORE"'
check_absent "the federation backend carries no rank member (the map key is the rank)" \
  "$FED_URL" '"rank"'
check "the service account exists" \
  "$ENDPOINT/v1/projects/$PROJECT/serviceAccounts/$JOB_SA" "$JOB_SA"
check_post "the database IAM policy carries the job account's grant" \
  "$MS_URL/databases/default:getIamPolicy" \
  'roles/metastore.admin' "$JOB_SA"

plan_no_changes "re-plan of the data-lake stack reports no changes"

# ── Targeted teardown: the job and the virtual cluster, not the pools ───────
# Real Dataproc does not own the node pools it creates in a customer's GKE
# cluster, so deleting the virtual cluster must leave them readable; only the
# GKE cluster delete reclaims them. Destroy the job first so the job record
# does not outlive the cluster it runs on.
echo "== targeted destroy (job, then virtual cluster)"
targeted_destroy "the job" -target=google_dataproc_job.wordcount
check_status "the job record is gone after its destroy" "$JOB_URL" 404

targeted_destroy "the virtual cluster" -target=google_dataproc_cluster.datalake
check_status "the virtual cluster is gone after its destroy" "$DP_CLUSTER_URL" 404
check "the controller pool survives the virtual-cluster delete" \
  "$GKE_POOLS/$CONTROLLER_POOL" '"machineType":"n1-standard-4"'
check "the spark pool survives the virtual-cluster delete" \
  "$GKE_POOLS/$SPARK_POOL" '"minNodeCount":1'

destroy_phase

echo "== post-destroy (the stack is gone, nothing orphaned)"
check_status "the GKE cluster is gone" "$GKE_CLUSTER_URL" 404
check_status "the controller pool is gone with its GKE cluster" \
  "$GKE_POOLS/$CONTROLLER_POOL" 404
check_status "the input bucket is gone" "$ENDPOINT/storage/v1/b/$IN_BUCKET" 404
check_status "the output bucket is gone" "$ENDPOINT/storage/v1/b/$OUT_BUCKET" 404
check_status "the staging bucket is gone" "$ENDPOINT/storage/v1/b/$STAGING_BUCKET" 404
check_status "the BigQuery dataset is gone" "$BQ_DATASET_URL" 404
check_status "the Metastore service is gone" "$MS_URL" 404
check_status "the Metastore federation is gone" "$FED_URL" 404
check_post_absent "the database IAM grant is gone with its service" \
  "$MS_URL/databases/default:getIamPolicy" "$JOB_SA"

# List-level orphan checks: a deleted name must not linger in the collection.
check_absent "no Dataproc cluster orphan remains" "$DP_PARENT" "$DP_CLUSTER"
check_absent "no Metastore service orphan remains" "$MS_PARENT" "$MS_SERVICE"
check_absent "no Metastore federation orphan remains" "$FED_PARENT" "$MS_FEDERATION"
check_absent "no BigQuery dataset orphan remains" "$BQ_PARENT" "$BQ_DATASET"

summary
