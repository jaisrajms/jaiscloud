# Variable surface for the Dataproc-on-GKE lifecycle module.
#
# Names are deliberately neutral between zonal and regional clusters: the same
# module has to run both shapes, so the location variables are named for what
# they are (a GKE location, a Dataproc region) rather than for a zone.

variable "project" {
  type        = string
  description = "GCP project the GKE and Dataproc clusters are created in."
  default     = "dpgke-demo"
}

variable "region" {
  type        = string
  description = "Region of the Dataproc virtual cluster (a Dataproc cluster is a regional resource even when the GKE target is zonal)."
  default     = "us-central1"
}

# A zone is the real-GCP quickstart shape, and the shape that was broken here
# before the GKP1 fix: Dataproc accepts a region of us-central1 together with a
# target of projects/p/locations/us-central1-a/clusters/<name>, and an unknown
# target is 400 INVALID_ARGUMENT (not 404), so a typo fails the create loudly
# instead of looking like a missing resource. A region works too — then the
# cluster is regional — which is why this is not called `zone`.
variable "gke_location" {
  type        = string
  description = "Location of the GKE cluster: a zone for a zonal cluster (us-central1-a) or a region for a regional one. Must be in var.region. The same value feeds the cluster and the Dataproc gkeClusterTarget."
  default     = "us-central1-a"
}

# DGD9: this emulator's bucket IAM is metadata-only — a grant is stored and
# echoed but never enforced — and requester-pays/userProject is not read at all.
# The cross-project shape is therefore demonstrated, not authorised: the demo
# must not assert that a job in one project can or cannot read a bucket in
# another. The grants in main.tf are the real-GCP shape, not a working control.
variable "storage_project" {
  type        = string
  description = "Project that owns the three buckets; null means var.project. Set it to a second project to stage across projects."
  default     = null
}

# The input/output/staging bucket names are derived from this in main.tf rather
# than listed as variables: GCS bucket names are a global namespace, so a
# per-project prefix is what keeps them unique without a second knob.
variable "bucket_prefix" {
  type        = string
  description = "Prefix for the derived bucket names; null means var.project. Override it when the generated names collide with an existing global bucket."
  default     = null
}

variable "gke_cluster_name" {
  type        = string
  description = "Name of the GKE cluster the virtual cluster targets."
  default     = "dpgke-gke"
}

variable "dataproc_cluster_name" {
  type        = string
  description = "Name of the Dataproc virtual cluster; its two node pools are named from it."
  default     = "dpgke-dataproc"
}

variable "namespace" {
  type        = string
  description = "Kubernetes namespace Dataproc creates the Spark workload in (kubernetesNamespace)."
  default     = "dpgke-demo"
}

# The job's object names are variables so a later leg (Helm/Java) can point at
# the same input and output without re-deriving them, and so the gate's REST
# checks never hardcode a path that only this module knows.
variable "job_input_object" {
  type        = string
  description = "Object name of the seed text the job reads, inside the input bucket."
  default     = "input/words.txt"
}

variable "job_output_prefix" {
  type        = string
  description = "Object prefix, inside the output bucket, the job writes its word count under."
  default     = "wordcount"
}

variable "machine_type" {
  type        = string
  description = "Machine type of the Dataproc-reconciled node pools."
  default     = "e2-small"
}

variable "min_nodes" {
  type        = number
  description = "Autoscaler lower bound for each Dataproc node pool."
  default     = 1
}

variable "max_nodes" {
  type        = number
  description = "Autoscaler upper bound for each Dataproc node pool."
  default     = 3
}

# Aliases are dereferenced at cluster creation on real GCP, so a config carrying
# "3.5" would show a permanent diff against the stored "3.5-dataproc-28". Pin
# the full release string.
variable "spark_release" {
  type        = string
  description = "Spark component version, fully qualified (for example 3.5-dataproc-28). Do not shorten it to an alias such as 3.5."
  default     = "3.5-dataproc-28"
}
