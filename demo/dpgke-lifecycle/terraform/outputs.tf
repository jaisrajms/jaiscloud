# What the later demo legs read.
#
# These are the values a job or a chart would template: where the clusters are,
# which buckets to read and write, which service account to run as, and which
# Spark release the virtual cluster pins. Keeping them here means nothing
# downstream has to know an id or a derived bucket name.

output "project" {
  description = "Project the clusters live in."
  value       = var.project
}

output "region" {
  description = "Region of the Dataproc virtual cluster."
  value       = var.region
}

output "gke_location" {
  description = "Zone or region of the GKE cluster (whatever var.gke_location was)."
  value       = var.gke_location
}

output "storage_project" {
  description = "Project that owns the three buckets; equal to var.project unless storage_project was overridden."
  value       = local.storage_project
}

output "gke_cluster_name" {
  description = "Name of the GKE cluster."
  value       = google_container_cluster.gke.name
}

output "gke_cluster_target" {
  description = "Full resource name of the GKE cluster, as Dataproc's gkeClusterTarget takes it."
  value       = local.gke_cluster_target
}

output "dataproc_cluster_name" {
  description = "Name of the Dataproc virtual cluster."
  value       = google_dataproc_cluster.gke.name
}

output "namespace" {
  description = "Kubernetes namespace Dataproc creates the Spark workload in."
  value       = var.namespace
}

output "buckets" {
  description = "The three bucket names, keyed input/output/staging."
  value       = local.buckets
}

output "service_account_email" {
  description = "Email of the service account the job runs as."
  value       = google_service_account.job.email
}

output "spark_release" {
  description = "Fully-qualified Spark component version pinned on the virtual cluster."
  value       = var.spark_release
}

# The job leg. The id/name are what a submitter (or the gate) needs to address
# the job; state and the driver output URI are the provider's read-back of the
# resource, so asserting them proves google_dataproc_job's read path, not just
# that the API accepted the submit.
output "job_id" {
  description = "Server-assigned Dataproc job id (reference.jobId)."
  value       = google_dataproc_job.wordcount.reference[0].job_id
}

output "job_name" {
  description = "Full resource name of the job (projects/{p}/regions/{r}/jobs/{id})."
  value       = google_dataproc_job.wordcount.id
}

output "job_state" {
  description = "Job state as the provider last read it; DONE once the mock executor settles."
  value       = google_dataproc_job.wordcount.status[0].state
}

output "job_placement_cluster_name" {
  description = "Virtual cluster name the job was placed on."
  value       = google_dataproc_job.wordcount.placement[0].cluster_name
}

output "job_placement_cluster_uuid" {
  description = "Output-only cluster UUID Dataproc captured when the job was submitted."
  value       = google_dataproc_job.wordcount.placement[0].cluster_uuid
}

output "job_driver_output_resource_uri" {
  description = "gs:// URI of the job's driver stdout, allocated in the staging bucket."
  value       = google_dataproc_job.wordcount.driver_output_resource_uri
}

output "job_main_python_file_uri" {
  description = "gs:// URI of the uploaded PySpark file the job runs."
  value       = google_dataproc_job.wordcount.pyspark_config[0].main_python_file_uri
}

output "job_input_uri" {
  description = "gs:// URI of the seed object the job reads."
  value       = "gs://${google_storage_bucket.input.name}/${google_storage_bucket_object.job_input.name}"
}

output "job_output_uri" {
  description = "gs:// prefix the job writes its word count under."
  value       = "gs://${google_storage_bucket.output.name}/${var.job_output_prefix}"
}
