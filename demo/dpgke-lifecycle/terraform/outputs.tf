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
