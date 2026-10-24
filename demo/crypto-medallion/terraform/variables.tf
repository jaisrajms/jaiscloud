# Variable surface for the crypto-medallion infra subset. The demo creates the
# project before Terraform runs (see provider.tf), so `project` is an input.

variable "buckets" {
  type = object({
    bronze      = string
    silver      = string
    gold        = string
    leaderboard = string
    rollup      = string
    capture     = string
    jobs        = string
  })
  description = "GCS bucket names for the medallion layers and demo assets."
  default = {
    bronze      = "crypto-medallion-bronze"
    silver      = "crypto-medallion-silver"
    gold        = "crypto-medallion-gold"
    leaderboard = "crypto-medallion-leaderboard"
    rollup      = "crypto-medallion-rollup"
    capture     = "crypto-medallion-capture"
    jobs        = "crypto-medallion-jobs"
  }
}

variable "enabled_services" {
  type        = list(string)
  description = "APIs the demo enables on the project (Service Usage)."
  default = [
    "storage.googleapis.com",
    "iam.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudkms.googleapis.com",
    "pubsub.googleapis.com",
    "dataproc.googleapis.com",
    "metastore.googleapis.com",
    "managedkafka.googleapis.com",
    "bigquery.googleapis.com",
    "firestore.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "cloudscheduler.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
  ]
}
