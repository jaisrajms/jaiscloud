/**
 * Per-service Google Cloud documentation URLs, driving the docs link in
 * `GcpPageHeader`. Unknown services fall back to the cloud-level docs index.
 */
export const SERVICE_DOCS: Record<string, string> = {
  storage: 'https://cloud.google.com/storage/docs',
  pubsub: 'https://cloud.google.com/pubsub/docs',
  firestore: 'https://cloud.google.com/firestore/docs',
  compute: 'https://cloud.google.com/compute/docs',
  run: 'https://cloud.google.com/run/docs',
  functions: 'https://cloud.google.com/functions/docs',
  scheduler: 'https://cloud.google.com/scheduler/docs',
  tasks: 'https://cloud.google.com/tasks/docs',
  workflows: 'https://cloud.google.com/workflows/docs',
  eventarc: 'https://cloud.google.com/eventarc/docs',
  bigquery: 'https://cloud.google.com/bigquery/docs',
  dataproc: 'https://cloud.google.com/dataproc/docs',
  managedkafka: 'https://cloud.google.com/managed-service-for-apache-kafka/docs',
  iam: 'https://cloud.google.com/iam/docs',
  kms: 'https://cloud.google.com/kms/docs',
  secretmanager: 'https://cloud.google.com/secret-manager/docs',
  logging: 'https://cloud.google.com/logging/docs',
  monitoring: 'https://cloud.google.com/monitoring/docs',
  resourcemanager: 'https://cloud.google.com/resource-manager/docs',
}

const FALLBACK_DOCS = 'https://cloud.google.com/docs'

/** Documentation URL for a service descriptor id (falls back for unknown ids). */
export function serviceDocsHref(id: string): string {
  return SERVICE_DOCS[id] ?? FALLBACK_DOCS
}
