import { listBuckets } from '../../api/gcp/storage'
import { listInstances } from '../../api/gcp/compute'
import { listServices as listRunServices } from '../../api/gcp/run'
import { listFunctions } from '../../api/gcp/functions'
import { listTopics } from '../../api/gcp/pubsub'
import { listCollections } from '../../api/gcp/firestore'
import { listDatasets } from '../../api/gcp/bigquery'
import { listClusters } from '../../api/gcp/dataproc'
import { listClusters as listManagedKafkaClusters } from '../../api/gcp/managedkafka'
import { listWorkflows } from '../../api/gcp/workflows'
import { listJobs as listSchedulerJobs } from '../../api/gcp/scheduler'
import { listQueues } from '../../api/gcp/tasks'
import { listTriggers } from '../../api/gcp/eventarc'
import { listSecrets } from '../../api/gcp/secretmanager'
import { listServiceAccounts } from '../../api/gcp/iam'
import { listSinks } from '../../api/gcp/logging'
import { listAlertPolicies } from '../../api/gcp/monitoring'

/**
 * Counts resources from a list response. Total-bearing responses (`total` /
 * `totalSize`) win; otherwise the named array property is counted, which covers
 * the GCP-style list endpoints that omit a total.
 */
export function resourceCount(data: unknown, arrayKey: string): number {
  if (!data || typeof data !== 'object') return 0
  const record = data as Record<string, unknown>
  if (typeof record.total === 'number') return record.total
  if (typeof record.totalSize === 'number') return record.totalSize
  const value = record[arrayKey]
  return Array.isArray(value) ? value.length : 0
}

/** The primary resource a service links to on the console home. */
export interface ResourceSummarySource {
  /** Service descriptor id (matches `ServiceDescriptor.id`). */
  service: string
  /** Plural resource label, e.g. `Buckets`. */
  label: string
  /** Console route for the resource list. */
  path: string
  /** List key counted when the response carries neither `total` nor `totalSize`. */
  arrayKey: string
  /** Zero-argument list call for this resource. */
  list: () => Promise<unknown>
}

/**
 * One primary resource per service, using the existing list APIs. Services
 * whose list requires a location (Cloud KMS) are intentionally absent — the
 * console home has no location to scope them to.
 */
export const RESOURCE_SUMMARY_SOURCES: ResourceSummarySource[] = [
  { service: 'storage', label: 'Buckets', path: '/gcp/storage/buckets', arrayKey: 'items', list: listBuckets },
  { service: 'compute', label: 'Instances', path: '/gcp/compute/instances', arrayKey: 'instances', list: listInstances },
  { service: 'run', label: 'Services', path: '/gcp/run/services', arrayKey: 'services', list: listRunServices },
  { service: 'functions', label: 'Functions', path: '/gcp/functions', arrayKey: 'functions', list: listFunctions },
  { service: 'pubsub', label: 'Topics', path: '/gcp/pubsub/topics', arrayKey: 'topics', list: listTopics },
  { service: 'firestore', label: 'Collections', path: '/gcp/firestore/collections', arrayKey: 'collections', list: listCollections },
  { service: 'bigquery', label: 'Datasets', path: '/gcp/bigquery/datasets', arrayKey: 'datasets', list: listDatasets },
  { service: 'dataproc', label: 'Clusters', path: '/gcp/dataproc/clusters', arrayKey: 'clusters', list: listClusters },
  { service: 'managedkafka', label: 'Clusters', path: '/gcp/managedkafka/clusters', arrayKey: 'clusters', list: listManagedKafkaClusters },
  { service: 'workflows', label: 'Workflows', path: '/gcp/workflows', arrayKey: 'workflows', list: listWorkflows },
  { service: 'scheduler', label: 'Jobs', path: '/gcp/scheduler/jobs', arrayKey: 'jobs', list: listSchedulerJobs },
  { service: 'tasks', label: 'Queues', path: '/gcp/tasks/queues', arrayKey: 'queues', list: listQueues },
  { service: 'eventarc', label: 'Triggers', path: '/gcp/eventarc/triggers', arrayKey: 'triggers', list: listTriggers },
  { service: 'secretmanager', label: 'Secrets', path: '/gcp/secretmanager/secrets', arrayKey: 'secrets', list: listSecrets },
  { service: 'iam', label: 'Service accounts', path: '/gcp/iam/service-accounts', arrayKey: 'accounts', list: listServiceAccounts },
  { service: 'logging', label: 'Sinks', path: '/gcp/logging/sinks', arrayKey: 'sinks', list: listSinks },
  { service: 'monitoring', label: 'Alerting policies', path: '/gcp/monitoring/alerting', arrayKey: 'alertPolicies', list: listAlertPolicies },
]

/** Registry entries whose service is wired into the running build. */
export function summarySourcesFor(serviceIds: string[]): ResourceSummarySource[] {
  const ids = new Set(serviceIds)
  return RESOURCE_SUMMARY_SOURCES.filter((source) => ids.has(source.service))
}
