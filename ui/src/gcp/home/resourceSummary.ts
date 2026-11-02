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
import { listAlertPolicies, listMetricDescriptors } from '../../api/gcp/monitoring'
import { listChannels } from '../../api/gcp/eventarc'
import { listSubscriptions } from '../../api/gcp/pubsub'
import { listAllKeyRings } from '../../api/gcp/kms'

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

/** A secondary collection counted alongside a service's primary resource. */
export interface ResourceSummaryExtra {
  /** Plural resource label, e.g. `Subscriptions`. */
  label: string
  /** List key counted when the response carries neither `total` nor `totalSize`. */
  arrayKey: string
  /** Zero-argument list call for this resource. */
  list: () => Promise<unknown>
  /** Further collections of the same service the home also counts. */
  extras?: ResourceSummaryExtra[]
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
  /** Further collections of the same service the home also counts. */
  extras?: ResourceSummaryExtra[]
}

/**
 * One primary resource per service, using the existing list APIs. Cloud KMS's
 * primary list is the location-less aggregate route, which the console serves
 * for exactly this purpose.
 */
export const RESOURCE_SUMMARY_SOURCES: ResourceSummarySource[] = [
  { service: 'storage', label: 'Buckets', path: '/gcp/storage/buckets', arrayKey: 'items', list: listBuckets },
  { service: 'compute', label: 'Instances', path: '/gcp/compute/instances', arrayKey: 'instances', list: listInstances },
  { service: 'run', label: 'Services', path: '/gcp/run/services', arrayKey: 'services', list: listRunServices },
  { service: 'functions', label: 'Functions', path: '/gcp/functions', arrayKey: 'functions', list: listFunctions },
  {
    service: 'pubsub',
    label: 'Topics',
    path: '/gcp/pubsub/topics',
    arrayKey: 'topics',
    list: listTopics,
    extras: [{ label: 'Subscriptions', arrayKey: 'subscriptions', list: () => listSubscriptions() }],
  },
  { service: 'firestore', label: 'Collections', path: '/gcp/firestore/collections', arrayKey: 'collections', list: listCollections },
  { service: 'bigquery', label: 'Datasets', path: '/gcp/bigquery/datasets', arrayKey: 'datasets', list: listDatasets },
  { service: 'dataproc', label: 'Clusters', path: '/gcp/dataproc/clusters', arrayKey: 'clusters', list: listClusters },
  { service: 'managedkafka', label: 'Clusters', path: '/gcp/managedkafka/clusters', arrayKey: 'clusters', list: listManagedKafkaClusters },
  { service: 'workflows', label: 'Workflows', path: '/gcp/workflows', arrayKey: 'workflows', list: listWorkflows },
  { service: 'scheduler', label: 'Jobs', path: '/gcp/scheduler/jobs', arrayKey: 'jobs', list: listSchedulerJobs },
  { service: 'tasks', label: 'Queues', path: '/gcp/tasks/queues', arrayKey: 'queues', list: listQueues },
  {
    service: 'eventarc',
    label: 'Triggers',
    path: '/gcp/eventarc/triggers',
    arrayKey: 'triggers',
    list: listTriggers,
    extras: [{ label: 'Channels', arrayKey: 'channels', list: listChannels }],
  },
  { service: 'kms', label: 'Key rings', path: '/gcp/kms/keyrings', arrayKey: 'keyRings', list: listAllKeyRings },
  { service: 'secretmanager', label: 'Secrets', path: '/gcp/secretmanager/secrets', arrayKey: 'secrets', list: listSecrets },
  { service: 'iam', label: 'Service accounts', path: '/gcp/iam/service-accounts', arrayKey: 'accounts', list: listServiceAccounts },
  { service: 'logging', label: 'Sinks', path: '/gcp/logging/sinks', arrayKey: 'sinks', list: listSinks },
  {
    service: 'monitoring',
    label: 'Alerting policies',
    path: '/gcp/monitoring/alerting',
    arrayKey: 'alertPolicies',
    list: listAlertPolicies,
    extras: [
      {
        label: 'Metric descriptors',
        arrayKey: 'metricDescriptors',
        list: () => listMetricDescriptors(),
      },
    ],
  },
]

/** Registry entries whose service is wired into the running build. */
export function summarySourcesFor(serviceIds: string[]): ResourceSummarySource[] {
  const ids = new Set(serviceIds)
  return RESOURCE_SUMMARY_SOURCES.filter((source) => ids.has(source.service))
}

/** One home tile: a primary resource or one of a service's extras. */
export interface SummaryRow {
  service: string
  label: string
  path: string
  arrayKey: string
  list: () => Promise<unknown>
}

/** Flattens the wired services into renderable home tiles (primary + extras). */
export function summaryRowsFor(serviceIds: string[]): SummaryRow[] {
  return summarySourcesFor(serviceIds).flatMap((source) => [
    { service: source.service, label: source.label, path: source.path, arrayKey: source.arrayKey, list: source.list },
    ...(source.extras ?? []).map((extra) => ({
      service: source.service,
      label: extra.label,
      path: source.path,
      arrayKey: extra.arrayKey,
      list: extra.list,
    })),
  ])
}
