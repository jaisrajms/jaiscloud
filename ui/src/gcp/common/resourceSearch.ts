/**
 * Resource search for the GCP console's global search.
 *
 * The service catalog alone makes the global search useless once a user knows
 * the name of the thing they want (a bucket, a function, a secret): real Google
 * Cloud searches across the project's resources via Cloud Asset Inventory. The
 * emulator's equivalent is this module: it indexes the resources the console
 * already has list endpoints for, so the search can jump straight to a
 * resource's page.
 *
 * The index is built client-side from the existing per-service UI APIs (the AWS
 * console's `ui/src/lib/resourceLookup.ts` follows the same pattern), which
 * keeps this free of new Go endpoints. Every lister is best-effort: a service
 * that is disabled, still starting or simply fails contributes nothing instead
 * of breaking the whole search.
 */
import * as bigquery from '../../api/gcp/bigquery'
import * as compute from '../../api/gcp/compute'
import * as dataproc from '../../api/gcp/dataproc'
import * as datastore from '../../api/gcp/datastore'
import * as eventarc from '../../api/gcp/eventarc'
import * as firestore from '../../api/gcp/firestore'
import * as functions from '../../api/gcp/functions'
import * as iam from '../../api/gcp/iam'
import * as kms from '../../api/gcp/kms'
import * as logging from '../../api/gcp/logging'
import * as managedkafka from '../../api/gcp/managedkafka'
import * as monitoring from '../../api/gcp/monitoring'
import * as pubsub from '../../api/gcp/pubsub'
import * as resourcemanager from '../../api/gcp/resourcemanager'
import * as run from '../../api/gcp/run'
import * as scheduler from '../../api/gcp/scheduler'
import * as secretmanager from '../../api/gcp/secretmanager'
import * as storage from '../../api/gcp/storage'
import * as tasks from '../../api/gcp/tasks'
import * as workflows from '../../api/gcp/workflows'
import { encodeFirestorePath } from '../firestore/util'

/** Shortest query the resource index answers for; below it the service catalog
 * is enough and no requests are made. */
export const RESOURCE_SEARCH_MIN_LENGTH = 2

/** One console resource the global search can jump to. */
export interface SearchableResource {
  /** Stable identity, unique across services (`service:type:key`, where `key`
   * is the resource's identity inside its service — the name plus whatever
   * scope it lives in). Used as the search option's React key. */
  id: string
  /** Short name shown in the result row and matched first. */
  label: string
  /** Resource type, e.g. `Bucket`. */
  type: string
  /** Console service id from the catalog, e.g. `storage`. */
  service: string
  /** Console route: the resource's detail page, or its list page when the
   * console has no per-resource route for it. */
  path: string
  /** Extra context (location, parent), shown after the service name. */
  detail?: string
}

/** Encode one URL path segment the way the console's detail routes expect. */
const seg = encodeURIComponent

/** Build one index entry. `key` is the resource's identity within `service` +
 * `type` (its name, plus its location/parent when the name alone is scoped), so
 * two resources sharing a display name — a topic and a subscription called
 * `orders` — never share an option id. */
function resource(
  service: string,
  type: string,
  key: string,
  label: string,
  path: string,
  detail?: string,
): SearchableResource {
  return { id: `${service}:${type}:${key}`, label, type, service, path, detail }
}

/**
 * Run listers in parallel and keep whatever succeeds. Used per service so one
 * failed resource type (say, log sinks on a disabled Logging API) still leaves
 * that service's other types searchable.
 */
async function collect(
  ...listers: Array<() => Promise<SearchableResource[]>>
): Promise<SearchableResource[]> {
  const results = await Promise.allSettled(listers.map((list) => list()))
  const out: SearchableResource[] = []
  for (const result of results) {
    if (result.status === 'fulfilled') out.push(...result.value)
  }
  return out
}

// ─── Cloud Storage ────────────────────────────────────────────────────────────

const bucketResources = async () => {
  const { items } = await storage.listBuckets()
  return items.map((bucket) =>
    resource(
      'storage',
      'Bucket',
      bucket.name,
      bucket.name,
      `/gcp/storage/buckets/${seg(bucket.name)}`,
      bucket.location,
    ),
  )
}

// ─── Pub/Sub ─────────────────────────────────────────────────────────────────

const topicResources = async () => {
  const { topics } = await pubsub.listTopics()
  return topics.map((topic) =>
    resource('pubsub', 'Topic', topic.name, topic.name, `/gcp/pubsub/topics/${seg(topic.name)}`),
  )
}

const subscriptionResources = async () => {
  const { subscriptions } = await pubsub.listSubscriptions()
  return subscriptions.map((subscription) =>
    resource(
      'pubsub',
      'Subscription',
      subscription.name,
      subscription.name,
      `/gcp/pubsub/subscriptions/${seg(subscription.name)}`,
      subscription.topic,
    ),
  )
}

// ─── Firestore / Datastore ───────────────────────────────────────────────────

const collectionResources = async () => {
  const { collections } = await firestore.listCollections()
  return collections.map((collection) =>
    resource(
      'firestore',
      'Collection',
      collection.id,
      collection.id,
      `/gcp/firestore/collections/${encodeFirestorePath(collection.id)}`,
    ),
  )
}

const firestoreIndexResources = async () => {
  const { indexes } = await firestore.listIndexes()
  // The console has no per-index route; the indexes page lists them all.
  return indexes.map((index) =>
    resource(
      'firestore',
      'Index',
      index.id || index.name,
      index.id || index.name,
      '/gcp/firestore/indexes',
      index.collectionGroup,
    ),
  )
}

const kindResources = async () => {
  const { kinds } = await datastore.listKinds()
  return kinds.map((kind) =>
    resource('datastore', 'Kind', kind.name, kind.name, `/gcp/datastore/kinds/${seg(kind.name)}`),
  )
}

// ─── Compute ─────────────────────────────────────────────────────────────────

const instanceResources = async () => {
  const { instances } = await compute.listInstances()
  return instances.map((instance) =>
    resource(
      'compute',
      'Instance',
      `${instance.zone}/${instance.name}`,
      instance.name,
      `/gcp/compute/instances/${seg(instance.zone)}/${seg(instance.name)}`,
      instance.zone,
    ),
  )
}

const runServiceResources = async () => {
  const { services } = await run.listServices()
  return services.map((service) =>
    resource(
      'run',
      'Service',
      `${service.region}/${service.id}`,
      service.id,
      `/gcp/run/services/${seg(service.region)}/${seg(service.id)}`,
      service.region,
    ),
  )
}

const functionResources = async () => {
  const { functions: fns } = await functions.listFunctions()
  return fns.map((fn) =>
    resource(
      'functions',
      'Function',
      `${fn.location}/${fn.id}`,
      fn.id,
      `/gcp/functions/${seg(fn.location)}/${seg(fn.id)}`,
      fn.location,
    ),
  )
}

// ─── Integration ─────────────────────────────────────────────────────────────

const schedulerJobResources = async () => {
  const { jobs } = await scheduler.listJobs()
  return jobs.map((job) =>
    resource(
      'scheduler',
      'Job',
      `${job.location}/${job.name}`,
      job.name,
      `/gcp/scheduler/jobs/${seg(job.location)}/${seg(job.name)}`,
      job.location,
    ),
  )
}

const queueResources = async () => {
  const { queues } = await tasks.listQueues()
  return queues.map((queue) =>
    resource(
      'tasks',
      'Queue',
      `${queue.location}/${queue.name}`,
      queue.name,
      `/gcp/tasks/queues/${seg(queue.location)}/${seg(queue.name)}`,
      queue.location,
    ),
  )
}

const workflowResources = async () => {
  const { workflows: found } = await workflows.listWorkflows()
  return found.map((workflow) =>
    resource(
      'workflows',
      'Workflow',
      `${workflow.location}/${workflow.id}`,
      workflow.id,
      `/gcp/workflows/${seg(workflow.location)}/${seg(workflow.id)}`,
      workflow.location,
    ),
  )
}

const triggerResources = async () => {
  const { triggers } = await eventarc.listTriggers()
  return triggers.map((trigger) =>
    resource(
      'eventarc',
      'Trigger',
      `${trigger.location}/${trigger.name}`,
      trigger.name,
      `/gcp/eventarc/triggers/${seg(trigger.location)}/${seg(trigger.name)}`,
      trigger.location,
    ),
  )
}

const channelResources = async () => {
  const { channels } = await eventarc.listChannels()
  return channels.map((channel) =>
    resource(
      'eventarc',
      'Channel',
      `${channel.location}/${channel.name}`,
      channel.name,
      `/gcp/eventarc/channels/${seg(channel.location)}/${seg(channel.name)}`,
      channel.location,
    ),
  )
}

const kafkaClusterResources = async () => {
  const { clusters } = await managedkafka.listClusters()
  return clusters.map((cluster) =>
    resource(
      'managedkafka',
      'Cluster',
      `${cluster.location}/${cluster.id}`,
      cluster.id,
      `/gcp/managedkafka/clusters/${seg(cluster.location)}/${seg(cluster.id)}`,
      cluster.location,
    ),
  )
}

const kafkaTopicResources = async () => {
  const { topics } = await managedkafka.listTopics()
  return topics.map((topic) =>
    resource(
      'managedkafka',
      'Topic',
      `${topic.location}/${topic.cluster}/${topic.id}`,
      topic.id,
      `/gcp/managedkafka/clusters/${seg(topic.location)}/${seg(topic.cluster)}/topics/${seg(topic.id)}`,
      `cluster ${topic.cluster}`,
    ),
  )
}

// ─── Analytics ───────────────────────────────────────────────────────────────

const datasetResources = async () => {
  const { datasets } = await bigquery.listDatasets()
  return datasets.map((dataset) =>
    resource(
      'bigquery',
      'Dataset',
      dataset.datasetId,
      dataset.datasetId,
      `/gcp/bigquery/datasets/${seg(dataset.datasetId)}`,
      dataset.location,
    ),
  )
}

const queryJobResources = async () => {
  const { jobs } = await bigquery.listJobs()
  return jobs.map((job) =>
    resource(
      'bigquery',
      'Job',
      job.jobId,
      job.jobId,
      `/gcp/bigquery/jobs/${seg(job.jobId)}`,
      job.statementType,
    ),
  )
}

const dataprocClusterResources = async () => {
  const { clusters } = await dataproc.listClusters()
  return clusters.map((cluster) =>
    resource(
      'dataproc',
      'Cluster',
      `${cluster.region}/${cluster.id}`,
      cluster.id,
      `/gcp/dataproc/clusters/${seg(cluster.region)}/${seg(cluster.id)}`,
      cluster.region,
    ),
  )
}

const dataprocJobResources = async () => {
  const { jobs } = await dataproc.listJobs()
  return jobs.map((job) =>
    resource(
      'dataproc',
      'Job',
      `${job.region}/${job.id}`,
      job.id,
      `/gcp/dataproc/jobs/${seg(job.region)}/${seg(job.id)}`,
      job.type,
    ),
  )
}

const dataprocTemplateResources = async () => {
  const { templates } = await dataproc.listWorkflowTemplates()
  return templates.map((template) =>
    resource(
      'dataproc',
      'Workflow template',
      `${template.region}/${template.id}`,
      template.id,
      `/gcp/dataproc/workflow-templates/${seg(template.region)}/${seg(template.id)}`,
      template.region,
    ),
  )
}

// ─── Security ────────────────────────────────────────────────────────────────

const serviceAccountResources = async () => {
  const { accounts } = await iam.listServiceAccounts()
  return accounts.map((account) =>
    resource(
      'iam',
      'Service account',
      account.email,
      account.email,
      `/gcp/iam/service-accounts/${seg(account.email)}`,
      account.displayName,
    ),
  )
}

const keyRingResources = async () => {
  // Cloud KMS lists key rings per location and the console has no location
  // enumerator, so the index covers `global` — the location the key-rings page
  // opens on. Non-global rings are reachable by the location field on that page
  // (UI78). A ring without a keyRingId cannot be routed, so it is skipped.
  const { keyRings } = await kms.listKeyRings('global')
  return keyRings.flatMap((keyRing) => {
    const location = keyRing.location || 'global'
    const id = keyRing.keyRingId
    if (!id) return []
    return [
      resource(
        'kms',
        'Key ring',
        `${location}/${id}`,
        id,
        `/gcp/kms/keyrings/${seg(location)}/${seg(id)}`,
        location,
      ),
    ]
  })
}

const secretResources = async () => {
  const { secrets } = await secretmanager.listSecrets()
  return secrets.flatMap((secret) => {
    // The detail route is keyed by the short id; a name-only row would build a
    // route the page cannot resolve.
    const id = secret.secretId
    if (!id) return []
    return [resource('secretmanager', 'Secret', id, id, `/gcp/secretmanager/secrets/${seg(id)}`)]
  })
}

// ─── Operations ──────────────────────────────────────────────────────────────

const logResources = async () => {
  const { logNames } = await logging.listLogs()
  return (logNames ?? []).map((logName) => {
    // Log names are fully-qualified (`projects/p/logs/app`): the short id is
    // what the logs explorer shows, the qualifier stays searchable as detail.
    // The route filters the explorer to that log.
    const marker = logName.indexOf('/logs/')
    const id = marker >= 0 ? logName.slice(marker + '/logs/'.length) : logName
    return resource(
      'logging',
      'Log',
      logName,
      id,
      `/gcp/logging/entries?logName=${encodeURIComponent(logName)}`,
      logName,
    )
  })
}

const logMetricResources = async () => {
  const { metrics } = await logging.listMetrics()
  return (metrics ?? []).map((metric) =>
    resource(
      'logging',
      'Log metric',
      metric.name,
      metric.name,
      '/gcp/logging/metrics',
      metric.description,
    ),
  )
}

const logSinkResources = async () => {
  const { sinks } = await logging.listSinks()
  return (sinks ?? []).map((sink) =>
    resource(
      'logging',
      'Log sink',
      sink.name,
      sink.name,
      '/gcp/logging/sinks',
      sink.destination,
    ),
  )
}

const logExclusionResources = async () => {
  const { exclusions } = await logging.listExclusions()
  return (exclusions ?? []).map((exclusion) =>
    resource(
      'logging',
      'Log exclusion',
      exclusion.name,
      exclusion.name,
      '/gcp/logging/exclusions',
    ),
  )
}

const alertPolicyResources = async () => {
  const { alertPolicies } = await monitoring.listAlertPolicies()
  return (alertPolicies ?? []).map((policy) =>
    resource(
      'monitoring',
      'Alert policy',
      policy.name,
      policy.displayName || policy.name,
      '/gcp/monitoring/alerting',
    ),
  )
}

const notificationChannelResources = async () => {
  const { notificationChannels } = await monitoring.listNotificationChannels()
  return (notificationChannels ?? []).map((channel) =>
    resource(
      'monitoring',
      'Notification channel',
      channel.name,
      channel.displayName || channel.name,
      '/gcp/monitoring/channels',
      channel.type,
    ),
  )
}

// ─── Management ──────────────────────────────────────────────────────────────

const projectResources = async () => {
  const { projects } = await resourcemanager.listProjects()
  return projects.map((project) =>
    resource(
      'resourcemanager',
      'Project',
      project.projectId,
      project.projectId,
      '/gcp/resourcemanager/projects',
      project.displayName,
    ),
  )
}
/** Per-service index loaders, keyed by console service id. Every loader covers
 * that service's top-level listable resources; nested children (objects, crypto
 * keys, tables, documents, tasks) stay one click away on the resource's page. */
export const RESOURCE_LOADERS: Record<string, () => Promise<SearchableResource[]>> = {
  storage: bucketResources,
  pubsub: () => collect(topicResources, subscriptionResources),
  firestore: () => collect(collectionResources, firestoreIndexResources),
  datastore: kindResources,
  compute: instanceResources,
  run: runServiceResources,
  functions: functionResources,
  scheduler: schedulerJobResources,
  tasks: queueResources,
  workflows: workflowResources,
  eventarc: () => collect(triggerResources, channelResources),
  managedkafka: () => collect(kafkaClusterResources, kafkaTopicResources),
  bigquery: () => collect(datasetResources, queryJobResources),
  dataproc: () => collect(dataprocClusterResources, dataprocJobResources, dataprocTemplateResources),
  iam: serviceAccountResources,
  kms: keyRingResources,
  secretmanager: secretResources,
  logging: () =>
    collect(logResources, logMetricResources, logSinkResources, logExclusionResources),
  monitoring: () => collect(alertPolicyResources, notificationChannelResources),
  resourcemanager: projectResources,
}

/** Build the whole index. Service failures are contained: the search shows
 * whatever loaded and silently omits the rest. */
export async function loadResourceIndex(): Promise<SearchableResource[]> {
  return collect(...Object.values(RESOURCE_LOADERS))
}

export interface FilterOptions {
  /** Maximum results from one service, so a single noisy service (log names)
   * cannot crowd out everything else. Defaults to 5. */
  perService?: number
  /** Maximum results overall. Defaults to 20. */
  total?: number
}

/** Case-insensitive match over the resource name, type and detail. Name matches
 * rank first (exact, then prefix, then substring), then type/detail matches;
 * equal ranks sort by name. The service id is deliberately not matched: typing
 * `storage` should surface the Cloud Storage service entry, not every bucket. */
export function filterResources(
  index: SearchableResource[],
  query: string,
  options: FilterOptions = {},
): SearchableResource[] {
  const q = query.trim().toLowerCase()
  if (q.length < RESOURCE_SEARCH_MIN_LENGTH) return []
  const perService = options.perService ?? 5
  const total = options.total ?? 20

  const rank = (candidate: SearchableResource): number | null => {
    const label = candidate.label.toLowerCase()
    if (label === q) return 0
    if (label.startsWith(q)) return 1
    if (label.includes(q)) return 2
    const rest = `${candidate.type} ${candidate.detail ?? ''}`.toLowerCase()
    return rest.includes(q) ? 3 : null
  }

  const scored: Array<{ candidate: SearchableResource; score: number }> = []
  for (const candidate of index) {
    const score = rank(candidate)
    if (score !== null) scored.push({ candidate, score })
  }
  scored.sort(
    (a, b) =>
      a.score - b.score ||
      a.candidate.label.localeCompare(b.candidate.label) ||
      a.candidate.id.localeCompare(b.candidate.id),
  )

  const perServiceCount = new Map<string, number>()
  const out: SearchableResource[] = []
  for (const { candidate } of scored) {
    const count = perServiceCount.get(candidate.service) ?? 0
    if (count >= perService) continue
    perServiceCount.set(candidate.service, count + 1)
    out.push(candidate)
    if (out.length >= total) break
  }
  return out
}
