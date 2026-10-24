import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  filterResources,
  loadResourceIndex,
  RESOURCE_SEARCH_MIN_LENGTH,
  type SearchableResource,
} from './resourceSearch'

/**
 * Every UI API module the index reads from, mocked with one list call per
 * resource type, so the tests exercise the real mapping code (labels, routes,
 * the per-service failure isolation) instead of a hand-built index.
 */
const api = vi.hoisted(() => ({
  storage: { listBuckets: vi.fn() },
  pubsub: { listTopics: vi.fn(), listSubscriptions: vi.fn() },
  firestore: { listCollections: vi.fn(), listIndexes: vi.fn() },
  datastore: { listKinds: vi.fn() },
  compute: { listInstances: vi.fn() },
  run: { listServices: vi.fn() },
  functions: { listFunctions: vi.fn() },
  scheduler: { listJobs: vi.fn() },
  tasks: { listQueues: vi.fn() },
  workflows: { listWorkflows: vi.fn() },
  eventarc: { listTriggers: vi.fn(), listChannels: vi.fn() },
  managedkafka: { listClusters: vi.fn(), listTopics: vi.fn() },
  bigquery: { listDatasets: vi.fn(), listJobs: vi.fn() },
  dataproc: { listClusters: vi.fn(), listJobs: vi.fn(), listWorkflowTemplates: vi.fn() },
  iam: { listServiceAccounts: vi.fn() },
  kms: { listKeyRings: vi.fn() },
  secretmanager: { listSecrets: vi.fn() },
  logging: {
    listLogs: vi.fn(),
    listMetrics: vi.fn(),
    listSinks: vi.fn(),
    listExclusions: vi.fn(),
  },
  monitoring: { listAlertPolicies: vi.fn(), listNotificationChannels: vi.fn() },
  resourcemanager: { listProjects: vi.fn() },
}))

vi.mock('../../api/gcp/storage', () => api.storage)
vi.mock('../../api/gcp/pubsub', () => api.pubsub)
vi.mock('../../api/gcp/firestore', () => api.firestore)
vi.mock('../../api/gcp/datastore', () => api.datastore)
vi.mock('../../api/gcp/compute', () => api.compute)
vi.mock('../../api/gcp/run', () => api.run)
vi.mock('../../api/gcp/functions', () => api.functions)
vi.mock('../../api/gcp/scheduler', () => api.scheduler)
vi.mock('../../api/gcp/tasks', () => api.tasks)
vi.mock('../../api/gcp/workflows', () => api.workflows)
vi.mock('../../api/gcp/eventarc', () => api.eventarc)
vi.mock('../../api/gcp/managedkafka', () => api.managedkafka)
vi.mock('../../api/gcp/bigquery', () => api.bigquery)
vi.mock('../../api/gcp/dataproc', () => api.dataproc)
vi.mock('../../api/gcp/iam', () => api.iam)
vi.mock('../../api/gcp/kms', () => api.kms)
vi.mock('../../api/gcp/secretmanager', () => api.secretmanager)
vi.mock('../../api/gcp/logging', () => api.logging)
vi.mock('../../api/gcp/monitoring', () => api.monitoring)
vi.mock('../../api/gcp/resourcemanager', () => api.resourcemanager)

/** One resource of each indexed type, so every loader and route is covered. */
function seedOneOfEach() {
  api.storage.listBuckets.mockResolvedValue({
    items: [{ name: 'two-dogs', location: 'us-central1', versioning: false }],
    total: 1,
  })
  api.pubsub.listTopics.mockResolvedValue({ topics: [{ name: 'orders' }], total: 1 })
  api.pubsub.listSubscriptions.mockResolvedValue({
    subscriptions: [{ name: 'orders-sub', topic: 'orders' }],
    total: 1,
  })
  api.firestore.listCollections.mockResolvedValue({ collections: [{ id: 'users' }], total: 1 })
  api.firestore.listIndexes.mockResolvedValue({
    indexes: [{ name: 'idx', id: 'idx', collectionGroup: 'users', fields: [] }],
    total: 1,
  })
  api.datastore.listKinds.mockResolvedValue({ kinds: [{ name: 'Order' }], total: 1 })
  api.compute.listInstances.mockResolvedValue({
    instances: [{ name: 'vm-1', zone: 'us-central1-a', status: 'RUNNING', machineType: 'e2' }],
    total: 1,
  })
  api.run.listServices.mockResolvedValue({
    services: [{ id: 'web', region: 'us-central1', name: 'web', ready: true }],
    total: 1,
  })
  api.functions.listFunctions.mockResolvedValue({
    functions: [{ id: 'thumb', location: 'us-central1', name: 'thumb' }],
    total: 1,
  })
  api.scheduler.listJobs.mockResolvedValue({
    jobs: [{ name: 'nightly', location: 'us-central1', state: 'ENABLED' }],
    total: 1,
  })
  api.tasks.listQueues.mockResolvedValue({
    queues: [{ name: 'default', location: 'us-central1', state: 'RUNNING' }],
    total: 1,
  })
  api.workflows.listWorkflows.mockResolvedValue({
    workflows: [{ id: 'etl', location: 'us-central1', name: 'etl' }],
    total: 1,
  })
  api.eventarc.listTriggers.mockResolvedValue({
    triggers: [{ name: 'on-upload', location: 'us-central1' }],
    total: 1,
  })
  api.eventarc.listChannels.mockResolvedValue({
    channels: [{ name: 'partner', location: 'us-central1' }],
    total: 1,
  })
  api.managedkafka.listClusters.mockResolvedValue({
    clusters: [{ id: 'k1', location: 'us-central1', name: 'projects/p/locations/us-central1/clusters/k1' }],
    total: 1,
  })
  api.managedkafka.listTopics.mockResolvedValue({
    topics: [
      {
        id: 'events',
        name: 'projects/p/locations/us-central1/clusters/k1/topics/events',
        location: 'us-central1',
        cluster: 'k1',
        partitionCount: 1,
        replicationFactor: 1,
      },
    ],
    total: 1,
  })
  api.bigquery.listDatasets.mockResolvedValue({
    datasets: [{ id: 'sales', datasetId: 'sales', location: 'US' }],
    total: 1,
  })
  api.bigquery.listJobs.mockResolvedValue({
    jobs: [{ id: 'job_1', jobId: 'job_1', statementType: 'SELECT' }],
    total: 1,
  })
  api.dataproc.listClusters.mockResolvedValue({
    clusters: [{ id: 'spark', name: 'spark', region: 'us-central1', status: 'RUNNING' }],
    total: 1,
  })
  api.dataproc.listJobs.mockResolvedValue({
    jobs: [{ id: 'dp-job', name: 'dp-job', region: 'us-central1', status: 'DONE' }],
    total: 1,
  })
  api.dataproc.listWorkflowTemplates.mockResolvedValue({
    templates: [{ id: 'tmpl', name: 'tmpl', region: 'us-central1', version: 1 }],
    total: 1,
  })
  api.iam.listServiceAccounts.mockResolvedValue({
    accounts: [{ name: 'sa', email: 'sa@demo.iam.gserviceaccount.com' }],
    total: 1,
  })
  api.kms.listKeyRings.mockResolvedValue({
    keyRings: [{ name: 'ring', keyRingId: 'ring', location: 'global' }],
    total: 1,
  })
  api.secretmanager.listSecrets.mockResolvedValue({
    secrets: [{ name: 'db-pass', secretId: 'db-pass' }],
    total: 1,
  })
  api.logging.listLogs.mockResolvedValue({ logNames: ['projects/demo/logs/app'] })
  api.logging.listMetrics.mockResolvedValue({
    metrics: [{ name: 'errors', filter: 'severity=ERROR' }],
  })
  api.logging.listSinks.mockResolvedValue({
    sinks: [{ name: 'to-gcs', destination: 'storage.googleapis.com/archive' }],
  })
  api.logging.listExclusions.mockResolvedValue({ exclusions: [{ name: 'noisy', filter: 'x' }] })
  api.monitoring.listAlertPolicies.mockResolvedValue({
    alertPolicies: [{ name: 'policy', displayName: 'High CPU' }],
  })
  api.monitoring.listNotificationChannels.mockResolvedValue({
    notificationChannels: [{ name: 'email', displayName: 'Ops email', type: 'email' }],
  })
  api.resourcemanager.listProjects.mockResolvedValue({
    projects: [{ projectId: 'demo', state: 'ACTIVE', displayName: 'Demo' }],
    total: 1,
  })
}

beforeEach(() => {
  vi.resetAllMocks()
})

describe('loadResourceIndex', () => {
  it('indexes one entry for every top-level resource type', async () => {
    seedOneOfEach()
    const index = await loadResourceIndex()
    expect(index.map((entry) => `${entry.service}:${entry.type}`).sort()).toEqual(
      [
        'bigquery:Dataset',
        'bigquery:Job',
        'compute:Instance',
        'dataproc:Cluster',
        'dataproc:Job',
        'dataproc:Workflow template',
        'datastore:Kind',
        'eventarc:Channel',
        'eventarc:Trigger',
        'firestore:Collection',
        'firestore:Index',
        'functions:Function',
        'iam:Service account',
        'kms:Key ring',
        'logging:Log',
        'logging:Log exclusion',
        'logging:Log metric',
        'logging:Log sink',
        'managedkafka:Cluster',
        'managedkafka:Topic',
        'monitoring:Alert policy',
        'monitoring:Notification channel',
        'pubsub:Subscription',
        'pubsub:Topic',
        'resourcemanager:Project',
        'run:Service',
        'scheduler:Job',
        'secretmanager:Secret',
        'storage:Bucket',
        'tasks:Queue',
        'workflows:Workflow',
      ].sort(),
    )
    // Ids are unique, which the Autocomplete's option keys require.
    const ids = index.map((entry) => entry.id)
    expect(new Set(ids).size).toBe(ids.length)
  })

  it('routes every resource to its console page', async () => {
    seedOneOfEach()
    const index = await loadResourceIndex()
    const routes = Object.fromEntries(
      index.map((entry) => [`${entry.service}:${entry.type}`, entry.path]),
    )
    expect(routes).toEqual({
      'storage:Bucket': '/gcp/storage/buckets/two-dogs',
      'pubsub:Topic': '/gcp/pubsub/topics/orders',
      'pubsub:Subscription': '/gcp/pubsub/subscriptions/orders-sub',
      'firestore:Collection': '/gcp/firestore/collections/users',
      'firestore:Index': '/gcp/firestore/indexes',
      'datastore:Kind': '/gcp/datastore/kinds/Order',
      'compute:Instance': '/gcp/compute/instances/us-central1-a/vm-1',
      'run:Service': '/gcp/run/services/us-central1/web',
      'functions:Function': '/gcp/functions/us-central1/thumb',
      'scheduler:Job': '/gcp/scheduler/jobs/us-central1/nightly',
      'tasks:Queue': '/gcp/tasks/queues/us-central1/default',
      'workflows:Workflow': '/gcp/workflows/us-central1/etl',
      'eventarc:Trigger': '/gcp/eventarc/triggers/us-central1/on-upload',
      'eventarc:Channel': '/gcp/eventarc/channels/us-central1/partner',
      'managedkafka:Cluster': '/gcp/managedkafka/clusters/us-central1/k1',
      'managedkafka:Topic': '/gcp/managedkafka/clusters/us-central1/k1/topics/events',
      'bigquery:Dataset': '/gcp/bigquery/datasets/sales',
      'bigquery:Job': '/gcp/bigquery/jobs/job_1',
      'dataproc:Cluster': '/gcp/dataproc/clusters/us-central1/spark',
      'dataproc:Job': '/gcp/dataproc/jobs/us-central1/dp-job',
      'dataproc:Workflow template': '/gcp/dataproc/workflow-templates/us-central1/tmpl',
      'iam:Service account': '/gcp/iam/service-accounts/sa%40demo.iam.gserviceaccount.com',
      'kms:Key ring': '/gcp/kms/keyrings/global/ring',
      'secretmanager:Secret': '/gcp/secretmanager/secrets/db-pass',
      'logging:Log': '/gcp/logging/entries?logName=projects%2Fdemo%2Flogs%2Fapp',
      'logging:Log metric': '/gcp/logging/metrics',
      'logging:Log sink': '/gcp/logging/sinks',
      'logging:Log exclusion': '/gcp/logging/exclusions',
      'monitoring:Alert policy': '/gcp/monitoring/alerting',
      'monitoring:Notification channel': '/gcp/monitoring/channels',
      'resourcemanager:Project': '/gcp/resourcemanager/projects',
    })
    // Log names keep the full path searchable, but the row shows the short id.
    const log = index.find((entry) => entry.type === 'Log')
    expect(log?.label).toBe('app')
    expect(log?.detail).toBe('projects/demo/logs/app')
  })

  it('keeps same-named resources apart', async () => {
    seedOneOfEach()
    // A topic and a subscription can share a display name, and so can two
    // functions in different regions.
    api.pubsub.listSubscriptions.mockResolvedValue({
      subscriptions: [{ name: 'orders' }],
      total: 1,
    })
    api.functions.listFunctions.mockResolvedValue({
      functions: [
        { id: 'thumb', location: 'us-central1', name: 'thumb' },
        { id: 'thumb', location: 'europe-west1', name: 'thumb' },
      ],
      total: 2,
    })

    const index = await loadResourceIndex()
    const ids = index
      .filter((entry) => entry.label === 'orders' || entry.label === 'thumb')
      .map((entry) => entry.id)
    expect(new Set(ids).size).toBe(ids.length)
  })

  it('omits resources whose id is missing (they cannot be routed)', async () => {
    seedOneOfEach()
    api.kms.listKeyRings.mockResolvedValue({
      keyRings: [{ name: 'unnamed', location: 'global' }],
      total: 1,
    })
    api.secretmanager.listSecrets.mockResolvedValue({ secrets: [{ name: 'unnamed' }], total: 1 })

    const types = (await loadResourceIndex()).map((entry) => `${entry.service}:${entry.type}`)
    expect(types).not.toContain('kms:Key ring')
    expect(types).not.toContain('secretmanager:Secret')
  })

  it('keeps the services that answer when others fail', async () => {
    seedOneOfEach()
    api.storage.listBuckets.mockRejectedValue(new Error('storage disabled'))
    api.pubsub.listSubscriptions.mockRejectedValue(new Error('denied'))

    const index = await loadResourceIndex()
    const types = index.map((entry) => `${entry.service}:${entry.type}`)
    expect(types).not.toContain('storage:Bucket')
    expect(types).not.toContain('pubsub:Subscription')
    // Pub/Sub's other resource type and every other service still load.
    expect(types).toContain('pubsub:Topic')
    expect(types).toContain('functions:Function')
  })
})

describe('filterResources', () => {
  const entries: SearchableResource[] = [
    { id: 'storage:Bucket:a', label: 'orders-archive', type: 'Bucket', service: 'storage', path: '/a' },
    { id: 'storage:Bucket:b', label: 'orders', type: 'Bucket', service: 'storage', path: '/b' },
    { id: 'pubsub:Topic:c', label: 'orders', type: 'Topic', service: 'pubsub', path: '/c' },
    { id: 'pubsub:Topic:d', label: 'orders-sub', type: 'Topic', service: 'pubsub', path: '/d' },
    {
      id: 'compute:Instance:e',
      label: 'vm-1',
      type: 'Instance',
      service: 'compute',
      path: '/e',
      detail: 'us-central1-a',
    },
  ]

  it('answers nothing below the minimum query length', () => {
    expect(RESOURCE_SEARCH_MIN_LENGTH).toBe(2)
    expect(filterResources(entries, '')).toEqual([])
    expect(filterResources(entries, ' o ')).toEqual([])
  })

  it('ranks exact, prefix and substring name matches before metadata matches', () => {
    expect(filterResources(entries, 'orders').map((entry) => entry.label)).toEqual([
      'orders',
      'orders',
      'orders-archive',
      'orders-sub',
    ])
    expect(filterResources(entries, 'orders-archive').map((entry) => entry.label)).toEqual([
      'orders-archive',
    ])
    // Matches outside the name (type, detail) rank last.
    expect(filterResources(entries, 'topic').map((entry) => entry.label)).toEqual([
      'orders',
      'orders-sub',
    ])
    expect(filterResources(entries, 'us-central1').map((entry) => entry.label)).toEqual(['vm-1'])
  })

  it('does not match the console service id', () => {
    // Typing a service name must surface the service entry, not every resource
    // that service owns.
    expect(filterResources(entries, 'pubsub')).toEqual([])
    expect(filterResources(entries, 'storage')).toEqual([])
  })

  it('matches case-insensitively', () => {
    expect(filterResources(entries, 'ORDERS-SUB').map((entry) => entry.id)).toEqual([
      'pubsub:Topic:d',
    ])
  })

  it('caps results per service and overall, keeping the better-ranked ones', () => {
    const many: SearchableResource[] = Array.from({ length: 8 }, (_, i) => ({
      id: `storage:Bucket:b${i}`,
      label: `bucket-${i}`,
      type: 'Bucket',
      service: 'storage',
      path: `/b${i}`,
    }))
    expect(filterResources(many, 'bucket')).toHaveLength(5)
    expect(filterResources(many, 'bucket', { perService: 2 })).toHaveLength(2)
    expect(filterResources(many, 'bucket', { perService: 8, total: 3 })).toHaveLength(3)
  })

  it('returns nothing for a query that matches no field', () => {
    expect(filterResources(entries, 'zzz')).toEqual([])
  })
})
