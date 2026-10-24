/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

type ChipColor = 'success' | 'default' | 'warning' | 'error' | 'info'

/** clusterStateColor maps a Dataproc cluster state onto an MUI Chip color. */
export function clusterStateColor(state: string): ChipColor {
  switch (state) {
    case 'RUNNING':
      return 'success'
    case 'STOPPED':
    case 'DELETED':
      return 'default'
    case 'ERROR':
      return 'error'
    default:
      // CREATING / UPDATING / STARTING / STOPPING / DELETING
      return 'warning'
  }
}

/** jobStateColor maps a Dataproc job state onto an MUI Chip color. */
export function jobStateColor(state: string): ChipColor {
  switch (state) {
    case 'DONE':
      return 'success'
    case 'RUNNING':
      return 'info'
    case 'ERROR':
    case 'ATTEMPT_FAILURE':
      return 'error'
    case 'CANCELLED':
      return 'default'
    default:
      // PENDING / SETUP_DONE / CANCEL_PENDING / CANCEL_STARTED
      return 'warning'
  }
}

/** jobTypeLabel renders the job type (sparkJob -> Spark) for the list column. */
export function jobTypeLabel(type?: string): string {
  switch (type) {
    case 'sparkJob':
      return 'Spark'
    case 'pysparkJob':
      return 'PySpark'
    case 'sparkSqlJob':
      return 'Spark SQL'
    case 'sparkRJob':
      return 'SparkR'
    case 'hadoopJob':
      return 'Hadoop'
    case 'hiveJob':
      return 'Hive'
    case 'pigJob':
      return 'Pig'
    case undefined:
    case '':
      return '—'
    default:
      return type
  }
}

/**
 * parseJsonObject parses a raw JSON object typed into the create form. It
 * returns the parsed value or a human-readable error for a non-object/invalid
 * payload.
 */
export function parseJsonObject(text: string): { value?: Record<string, unknown>; error?: string } {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { error: `Invalid JSON: ${(err as Error).message}` }
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { error: 'Config must be a JSON object.' }
  }
  return { value: parsed as Record<string, unknown> }
}

/** Fields collected by the GKE branch of the create-cluster form. */
export interface GkeClusterInput {
  /** `gkeClusterTarget` (the target GKE cluster resource name), if given. */
  gkeClusterTarget: string
  /** `kubernetesNamespace` the Dataproc jobs land in. */
  kubernetesNamespace: string
  /** Optional `nodePoolTarget` resource name. */
  nodePool?: string
}

/**
 * buildVirtualClusterConfig composes a Dataproc-on-GKE VirtualClusterConfig
 * from the structured GKE fields. A target cluster or at least one node pool is
 * required by the API, so an empty gkeClusterConfig is omitted rather than sent;
 * callers validate presence before submitting.
 */
export function buildVirtualClusterConfig(input: GkeClusterInput): Record<string, unknown> {
  const gkeClusterConfig: Record<string, unknown> = {}
  const target = input.gkeClusterTarget.trim()
  if (target) gkeClusterConfig.gkeClusterTarget = target
  const nodePool = input.nodePool?.trim()
  if (nodePool) gkeClusterConfig.nodePoolTarget = [{ nodePool, roles: ['DEFAULT'] }]

  const kubernetesClusterConfig: Record<string, unknown> = {
    kubernetesNamespace: input.kubernetesNamespace.trim(),
    gkeClusterConfig,
  }
  return { kubernetesClusterConfig }
}
