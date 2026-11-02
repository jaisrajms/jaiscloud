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

/** One editable field of a Dataproc job type body. */
export interface JobTypeField {
  key: string
  label: string
  hint?: string
  required?: boolean
  /** Renders a textarea; the value is split into a string array. */
  multiline?: boolean
}

/**
 * JOB_TYPE_FIELDS is the dataproc.v1 oneof job bodies the core accepts, with
 * the fields the submit form offers. Only Spark-family jobs run in the
 * emulator; the others are still selectable because the API accepts them and
 * the core reports the run outcome.
 */
export const JOB_TYPE_FIELDS: Record<string, JobTypeField[]> = {
  sparkJob: [
    { key: 'mainClass', label: 'Main class', hint: 'com.example.Main' },
    { key: 'mainJarFileUri', label: 'Main jar URI', hint: 'gs://bucket/app.jar' },
    { key: 'jarFileUris', label: 'Additional jars (comma or line separated)' },
    { key: 'fileUris', label: 'Additional files (comma or line separated)' },
  ],
  pysparkJob: [
    { key: 'mainPythonFileUri', label: 'Main Python file URI', required: true },
    { key: 'pythonFileUris', label: 'Additional Python files (comma or line separated)' },
    { key: 'jarFileUris', label: 'Additional jars (comma or line separated)' },
  ],
  sparkSqlJob: [
    { key: 'queryList', label: 'Queries (one per line)', multiline: true, required: true },
  ],
  sparkRJob: [{ key: 'mainRFileUri', label: 'Main R file URI', required: true }],
  hadoopJob: [
    { key: 'mainClass', label: 'Main class' },
    { key: 'mainJarFileUri', label: 'Main jar URI' },
  ],
  hiveJob: [{ key: 'queryList', label: 'Queries (one per line)', multiline: true }],
  pigJob: [{ key: 'queryList', label: 'Queries (one per line)', multiline: true }],
}

/** JOB_TYPE_OPTIONS is the select-order list of job types. */
export const JOB_TYPE_OPTIONS = Object.keys(JOB_TYPE_FIELDS)

/** splitList splits comma/newline separated input into trimmed values. */
export function splitList(value: string): string[] {
  return value
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

/** splitLines splits newline separated input into trimmed values (no commas:
 * a SQL query or a job argument may legitimately contain one). */
export function splitLines(value: string): string[] {
  return value
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean)
}

/**
 * parseKeyValueLines parses `key=value` lines (blank lines and `#` comments
 * ignored) into an object. Throws on a line without `=`.
 */
export function parseKeyValueLines(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const at = line.indexOf('=')
    if (at <= 0) throw new Error(`Expected key=value, got "${line}"`)
    out[line.slice(0, at).trim()] = line.slice(at + 1).trim()
  }
  return out
}

/** JobFormInput is the submit-job form's state. */
export interface JobFormInput {
  clusterName: string
  type: string
  fields: Record<string, string>
  args?: string
  properties?: string
}

/**
 * buildJobBody composes the dataproc.v1.Job object the submit endpoint takes:
 * placement, the selected oneof type body, optional args and properties.
 */
export function buildJobBody(input: JobFormInput): Record<string, unknown> {
  const typeBody: Record<string, unknown> = {}
  for (const field of JOB_TYPE_FIELDS[input.type] ?? []) {
    const raw = (input.fields[field.key] ?? '').trim()
    if (!raw) continue
    typeBody[field.key] = field.multiline
      ? splitLines(raw)
      : /FileUris$/.test(field.key)
        ? splitList(raw)
        : raw
  }
  if (input.args?.trim()) typeBody.args = input.args.split('\n').map((a) => a.trim()).filter(Boolean)
  if (input.properties?.trim()) {
    typeBody.properties = parseKeyValueLines(input.properties)
  }
  return {
    placement: { clusterName: input.clusterName.trim() },
    [input.type]: typeBody,
  }
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

/**
 * changedJsonPaths returns the dotted leaf paths whose value differs between
 * before and after. Arrays and scalars are compared whole, which is what an
 * update mask needs: a changed worker list is one config.workerConfig path.
 */
export function changedJsonPaths(before: unknown, after: unknown, prefix = ''): string[] {
  if (JSON.stringify(before) === JSON.stringify(after)) return []
  if (isObject(before) && isObject(after)) {
    const keys = new Set([...Object.keys(before), ...Object.keys(after)])
    const out: string[] = []
    for (const key of keys) {
      out.push(...changedJsonPaths(before[key], after[key], prefix ? `${prefix}.${key}` : key))
    }
    return out
  }
  return prefix ? [prefix] : []
}

/**
 * buildClusterUpdateMask composes a cluster PATCH mask from the fields the user
 * touched. An empty result means the form is unchanged, and the caller must not
 * send the patch: an empty mask applies everything in the core.
 */
export function buildClusterUpdateMask(
  labelsChanged: boolean,
  beforeConfig: unknown,
  afterConfig: unknown,
): string[] {
  const mask = labelsChanged ? ['labels'] : []
  mask.push(...changedJsonPaths(beforeConfig ?? {}, afterConfig ?? {}, 'config'))
  return [...new Set(mask)]
}
