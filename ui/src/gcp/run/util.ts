/** lastSegment returns the final '/'-separated segment of a resource name. */
export function lastSegment(name?: string): string {
  if (!name) return ''
  const trimmed = name.replace(/\/+$/, '')
  const i = trimmed.lastIndexOf('/')
  return i >= 0 ? trimmed.slice(i + 1) : trimmed
}

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** firstImage returns the image of the first container in a containers array,
 * which may be absent or malformed in a caller-supplied template. */
export function firstImage(containers: unknown): string {
  if (!Array.isArray(containers) || containers.length === 0) return ''
  const first = containers[0]
  if (first === null || typeof first !== 'object') return ''
  const image = (first as Record<string, unknown>).image
  return typeof image === 'string' ? image : ''
}

/** templatesOf returns the containers array nested under an object's template
 * (a service), or an empty array when absent. */
export function templateContainers(detail: Record<string, unknown> | undefined): unknown {
  const template = detail?.template
  if (template === null || typeof template !== 'object') return undefined
  return (template as Record<string, unknown>).containers
}

/** splitLines splits newline separated input into trimmed values. */
export function splitLines(value: string): string[] {
  return value
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean)
}

/** parseKeyValueLines parses `key=value` lines (blank lines and `#` comments
 * ignored) into an object. Throws on a line without `=`. */
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

/** A structured deploy form. */
export interface ServiceForm {
  image: string
  command?: string
  args?: string
  env?: string
  ports?: string
  cpu?: string
  memory?: string
  serviceAccount?: string
  ingress?: string
}

/**
 * buildServiceBody composes a google.cloud.run.v2.Service body from the deploy
 * form. Empty fields are omitted so the API's defaults apply; image is
 * required by the core.
 */
export function buildServiceBody(form: ServiceForm): Record<string, unknown> {
  const container: Record<string, unknown> = { image: form.image.trim() }
  const command = splitLines(form.command ?? '')
  if (command.length) container.command = command
  const args = splitLines(form.args ?? '')
  if (args.length) container.args = args
  const env = form.env?.trim() ? parseKeyValueLines(form.env) : {}
  if (Object.keys(env).length) {
    container.env = Object.entries(env).map(([name, value]) => ({ name, value }))
  }
  const ports = (form.ports ?? '')
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => ({ containerPort: Number(p) }))
  if (ports.length) container.ports = ports
  const limits: Record<string, string> = {}
  if (form.cpu?.trim()) limits.cpu = form.cpu.trim()
  if (form.memory?.trim()) limits.memory = form.memory.trim()
  if (Object.keys(limits).length) container.resources = { limits }

  const template: Record<string, unknown> = { containers: [container] }
  if (form.serviceAccount?.trim()) template.serviceAccount = form.serviceAccount.trim()

  const body: Record<string, unknown> = { template }
  if (form.ingress?.trim()) body.ingress = form.ingress.trim()
  return body
}

/**
 * serviceUpdateMask derives the top-level field mask for an edit: template and
 * traffic are compared whole (a changed container is one template patch), and
 * any other supplied top-level field that differs is added by name. An empty
 * result means nothing changed and the caller must not send the patch.
 */
export function serviceUpdateMask(
  before: Record<string, unknown> | undefined,
  after: Record<string, unknown>,
): string[] {
  const mask: string[] = []
  for (const key of new Set([...Object.keys(before ?? {}), ...Object.keys(after)])) {
    if (JSON.stringify(before?.[key]) === JSON.stringify(after[key])) continue
    // Rendered system fields are not writable.
    if (['name', 'uid', 'generation', 'observedGeneration', 'etag', 'createTime',
      'updateTime', 'uri', 'latestReadyRevision', 'latestCreatedRevision',
      'terminalCondition', 'conditions', 'reconciling'].includes(key)) continue
    mask.push(key)
  }
  return mask
}

// runSystemFields are the fields the service renderer adds to the stored
// template; they are not writable and must not appear in an update body.
const runSystemFields = [
  'name', 'uid', 'generation', 'observedGeneration', 'etag', 'createTime',
  'updateTime', 'deleteTime', 'uri', 'urls', 'latestReadyRevision',
  'latestCreatedRevision', 'terminalCondition', 'conditions', 'reconciling',
  'trafficStatuses', 'service', '@type',
]

/** editableService clones a service detail without the rendered system fields,
 * so an edit starts from the stored template instead of rebuilding it (which
 * would drop traffic, volumes, probes and other unmodelled fields). */
export function editableService(service: Record<string, unknown>): Record<string, unknown> {
  const clone = JSON.parse(JSON.stringify(service)) as Record<string, unknown>
  for (const key of runSystemFields) delete clone[key]
  return clone
}

/** applyFormToService merges the structured deploy form onto an editable
 * service, preserving fields the form does not model. */
export function applyFormToService(
  service: Record<string, unknown>,
  form: ServiceForm,
): Record<string, unknown> {
  const out = editableService(service)
  const template = (out.template as Record<string, unknown> | undefined) ?? {}
  const containers = Array.isArray(template.containers)
    ? (template.containers as Record<string, unknown>[])
    : []
  const container: Record<string, unknown> = containers[0] ?? {}

  container.image = form.image.trim()
  const command = splitLines(form.command ?? '')
  if (command.length) container.command = command
  else delete container.command
  const args = splitLines(form.args ?? '')
  if (args.length) container.args = args
  else delete container.args

  const env = form.env?.trim() ? parseKeyValueLines(form.env) : {}
  if (Object.keys(env).length) {
    container.env = Object.entries(env).map(([name, value]) => ({ name, value }))
  } else {
    delete container.env
  }

  const ports = (form.ports ?? '')
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => ({ containerPort: Number(p) }))
  if (ports.length) container.ports = ports
  else delete container.ports

  const limits: Record<string, string> = {}
  if (form.cpu?.trim()) limits.cpu = form.cpu.trim()
  if (form.memory?.trim()) limits.memory = form.memory.trim()
  if (Object.keys(limits).length) {
    container.resources = {
      ...((container.resources as Record<string, unknown> | undefined) ?? {}),
      limits,
    }
  } else if (container.resources) {
    delete (container.resources as Record<string, unknown>).limits
  }

  template.containers = containers.length ? [container, ...containers.slice(1)] : [container]
  if (form.serviceAccount?.trim()) template.serviceAccount = form.serviceAccount.trim()
  else delete template.serviceAccount
  out.template = template
  if (form.ingress?.trim()) out.ingress = form.ingress.trim()
  else delete out.ingress
  return out
}

/** parseJsonObject parses a raw JSON object, returning a human-readable error
 * for a non-object or invalid payload. */
export function parseJsonObject(text: string): { value?: Record<string, unknown>; error?: string } {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { error: `Invalid JSON: ${(err as Error).message}` }
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { error: 'Body must be a JSON object.' }
  }
  return { value: parsed as Record<string, unknown> }
}
