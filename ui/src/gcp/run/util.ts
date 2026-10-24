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
