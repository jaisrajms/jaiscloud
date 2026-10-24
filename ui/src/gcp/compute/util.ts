/** statusColor maps a Compute Engine instance status to an MUI Chip color. */
export function statusColor(status: string): 'success' | 'warning' | 'default' {
  switch (status) {
    case 'RUNNING':
      return 'success'
    case 'STOPPING':
    case 'PROVISIONING':
    case 'STAGING':
    case 'REPAIRING':
      return 'warning'
    default:
      return 'default'
  }
}

/** shortDate renders an RFC3339 timestamp for display, falling back to the raw
 * value (or an em dash) when it is missing or unparseable. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** asArray coerces an opaque provider value into a typed array. */
export function asArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : []
}

/** asRecord coerces an opaque provider value into a string-keyed record. */
export function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
}

/** labelValue renders a map entry value defensively. */
export function text(value: unknown): string {
  return typeof value === 'string' ? value : value == null ? '' : String(value)
}
