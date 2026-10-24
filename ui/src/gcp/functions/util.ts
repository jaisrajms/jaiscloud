import type { GcpFunction } from '../../api/gcp/functions'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** functionStateColor maps a function status onto an MUI Chip color. */
export function functionStateColor(status?: string): 'success' | 'default' | 'warning' | 'error' {
  switch (status) {
    case 'ACTIVE':
      return 'success'
    case 'FAILED':
    case 'OFFLINE':
      return 'error'
    case 'DEPLOYING':
    case 'DELETING':
      return 'warning'
    default:
      return 'default'
  }
}

/** deliveryStatusColor maps a delivery status onto an MUI Chip color. */
export function deliveryStatusColor(status?: string): 'success' | 'default' | 'warning' | 'error' {
  switch (status) {
    case 'delivered':
      return 'success'
    case 'failed':
      return 'error'
    case 'dead_letter':
      return 'warning'
    case 'pending':
      return 'default'
    default:
      return 'default'
  }
}

/** triggerSummary is a one-line description of a function's trigger. */
export function triggerSummary(fn: GcpFunction): string {
  if (fn.triggerType === 'event') {
    const et = fn.eventTrigger
    if (!et) return 'Event'
    return [et.eventType, et.resource].filter(Boolean).join(' · ') || 'Event'
  }
  return fn.url || 'HTTPS'
}

/** memoryLabel renders the function's memory allocation as "256 MB". */
export function memoryLabel(mb?: number): string {
  return mb && mb > 0 ? `${mb} MB` : '—'
}

/** parseKV parses "key=value" lines into a map, ignoring blank lines. */
export function parseKV(text: string): Record<string, string> | undefined {
  const out: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const eq = trimmed.indexOf('=')
    if (eq <= 0) continue
    out[trimmed.slice(0, eq).trim()] = trimmed.slice(eq + 1).trim()
  }
  return Object.keys(out).length > 0 ? out : undefined
}
