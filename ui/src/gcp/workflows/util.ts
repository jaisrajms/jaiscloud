import type { Execution } from '../../api/gcp/workflows'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** workflowStateColor maps a workflow state onto an MUI Chip color. */
export function workflowStateColor(state?: string): 'success' | 'default' | 'warning' | 'error' {
  switch (state) {
    case 'ACTIVE':
      return 'success'
    case 'UNAVAILABLE':
      return 'error'
    default:
      return 'default'
  }
}

/** executionStateColor maps an execution state onto an MUI Chip color. */
export function executionStateColor(state?: string): 'success' | 'default' | 'warning' | 'error' | 'info' {
  switch (state) {
    case 'SUCCEEDED':
      return 'success'
    case 'FAILED':
      return 'error'
    case 'ACTIVE':
      return 'info'
    case 'QUEUED':
      return 'warning'
    case 'CANCELLED':
      return 'default'
    default:
      return 'default'
  }
}

/** callLogLevelOptions are the values the workflow/execution call-log selector offers. */
export const callLogLevelOptions = [
  { value: '', label: 'Unspecified' },
  { value: 'LOG_NONE', label: 'Log none' },
  { value: 'LOG_ERRORS_ONLY', label: 'Log errors only' },
  { value: 'LOG_ALL_CALLS', label: 'Log all calls' },
] as const

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

/** formatKV renders a string map as "key=value" lines for editing. */
export function formatKV(map?: Record<string, string>): string {
  if (!map) return ''
  return Object.entries(map)
    .map(([k, v]) => `${k}=${v}`)
    .join('\n')
}

/** executionSummary is a one-line description of an execution's outcome. */
export function executionSummary(exec: Execution): string {
  if (exec.state === 'FAILED' && exec.error?.context) return exec.error.context
  if (exec.error?.payload) return exec.error.payload
  return exec.state || '—'
}
