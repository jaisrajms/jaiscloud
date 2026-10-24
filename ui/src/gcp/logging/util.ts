import type { LogEntry } from '../../api/gcp/logging'

/** severityColor maps a Cloud Logging severity to an MUI Chip color. */
export function severityColor(severity?: string): 'error' | 'warning' | 'info' | 'success' | 'default' {
  switch (severity) {
    case 'EMERGENCY':
    case 'ALERT':
    case 'CRITICAL':
    case 'ERROR':
      return 'error'
    case 'WARNING':
      return 'warning'
    case 'NOTICE':
      return 'info'
    case 'INFO':
      return 'success'
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

/** lastSegment returns the final "/"-separated segment of a resource name. */
export function lastSegment(name?: string): string {
  if (!name) return ''
  const i = name.lastIndexOf('/')
  return i >= 0 ? name.slice(i + 1) : name
}

/** payloadPreview renders a one-line summary of a log entry's payload. */
export function payloadPreview(entry: LogEntry): string {
  if (entry.textPayload) return entry.textPayload
  if (entry.jsonPayload) {
    try {
      return JSON.stringify(entry.jsonPayload)
    } catch {
      return '[json payload]'
    }
  }
  return '—'
}

/** parseKeyValueLines parses "key=value" lines into a record, skipping blanks. */
export function parseKeyValueLines(input: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const raw of input.split('\n')) {
    const line = raw.trim()
    if (!line) continue
    const idx = line.indexOf('=')
    if (idx <= 0) continue
    out[line.slice(0, idx).trim()] = line.slice(idx + 1).trim()
  }
  return out
}
