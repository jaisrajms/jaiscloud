import type { EventarcChannel, EventarcTrigger } from '../../api/gcp/eventarc'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

const destinationLabels: Record<string, string> = {
  cloudFunction: 'Cloud Function',
  cloudRun: 'Cloud Run',
  workflow: 'Workflow',
  gke: 'GKE',
  httpEndpoint: 'HTTP endpoint',
}

/** destinationLabel names a trigger's destination kind. */
export function destinationLabel(type?: string): string {
  if (!type) return '—'
  return destinationLabels[type] ?? type
}

/** filterSummary condenses a trigger's event filters into one line. */
export function filterSummary(trigger: EventarcTrigger): string {
  const filters = trigger.eventFilters ?? []
  if (filters.length === 0) return '—'
  return filters.map((f) => `${f.attribute}${f.operator ? ` ${f.operator}` : ''}=${f.value}`).join(', ')
}

/** channelStateColor maps a channel state onto an MUI Chip color. */
export function channelStateColor(state?: string): 'success' | 'default' | 'warning' {
  switch (state) {
    case 'ACTIVE':
      return 'success'
    case 'PENDING':
      return 'warning'
    default:
      return 'default'
  }
}

/** lastSegment returns the final path segment of a resource name. */
export function lastSegment(name?: string): string {
  if (!name) return '—'
  const parts = name.split('/')
  return parts[parts.length - 1] || name
}

/** channelProviderLabel shortens a channel provider resource name. */
export function channelProviderLabel(channel: EventarcChannel): string {
  return channel.provider ? lastSegment(channel.provider) : '—'
}
