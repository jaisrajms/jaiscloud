import type { TimeSeries, TypedValue } from '../../api/gcp/monitoring'

/** formatTypedValue renders a monitoring data-point value as a compact string. */
export function formatTypedValue(value?: TypedValue): string {
  if (!value) return '—'
  if (value.boolValue !== undefined) return String(value.boolValue)
  if (value.int64Value !== undefined) return value.int64Value
  if (value.doubleValue !== undefined) return String(value.doubleValue)
  if (value.stringValue !== undefined) return value.stringValue
  if (value.distributionValue) return '[distribution]'
  return '—'
}

/** seriesLabel renders the metric type + resource type of a time series. */
export function seriesLabel(series: TimeSeries): string {
  const metric = series.metric?.type ?? '—'
  const resource = series.resource?.type
  return resource ? `${metric} · ${resource}` : metric
}

/** shortDate renders an RFC3339 timestamp for display, falling back to the raw
 * value (or an em dash) when it is missing or unparseable. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** verificationColor maps a channel verification status to an MUI Chip color. */
export function verificationColor(status?: string): 'success' | 'warning' | 'default' {
  switch (status) {
    case 'VERIFIED':
      return 'success'
    case 'UNVERIFIED':
      return 'warning'
    default:
      return 'default'
  }
}

/** resourceID returns the trailing segment of a resource name. */
export function resourceID(name?: string): string {
  if (!name) return ''
  const i = name.lastIndexOf('/')
  return i >= 0 ? name.slice(i + 1) : name
}
