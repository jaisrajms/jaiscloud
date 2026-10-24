import type { SchedulerJob } from '../../api/gcp/scheduler'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** targetLabel summarises a job's delivery target for the list column. */
export function targetLabel(job: SchedulerJob): string {
  switch (job.target) {
    case 'http':
      return `HTTP · ${job.httpUri || '—'}`
    case 'pubsub':
      return `Pub/Sub · ${job.pubsubTopic || '—'}`
    case 'appengine':
      return `App Engine · ${job.appEngineUri || '—'}`
    default:
      return job.target || '—'
  }
}

/** stateColor maps a job state onto an MUI Chip color. */
export function stateColor(state: string): 'success' | 'default' | 'warning' | 'error' {
  switch (state) {
    case 'ENABLED':
      return 'success'
    case 'PAUSED':
      return 'default'
    case 'UPDATE_FAILED':
      return 'error'
    default:
      return 'warning'
  }
}
