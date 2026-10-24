import type { CloudTask, TaskQueue } from '../../api/gcp/tasks'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** queueStateColor maps a queue state onto an MUI Chip color. */
export function queueStateColor(state: string): 'success' | 'default' | 'warning' | 'error' {
  switch (state) {
    case 'RUNNING':
      return 'success'
    case 'PAUSED':
      return 'default'
    case 'DISABLED':
      return 'warning'
    default:
      return 'warning'
  }
}

/** taskTargetLabel summarises a task's delivery target for the list column. */
export function taskTargetLabel(task: CloudTask): string {
  switch (task.target) {
    case 'http':
      return `HTTP · ${task.httpUrl || '—'}`
    case 'appengine':
      return `App Engine · ${task.appEngineUri || '—'}`
    default:
      return task.target || '—'
  }
}

/** rateLabel summarises a queue's dispatch rate limit. */
export function rateLabel(queue: TaskQueue): string {
  if (queue.maxDispatchesPerSecond && queue.maxConcurrentDispatches) {
    return `${queue.maxDispatchesPerSecond}/s · ${queue.maxConcurrentDispatches} concurrent`
  }
  if (queue.maxDispatchesPerSecond) return `${queue.maxDispatchesPerSecond}/s`
  return '—'
}
