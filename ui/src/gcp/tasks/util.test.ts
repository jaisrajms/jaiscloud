import { describe, expect, it } from 'vitest'
import type { CloudTask, TaskQueue } from '../../api/gcp/tasks'
import { queueStateColor, rateLabel, shortDate, taskTargetLabel } from './util'

function queue(overrides: Partial<TaskQueue>): TaskQueue {
  return { name: 'q', location: 'us-central1', state: 'RUNNING', ...overrides }
}

function task(overrides: Partial<CloudTask>): CloudTask {
  return {
    name: 't',
    location: 'us-central1',
    queue: 'q',
    dispatchCount: 0,
    responseCount: 0,
    ...overrides,
  }
}

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('queueStateColor', () => {
  it('maps known states', () => {
    expect(queueStateColor('RUNNING')).toBe('success')
    expect(queueStateColor('PAUSED')).toBe('default')
  })

  it('warns on an unknown state', () => {
    expect(queueStateColor('DISABLED')).toBe('warning')
    expect(queueStateColor('')).toBe('warning')
  })
})

describe('taskTargetLabel', () => {
  it('labels an HTTP target with its url', () => {
    expect(taskTargetLabel(task({ target: 'http', httpUrl: 'https://x/hook' }))).toBe(
      'HTTP · https://x/hook',
    )
  })

  it('labels an App Engine target with its uri', () => {
    expect(taskTargetLabel(task({ target: 'appengine', appEngineUri: '/hook' }))).toBe(
      'App Engine · /hook',
    )
  })

  it('falls back to the raw target when unknown', () => {
    expect(taskTargetLabel(task({ target: '' }))).toBe('—')
  })
})

describe('rateLabel', () => {
  it('summarises rate and concurrency', () => {
    expect(rateLabel(queue({ maxDispatchesPerSecond: 50, maxConcurrentDispatches: 20 }))).toBe(
      '50/s · 20 concurrent',
    )
  })

  it('shows rate only when concurrency is absent', () => {
    expect(rateLabel(queue({ maxDispatchesPerSecond: 50 }))).toBe('50/s')
  })

  it('renders an em dash when unset', () => {
    expect(rateLabel(queue({}))).toBe('—')
  })
})
