import { describe, expect, it } from 'vitest'
import type { SchedulerJob } from '../../api/gcp/scheduler'
import { shortDate, stateColor, targetLabel } from './util'

function job(overrides: Partial<SchedulerJob>): SchedulerJob {
  return { name: 'j', location: 'us-central1', state: 'ENABLED', ...overrides }
}

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('targetLabel', () => {
  it('labels an HTTP target with its uri', () => {
    expect(targetLabel(job({ target: 'http', httpUri: 'https://x/hook' }))).toBe(
      'HTTP · https://x/hook',
    )
  })

  it('labels a Pub/Sub target with its topic', () => {
    expect(targetLabel(job({ target: 'pubsub', pubsubTopic: 'projects/p/topics/t' }))).toBe(
      'Pub/Sub · projects/p/topics/t',
    )
  })

  it('labels an App Engine target with its uri', () => {
    expect(targetLabel(job({ target: 'appengine', appEngineUri: '/hook' }))).toBe(
      'App Engine · /hook',
    )
  })

  it('falls back to the raw target when unknown', () => {
    expect(targetLabel(job({ target: '' }))).toBe('—')
  })
})

describe('stateColor', () => {
  it('maps known states', () => {
    expect(stateColor('ENABLED')).toBe('success')
    expect(stateColor('PAUSED')).toBe('default')
    expect(stateColor('UPDATE_FAILED')).toBe('error')
  })

  it('warns on an unknown state', () => {
    expect(stateColor('DISABLED')).toBe('warning')
  })
})
