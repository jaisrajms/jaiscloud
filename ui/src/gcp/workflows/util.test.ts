import { describe, expect, it } from 'vitest'
import type { Execution } from '../../api/gcp/workflows'
import {
  executionStateColor,
  executionSummary,
  formatKV,
  parseKV,
  shortDate,
  workflowStateColor,
} from './util'

function execution(overrides: Partial<Execution>): Execution {
  return {
    id: 'e1',
    workflowId: 'w1',
    location: 'us-central1',
    name: 'projects/p/locations/us-central1/workflows/w1/executions/e1',
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

describe('workflowStateColor', () => {
  it('maps ACTIVE to success', () => {
    expect(workflowStateColor('ACTIVE')).toBe('success')
  })

  it('maps UNAVAILABLE to error', () => {
    expect(workflowStateColor('UNAVAILABLE')).toBe('error')
  })

  it('defaults unknown states', () => {
    expect(workflowStateColor(undefined)).toBe('default')
  })
})

describe('executionStateColor', () => {
  it('maps known states', () => {
    expect(executionStateColor('SUCCEEDED')).toBe('success')
    expect(executionStateColor('FAILED')).toBe('error')
    expect(executionStateColor('ACTIVE')).toBe('info')
    expect(executionStateColor('CANCELLED')).toBe('default')
  })
})

describe('parseKV', () => {
  it('parses key=value lines and ignores blanks', () => {
    expect(parseKV('env=prod\n\nregion=us-central1')).toEqual({ env: 'prod', region: 'us-central1' })
  })

  it('returns undefined when empty', () => {
    expect(parseKV('')).toBeUndefined()
    expect(parseKV('no-equals')).toBeUndefined()
  })
})

describe('formatKV', () => {
  it('round-trips through parseKV', () => {
    const map = { env: 'prod', region: 'us-central1' }
    expect(parseKV(formatKV(map))).toEqual(map)
  })

  it('renders an empty string for an absent map', () => {
    expect(formatKV(undefined)).toBe('')
  })
})

describe('executionSummary', () => {
  it('prefers the error context for a failed execution', () => {
    expect(executionSummary(execution({ state: 'FAILED', error: { payload: '{}', context: 'boom' } }))).toBe(
      'boom',
    )
  })

  it('falls back to the state', () => {
    expect(executionSummary(execution({ state: 'SUCCEEDED' }))).toBe('SUCCEEDED')
  })
})
