import { describe, expect, it } from 'vitest'
import type { GcpFunction } from '../../api/gcp/functions'
import {
  deliveryStatusColor,
  functionStateColor,
  memoryLabel,
  parseKV,
  shortDate,
  triggerSummary,
} from './util'

function fn(overrides: Partial<GcpFunction>): GcpFunction {
  return { id: 'f1', location: 'us-central1', name: 'projects/p/locations/us-central1/functions/f1', ...overrides }
}

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('functionStateColor', () => {
  it('maps ACTIVE to success', () => {
    expect(functionStateColor('ACTIVE')).toBe('success')
  })

  it('maps FAILED to error', () => {
    expect(functionStateColor('FAILED')).toBe('error')
  })

  it('defaults unknown states', () => {
    expect(functionStateColor(undefined)).toBe('default')
  })
})

describe('deliveryStatusColor', () => {
  it('maps known statuses', () => {
    expect(deliveryStatusColor('delivered')).toBe('success')
    expect(deliveryStatusColor('failed')).toBe('error')
    expect(deliveryStatusColor('dead_letter')).toBe('warning')
  })
})

describe('triggerSummary', () => {
  it('renders the https url for an http trigger', () => {
    expect(triggerSummary(fn({ triggerType: 'http', url: 'https://x/y' }))).toBe('https://x/y')
  })

  it('renders the event type and resource for an event trigger', () => {
    expect(
      triggerSummary(
        fn({ triggerType: 'event', eventTrigger: { eventType: 'google.cloud.pubsub', resource: 'projects/p/topics/t' } }),
      ),
    ).toBe('google.cloud.pubsub · projects/p/topics/t')
  })
})

describe('memoryLabel', () => {
  it('renders megabytes', () => {
    expect(memoryLabel(256)).toBe('256 MB')
    expect(memoryLabel(0)).toBe('—')
    expect(memoryLabel(undefined)).toBe('—')
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
