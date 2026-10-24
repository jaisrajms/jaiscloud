import { describe, expect, it } from 'vitest'
import { formatTypedValue, resourceID, seriesLabel, verificationColor } from './util'

describe('formatTypedValue', () => {
  it('renders each typed variant', () => {
    expect(formatTypedValue({ boolValue: true })).toBe('true')
    expect(formatTypedValue({ int64Value: '42' })).toBe('42')
    expect(formatTypedValue({ doubleValue: 1.5 })).toBe('1.5')
    expect(formatTypedValue({ stringValue: 'x' })).toBe('x')
    expect(formatTypedValue({ distributionValue: {} })).toBe('[distribution]')
  })

  it('defaults when empty', () => {
    expect(formatTypedValue()).toBe('—')
    expect(formatTypedValue({})).toBe('—')
  })
})

describe('seriesLabel', () => {
  it('appends the resource type when present', () => {
    expect(seriesLabel({ metric: { type: 'm' }, resource: { type: 'gce_instance' } })).toBe(
      'm · gce_instance',
    )
    expect(seriesLabel({ metric: { type: 'm' } })).toBe('m')
    expect(seriesLabel({})).toBe('—')
  })
})

describe('verificationColor', () => {
  it('maps verification status', () => {
    expect(verificationColor('VERIFIED')).toBe('success')
    expect(verificationColor('UNVERIFIED')).toBe('warning')
    expect(verificationColor('x')).toBe('default')
  })
})

describe('resourceID', () => {
  it('returns the trailing segment', () => {
    expect(resourceID('projects/p/alertPolicies/abc')).toBe('abc')
    expect(resourceID()).toBe('')
  })
})
