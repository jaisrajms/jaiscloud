import { describe, expect, it } from 'vitest'
import { asArray, asRecord, shortDate, statusColor, text } from './util'

describe('statusColor', () => {
  it('maps running to success', () => {
    expect(statusColor('RUNNING')).toBe('success')
  })

  it('maps transitional states to warning', () => {
    for (const s of ['STOPPING', 'PROVISIONING', 'STAGING', 'REPAIRING']) {
      expect(statusColor(s)).toBe('warning')
    }
  })

  it('defaults unknown states', () => {
    expect(statusColor('TERMINATED')).toBe('default')
    expect(statusColor('')).toBe('default')
  })
})

describe('shortDate', () => {
  it('falls back to an em dash when empty', () => {
    expect(shortDate()).toBe('—')
    expect(shortDate('')).toBe('—')
  })

  it('returns the raw value when unparseable', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('asArray / asRecord', () => {
  it('coerces arrays and objects', () => {
    expect(asArray<number>([1, 2])).toEqual([1, 2])
    expect(asArray(undefined)).toEqual([])
    expect(asRecord({ a: 1 })).toEqual({ a: 1 })
    expect(asRecord([1, 2])).toEqual({})
    expect(asRecord(null)).toEqual({})
  })
})

describe('text', () => {
  it('renders non-strings defensively', () => {
    expect(text('x')).toBe('x')
    expect(text(3)).toBe('3')
    expect(text(undefined)).toBe('')
  })
})
