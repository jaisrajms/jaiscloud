import { describe, expect, it } from 'vitest'
import {
  basicSli,
  formatGoal,
  formatRollingPeriod,
  goalToPercent,
  parseRollingDurationDays,
  percentToGoal,
  periodError,
  periodLabel,
  rollingDaysToDuration,
  sliFormFromIndicator,
  sliLabel,
  validateCustomSli,
  validateGoalPercent,
  validateGoalRatio,
  validateRollingDays,
} from './sloUtil'

describe('goal validation', () => {
  it('mirrors the core range (0, 0.9999]', () => {
    expect(validateGoalRatio(0)).not.toBeNull()
    expect(validateGoalRatio(-0.1)).not.toBeNull()
    expect(validateGoalRatio(0.0001)).toBeNull()
    expect(validateGoalRatio(0.9999)).toBeNull()
    expect(validateGoalRatio(0.99991)).not.toBeNull()
    expect(validateGoalRatio(Number.NaN)).not.toBeNull()
  })

  it('validates the percent form the dialog collects', () => {
    expect(validateGoalPercent(0)).not.toBeNull()
    expect(validateGoalPercent(99.9)).toBeNull()
    expect(validateGoalPercent(99.99)).toBeNull()
    expect(validateGoalPercent(100)).not.toBeNull()
  })

  it('converts between percent and ratio', () => {
    expect(percentToGoal(99.9)).toBeCloseTo(0.999)
    expect(goalToPercent(0.999)).toBeCloseTo(99.9)
    expect(goalToPercent()).toBe(100)
    expect(formatGoal(0.999)).toBe('99.9%')
    expect(formatGoal(0.9999)).toBe('99.99%')
    expect(formatGoal()).toBe('—')
  })
})

describe('rolling period', () => {
  it('accepts only whole days in 1..30', () => {
    expect(validateRollingDays(1)).toBeNull()
    expect(validateRollingDays(30)).toBeNull()
    expect(validateRollingDays(0)).not.toBeNull()
    expect(validateRollingDays(31)).not.toBeNull()
    expect(validateRollingDays(1.5)).not.toBeNull()
    expect(validateRollingDays(Number.NaN)).not.toBeNull()
  })

  it('renders and parses the proto duration', () => {
    expect(rollingDaysToDuration(1)).toBe('86400s')
    expect(rollingDaysToDuration(7)).toBe('604800s')
    expect(parseRollingDurationDays('86400s')).toBe(1)
    expect(parseRollingDurationDays('86400.000s')).toBe(1)
    expect(parseRollingDurationDays('604800s')).toBe(7)
    expect(parseRollingDurationDays('24h')).toBe(1)
    expect(parseRollingDurationDays('36h')).toBe(1.5)
    expect(parseRollingDurationDays('1d')).toBeNull()
    expect(parseRollingDurationDays()).toBeNull()
  })

  it('formats the period for display', () => {
    expect(formatRollingPeriod('86400s')).toBe('1 day')
    expect(formatRollingPeriod('604800s')).toBe('7 days')
    expect(formatRollingPeriod('36h')).toBe('1.5 days')
    expect(formatRollingPeriod('')).toBe('—')
    expect(formatRollingPeriod('unknown')).toBe('unknown')
  })
})

describe('period exclusivity', () => {
  it('requires exactly one period', () => {
    expect(periodError(true, false)).toBeNull()
    expect(periodError(false, true)).toBeNull()
    expect(periodError(true, true)).not.toBeNull()
    expect(periodError(false, false)).not.toBeNull()
  })

  it('labels the stored period', () => {
    expect(periodLabel({ rollingPeriod: '86400s' })).toBe('1 day')
    expect(periodLabel({ calendarPeriod: 'WEEK' })).toBe('WEEK')
    expect(periodLabel({})).toBe('—')
  })
})

describe('basic SLI form', () => {
  it('maps availability and latency indicators', () => {
    expect(sliFormFromIndicator({ basicSli: { availability: {} } })).toMatchObject({
      mode: 'availability',
      method: '',
    })
    expect(
      sliFormFromIndicator({
        basicSli: { method: ['GET'], latency: { threshold: '400ms' } },
      }),
    ).toMatchObject({ mode: 'latency', method: 'GET', threshold: '400ms' })
  })

  it('opens a non-basic indicator in custom mode so it is preserved', () => {
    const custom = { requestBased: { goodTotalRatio: {} } }
    const form = sliFormFromIndicator(custom)
    expect(form.mode).toBe('custom')
    expect(JSON.parse(form.customJson)).toEqual(custom)
  })

  it('builds a basic SLI from the form fields', () => {
    expect(basicSli('availability', 'GET', '')).toEqual({
      method: ['GET'],
      availability: {},
    })
    expect(basicSli('latency', '', '500ms')).toEqual({
      latency: { threshold: '500ms' },
    })
  })

  it('preserves the basic-SLI fields the form does not edit', () => {
    expect(
      basicSli('availability', 'GET', '', {
        location: ['us-central1'],
        version: ['v1'],
        method: ['POST'],
        latency: { threshold: '1s' },
      }),
    ).toEqual({
      location: ['us-central1'],
      version: ['v1'],
      method: ['GET'],
      availability: {},
    })
  })

  it('labels the indicator shape', () => {
    expect(sliLabel({ basicSli: { availability: {} } })).toBe('Basic · availability')
    expect(sliLabel({ basicSli: { latency: { threshold: '1s' } } })).toBe('Basic · latency')
    expect(sliLabel({ requestBased: {} })).toBe('Request-based')
    expect(sliLabel({ windowsBased: {} })).toBe('Windows-based')
    expect(sliLabel()).toBe('—')
  })

  it('validates custom SLI JSON', () => {
    expect(validateCustomSli('')).not.toBeNull()
    expect(validateCustomSli('not json')).not.toBeNull()
    expect(validateCustomSli('[]')).not.toBeNull()
    expect(validateCustomSli('{}')).not.toBeNull()
    expect(validateCustomSli('{"requestBased":{"goodTotalRatio":{}}}')).toBeNull()
  })
})
