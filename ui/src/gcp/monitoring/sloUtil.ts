import type {
  BasicSli,
  ServiceLevelIndicator,
  ServiceLevelObjective,
} from '../../api/gcp/monitoring'

/** The calendar periods the core accepts (google.monitoring.v3.CalendarPeriod
 * DAY=1, WEEK=2, FORTNIGHT=3, MONTH=4). */
export const CALENDAR_PERIODS = ['DAY', 'WEEK', 'FORTNIGHT', 'MONTH'] as const
export type CalendarPeriod = (typeof CALENDAR_PERIODS)[number]

const SECONDS_PER_DAY = 24 * 60 * 60

/**
 * validateGoalRatio mirrors the core's goal check exactly: the ratio must be in
 * the open-closed range (0, 0.9999]. Returning null means the value is valid.
 */
export function validateGoalRatio(goal: number): string | null {
  if (!Number.isFinite(goal) || goal <= 0 || goal > 0.9999) {
    return 'Goal must be greater than 0 and at most 99.99%.'
  }
  return null
}

/** validateGoalPercent checks the percent form the dialog collects by converting
 * it to the ratio the core validates. */
export function validateGoalPercent(percent: number): string | null {
  if (!Number.isFinite(percent)) return 'Goal must be a number.'
  return validateGoalRatio(percent / 100)
}

export function percentToGoal(percent: number): number {
  return percent / 100
}

export function goalToPercent(goal?: number): number {
  return goal === undefined ? 100 : goal * 100
}

/** formatGoal renders a goal ratio as a percentage without float noise. */
export function formatGoal(goal?: number): string {
  if (goal === undefined || !Number.isFinite(goal)) return '—'
  const percent = Math.round(goal * 100 * 10000) / 10000
  return `${percent}%`
}

/** validateRollingDays mirrors the core: a positive whole number of days, at
 * most 30 (the core requires an integer multiple of one day). */
export function validateRollingDays(days: number): string | null {
  if (!Number.isInteger(days) || days < 1 || days > 30) {
    return 'Rolling period must be a whole number of days between 1 and 30.'
  }
  return null
}

/** rollingDaysToDuration renders the proto duration the API expects. */
export function rollingDaysToDuration(days: number): string {
  return `${days * SECONDS_PER_DAY}s`
}

/**
 * parseRollingDurationDays reads the proto duration the API returns ("86400s")
 * and also a Go-style hour duration ("24h"). A fractional result is returned
 * as-is; the form's whole-day validation rejects it.
 */
export function parseRollingDurationDays(value?: string): number | null {
  if (!value) return null
  const seconds = /^([0-9]+(?:\.[0-9]+)?)s$/.exec(value)
  if (seconds) {
    const days = Number(seconds[1]) / SECONDS_PER_DAY
    return Number.isFinite(days) ? days : null
  }
  const hours = /^([0-9]+(?:\.[0-9]+)?)h$/.exec(value)
  if (hours) {
    const days = Number(hours[1]) / 24
    return Number.isFinite(days) ? days : null
  }
  return null
}

export function formatRollingPeriod(value?: string): string {
  const days = parseRollingDurationDays(value)
  if (days === null) return value || '—'
  return days === 1 ? '1 day' : `${days} days`
}

/** periodError reports the core's exactly-one-period requirement. The dialog
 * enforces it structurally with a mode select; the helper keeps that mirror
 * explicit and tested. */
export function periodError(hasRolling: boolean, hasCalendar: boolean): string | null {
  if (hasRolling === hasCalendar) {
    return 'Specify exactly one of a rolling period or a calendar period.'
  }
  return null
}

/** periodLabel renders whichever period an SLO carries. */
export function periodLabel(
  slo: Pick<ServiceLevelObjective, 'rollingPeriod' | 'calendarPeriod'>,
): string {
  if (slo.rollingPeriod) return formatRollingPeriod(slo.rollingPeriod)
  if (slo.calendarPeriod) return slo.calendarPeriod
  return '—'
}

export type SliMode = 'availability' | 'latency' | 'custom'

export interface SliForm {
  mode: SliMode
  method: string
  threshold: string
  customJson: string
}

/** sliFormFromIndicator maps a stored indicator onto the dialog's fields. An
 * indicator that is not a basic SLI opens in the custom-JSON mode so editing
 * never silently replaces it. */
export function sliFormFromIndicator(indicator?: ServiceLevelIndicator): SliForm {
  const basic = indicator?.basicSli
  const method = basic?.method?.[0] ?? ''
  if (basic?.latency) {
    return { mode: 'latency', method, threshold: basic.latency.threshold ?? '500ms', customJson: '' }
  }
  if (basic?.availability) {
    return { mode: 'availability', method, threshold: '500ms', customJson: '' }
  }
  return {
    mode: indicator ? 'custom' : 'availability',
    method: '',
    threshold: '500ms',
    customJson: indicator ? JSON.stringify(indicator, null, 2) : '',
  }
}

/**
 * basicSli builds the basic SLI the form collects. `base` carries the
 * BasicSli fields the form does not edit (location/version) so an edit does not
 * silently drop them.
 */
export function basicSli(
  mode: 'availability' | 'latency',
  method: string,
  threshold: string,
  base?: BasicSli,
): BasicSli {
  const out: BasicSli = {}
  if (base?.location?.length) out.location = base.location
  if (base?.version?.length) out.version = base.version
  const trimmed = method.trim()
  if (trimmed) out.method = [trimmed]
  if (mode === 'latency') out.latency = { threshold: threshold.trim() }
  else out.availability = {}
  return out
}

/** validateCustomSli checks the custom-SLI JSON: a non-empty object. */
export function validateCustomSli(json: string): string | null {
  if (!json.trim()) return 'SLI JSON is required.'
  try {
    const parsed: unknown = JSON.parse(json)
    if (
      !parsed ||
      typeof parsed !== 'object' ||
      Array.isArray(parsed) ||
      Object.keys(parsed).length === 0
    ) {
      return 'SLI must be a non-empty JSON object.'
    }
    return null
  } catch (err) {
    return `Invalid JSON: ${(err as Error).message}`
  }
}

/** sliLabel names the indicator shape for the list/detail views. */
export function sliLabel(indicator?: ServiceLevelIndicator): string {
  if (indicator?.basicSli?.latency) return 'Basic · latency'
  if (indicator?.basicSli) return 'Basic · availability'
  if (indicator?.requestBased) return 'Request-based'
  if (indicator?.windowsBased) return 'Windows-based'
  return '—'
}
