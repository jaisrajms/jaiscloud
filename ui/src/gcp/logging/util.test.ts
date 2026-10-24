import { describe, expect, it } from 'vitest'
import { lastSegment, parseKeyValueLines, payloadPreview, severityColor, shortDate } from './util'

describe('severityColor', () => {
  it('maps high severities to error', () => {
    for (const s of ['EMERGENCY', 'ALERT', 'CRITICAL', 'ERROR']) {
      expect(severityColor(s)).toBe('error')
    }
  })

  it('maps warning/notice/info', () => {
    expect(severityColor('WARNING')).toBe('warning')
    expect(severityColor('NOTICE')).toBe('info')
    expect(severityColor('INFO')).toBe('success')
  })

  it('defaults unknown and empty', () => {
    expect(severityColor('DEFAULT')).toBe('default')
    expect(severityColor()).toBe('default')
  })
})

describe('shortDate', () => {
  it('falls back to an em dash when empty', () => {
    expect(shortDate()).toBe('—')
  })

  it('returns the raw value when unparseable', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('lastSegment', () => {
  it('returns the trailing segment', () => {
    expect(lastSegment('projects/p/logs/nginx/requests')).toBe('requests')
    expect(lastSegment('plain')).toBe('plain')
    expect(lastSegment()).toBe('')
  })
})

describe('payloadPreview', () => {
  it('prefers text payload', () => {
    expect(payloadPreview({ textPayload: 'hello' })).toBe('hello')
  })

  it('serializes json payload', () => {
    expect(payloadPreview({ jsonPayload: { a: 1 } })).toBe('{"a":1}')
  })

  it('returns an em dash when empty', () => {
    expect(payloadPreview({})).toBe('—')
  })
})

describe('parseKeyValueLines', () => {
  it('parses key=value lines', () => {
    expect(parseKeyValueLines('a=1\nb = two\n\nbad')).toEqual({ a: '1', b: 'two' })
  })
})
