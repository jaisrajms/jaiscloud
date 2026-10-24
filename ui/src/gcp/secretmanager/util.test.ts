import { describe, expect, it } from 'vitest'
import { fromBase64, parseKeyValueLines, toBase64 } from './util'

describe('parseKeyValueLines', () => {
  it('parses key=value lines', () => {
    expect(parseKeyValueLines('env=prod\nteam=platform')).toEqual({
      env: 'prod',
      team: 'platform',
    })
  })
  it('returns undefined for empty input', () => {
    expect(parseKeyValueLines('  \n ')).toBeUndefined()
  })
  it('trims whitespace around keys and values', () => {
    expect(parseKeyValueLines(' env = prod ')).toEqual({ env: 'prod' })
  })
})

describe('base64 round trip', () => {
  it('encodes and decodes UTF-8 text', () => {
    const text = 'p@ssw0rd—ünicode'
    expect(fromBase64(toBase64(text))).toBe(text)
  })
  it('falls back to the raw string for invalid base64', () => {
    expect(fromBase64('not base64!')).toBe('not base64!')
  })
})
