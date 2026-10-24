import { describe, expect, it } from 'vitest'
import { base64Encode } from './util'

describe('base64Encode', () => {
  it('encodes ASCII', () => {
    expect(base64Encode('hello')).toBe('aGVsbG8=')
  })

  it('encodes multi-byte UTF-8 without Latin-1 mangling', () => {
    expect(base64Encode('héllo')).toBe('aMOpbGxv')
    expect(base64Encode('🐑')).toBe('8J+QkQ==')
  })

  it('encodes the empty string', () => {
    expect(base64Encode('')).toBe('')
  })
})
