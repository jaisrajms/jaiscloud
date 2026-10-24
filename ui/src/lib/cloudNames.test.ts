import { describe, expect, it } from 'vitest'
import { cloudName } from './cloudNames'

describe('cloudName', () => {
  it('maps every known cloud', () => {
    expect(cloudName('aws')).toBe('AWS')
    expect(cloudName('gcp')).toBe('Google Cloud')
    expect(cloudName('azure')).toBe('Azure')
  })

  it('falls back to AWS for unknown or missing clouds', () => {
    expect(cloudName(undefined)).toBe('AWS')
    expect(cloudName('')).toBe('AWS')
    expect(cloudName('oracle')).toBe('AWS')
  })
})
