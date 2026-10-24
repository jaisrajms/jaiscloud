import { describe, expect, it } from 'vitest'
import { docsLink } from './cloudLinks'

describe('docsLink', () => {
  it('returns the Google Cloud docs for gcp', () => {
    expect(docsLink('gcp')).toEqual({
      href: 'https://cloud.google.com/docs',
      label: 'Google Cloud documentation',
    })
  })

  it('returns AWS docs by default', () => {
    expect(docsLink('aws').href).toBe('https://docs.aws.amazon.com/')
    expect(docsLink(undefined).href).toBe('https://docs.aws.amazon.com/')
    expect(docsLink('unknown').href).toBe('https://docs.aws.amazon.com/')
  })

  it('returns the Azure docs for azure', () => {
    expect(docsLink('azure').href).toBe('https://learn.microsoft.com/azure/')
  })
})
