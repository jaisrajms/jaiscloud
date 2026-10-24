import { describe, expect, it } from 'vitest'
import { SERVICE_DOCS, serviceDocsHref } from './serviceDocs'

describe('serviceDocsHref', () => {
  it('maps known service ids to their product documentation', () => {
    expect(serviceDocsHref('storage')).toBe('https://cloud.google.com/storage/docs')
    expect(serviceDocsHref('secretmanager')).toBe('https://cloud.google.com/secret-manager/docs')
  })

  it('falls back to the cloud documentation index for unknown ids', () => {
    expect(serviceDocsHref('mystery')).toBe('https://cloud.google.com/docs')
  })

  it('only carries https links', () => {
    for (const href of Object.values(SERVICE_DOCS)) {
      expect(href).toMatch(/^https:\/\//)
    }
  })
})
