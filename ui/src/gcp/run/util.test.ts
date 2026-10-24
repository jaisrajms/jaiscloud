import { describe, expect, it } from 'vitest'
import { firstImage, lastSegment, shortDate, templateContainers } from './util'

describe('lastSegment', () => {
  it('returns the final segment', () => {
    expect(lastSegment('projects/p/locations/us-central1/services/svc')).toBe('svc')
  })

  it('handles trailing slashes and empty names', () => {
    expect(lastSegment('a/b/')).toBe('b')
    expect(lastSegment('')).toBe('')
    expect(lastSegment(undefined)).toBe('')
  })

  it('returns a bare name unchanged', () => {
    expect(lastSegment('svc')).toBe('svc')
  })
})

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('firstImage', () => {
  it('returns the first container image', () => {
    expect(firstImage([{ image: 'gcr.io/p/img:1' }])).toBe('gcr.io/p/img:1')
  })

  it('returns empty for malformed containers', () => {
    expect(firstImage(undefined)).toBe('')
    expect(firstImage([])).toBe('')
    expect(firstImage(['not-an-object'])).toBe('')
    expect(firstImage([{}])).toBe('')
  })
})

describe('templateContainers', () => {
  it('reads containers nested under template', () => {
    expect(templateContainers({ template: { containers: [{ image: 'x' }] } })).toEqual([
      { image: 'x' },
    ])
  })

  it('returns undefined when the template is absent', () => {
    expect(templateContainers({})).toBeUndefined()
    expect(templateContainers(undefined)).toBeUndefined()
  })
})
