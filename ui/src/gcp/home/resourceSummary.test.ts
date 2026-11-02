import { describe, expect, it } from 'vitest'
import {
  RESOURCE_SUMMARY_SOURCES,
  resourceCount,
  summaryRowsFor,
  summarySourcesFor,
} from './resourceSummary'

describe('resourceCount', () => {
  it('prefers an explicit total', () => {
    expect(resourceCount({ items: [1, 2], total: 7 }, 'items')).toBe(7)
  })

  it('uses totalSize when present (monitoring)', () => {
    expect(resourceCount({ alertPolicies: [1], totalSize: 4 }, 'alertPolicies')).toBe(4)
  })

  it('counts the named array when the response omits a total', () => {
    expect(resourceCount({ sinks: [1, 2, 3] }, 'sinks')).toBe(3)
    expect(resourceCount({ logNames: ['a'] }, 'logNames')).toBe(1)
  })

  it('treats a missing or malformed array as zero', () => {
    expect(resourceCount({}, 'items')).toBe(0)
    expect(resourceCount({ items: null }, 'items')).toBe(0)
    expect(resourceCount(undefined, 'items')).toBe(0)
  })
})

describe('RESOURCE_SUMMARY_SOURCES', () => {
  it('maps one zero-argument list per service with a unique id', () => {
    const services = RESOURCE_SUMMARY_SOURCES.map((source) => source.service)
    expect(new Set(services).size).toBe(services.length)
    for (const source of RESOURCE_SUMMARY_SOURCES) {
      expect(typeof source.list).toBe('function')
      expect(source.path.startsWith('/gcp/')).toBe(true)
    }
  })
})

describe('summarySourcesFor', () => {
  it('keeps only wired services, preserving registry order', () => {
    const sources = summarySourcesFor(['storage', 'logging'])
    expect(sources.map((source) => source.service)).toEqual(['storage', 'logging'])
  })

  it('returns nothing when no wired service has a primary list', () => {
    expect(summarySourcesFor(['unknown'])).toEqual([])
  })
})

describe('kms and secondary collections', () => {
  it('counts key rings through the location-less aggregate', () => {
    const kms = summarySourcesFor(['kms'])
    expect(kms).toHaveLength(1)
    expect(kms[0]!.label).toBe('Key rings')
    expect(kms[0]!.path).toBe('/gcp/kms/keyrings')
  })

  it('flattens extras into their own tiles', () => {
    const rows = summaryRowsFor(['pubsub', 'eventarc', 'monitoring'])
    const labels = rows.map((row) => row.label)
    expect(labels).toEqual(
      expect.arrayContaining(['Topics', 'Subscriptions', 'Triggers', 'Channels', 'Alerting policies', 'Metric descriptors']),
    )
    for (const row of rows) expect(typeof row.list).toBe('function')
  })
})
