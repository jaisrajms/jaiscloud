import { describe, expect, it } from 'vitest'
import { TIERS, type ServiceDescriptor } from '../api/services'
import { TIER_NAMES, tierDescription, tierLabel } from './tier'

const service = (over: Partial<ServiceDescriptor> = {}): ServiceDescriptor => ({
  id: 'compute',
  label: 'Compute Engine',
  category: 'Compute',
  rootPath: '/gcp/compute/instances',
  children: [],
  tier: 'full',
  ...over,
})

describe('tierLabel', () => {
  it('returns undefined for full services', () => {
    expect(tierLabel(service({ tier: 'full' }))).toBeUndefined()
  })

  it('labels metadata-only and shape-only services', () => {
    expect(tierLabel(service({ tier: 'metadata' }))).toBe('Metadata only')
    expect(tierLabel(service({ tier: 'shape' }))).toBe('Shape only')
  })

  // Every tier the API can return is handled: full has no badge, the others do.
  it('covers the whole tier vocabulary', () => {
    for (const tier of TIERS) {
      const label = tierLabel(service({ tier }))
      if (tier === 'full') expect(label).toBeUndefined()
      else expect(label).toBeTruthy()
    }
  })

  it('names every tier', () => {
    for (const tier of TIERS) {
      expect(TIER_NAMES[tier]).toBeTruthy()
    }
  })
})

describe('tierDescription', () => {
  it('returns undefined for full services', () => {
    expect(tierDescription(service({ tier: 'full' }))).toBeUndefined()
  })

  it('explains the tier and appends the note', () => {
    const metadata = tierDescription(service({ tier: 'metadata', note: 'no data plane' }))
    expect(metadata).toContain('nothing ever runs here')
    expect(metadata).toContain('(no data plane)')

    const shape = tierDescription(service({ tier: 'shape', note: 'shape only' }))
    expect(shape).toContain('partial version of the real behaviour')
    expect(shape).toContain('(shape only)')
  })

  // The labels/descriptions are shared by the AWS, Azure and GCP consoles, so
  // they must not name a specific cloud.
  it('is cloud-neutral', () => {
    for (const tier of TIERS) {
      expect(tierDescription(service({ tier })) ?? '').not.toMatch(/GCP|AWS|Azure/)
    }
  })
})
