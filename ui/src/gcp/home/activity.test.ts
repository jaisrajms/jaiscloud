import { describe, expect, it } from 'vitest'
import type { DiscoveryProject } from '../../api/gcp/projects'
import { activeBannerProject, activityRows } from './activity'

const projects: DiscoveryProject[] = [
  { id: 'jaiscloud-project', kind: 'configured' },
  { id: 'billing-demo', kind: 'registered' },
  { id: 'old-e2e', kind: 'observed', lastSeen: '2026-10-31T10:00:00Z', services: ['bigquery'] },
  {
    id: 'acme-e2e',
    kind: 'observed',
    lastSeen: '2026-10-31T11:59:42Z',
    services: ['storage', 'pubsub'],
  },
]

describe('activityRows', () => {
  it('lists only observed projects other than the one in view, newest first', () => {
    const rows = activityRows(projects, 'jaiscloud-project')
    expect(rows.map((row) => row.id)).toEqual(['acme-e2e', 'old-e2e'])
    expect(rows[0]?.services).toEqual(['storage', 'pubsub'])
  })

  it('excludes the project in view', () => {
    const rows = activityRows(projects, 'acme-e2e')
    expect(rows.map((row) => row.id)).toEqual(['old-e2e'])
  })
})

describe('activeBannerProject', () => {
  const now = Date.parse('2026-10-31T12:00:00Z')

  it('picks the newest project written to within the window', () => {
    expect(activeBannerProject(projects, 'jaiscloud-project', now)?.id).toBe('acme-e2e')
  })

  it('ignores activity older than the window', () => {
    expect(activeBannerProject(projects, 'jaiscloud-project', now, 10_000)).toBeUndefined()
  })

  it('ignores the project in view', () => {
    expect(activeBannerProject(projects, 'acme-e2e', now, 3 * 3_600_000)?.id).toBe('old-e2e')
  })

  it('returns nothing without observed projects', () => {
    const configuredOnly = projects.filter((project) => project.kind !== 'observed')
    expect(activeBannerProject(configuredOnly, 'jaiscloud-project', now)).toBeUndefined()
  })
})
