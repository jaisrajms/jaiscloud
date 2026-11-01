import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../../api/services'
import {
  breadcrumbsFor,
  buildNavGroups,
  buildSearchGroups,
  groupProjects,
  pageTitleFor,
  projectServiceChips,
  pushRecent,
  readRecent,
  relativeProjectAge,
  withResourceGroup,
  type ProjectRowInfo,
  type StorageLike,
} from './navModel'

function fakeStorage(seed: Record<string, string> = {}): StorageLike {
  const data = new Map(Object.entries(seed))
  return {
    getItem: (key) => data.get(key) ?? null,
    setItem: (key, value) => {
      data.set(key, value)
    },
  }
}

const services: ServiceDescriptor[] = [
  {
    id: 'storage',
    label: 'Cloud Storage',
    category: 'Storage',
    rootPath: '/gcp/storage/buckets',
    children: [{ label: 'Buckets', path: '/gcp/storage/buckets' }],
    tier: 'full',
  },
  {
    id: 'pubsub',
    label: 'Pub/Sub',
    category: 'Integration',
    rootPath: '/gcp/pubsub/topics',
    children: [{ label: 'Topics', path: '/gcp/pubsub/topics' }],
    tier: 'full',
  },
  {
    id: 'iam',
    label: 'IAM',
    category: 'Security',
    rootPath: '/gcp/iam/service-accounts',
    children: [{ label: 'Service accounts', path: '/gcp/iam/service-accounts' }],
    tier: 'full',
  },
]

describe('recent list storage', () => {
  it('reads a de-duplicated, capped list and survives malformed JSON', () => {
    const storage = fakeStorage({ recent: '["a","b"]' })
    expect(readRecent('recent', storage)).toEqual(['a', 'b'])
    expect(pushRecent('recent', 'b', storage)).toEqual(['b', 'a'])
    expect(readRecent('recent', storage)).toEqual(['b', 'a'])

    const bad = fakeStorage({ recent: 'not json' })
    expect(readRecent('recent', bad)).toEqual([])

    const long = fakeStorage({ recent: '["a","b","c"]' })
    expect(pushRecent('recent', 'd', long, 3)).toEqual(['d', 'a', 'b'])
  })

  it('returns an empty list without storage', () => {
    expect(readRecent('missing', fakeStorage())).toEqual([])
  })
})

describe('buildSearchGroups', () => {
  it('orders favorites and recents before category groups', () => {
    const groups = buildSearchGroups(services, ['iam'], ['pubsub'], '')
    expect(groups.map((g) => g.label)).toEqual(['Favorites', 'Recent', 'Storage', 'Integration', 'Security'])
    expect(groups[0]?.options[0]?.id).toBe('iam')
    expect(groups[1]?.options[0]?.id).toBe('pubsub')
  })

  it('filters by label and category', () => {
    const byLabel = buildSearchGroups(services, [], [], 'storage')
    expect(byLabel.flatMap((g) => g.options).map((o) => o.id)).toEqual(['storage'])

    const byCategory = buildSearchGroups(services, [], [], 'security')
    expect(byCategory.flatMap((g) => g.options).map((o) => o.id)).toEqual(['iam'])

    expect(buildSearchGroups(services, [], [], 'nope')).toEqual([])
  })

  it('drops favorites that do not match the query', () => {
    const groups = buildSearchGroups(services, ['iam'], [], 'storage')
    expect(groups.map((g) => g.label)).not.toContain('Favorites')
  })
})

describe('buildNavGroups', () => {
  it('leads with pinned favorites and recents, then category groups', () => {
    const groups = buildNavGroups(services, ['iam'], ['pubsub'], '')
    expect(groups.map((g) => [g.kind, g.label])).toEqual([
      ['pinned', 'Favorites'],
      ['recent', 'Recent'],
      ['category', 'Storage'],
      ['category', 'Integration'],
      ['category', 'Security'],
    ])
    expect(groups[0]?.entries[0]?.id).toBe('iam')
    expect(groups[1]?.entries[0]?.id).toBe('pubsub')
  })

  it('carries children and the pinned flag onto entries', () => {
    const groups = buildNavGroups(services, ['storage'], [], '')
    const storage = groups
      .flatMap((group) => group.entries)
      .find((entry) => entry.id === 'storage')
    expect(storage?.children).toEqual([{ label: 'Buckets', path: '/gcp/storage/buckets' }])
    expect(storage?.pinned).toBe(true)
    const pubsub = groups
      .flatMap((group) => group.entries)
      .find((entry) => entry.id === 'pubsub')
    expect(pubsub?.pinned).toBe(false)
  })

  it('filters by query and drops non-matching favorites', () => {
    expect(buildNavGroups(services, [], [], 'nope')).toEqual([])
    const groups = buildNavGroups(services, ['iam'], [], 'storage')
    expect(groups.map((g) => g.label)).not.toContain('Favorites')
    expect(groups.flatMap((g) => g.entries).map((e) => e.id)).toEqual(['storage'])
  })
})

describe('withResourceGroup', () => {
  const resources = [
    { id: 'storage:Bucket:b1', label: 'b1', description: 'Bucket · Cloud Storage', path: '/x' },
  ]

  it('slots resources after the shortcuts and before the categories', () => {
    const groups = buildSearchGroups(services, ['iam'], ['pubsub'], '')
    expect(withResourceGroup(groups, resources).map((g) => g.label)).toEqual([
      'Favorites',
      'Recent',
      'Resources',
      'Storage',
      'Integration',
      'Security',
    ])
  })

  it('leads with resources when there are no shortcuts', () => {
    const groups = buildSearchGroups(services, [], [], '')
    expect(withResourceGroup(groups, resources).map((g) => g.label)).toEqual([
      'Resources',
      'Storage',
      'Integration',
      'Security',
    ])
  })

  it('leaves the service groups untouched without resource matches', () => {
    const groups = buildSearchGroups(services, [], [], '')
    expect(withResourceGroup(groups, [])).toBe(groups)
  })
})

describe('breadcrumbsFor', () => {
  it('builds the root, service and admin trails', () => {
    expect(breadcrumbsFor(services, '/gcp')).toEqual([{ label: 'JaisCloud', to: '/gcp' }])
    expect(breadcrumbsFor(services, '/gcp/storage/buckets/b1')).toEqual([
      { label: 'JaisCloud', to: '/gcp' },
      { label: 'Cloud Storage', to: '/gcp/storage/buckets' },
    ])
    expect(breadcrumbsFor(services, '/gcp/admin')).toEqual([
      { label: 'JaisCloud', to: '/gcp' },
      { label: 'Admin', to: '/gcp/admin' },
    ])
  })
})

describe('pageTitleFor', () => {
  it('titles the console, a service and admin', () => {
    expect(pageTitleFor(services, '/gcp')).toBe('JaisCloud Console')
    expect(pageTitleFor(services, '/gcp/pubsub/topics/t1')).toBe('Pub/Sub · JaisCloud')
    expect(pageTitleFor(services, '/gcp/admin')).toBe('Admin · JaisCloud')
  })
})

describe('groupProjects', () => {
  const projects: ProjectRowInfo[] = [
    { id: 'jaiscloud-project', kind: 'configured' },
    { id: 'billing-demo', kind: 'registered' },
    { id: 'old-e2e', kind: 'observed', lastSeen: '2026-10-31T10:00:00Z' },
    { id: 'acme-e2e', kind: 'observed', lastSeen: '2026-10-31T11:59:42Z' },
  ]

  it('splits observed, recent and known projects', () => {
    const groups = groupProjects(['jaiscloud-project'], projects, ['billing-demo'])
    expect(groups.active.map((p) => p.id)).toEqual(['acme-e2e', 'old-e2e'])
    expect(groups.recent.map((p) => p.id)).toEqual(['billing-demo'])
    expect(groups.all.map((p) => p.id)).toEqual(['jaiscloud-project'])
  })

  it('keeps recent ids that the server no longer lists', () => {
    const groups = groupProjects(['jaiscloud-project'], projects, ['evicted-e2e'])
    expect(groups.recent).toEqual([{ id: 'evicted-e2e', kind: 'unknown' }])
  })

  it('does not repeat an observed project under Recent', () => {
    const groups = groupProjects(['jaiscloud-project'], projects, ['acme-e2e'])
    expect(groups.recent).toEqual([])
    expect(groups.active.map((p) => p.id)).toContain('acme-e2e')
  })

  it('falls back to the accounts list when discovery is empty', () => {
    const groups = groupProjects(['jaiscloud-project', 'billing-demo'], [], [])
    expect(groups.all.map((p) => p.id)).toEqual(['billing-demo', 'jaiscloud-project'])
    expect(groups.active).toEqual([])
  })

  it('filters every group by the search query', () => {
    const groups = groupProjects(['jaiscloud-project'], projects, ['billing-demo'], 'acme')
    expect(groups.active.map((p) => p.id)).toEqual(['acme-e2e'])
    expect(groups.recent).toEqual([])
    expect(groups.all).toEqual([])
  })
})

describe('project presentation helpers', () => {
  it('caps service chips at three', () => {
    expect(projectServiceChips(['storage', 'pubsub', 'firestore', 'bigquery'])).toEqual([
      'storage',
      'pubsub',
      'firestore',
    ])
    expect(projectServiceChips(undefined)).toEqual([])
  })

  it('renders relative ages', () => {
    const now = Date.parse('2026-10-31T12:00:00Z')
    expect(relativeProjectAge('2026-10-31T11:59:42Z', now)).toBe('18s ago')
    expect(relativeProjectAge('2026-10-31T10:00:00Z', now)).toBe('2h ago')
    expect(relativeProjectAge(undefined, now)).toBe('')
    expect(relativeProjectAge('not-a-date', now)).toBe('')
  })
})
