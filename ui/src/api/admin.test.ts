import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { listSnapshots } from './admin'

// The backend returns a bare []SnapshotMetadata with snake_case tags; the
// console consumes { snapshots: [...] } with createdAt. These tests pin the
// adapter in listSnapshots() so the Admin snapshots tables cannot regress to
// always-empty.
describe('listSnapshots', () => {
  beforeEach(() => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:4567' } })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  const respond = (body: unknown, status = 200) =>
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }),
        ),
      ),
    )

  it('unwraps the bare metadata array and maps created_at -> createdAt', async () => {
    respond([
      {
        name: 'snap-1',
        description: 'first',
        created_at: '2026-10-01T00:00:00Z',
        cloud: 'gcp',
        jaiscloud_version: 'v1.2.3',
        schema_version: 3,
        size_bytes: 4096,
        store_counts: { objects: 2 },
      },
    ])

    await expect(listSnapshots()).resolves.toEqual({
      snapshots: [
        {
          name: 'snap-1',
          description: 'first',
          createdAt: '2026-10-01T00:00:00Z',
          cloud: 'gcp',
          version: 'v1.2.3',
        },
      ],
    })
  })

  it('returns an empty list when the backend has no snapshots', async () => {
    respond([])
    await expect(listSnapshots()).resolves.toEqual({ snapshots: [] })
  })
})
