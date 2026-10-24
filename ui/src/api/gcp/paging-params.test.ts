import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { listEntries } from './logging'
import { listTimeSeries } from './monitoring'

/**
 * The GCP query binding rejects an explicitly empty page size (`?pageSize=` is
 * an unparseable int32), so the console must omit the parameter instead of
 * sending `pageSize: ''` when the caller does not set one (DC63).
 */
describe('empty page-size binding', () => {
  let calls: string[]

  beforeEach(() => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:4567' } })
    calls = []
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        calls.push(String(input))
        return Promise.resolve(
          new Response(JSON.stringify({}), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }),
        )
      }),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  const pageSizeOf = (url: string) => new URL(url).searchParams.get('pageSize')
  const lastCall = () => calls[calls.length - 1]!

  it('omits pageSize from the logging entries request when unset', async () => {
    await listEntries()
    expect(calls).toHaveLength(1)
    expect(pageSizeOf(lastCall())).toBeNull()
    expect(new URL(lastCall()).searchParams.has('pageSize')).toBe(false)
  })

  it('omits pageSize from the monitoring time-series request when unset', async () => {
    await listTimeSeries()
    expect(calls).toHaveLength(1)
    expect(new URL(lastCall()).searchParams.has('pageSize')).toBe(false)
  })

  it('still sends a page size the caller sets', async () => {
    await listEntries({ pageSize: 100 })
    await listTimeSeries({ pageSize: 25 })
    expect(calls.map(pageSizeOf)).toEqual(['100', '25'])
  })
})
