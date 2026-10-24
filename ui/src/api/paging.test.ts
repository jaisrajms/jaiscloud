import { afterEach, describe, expect, it, vi } from 'vitest'
import { fetchAllPageResponses, fetchAllPages, type PagedResponse } from './paging'
import { listTopics } from './gcp/pubsub'

interface Page extends PagedResponse {
  items: number[]
}

describe('fetchAllPageResponses', () => {
  it('returns the single page when there is no cursor', async () => {
    const fetchPage = vi.fn(() => Promise.resolve<Page>({ items: [1, 2] }))
    const pages = await fetchAllPageResponses(fetchPage)
    expect(pages).toEqual([{ items: [1, 2] }])
    expect(fetchPage).toHaveBeenCalledTimes(1)
    expect(fetchPage).toHaveBeenCalledWith(undefined)
  })

  it('walks every page until the cursor is exhausted', async () => {
    const tokens: (string | undefined)[] = []
    const byToken: Record<string, Page> = {
      a: { items: [3, 4], nextPageToken: 'b' },
      b: { items: [5] },
    }
    const fetchPage = vi.fn((token?: string) => {
      tokens.push(token)
      if (token === undefined) return Promise.resolve<Page>({ items: [1, 2], nextPageToken: 'a' })
      return Promise.resolve(byToken[token] ?? { items: [] })
    })

    const pages = await fetchAllPageResponses(fetchPage)
    expect(pages.flatMap((page) => page.items)).toEqual([1, 2, 3, 4, 5])
    expect(tokens).toEqual([undefined, 'a', 'b'])
  })

  it('stops when a backend echoes back the cursor it was given', async () => {
    const fetchPage = vi.fn(() =>
      Promise.resolve<Page>({ items: [1], nextPageToken: 'same' }),
    )
    const pages = await fetchAllPageResponses(fetchPage)
    // First call is unbounded (token undefined -> 'same'), second sees 'same'
    // echo back and stops; it never spins on a token the backend ignores.
    expect(fetchPage).toHaveBeenCalledTimes(2)
    expect(pages).toHaveLength(2)
  })

  it('caps the number of pages fetched', async () => {
    let n = 0
    const fetchPage = vi.fn(() => {
      n += 1
      return Promise.resolve<Page>({ items: [n], nextPageToken: `t${n}` })
    })
    const pages = await fetchAllPageResponses(fetchPage, { maxPages: 3 })
    expect(fetchPage).toHaveBeenCalledTimes(3)
    expect(pages).toHaveLength(3)
  })
})

describe('fetchAllPages', () => {
  it('flattens the selected array across pages', async () => {
    const fetchPage = vi.fn((token?: string) =>
      Promise.resolve(token ? { items: [3, 4] } : { items: [1, 2], nextPageToken: 'a' }),
    )
    const items = await fetchAllPages(fetchPage, (page: Page) => page.items)
    expect(items).toEqual([1, 2, 3, 4])
  })
})

describe('GCP list clients drain the cursor', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('follows nextPageToken for a browse list and reports the drained total', async () => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:4567' } })
    const urls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = new URL(String(input))
        urls.push(url.toString())
        const body = url.searchParams.has('pageToken')
          ? { topics: [{ name: 'beta' }], total: 1 }
          : { topics: [{ name: 'alpha' }], total: 1, nextPageToken: 'tok' }
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }),
        )
      }),
    )

    const result = await listTopics()
    expect(result.topics.map((topic) => topic.name)).toEqual(['alpha', 'beta'])
    expect(result.total).toBe(2)
    expect(urls).toHaveLength(2)
    expect(urls[1]).toContain('pageToken=tok')
  })

  it('passes an explicit pageToken through as a single raw page', async () => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:4567' } })
    const urls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        urls.push(String(input))
        return Promise.resolve(
          new Response(JSON.stringify({ topics: [{ name: 'only' }], total: 1 }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }),
        )
      }),
    )

    const result = await listTopics({ pageToken: 'second' })
    expect(result.topics.map((topic) => topic.name)).toEqual(['only'])
    expect(urls).toHaveLength(1)
    expect(urls[0]).toContain('pageToken=second')
  })
})
