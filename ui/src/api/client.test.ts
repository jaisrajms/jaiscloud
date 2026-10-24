import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { APIError, api, refreshSession } from './client'

describe('api client session recovery', () => {
  beforeEach(() => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:4567' } })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('refreshes the session and retries once on 401', async () => {
    const calls: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        calls.push(url)
        if (url.endsWith('/ui/')) {
          return Promise.resolve(new Response('', { status: 200 }))
        }
        const apiCalls = calls.filter((u) => u.includes('/api/')).length
        if (apiCalls === 1) {
          return Promise.resolve(
            new Response(JSON.stringify({ code: 'Unauthorized', message: 'nope' }), {
              status: 401,
              headers: { 'Content-Type': 'application/json' },
            }),
          )
        }
        return Promise.resolve(
          new Response(JSON.stringify({ ok: true }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }),
        )
      }),
    )

    await expect(api.get('/api/ui/v1/thing')).resolves.toEqual({ ok: true })
    expect(calls.filter((u) => u.endsWith('/ui/'))).toHaveLength(1)
    expect(calls.filter((u) => u.includes('/api/'))).toHaveLength(2)
  })

  it('throws after a retry also returns 401', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ code: 'Unauthorized', message: 'no' }), {
            status: 401,
            headers: { 'Content-Type': 'application/json' },
          }),
        ),
      ),
    )

    await expect(api.get('/api/ui/v1/thing')).rejects.toBeInstanceOf(APIError)
  })

  it('dedupes concurrent session refreshes', async () => {
    let resolveFetch: (res: Response) => void = () => {}
    const pending = new Promise<Response>((resolve) => {
      resolveFetch = resolve
    })
    const fetchMock = vi.fn(() => pending)
    vi.stubGlobal('fetch', fetchMock)

    const first = refreshSession()
    const second = refreshSession()
    resolveFetch(new Response('', { status: 200 }))
    await Promise.all([first, second])

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})
