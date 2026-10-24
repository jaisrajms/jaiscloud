/**
 * Base fetch wrapper. The `session` cookie is HttpOnly, so JS cannot read it —
 * the browser attaches it automatically with `credentials: 'include'` (same
 * origin in prod, Vite proxy in dev). On a 401 (e.g. the emulator restarted and
 * rotated the token) we re-acquire the cookie and retry once.
 */

const BASE = ''

let _currentAccount = ''

/** Called by AccountContext whenever the selected account changes. */
export function setCurrentAccount(id: string) {
  _currentAccount = id
}

export class APIError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message)
    this.name = 'APIError'
  }
}

/** In-flight session refresh, shared by concurrent callers. */
let _refreshing: Promise<void> | null = null

/**
 * Re-acquire the session cookie. The server sets the HttpOnly `session` cookie
 * on every /ui/* document response, so a credentialed GET is enough; the value
 * is deliberately unreadable from JS. Concurrent callers share one request.
 */
export function refreshSession(): Promise<void> {
  if (_refreshing) return _refreshing
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 5000)
  _refreshing = fetch('/ui/', {
    method: 'GET',
    credentials: 'include',
    cache: 'no-store',
    signal: controller.signal,
  })
    .catch(() => undefined)
    .then(() => undefined)
    .finally(() => {
      clearTimeout(timer)
      _refreshing = null
    })
  return _refreshing
}

async function parseError(res: Response): Promise<APIError> {
  let code = 'UnknownError'
  let message = res.statusText
  try {
    const err = await res.json()
    code = err.code ?? code
    message = err.message ?? message
  } catch {
    // ignore JSON parse errors
  }
  return new APIError(res.status, code, message)
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  params?: Record<string, string | number>,
): Promise<T> {
  const url = new URL(`${BASE}${path}`, window.location.origin)
  if (_currentAccount) {
    url.searchParams.set('account', _currentAccount)
  }
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)))
  }

  const headers: Record<string, string> = {}
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  const send = () =>
    fetch(url.toString(), {
      method,
      headers,
      credentials: 'include',
      body: body !== undefined ? JSON.stringify(body) : undefined,
    })

  let res = await send()
  if (res.status === 401) {
    await refreshSession()
    res = await send()
  }

  if (!res.ok) {
    throw await parseError(res)
  }

  if (res.status === 204) {
    return undefined as T
  }
  return res.json() as Promise<T>
}

export const api = {
  get: <T>(path: string, params?: Record<string, string | number>) =>
    request<T>('GET', path, undefined, params),
  post: <T>(path: string, body?: unknown, params?: Record<string, string | number>) =>
    request<T>('POST', path, body, params),
  put: <T>(path: string, body?: unknown) => request<T>('PUT', path, body),
  patch: <T>(path: string, body?: unknown) => request<T>('PATCH', path, body),
  delete: <T>(path: string, params?: Record<string, string | number>) =>
    request<T>('DELETE', path, undefined, params),
}

/** PUT a binary body (e.g. an uploaded file) to a UI API path. */
export async function putBlob(
  path: string,
  params: Record<string, string | number>,
  blob: Blob,
  contentType: string,
): Promise<void> {
  const url = new URL(`${BASE}${path}`, window.location.origin)
  if (_currentAccount) {
    url.searchParams.set('account', _currentAccount)
  }
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)))

  const send = () =>
    fetch(url.toString(), {
      method: 'PUT',
      headers: { 'Content-Type': contentType },
      credentials: 'include',
      body: blob,
    })

  let res = await send()
  if (res.status === 401) {
    await refreshSession()
    res = await send()
  }
  if (!res.ok) {
    throw new APIError(res.status, 'UploadFailed', `Upload failed: ${res.status} ${res.statusText}`)
  }
}
