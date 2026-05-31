/**
 * Base fetch wrapper. Reads session token from cookie and sets
 * credentials: 'include' so the session cookie is sent cross-origin
 * in dev mode (Vite dev server → 4567).
 */

const BASE = ''

let _currentAccount = ''

/** Called by AccountContext whenever the selected account changes. */
export function setCurrentAccount(id: string) {
  _currentAccount = id
}

function getCookie(name: string): string | undefined {
  const match = document.cookie
    .split('; ')
    .find((row) => row.startsWith(`${name}=`))
  return match?.split('=')[1]
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

  const token = getCookie('session')
  const headers: Record<string, string> = {}
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  const res = await fetch(url.toString(), {
    method,
    headers,
    credentials: 'include',
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })

  if (!res.ok) {
    let code = 'UnknownError'
    let message = res.statusText
    try {
      const err = await res.json()
      code = err.code ?? code
      message = err.message ?? message
    } catch {
      // ignore JSON parse errors
    }
    throw new APIError(res.status, code, message)
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
