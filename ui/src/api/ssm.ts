import { api } from './client'

export interface Parameter {
  name: string
  type: string
  value?: string
  version: number
  lastModifiedDate?: string
  lastModifiedUser?: string
  arn?: string
  dataType?: string
  description?: string
  keyId?: string
  tier?: string
}

export interface ListParametersResponse {
  items: Parameter[]
  nextToken?: string
  total: number
}

export interface ParameterHistoryEntry {
  name: string
  type: string
  value?: string
  version: number
  lastModifiedDate?: string
  description?: string
}

export interface ListParameterHistoryResponse {
  items: ParameterHistoryEntry[]
  nextToken?: string
}

const BASE = '/api/ui/v1/ssm'

export function listParameters(params?: { path?: string; nextToken?: string }): Promise<ListParametersResponse> {
  return api.get<ListParametersResponse>(`${BASE}/parameters`, params as Record<string, string | number>)
}

export function putParameter(req: {
  name: string
  value: string
  type: string
  description?: string
  keyId?: string
  overwrite?: boolean
}): Promise<unknown> {
  return api.post<unknown>(`${BASE}/parameters`, req)
}

export function getParameter(name: string): Promise<{ Parameter?: Parameter }> {
  return api.get<{ Parameter?: Parameter }>(`${BASE}/parameters/value`, { name })
}

export function deleteParameter(name: string): Promise<void> {
  // Name can start with /; strip leading slash for the URL path param
  const encoded = name.startsWith('/') ? name.slice(1) : name
  return fetch(
    `/api/ui/v1/ssm/parameters/${encodeURIComponent(encoded)}`,
    { method: 'DELETE', credentials: 'include' },
  ).then((r) => {
    if (!r.ok && r.status !== 204) throw new Error(`Delete failed: ${r.status}`)
  })
}

export function getParameterHistory(name: string, params?: { nextToken?: string }): Promise<ListParameterHistoryResponse> {
  return api.get<ListParameterHistoryResponse>(
    `${BASE}/parameters/history`,
    { name, ...(params ?? {}) } as Record<string, string | number>,
  )
}
