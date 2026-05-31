import { api } from './client'

export interface KMSKey {
  keyId: string
  arn: string
  description?: string
  keyUsage: string
  keySpec: string
  keyState: string
  enabled: boolean
  origin?: string
  createdAt?: string
  deletionDate?: string
  aliases?: string[]
}

export interface ListKeysResponse {
  items: KMSKey[]
  nextToken?: string
  total: number
}

export interface KMSAlias {
  aliasName: string
  aliasArn: string
  targetKeyId?: string
}

export interface ListAliasesResponse {
  items: KMSAlias[]
  nextToken?: string
}

const BASE = '/api/ui/v1/kms'

export function listKeys(params?: { nextToken?: string }): Promise<ListKeysResponse> {
  return api.get<ListKeysResponse>(`${BASE}/keys`, params as Record<string, string | number>)
}

export function createKey(req: {
  description?: string
  keyUsage?: string
  keySpec?: string
  tags?: Record<string, string>
}): Promise<unknown> {
  return api.post<unknown>(`${BASE}/keys`, req)
}

export function getKey(keyId: string): Promise<unknown> {
  return api.get<unknown>(`${BASE}/keys/detail`, { keyId })
}

export function enableKey(keyId: string): Promise<void> {
  return api.post<void>(`${BASE}/keys/enable`, undefined, { keyId })
}

export function disableKey(keyId: string): Promise<void> {
  return api.post<void>(`${BASE}/keys/disable`, undefined, { keyId })
}

export function scheduleKeyDeletion(keyId: string, pendingWindowInDays = 30): Promise<unknown> {
  return api.post<unknown>(`${BASE}/keys/schedule-deletion`, { pendingWindowInDays }, { keyId })
}

export function cancelKeyDeletion(keyId: string): Promise<unknown> {
  return api.post<unknown>(`${BASE}/keys/cancel-deletion`, undefined, { keyId })
}

export function listAliases(params?: { keyId?: string; nextToken?: string }): Promise<ListAliasesResponse> {
  return api.get<ListAliasesResponse>(`${BASE}/aliases`, params as Record<string, string | number>)
}

export function createAlias(req: { aliasName: string; targetKeyId: string }): Promise<KMSAlias> {
  return api.post<KMSAlias>(`${BASE}/aliases`, req)
}

export function deleteAlias(aliasName: string): Promise<void> {
  return api.delete<void>(`${BASE}/aliases`, { aliasName })
}
