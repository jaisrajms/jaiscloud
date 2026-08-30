import { api } from './client'

export interface Secret {
  arn: string
  name: string
  description?: string
  kmsKeyId?: string
  lastChangedDate?: string
  createdDate?: string
  deletedDate?: string
  tags?: Record<string, string>
}

export interface ListSecretsResponse {
  items: Secret[]
  nextToken?: string
  total: number
}

export interface SecretVersion {
  versionId: string
  versionStages?: string[]
  createdDate?: string
  lastAccessedDate?: string
}

export interface ListSecretVersionsResponse {
  items: SecretVersion[]
  nextToken?: string
}

const BASE = '/api/ui/v1/secretsmanager'

export function listSecrets(params?: { nextToken?: string }): Promise<ListSecretsResponse> {
  return api.get<ListSecretsResponse>(`${BASE}/secrets`, params as Record<string, string | number>)
}

export function createSecret(req: {
  name: string
  secretString?: string
  description?: string
  kmsKeyId?: string
  tags?: Record<string, string>
}): Promise<unknown> {
  return api.post<unknown>(`${BASE}/secrets`, req)
}

export function getSecret(name: string): Promise<unknown> {
  return api.get<unknown>(`${BASE}/secrets/${encodeURIComponent(name)}`)
}

export function deleteSecret(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/secrets/${encodeURIComponent(name)}`)
}

export function getSecretValue(name: string): Promise<{ SecretString?: string; SecretBinary?: string; VersionId?: string }> {
  return api.get<{ SecretString?: string; SecretBinary?: string; VersionId?: string }>(
    `${BASE}/secrets/${encodeURIComponent(name)}/value`,
  )
}

export function putSecretValue(name: string, secretString: string): Promise<unknown> {
  return api.post<unknown>(`${BASE}/secrets/${encodeURIComponent(name)}/value`, { secretString })
}

export function listSecretVersions(name: string, params?: { nextToken?: string }): Promise<ListSecretVersionsResponse> {
  return api.get<ListSecretVersionsResponse>(
    `${BASE}/secrets/${encodeURIComponent(name)}/versions`,
    params as Record<string, string | number>,
  )
}
