import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/secretmanager'

/** A Secret Manager secret. */
export interface Secret {
  name: string
  secretId?: string
  createTime?: string
  etag?: string
  labels?: Record<string, string>
  annotations?: Record<string, string>
  /** Pub/Sub topics that receive this secret's control-plane events. */
  topics?: string[]
  rotationPeriod?: string
  nextRotationTime?: string
  kmsKeyName?: string
}

export interface ListSecretsResponse {
  secrets: Secret[]
  total: number
  nextPageToken?: string
}

export interface CreateSecretRequest {
  secretId: string
  labels?: Record<string, string>
  annotations?: Record<string, string>
  topics?: string[]
  rotationPeriod?: string
  nextRotationTime?: string
  kmsKeyName?: string
}

export interface UpdateSecretRequest {
  labels?: Record<string, string>
  annotations?: Record<string, string>
  topics?: string[]
  rotationPeriod?: string
  nextRotationTime?: string
  /** Send an empty rotation object to remove the schedule (DC65). */
  removeRotation?: boolean
  kmsKeyName?: string
}

export interface SecretVersion {
  name: string
  versionId?: string
  state?: string
  createTime?: string
  destroyTime?: string
  etag?: string
}

export interface ListSecretVersionsResponse {
  versions: SecretVersion[]
  total: number
  nextPageToken?: string
}

export interface AccessSecretVersionResponse {
  name: string
  data?: string
}

export interface IamBinding {
  role: string
  members: string[]
  condition?: Record<string, unknown>
}

export interface IamPolicy {
  bindings?: IamBinding[]
  etag?: string
  version?: number
}

const secretPath = (secret: string) => `${BASE}/secrets/${encodeURIComponent(secret)}`

const versionPath = (secret: string, version: string) =>
  `${secretPath(secret)}/versions/${encodeURIComponent(version)}`

// Secrets.
/** List every secret; pass `pageToken` for a single raw page. */
export async function listSecrets(params?: {
  pageToken?: string
}): Promise<ListSecretsResponse> {
  if (params?.pageToken) {
    return api.get<ListSecretsResponse>(`${BASE}/secrets`, { pageToken: params.pageToken })
  }
  const secrets = await fetchAllPages(
    (pageToken) =>
      api.get<ListSecretsResponse>(`${BASE}/secrets`, pageToken ? { pageToken } : undefined),
    (page) => page.secrets,
  )
  return { secrets, total: secrets.length }
}

export const createSecret = (body: CreateSecretRequest) =>
  api.post<Secret>(`${BASE}/secrets`, body)

export const getSecret = (secret: string) => api.get<Secret>(secretPath(secret))

export const updateSecret = (secret: string, body: UpdateSecretRequest) =>
  api.patch<Secret>(secretPath(secret), body)

export const deleteSecret = (secret: string) => api.delete<void>(secretPath(secret))

// IAM policy.
export const getSecretIam = (secret: string) =>
  api.get<IamPolicy>(`${secretPath(secret)}/iam`)

export const putSecretIam = (secret: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${secretPath(secret)}/iam`, { policy })

// Versions.
/** List every version of a secret; pass `pageToken` for a single raw page. */
export async function listSecretVersions(
  secret: string,
  params?: { pageToken?: string },
): Promise<ListSecretVersionsResponse> {
  const path = `${secretPath(secret)}/versions`
  if (params?.pageToken) {
    return api.get<ListSecretVersionsResponse>(path, { pageToken: params.pageToken })
  }
  const versions = await fetchAllPages(
    (pageToken) =>
      api.get<ListSecretVersionsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.versions,
  )
  return { versions, total: versions.length }
}

export const addSecretVersion = (secret: string, payload: string) =>
  api.post<SecretVersion>(`${secretPath(secret)}/versions`, { payload })

export const getSecretVersion = (secret: string, version: string) =>
  api.get<SecretVersion>(versionPath(secret, version))

export const accessSecretVersion = (secret: string, version: string) =>
  api.get<AccessSecretVersionResponse>(`${versionPath(secret, version)}/access`)

export const destroySecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/destroy`)

export const disableSecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/disable`)

export const enableSecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/enable`)
