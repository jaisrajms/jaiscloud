import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/iam'

/** An IAM service account (the serviceAccounts.list shape). */
export interface ServiceAccount {
  name: string
  email: string
  displayName?: string
  projectId?: string
  uniqueId?: string
  description?: string
  disabled?: boolean
  etag?: string
}

export interface ListServiceAccountsResponse {
  accounts: ServiceAccount[]
  total: number
  nextPageToken?: string
}

export interface CreateServiceAccountRequest {
  accountId: string
  displayName?: string
  description?: string
}

export interface UpdateServiceAccountRequest {
  displayName?: string
  description?: string
  etag?: string
}

/** A service-account key; privateKeyData is only present on create. */
export interface ServiceAccountKey {
  name: string
  keyId: string
  keyAlgorithm?: string
  keyOrigin?: string
  keyType?: string
  validAfterTime?: string
  disabled?: boolean
  disableTime?: string
  publicKeyData?: string
  privateKeyData?: string
}

export interface ListServiceAccountKeysResponse {
  keys: ServiceAccountKey[]
  total: number
  nextPageToken?: string
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

export interface SignBlobResponse {
  keyId?: string
  signature: string
}

export interface SignJwtResponse {
  keyId?: string
  signedJwt: string
}

// Service accounts.
/** List every service account; pass `pageToken` for a single raw page. */
export async function listServiceAccounts(params?: {
  pageToken?: string
}): Promise<ListServiceAccountsResponse> {
  if (params?.pageToken) {
    return api.get<ListServiceAccountsResponse>(`${BASE}/serviceAccounts`, {
      pageToken: params.pageToken,
    })
  }
  const accounts = await fetchAllPages(
    (pageToken) =>
      api.get<ListServiceAccountsResponse>(
        `${BASE}/serviceAccounts`,
        pageToken ? { pageToken } : undefined,
      ),
    (page) => page.accounts,
  )
  return { accounts, total: accounts.length }
}

export const createServiceAccount = (body: CreateServiceAccountRequest) =>
  api.post<ServiceAccount>(`${BASE}/serviceAccounts`, body)

export const getServiceAccount = (email: string) =>
  api.get<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`)

export const updateServiceAccount = (email: string, body: UpdateServiceAccountRequest) =>
  api.patch<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`, body)

export const deleteServiceAccount = (email: string) =>
  api.delete<void>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`)

// disable/enable answer the Empty response real GCP sends (the new state is
// read back through getServiceAccount).
export const disableServiceAccount = (email: string) =>
  api.post<void>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/disable`)

export const enableServiceAccount = (email: string) =>
  api.post<void>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/enable`)

// IAM policy.
export const getServiceAccountIam = (email: string) =>
  api.get<IamPolicy>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/iam`)

export const putServiceAccountIam = (email: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/iam`, { policy })

// Keys.
/** List every key of a service account; pass `pageToken` for a single raw page. */
export async function listServiceAccountKeys(
  email: string,
  params?: { pageToken?: string },
): Promise<ListServiceAccountKeysResponse> {
  const path = `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys`
  if (params?.pageToken) {
    return api.get<ListServiceAccountKeysResponse>(path, { pageToken: params.pageToken })
  }
  const keys = await fetchAllPages(
    (pageToken) =>
      api.get<ListServiceAccountKeysResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.keys,
  )
  return { keys, total: keys.length }
}

export const createServiceAccountKey = (email: string) =>
  api.post<ServiceAccountKey>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys`)

export const deleteServiceAccountKey = (email: string, key: string) =>
  api.delete<void>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}`,
  )

export const disableServiceAccountKey = (email: string, key: string) =>
  api.post<void>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}/disable`,
  )

export const enableServiceAccountKey = (email: string, key: string) =>
  api.post<void>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}/enable`,
  )

// Signing.
/** Sign an arbitrary (base64) blob with the account's key (IAM signBlob). */
export const signServiceAccountBlob = (email: string, bytesToSign: string) =>
  api.post<SignBlobResponse>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/signBlob`, {
    bytesToSign,
  })

/** Mint a signed JWT from a serialized JSON claims object (IAM signJwt). */
export const signServiceAccountJwt = (email: string, payload: string) =>
  api.post<SignJwtResponse>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/signJwt`, {
    payload,
  })
