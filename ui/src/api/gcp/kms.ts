import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/kms'

/** A KMS key ring. */
export interface KeyRing {
  name: string
  keyRingId?: string
  location?: string
  createTime?: string
}

export interface ListKeyRingsResponse {
  keyRings: KeyRing[]
  total: number
  nextPageToken?: string
}

export interface CryptoKey {
  name: string
  cryptoKeyId?: string
  purpose?: string
  algorithm?: string
  primaryState?: string
  primaryVersion?: string
  createTime?: string
  rotationPeriod?: string
  nextRotationTime?: string
  labels?: Record<string, string>
}

export interface ListCryptoKeysResponse {
  cryptoKeys: CryptoKey[]
  total: number
  nextPageToken?: string
}

export interface CreateCryptoKeyRequest {
  cryptoKeyId: string
  purpose?: string
  algorithm?: string
  rotationPeriod?: string
  labels?: Record<string, string>
}

export interface CryptoKeyVersion {
  name: string
  versionId?: string
  state?: string
  algorithm?: string
  createTime?: string
  destroyTime?: string
  destroyEventTime?: string
}

export interface ListCryptoKeyVersionsResponse {
  cryptoKeyVersions: CryptoKeyVersion[]
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

const locationPath = (location: string) =>
  `${BASE}/locations/${encodeURIComponent(location)}`

const keyRingPath = (location: string, keyRing: string) =>
  `${locationPath(location)}/keyRings/${encodeURIComponent(keyRing)}`

const cryptoKeyPath = (location: string, keyRing: string, key: string) =>
  `${keyRingPath(location, keyRing)}/cryptoKeys/${encodeURIComponent(key)}`

const cryptoKeyVersionPath = (location: string, keyRing: string, key: string, version: string) =>
  `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}`

// Key rings.
/** List every key ring in a location; pass `pageToken` for a single raw page. */
export async function listKeyRings(
  location: string,
  params?: { pageToken?: string },
): Promise<ListKeyRingsResponse> {
  const path = `${locationPath(location)}/keyRings`
  if (params?.pageToken) {
    return api.get<ListKeyRingsResponse>(path, { pageToken: params.pageToken })
  }
  const keyRings = await fetchAllPages(
    (pageToken) =>
      api.get<ListKeyRingsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.keyRings,
  )
  return { keyRings, total: keyRings.length }
}

export const createKeyRing = (location: string, keyRingId: string) =>
  api.post<KeyRing>(`${locationPath(location)}/keyRings`, { keyRingId })

export const getKeyRing = (location: string, keyRing: string) =>
  api.get<KeyRing>(keyRingPath(location, keyRing))

// Crypto keys.
/** List every crypto key in a key ring; pass `pageToken` for a single raw page. */
export async function listCryptoKeys(
  location: string,
  keyRing: string,
  params?: { pageToken?: string },
): Promise<ListCryptoKeysResponse> {
  const path = `${keyRingPath(location, keyRing)}/cryptoKeys`
  if (params?.pageToken) {
    return api.get<ListCryptoKeysResponse>(path, { pageToken: params.pageToken })
  }
  const cryptoKeys = await fetchAllPages(
    (pageToken) =>
      api.get<ListCryptoKeysResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.cryptoKeys,
  )
  return { cryptoKeys, total: cryptoKeys.length }
}

export const createCryptoKey = (
  location: string,
  keyRing: string,
  body: CreateCryptoKeyRequest,
) => api.post<CryptoKey>(`${keyRingPath(location, keyRing)}/cryptoKeys`, body)

export const getCryptoKey = (location: string, keyRing: string, key: string) =>
  api.get<CryptoKey>(cryptoKeyPath(location, keyRing, key))

export const setPrimaryVersion = (
  location: string,
  keyRing: string,
  key: string,
  versionId: string,
) =>
  api.post<CryptoKey>(`${cryptoKeyPath(location, keyRing, key)}/setPrimary`, {
    cryptoKeyVersionId: versionId,
  })

// Versions.
/** List every version of a crypto key; pass `pageToken` for a single raw page. */
export async function listCryptoKeyVersions(
  location: string,
  keyRing: string,
  key: string,
  params?: { pageToken?: string },
): Promise<ListCryptoKeyVersionsResponse> {
  const path = `${cryptoKeyPath(location, keyRing, key)}/versions`
  if (params?.pageToken) {
    return api.get<ListCryptoKeyVersionsResponse>(path, { pageToken: params.pageToken })
  }
  const cryptoKeyVersions = await fetchAllPages(
    (pageToken) =>
      api.get<ListCryptoKeyVersionsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.cryptoKeyVersions,
  )
  return { cryptoKeyVersions, total: cryptoKeyVersions.length }
}

export const createCryptoKeyVersion = (location: string, keyRing: string, key: string) =>
  api.post<CryptoKeyVersion>(`${cryptoKeyPath(location, keyRing, key)}/versions`)

export const getCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.get<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}`,
  )

export const destroyCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/destroy`,
  )

export const disableCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/disable`,
  )

export const enableCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/enable`,
  )

// IAM policy.
export const getKeyRingIam = (location: string, keyRing: string) =>
  api.get<IamPolicy>(`${keyRingPath(location, keyRing)}/iam`)

export const putKeyRingIam = (location: string, keyRing: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${keyRingPath(location, keyRing)}/iam`, { policy })

export const getCryptoKeyIam = (location: string, keyRing: string, key: string) =>
  api.get<IamPolicy>(`${cryptoKeyPath(location, keyRing, key)}/iam`)

export const putCryptoKeyIam = (
  location: string,
  keyRing: string,
  key: string,
  policy: IamPolicy,
) => api.put<IamPolicy>(`${cryptoKeyPath(location, keyRing, key)}/iam`, { policy })

// Crypto operations.
export interface EncryptRequest {
  plaintext: string
  additionalAuthenticatedData?: string
}

export interface EncryptResponse {
  name?: string
  ciphertext: string
  ciphertextCrc32c?: string
  protectionLevel?: string
  verifiedPlaintextCrc32c?: boolean
  verifiedAdditionalAuthenticatedDataCrc32c?: boolean
}

export interface DecryptRequest {
  ciphertext: string
  additionalAuthenticatedData?: string
}

export interface DecryptResponse {
  plaintext: string
  plaintextCrc32c?: string
  protectionLevel?: string
  usedPrimary?: boolean
}

export interface AsymmetricSignRequest {
  digest: Record<string, string>
}

export interface AsymmetricSignResponse {
  name?: string
  signature: string
  signatureCrc32c?: string
  protectionLevel?: string
}

export interface AsymmetricDecryptRequest {
  ciphertext: string
}

export interface AsymmetricDecryptResponse {
  plaintext: string
  plaintextCrc32c?: string
  protectionLevel?: string
}

export interface MacSignRequest {
  data: string
}

export interface MacSignResponse {
  name?: string
  mac: string
  macCrc32c?: string
  protectionLevel?: string
}

export interface MacVerifyRequest {
  data: string
  mac: string
}

export interface MacVerifyResponse {
  success: boolean
  protectionLevel?: string
}

export interface PublicKeyResponse {
  pem: string
  algorithm?: string
  name?: string
  pemCrc32c?: string
  protectionLevel?: string
}

/** Encrypt base64 plaintext with the key's primary version. */
export const encryptCryptoKey = (
  location: string,
  keyRing: string,
  key: string,
  body: EncryptRequest,
) => api.post<EncryptResponse>(`${cryptoKeyPath(location, keyRing, key)}/encrypt`, body)

/** Decrypt base64 ciphertext with the key's primary version. */
export const decryptCryptoKey = (
  location: string,
  keyRing: string,
  key: string,
  body: DecryptRequest,
) => api.post<DecryptResponse>(`${cryptoKeyPath(location, keyRing, key)}/decrypt`, body)

/** Sign a base64 digest with an asymmetric-sign version. */
export const asymmetricSign = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
  body: AsymmetricSignRequest,
) =>
  api.post<AsymmetricSignResponse>(`${cryptoKeyVersionPath(location, keyRing, key, version)}/asymmetricSign`, body)

/** Decrypt base64 ciphertext with an RSA_DECRYPT version. */
export const asymmetricDecrypt = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
  body: AsymmetricDecryptRequest,
) =>
  api.post<AsymmetricDecryptResponse>(
    `${cryptoKeyVersionPath(location, keyRing, key, version)}/asymmetricDecrypt`,
    body,
  )

/** Compute an HMAC tag over base64 data with a MAC version. */
export const macSign = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
  body: MacSignRequest,
) => api.post<MacSignResponse>(`${cryptoKeyVersionPath(location, keyRing, key, version)}/macSign`, body)

/** Verify an HMAC tag against base64 data with a MAC version. */
export const macVerify = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
  body: MacVerifyRequest,
) => api.post<MacVerifyResponse>(`${cryptoKeyVersionPath(location, keyRing, key, version)}/macVerify`, body)

/** Download a version's public key (PEM). */
export const getCryptoKeyVersionPublicKey = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.get<PublicKeyResponse>(`${cryptoKeyVersionPath(location, keyRing, key, version)}/publicKey`)
