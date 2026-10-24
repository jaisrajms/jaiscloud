import { api, putBlob } from '../client'
import { fetchAllPageResponses } from '../paging'

const BASE = '/api/ui/v1/gcp/storage'

export interface Bucket {
  name: string
  location?: string
  storageClass?: string
  timeCreated?: string
  updated?: string
  versioning: boolean
}

export interface ListBucketsResponse {
  items: Bucket[]
  total: number
}

export interface GCSObject {
  name: string
  bucket?: string
  size?: string
  contentType?: string
  storageClass?: string
  updated?: string
  timeCreated?: string
  timeDeleted?: string
  md5Hash?: string
  generation?: string
  metageneration?: string
  temporaryHold?: boolean
  eventBasedHold?: boolean
  retentionExpirationTime?: string
  metadata?: Record<string, string>
}

export interface ListObjectsResponse {
  items: GCSObject[]
  prefixes?: string[]
  nextPageToken?: string
}

export interface CreateBucketRequest {
  name: string
  location?: string
  storageClass?: string
}

export interface IamBinding {
  role: string
  members: string[]
}

export interface IamPolicy {
  kind?: string
  resourceId?: string
  bindings?: IamBinding[]
  etag?: string
  version?: number
}

export interface AclEntry {
  kind?: string
  id?: string
  entity: string
  role: string
  bucket?: string
  object?: string
  etag?: string
}

export interface AclListResponse {
  kind?: string
  items: AclEntry[]
}

export interface RetentionPolicy {
  retentionPeriod?: string
  effectiveTime?: string
  isLocked?: boolean
}

export interface LifecycleRule {
  action?: Record<string, unknown>
  condition?: Record<string, unknown>
}

export interface LifecycleConfig {
  rule?: LifecycleRule[]
}

// ─── Buckets ─────────────────────────────────────────────────────────────────

export const listBuckets = () => api.get<ListBucketsResponse>(`${BASE}/buckets`)

export const createBucket = (body: CreateBucketRequest) => api.post<Bucket>(`${BASE}/buckets`, body)

export const getBucket = (name: string) =>
  api.get<Record<string, unknown>>(`${BASE}/buckets/${encodeURIComponent(name)}`)

export const deleteBucket = (name: string) =>
  api.delete<void>(`${BASE}/buckets/${encodeURIComponent(name)}`)

export const getBucketVersioning = (bucket: string) =>
  api.get<{ versioning?: { enabled?: boolean } }>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`,
  )

export const putBucketVersioning = (bucket: string, enabled: boolean) =>
  api.put<Bucket>(`${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`, { enabled })

export const getBucketLifecycle = (bucket: string) =>
  api.get<{ lifecycle?: LifecycleConfig }>(`${BASE}/buckets/${encodeURIComponent(bucket)}/lifecycle`)

export const putBucketLifecycle = (bucket: string, lifecycle: LifecycleConfig | null) =>
  api.put<Bucket>(`${BASE}/buckets/${encodeURIComponent(bucket)}/lifecycle`, { lifecycle })

export const getBucketRetention = (bucket: string) =>
  api.get<{ retentionPolicy?: RetentionPolicy }>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/retention`,
  )

export const putBucketRetention = (bucket: string, retentionPolicy: RetentionPolicy | null) =>
  api.put<Bucket>(`${BASE}/buckets/${encodeURIComponent(bucket)}/retention`, { retentionPolicy })

export const lockBucketRetention = (bucket: string) =>
  api.post<Bucket>(`${BASE}/buckets/${encodeURIComponent(bucket)}/retention/lock`, {})

export const putDefaultEventBasedHold = (bucket: string, defaultEventBasedHold: boolean) =>
  api.put<Bucket>(`${BASE}/buckets/${encodeURIComponent(bucket)}/default-event-based-hold`, {
    defaultEventBasedHold,
  })

// ─── IAM + ACL ───────────────────────────────────────────────────────────────

export const getBucketIam = (bucket: string) =>
  api.get<IamPolicy>(`${BASE}/buckets/${encodeURIComponent(bucket)}/iam`)

export const putBucketIam = (bucket: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${BASE}/buckets/${encodeURIComponent(bucket)}/iam`, policy)

export const listBucketAcl = (bucket: string) =>
  api.get<AclListResponse>(`${BASE}/buckets/${encodeURIComponent(bucket)}/acl`)

export const insertBucketAcl = (bucket: string, entity: string, role: string) =>
  api.post<AclEntry>(`${BASE}/buckets/${encodeURIComponent(bucket)}/acl`, { entity, role })

export const getObjectIam = (bucket: string, name: string) =>
  api.get<IamPolicy>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/iam`, { name })

export const putObjectIam = (bucket: string, name: string, policy: IamPolicy) =>
  api.put<IamPolicy>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/iam?name=${encodeURIComponent(name)}`,
    policy,
  )

export const listObjectAcl = (bucket: string, name: string) =>
  api.get<AclListResponse>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/acl`, { name })

export const insertObjectAcl = (bucket: string, name: string, entity: string, role: string) =>
  api.post<AclEntry>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/acl`,
    { entity, role },
    { name },
  )

// ─── Objects ─────────────────────────────────────────────────────────────────

export interface ListObjectsParams {
  prefix?: string
  delimiter?: string
  versions?: boolean
  maxResults?: number
  pageToken?: string
}

/**
 * List a bucket's objects (and pseudo-directory `prefixes`). Drains every
 * `nextPageToken` page; pass `pageToken` for a single raw page.
 */
export async function listObjects(
  bucket: string,
  params?: ListObjectsParams,
): Promise<ListObjectsResponse> {
  const path = `${BASE}/buckets/${encodeURIComponent(bucket)}/objects`
  const qp: Record<string, string | number> = {}
  if (params?.prefix) qp.prefix = params.prefix
  if (params?.delimiter) qp.delimiter = params.delimiter
  if (params?.versions) qp.versions = 'true'
  if (params?.maxResults) qp.maxResults = params.maxResults
  const pageQuery = (pageToken?: string) => (pageToken ? { ...qp, pageToken } : qp)

  if (params?.pageToken) {
    return api.get<ListObjectsResponse>(path, pageQuery(params.pageToken))
  }
  const pages = await fetchAllPageResponses((pageToken) =>
    api.get<ListObjectsResponse>(path, pageQuery(pageToken)),
  )
  const items = pages.flatMap((page) => page.items)
  const prefixes = pages.flatMap((page) => page.prefixes ?? [])
  return prefixes.length > 0 ? { items, prefixes } : { items }
}

/** List every generation of every object in a bucket. */
export const listObjectVersions = (bucket: string, prefix?: string) =>
  listObjects(bucket, { versions: true, prefix })

export const getObject = (bucket: string, name: string, generation?: string) =>
  api.get<GCSObject>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/metadata`, {
    name,
    ...(generation ? { generation } : {}),
  })

export function downloadObjectUrl(bucket: string, name: string, generation?: string): string {
  const params = new URLSearchParams({ name })
  if (generation) params.set('generation', generation)
  return `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/download?${params.toString()}`
}

/** Upload (or overwrite) an object's bytes. */
export function uploadObject(bucket: string, name: string, blob: Blob): Promise<GCSObject> {
  return putBlob(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects`,
    { name },
    blob,
    blob.type || 'application/octet-stream',
  ).then(() => getObject(bucket, name))
}

export const patchObject = (
  bucket: string,
  name: string,
  body: Partial<Pick<GCSObject, 'contentType' | 'metadata' | 'temporaryHold' | 'eventBasedHold'>>,
  generation?: string,
) => {
  const params = new URLSearchParams({ name })
  if (generation) params.set('generation', generation)
  return api.patch<GCSObject>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects?${params.toString()}`,
    body,
  )
}

export const deleteObject = (bucket: string, name: string, generation?: string) =>
  api.delete<void>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects`, {
    name,
    ...(generation ? { generation } : {}),
  })

export const restoreObject = (bucket: string, name: string, generation: string) =>
  api.post<GCSObject>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/restore`, undefined, {
    name,
    generation,
  })
