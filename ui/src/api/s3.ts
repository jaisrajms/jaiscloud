import { api } from './client'

export interface Bucket {
  name: string
  region: string
  creationDate?: string
  versioning: 'Enabled' | 'Suspended' | 'Off'
  objectCount: number
}

export interface ListBucketsResponse {
  items: Bucket[]
  total: number
}

export interface S3Object {
  key: string
  size: number
  etag?: string
  lastModified?: string
  storageClass?: string
  contentType?: string
}

export interface ListObjectsResponse {
  items: S3Object[]
  commonPrefixes: string[]
  isTruncated: boolean
  nextContinuationToken?: string
  keyCount: number
}

export interface ObjectVersion {
  key: string
  versionId: string
  isLatest: boolean
  size: number
  etag?: string
  lastModified?: string
  isDeleteMarker: boolean
}

export interface ListVersionsResponse {
  items: ObjectVersion[]
  isTruncated: boolean
}

const BASE = '/api/ui/v1/s3'

export function listBuckets(): Promise<ListBucketsResponse> {
  return api.get<ListBucketsResponse>(`${BASE}/buckets`)
}

export function createBucket(req: { name: string; region?: string }): Promise<Bucket> {
  return api.post<Bucket>(`${BASE}/buckets`, req)
}

export function deleteBucket(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/buckets/${encodeURIComponent(name)}`)
}

export function listObjects(
  bucket: string,
  params?: { prefix?: string; delimiter?: string; maxKeys?: number; continuationToken?: string },
): Promise<ListObjectsResponse> {
  const qp: Record<string, string | number> = {}
  if (params?.prefix != null) qp.prefix = params.prefix
  if (params?.delimiter != null) qp.delimiter = params.delimiter
  if (params?.maxKeys) qp.maxKeys = params.maxKeys
  if (params?.continuationToken) qp.continuationToken = params.continuationToken
  return api.get<ListObjectsResponse>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects`, qp)
}

export function headObject(bucket: string, key: string): Promise<Record<string, unknown>> {
  return api.get<Record<string, unknown>>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/head`,
    { key },
  )
}

export function downloadObjectUrl(bucket: string, key: string): string {
  return `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/download?key=${encodeURIComponent(key)}`
}

export function deleteObject(bucket: string, key: string): Promise<void> {
  return api.delete<void>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects`, { key })
}

export function deleteObjects(bucket: string, keys: string[]): Promise<unknown> {
  return api.post<unknown>(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/delete-batch`, { keys })
}

export function getBucketVersioning(bucket: string): Promise<{ Status?: string }> {
  return api.get<{ Status?: string }>(`${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`)
}

export function putBucketVersioning(bucket: string, status: 'Enabled' | 'Suspended'): Promise<void> {
  return api.put<void>(`${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`, { status })
}

export function listObjectVersions(bucket: string, prefix?: string): Promise<ListVersionsResponse> {
  const qp: Record<string, string | number> = {}
  if (prefix) qp.prefix = prefix
  return api.get<ListVersionsResponse>(`${BASE}/buckets/${encodeURIComponent(bucket)}/versions`, qp)
}

export function getBucketTags(bucket: string): Promise<{ tags: Record<string, string> }> {
  return api.get<{ tags: Record<string, string> }>(`${BASE}/buckets/${encodeURIComponent(bucket)}/tags`)
}
