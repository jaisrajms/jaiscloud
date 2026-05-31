import { api } from './client';
const BASE = '/api/ui/v1/s3';
export function listBuckets() {
    return api.get(`${BASE}/buckets`);
}
export function createBucket(req) {
    return api.post(`${BASE}/buckets`, req);
}
export function deleteBucket(name) {
    return api.delete(`${BASE}/buckets/${encodeURIComponent(name)}`);
}
export function listObjects(bucket, params) {
    const qp = {};
    if (params?.prefix != null)
        qp.prefix = params.prefix;
    if (params?.delimiter != null)
        qp.delimiter = params.delimiter;
    if (params?.maxKeys)
        qp.maxKeys = params.maxKeys;
    if (params?.continuationToken)
        qp.continuationToken = params.continuationToken;
    return api.get(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects`, qp);
}
export function headObject(bucket, key) {
    return api.get(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/head`, { key });
}
export function downloadObjectUrl(bucket, key) {
    return `${BASE}/buckets/${encodeURIComponent(bucket)}/objects/download?key=${encodeURIComponent(key)}`;
}
export function deleteObject(bucket, key) {
    return api.delete(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects`, { key });
}
export function deleteObjects(bucket, keys) {
    return api.post(`${BASE}/buckets/${encodeURIComponent(bucket)}/objects/delete-batch`, { keys });
}
export function getBucketVersioning(bucket) {
    return api.get(`${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`);
}
export function putBucketVersioning(bucket, status) {
    return api.put(`${BASE}/buckets/${encodeURIComponent(bucket)}/versioning`, { status });
}
export function listObjectVersions(bucket, prefix) {
    const qp = {};
    if (prefix)
        qp.prefix = prefix;
    return api.get(`${BASE}/buckets/${encodeURIComponent(bucket)}/versions`, qp);
}
export function getBucketTags(bucket) {
    return api.get(`${BASE}/buckets/${encodeURIComponent(bucket)}/tags`);
}
