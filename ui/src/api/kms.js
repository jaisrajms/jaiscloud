import { api } from './client';
const BASE = '/api/ui/v1/kms';
export function listKeys(params) {
    return api.get(`${BASE}/keys`, params);
}
export function createKey(req) {
    return api.post(`${BASE}/keys`, req);
}
export function getKey(keyId) {
    return api.get(`${BASE}/keys/detail`, { keyId });
}
export function enableKey(keyId) {
    return api.post(`${BASE}/keys/enable`, undefined, { keyId });
}
export function disableKey(keyId) {
    return api.post(`${BASE}/keys/disable`, undefined, { keyId });
}
export function scheduleKeyDeletion(keyId, pendingWindowInDays = 30) {
    return api.post(`${BASE}/keys/schedule-deletion`, { pendingWindowInDays }, { keyId });
}
export function cancelKeyDeletion(keyId) {
    return api.post(`${BASE}/keys/cancel-deletion`, undefined, { keyId });
}
export function listAliases(params) {
    return api.get(`${BASE}/aliases`, params);
}
export function createAlias(req) {
    return api.post(`${BASE}/aliases`, req);
}
export function deleteAlias(aliasName) {
    return api.delete(`${BASE}/aliases`, { aliasName });
}
