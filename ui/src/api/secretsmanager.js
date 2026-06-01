import { api } from './client';
const BASE = '/api/ui/v1/secretsmanager';
export function listSecrets(params) {
    return api.get(`${BASE}/secrets`, params);
}
export function createSecret(req) {
    return api.post(`${BASE}/secrets`, req);
}
export function getSecret(name) {
    return api.get(`${BASE}/secrets/${encodeURIComponent(name)}`);
}
export function deleteSecret(name) {
    return api.delete(`${BASE}/secrets/${encodeURIComponent(name)}`);
}
export function getSecretValue(name) {
    return api.get(`${BASE}/secrets/${encodeURIComponent(name)}/value`);
}
export function putSecretValue(name, secretString) {
    return api.post(`${BASE}/secrets/${encodeURIComponent(name)}/value`, { secretString });
}
export function listSecretVersions(name, params) {
    return api.get(`${BASE}/secrets/${encodeURIComponent(name)}/versions`, params);
}
