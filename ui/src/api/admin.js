import { api } from './client';
const BASE = '/api/ui/v1/admin';
export function getAdminStatus() {
    return api.get(`${BASE}/status`);
}
export function resetState() {
    return api.post(`${BASE}/reset`);
}
export function getExportInfo() {
    return api.get(`${BASE}/export-info`);
}
export function getClock() {
    return api.get(`${BASE}/clock`);
}
export function setClock(req) {
    return api.post(`${BASE}/clock`, req);
}
export function listSnapshots() {
    return api.get(`${BASE}/snapshots`);
}
export function createSnapshot(req) {
    return api.post(`${BASE}/snapshots`, req);
}
export function revertSnapshot(name) {
    return api.post(`${BASE}/snapshots/${encodeURIComponent(name)}/revert`);
}
export function deleteSnapshot(name) {
    return api.delete(`${BASE}/snapshots/${encodeURIComponent(name)}`);
}
