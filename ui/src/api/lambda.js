import { api } from './client';
const BASE = '/api/ui/v1/lambda';
export function listFunctions(params) {
    return api.get(`${BASE}/functions`, params);
}
export function getFunction(name) {
    return api.get(`${BASE}/functions/${encodeURIComponent(name)}`);
}
export function getFunctionStatus(name) {
    return api.get(`${BASE}/functions/${encodeURIComponent(name)}/status`);
}
export function deleteFunction(name) {
    return api.delete(`${BASE}/functions/${encodeURIComponent(name)}`);
}
export function invokeFunction(name, req) {
    return api.post(`${BASE}/functions/${encodeURIComponent(name)}/invoke`, req);
}
export function updateFunctionConfig(name, req) {
    return api.patch(`${BASE}/functions/${encodeURIComponent(name)}/config`, req);
}
