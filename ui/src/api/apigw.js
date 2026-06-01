import { api } from './client';
const BASE = '/api/ui/v1/apigateway';
export function listRestAPIs() {
    return api.get(`${BASE}/apis`);
}
export function createRestAPI(req) {
    return api.post(`${BASE}/apis`, req);
}
export function deleteRestAPI(id) {
    return api.delete(`${BASE}/apis/${encodeURIComponent(id)}`);
}
export function listResources(apiId) {
    return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/resources`);
}
export function listStages(apiId) {
    return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/stages`);
}
export function listDeployments(apiId) {
    return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/deployments`);
}
export function createDeployment(apiId, req) {
    return api.post(`${BASE}/apis/${encodeURIComponent(apiId)}/deployments`, req);
}
