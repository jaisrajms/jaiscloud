import { api } from './client';
const BASE = '/api/ui/v1/emr';
export function listClusters(params) {
    const p = {};
    if (params?.state)
        p.state = params.state;
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/clusters`, p);
}
export function describeCluster(id) {
    return api.get(`${BASE}/clusters/${encodeURIComponent(id)}`);
}
export function runJobFlow(req) {
    return api.post(`${BASE}/clusters`, req);
}
export function terminateCluster(id) {
    return api.delete(`${BASE}/clusters/${encodeURIComponent(id)}`);
}
export function getClusterStatus(id) {
    return api.get(`${BASE}/clusters/${encodeURIComponent(id)}/status`);
}
export function listSteps(clusterId, params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps`, p);
}
export function describeStep(clusterId, stepId) {
    return api.get(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps/${encodeURIComponent(stepId)}`);
}
export function addSteps(clusterId, steps) {
    return api.post(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps`, { steps });
}
export function cancelStep(clusterId, stepId) {
    return api.post(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps/${encodeURIComponent(stepId)}/cancel`);
}
