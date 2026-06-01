import { api } from './client';
const BASE = '/api/ui/v1/emr-containers';
export function listVirtualClusters(params) {
    const p = {};
    if (params?.state)
        p.state = params.state;
    return api.get(`${BASE}/virtual-clusters`, p);
}
export function describeVirtualCluster(id) {
    return api.get(`${BASE}/virtual-clusters/${encodeURIComponent(id)}`);
}
export function createVirtualCluster(req) {
    return api.post(`${BASE}/virtual-clusters`, req);
}
export function deleteVirtualCluster(id) {
    return api.delete(`${BASE}/virtual-clusters/${encodeURIComponent(id)}`);
}
export function listJobRuns(vcId) {
    return api.get(`${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs`);
}
export function describeJobRun(vcId, jobId) {
    return api.get(`${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs/${encodeURIComponent(jobId)}`);
}
export function startJobRun(vcId, req) {
    return api.post(`${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs`, req);
}
export function cancelJobRun(vcId, jobId) {
    return api.delete(`${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs/${encodeURIComponent(jobId)}`);
}
