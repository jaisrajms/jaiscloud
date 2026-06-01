import { api } from './client';
const BASE = '/api/ui/v1/sfn';
export function listStateMachines(params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/state-machines`, p);
}
export function createStateMachine(req) {
    return api.post(`${BASE}/state-machines`, req);
}
export function deleteStateMachine(arn) {
    return api.delete(`${BASE}/state-machines/${encodeURIComponent(arn)}`);
}
export function startExecution(arn, req) {
    return api.post(`${BASE}/state-machines/${encodeURIComponent(arn)}/executions`, req);
}
export function listExecutions(arn, params) {
    const p = {};
    if (params?.statusFilter)
        p.statusFilter = params.statusFilter;
    return api.get(`${BASE}/state-machines/${encodeURIComponent(arn)}/executions`, p);
}
export function stopExecution(arn) {
    return api.post(`${BASE}/executions/${encodeURIComponent(arn)}/stop`);
}
export function getExecutionHistory(arn) {
    return api.get(`${BASE}/executions/${encodeURIComponent(arn)}/history`);
}
