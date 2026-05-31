import { api } from './client';
const BASE = '/api/ui/v1/logs';
export function listLogGroups(params) {
    return api.get(`${BASE}/groups`, params);
}
export function createLogGroup(logGroupName) {
    return api.post(`${BASE}/groups`, { logGroupName });
}
export function deleteLogGroup(name) {
    return api.delete(`${BASE}/groups/${encodeURIComponent(name)}`);
}
export function setRetention(name, retentionDays) {
    return api.put(`${BASE}/groups/${encodeURIComponent(name)}/retention`, { retentionDays });
}
export function listLogStreams(groupName, params) {
    return api.get(`${BASE}/groups/${encodeURIComponent(groupName)}/streams`, params);
}
export function getLogEvents(groupName, streamName, params) {
    return api.get(`${BASE}/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(streamName)}/events`, params);
}
export function filterLogEvents(groupName, req) {
    return api.post(`${BASE}/groups/${encodeURIComponent(groupName)}/filter`, {
        ...req,
        logGroupName: groupName,
    });
}
