import { api } from './client';
const BASE = '/api/ui/v1/cloudwatch';
// Metrics
export function listMetrics(params) {
    return api.get(`${BASE}/metrics`, params);
}
export function getMetricStatistics(params) {
    const { statistics, ...rest } = params;
    const q = rest;
    if (statistics) {
        q['statistics'] = statistics.join(',');
    }
    return api.get(`${BASE}/metrics/statistics`, q);
}
// Alarms
export function listAlarms(params) {
    return api.get(`${BASE}/alarms`, params);
}
export function putAlarm(req) {
    return api.post(`${BASE}/alarms`, req);
}
export function deleteAlarm(alarmName) {
    return api.delete(`${BASE}/alarms`, { alarmName });
}
export function setAlarmState(req) {
    return api.post(`${BASE}/alarms/state`, req);
}
export function enableAlarmActions(alarmNames) {
    return api.post(`${BASE}/alarms/enable`, { alarmNames });
}
export function disableAlarmActions(alarmNames) {
    return api.post(`${BASE}/alarms/disable`, { alarmNames });
}
// Dashboards
export function listDashboards() {
    return api.get(`${BASE}/dashboards`);
}
export function getDashboard(name) {
    return api.get(`${BASE}/dashboards/detail`, { name });
}
export function putDashboard(req) {
    return api.put(`${BASE}/dashboards`, req);
}
export function deleteDashboard(name) {
    return api.delete(`${BASE}/dashboards`, { name });
}
