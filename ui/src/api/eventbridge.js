import { api } from './client';
const BASE = '/api/ui/v1/eventbridge';
export function listRules(params) {
    const p = {};
    if (params?.bus)
        p.bus = params.bus;
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/rules`, p);
}
export function putRule(req) {
    return api.post(`${BASE}/rules`, req);
}
export function deleteRule(name, bus) {
    const p = {};
    if (bus)
        p.bus = bus;
    return api.delete(`${BASE}/rules/${encodeURIComponent(name)}`, p);
}
export function enableRule(name) {
    return api.post(`${BASE}/rules/${encodeURIComponent(name)}/enable`);
}
export function disableRule(name) {
    return api.post(`${BASE}/rules/${encodeURIComponent(name)}/disable`);
}
export function listTargets(ruleName) {
    return api.get(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets`);
}
export function putTargets(ruleName, targets) {
    return api.post(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets`, { targets });
}
export function removeTarget(ruleName, targetId) {
    return api.delete(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets/${encodeURIComponent(targetId)}`);
}
export function putEvents(entries) {
    return api.post(`${BASE}/events`, { entries });
}
export function listEventBuses() {
    return api.get(`${BASE}/buses`);
}
export function createEventBus(name) {
    return api.post(`${BASE}/buses`, { name });
}
export function deleteEventBus(name) {
    return api.delete(`${BASE}/buses/${encodeURIComponent(name)}`);
}
