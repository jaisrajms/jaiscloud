import { api } from './client';
const BASE = '/api/ui/v1/ssm';
export function listParameters(params) {
    return api.get(`${BASE}/parameters`, params);
}
export function putParameter(req) {
    return api.post(`${BASE}/parameters`, req);
}
export function getParameter(name) {
    return api.get(`${BASE}/parameters/value`, { name });
}
export function deleteParameter(name) {
    // Name can start with /; strip leading slash for the URL path param
    const encoded = name.startsWith('/') ? name.slice(1) : name;
    return fetch(`/api/ui/v1/ssm/parameters/${encodeURIComponent(encoded)}`, { method: 'DELETE', credentials: 'include' }).then((r) => {
        if (!r.ok && r.status !== 204)
            throw new Error(`Delete failed: ${r.status}`);
    });
}
export function getParameterHistory(name, params) {
    return api.get(`${BASE}/parameters/history`, { name, ...(params ?? {}) });
}
