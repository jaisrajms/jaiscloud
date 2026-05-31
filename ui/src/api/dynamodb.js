import { api } from './client';
const BASE = '/api/ui/v1/dynamodb';
export function listTables(params) {
    return api.get(`${BASE}/tables`, params);
}
export function createTable(req) {
    return api.post(`${BASE}/tables`, req);
}
export function describeTable(name) {
    return api.get(`${BASE}/tables/${encodeURIComponent(name)}`);
}
export function deleteTable(name) {
    return api.delete(`${BASE}/tables/${encodeURIComponent(name)}`);
}
export function scanTable(name, params) {
    const qp = {};
    if (params?.limit)
        qp.limit = params.limit;
    if (params?.nextToken)
        qp.nextToken = params.nextToken;
    return api.get(`${BASE}/tables/${encodeURIComponent(name)}/scan`, qp);
}
export function putItem(table, item) {
    return api.post(`${BASE}/tables/${encodeURIComponent(table)}/items`, { item });
}
export function deleteItem(table, key) {
    return fetch(`${BASE}/tables/${encodeURIComponent(table)}/items`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ key }),
    }).then((r) => {
        if (!r.ok && r.status !== 204)
            throw new Error('delete item failed');
    });
}
