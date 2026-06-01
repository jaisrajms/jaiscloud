import { api } from './client';
const BASE = '/api/ui/v1/glue';
export function listDatabases(params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/databases`, p);
}
export function createDatabase(req) {
    return api.post(`${BASE}/databases`, req);
}
export function deleteDatabase(name) {
    return api.delete(`${BASE}/databases/${encodeURIComponent(name)}`);
}
export function listTables(db, params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/databases/${encodeURIComponent(db)}/tables`, p);
}
export function createTable(db, req) {
    return api.post(`${BASE}/databases/${encodeURIComponent(db)}/tables`, req);
}
export function deleteTable(db, name) {
    return api.delete(`${BASE}/databases/${encodeURIComponent(db)}/tables/${encodeURIComponent(name)}`);
}
export function listJobs(params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/jobs`, p);
}
export function createJob(req) {
    return api.post(`${BASE}/jobs`, req);
}
export function deleteJob(name) {
    return api.delete(`${BASE}/jobs/${encodeURIComponent(name)}`);
}
export function startJobRun(jobName) {
    return api.post(`${BASE}/jobs/${encodeURIComponent(jobName)}/runs`);
}
export function listJobRuns(jobName, params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/jobs/${encodeURIComponent(jobName)}/runs`, p);
}
export function listCrawlers(params) {
    const p = {};
    if (params?.nextToken)
        p.nextToken = params.nextToken;
    return api.get(`${BASE}/crawlers`, p);
}
export function createCrawler(req) {
    return api.post(`${BASE}/crawlers`, req);
}
export function deleteCrawler(name) {
    return api.delete(`${BASE}/crawlers/${encodeURIComponent(name)}`);
}
export function startCrawler(name) {
    return api.post(`${BASE}/crawlers/${encodeURIComponent(name)}/start`);
}
