import { api } from './client';
const BASE = '/api/ui/v1/rds';
export const listInstances = () => api.get(`${BASE}/instances`);
export const createInstance = (req) => api.post(`${BASE}/instances`, req);
export const deleteInstance = (id) => api.delete(`${BASE}/instances/${encodeURIComponent(id)}`);
export const startInstance = (id) => api.post(`${BASE}/instances/${encodeURIComponent(id)}/start`);
export const stopInstance = (id) => api.post(`${BASE}/instances/${encodeURIComponent(id)}/stop`);
