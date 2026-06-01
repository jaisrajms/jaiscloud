import { api } from './client';
const BASE = '/api/ui/v1/ec2';
export const listInstances = (params) => api.get(`${BASE}/instances`, params);
export const terminateInstance = (id) => api.delete(`${BASE}/instances/${encodeURIComponent(id)}`);
export const startInstance = (id) => api.post(`${BASE}/instances/${encodeURIComponent(id)}/start`);
export const stopInstance = (id) => api.post(`${BASE}/instances/${encodeURIComponent(id)}/stop`);
