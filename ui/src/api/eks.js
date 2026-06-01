import { api } from './client';
const BASE = '/api/ui/v1/eks';
export const listClusters = () => api.get(`${BASE}/clusters`);
export const createCluster = (name) => api.post(`${BASE}/clusters`, { name });
export const deleteCluster = (name) => api.delete(`${BASE}/clusters/${encodeURIComponent(name)}`);
