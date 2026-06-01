import { api } from './client';
const BASE = '/api/ui/v1/elasticache';
export const listClusters = () => api.get(`${BASE}/clusters`);
export const createCluster = (req) => api.post(`${BASE}/clusters`, req);
export const deleteCluster = (id) => api.delete(`${BASE}/clusters/${encodeURIComponent(id)}`);
