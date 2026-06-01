import { api } from './client';
const BASE = '/api/ui/v1/route53';
export const listZones = () => api.get(`${BASE}/zones`);
export const createZone = (name) => api.post(`${BASE}/zones`, { name });
export const deleteZone = (id) => api.delete(`${BASE}/zones/${encodeURIComponent(id)}`);
export const listRecords = (zoneId) => api.get(`${BASE}/zones/${encodeURIComponent(zoneId)}/records`);
