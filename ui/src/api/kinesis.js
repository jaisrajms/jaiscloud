import { api } from './client';
const BASE = '/api/ui/v1/kinesis';
export const listStreams = () => api.get(`${BASE}/streams`);
export const createStream = (req) => api.post(`${BASE}/streams`, req);
export const deleteStream = (name) => api.delete(`${BASE}/streams/${encodeURIComponent(name)}`);
