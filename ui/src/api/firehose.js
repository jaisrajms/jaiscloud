import { api } from './client';
const BASE = '/api/ui/v1/firehose';
export const listDeliveryStreams = () => api.get(`${BASE}/streams`);
export const createDeliveryStream = (req) => api.post(`${BASE}/streams`, req);
export const deleteDeliveryStream = (name) => api.delete(`${BASE}/streams/${encodeURIComponent(name)}`);
