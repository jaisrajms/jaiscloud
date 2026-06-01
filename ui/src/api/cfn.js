import { api } from './client';
const BASE = '/api/ui/v1/cloudformation';
export const listStacks = () => api.get(`${BASE}/stacks`);
export const createStack = (req) => api.post(`${BASE}/stacks`, req);
export const deleteStack = (name) => api.delete(`${BASE}/stacks/${encodeURIComponent(name)}`);
