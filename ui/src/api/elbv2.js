import { api } from './client';
const BASE = '/api/ui/v1/elbv2';
export const listLoadBalancers = () => api.get(`${BASE}/load-balancers`);
export const createLoadBalancer = (req) => api.post(`${BASE}/load-balancers`, req);
export const deleteLoadBalancer = (arn) => api.delete(`${BASE}/load-balancers/${encodeURIComponent(arn)}`);
