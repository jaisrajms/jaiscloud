import { api } from './client';
const BASE = '/api/ui/v1/ses';
export const listIdentities = () => api.get(`${BASE}/identities`);
export const verifyEmailIdentity = (identity) => api.post(`${BASE}/identities`, { identity });
export const deleteIdentity = (identity) => api.delete(`${BASE}/identities/${encodeURIComponent(identity)}`);
