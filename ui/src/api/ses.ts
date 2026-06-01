import { api } from './client'

const BASE = '/api/ui/v1/ses'

export interface Identity { identity: string; type: string }
export interface ListIdentitiesResponse { items: Identity[]; total: number }

export const listIdentities = () => api.get<ListIdentitiesResponse>(`${BASE}/identities`)

export const verifyEmailIdentity = (identity: string) =>
  api.post<unknown>(`${BASE}/identities`, { identity })

export const deleteIdentity = (identity: string) =>
  api.delete<void>(`${BASE}/identities/${encodeURIComponent(identity)}`)
