import { api } from './client'

const BASE = '/api/ui/v1/rds'

export interface DBInstance { id: string; status: string; engine: string; class: string; endpoint: string; port: number }
export interface ListDBInstancesResponse { items: DBInstance[]; total: number }

export const listInstances = () => api.get<ListDBInstancesResponse>(`${BASE}/instances`)

export const createInstance = (req: { id: string; engine: string; class?: string; username?: string; password?: string }) =>
  api.post<unknown>(`${BASE}/instances`, req)

export const deleteInstance = (id: string) =>
  api.delete<void>(`${BASE}/instances/${encodeURIComponent(id)}`)

export const startInstance = (id: string) =>
  api.post<void>(`${BASE}/instances/${encodeURIComponent(id)}/start`)

export const stopInstance = (id: string) =>
  api.post<void>(`${BASE}/instances/${encodeURIComponent(id)}/stop`)
