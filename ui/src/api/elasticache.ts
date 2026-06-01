import { api } from './client'

const BASE = '/api/ui/v1/elasticache'

export interface CacheCluster { id: string; status: string; engine: string; nodeType: string; numNodes: number; endpoint: string }
export interface ListCacheClustersResponse { items: CacheCluster[]; total: number }

export const listClusters = () => api.get<ListCacheClustersResponse>(`${BASE}/clusters`)

export const createCluster = (req: { id: string; engine?: string; nodeType?: string; numNodes?: number }) =>
  api.post<unknown>(`${BASE}/clusters`, req)

export const deleteCluster = (id: string) =>
  api.delete<void>(`${BASE}/clusters/${encodeURIComponent(id)}`)
