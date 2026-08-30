import { api } from './client'

const BASE = '/api/ui/v1/eks'

export interface EKSCluster { name: string; status: string; arn: string; version: string; createdAt: string }
export interface ListClustersResponse { items: EKSCluster[]; total: number }

export const listClusters = () => api.get<ListClustersResponse>(`${BASE}/clusters`)

export const createCluster = (name: string) =>
  api.post<unknown>(`${BASE}/clusters`, { name })

export const deleteCluster = (name: string) =>
  api.delete<void>(`${BASE}/clusters/${encodeURIComponent(name)}`)
