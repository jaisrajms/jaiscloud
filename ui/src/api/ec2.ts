import { api } from './client'

const BASE = '/api/ui/v1/ec2'

export interface Instance {
  id: string
  state: string
  imageId: string
  instanceType: string
  publicIp: string
  privateIp: string
  launchTime: string
}

export interface ListInstancesResponse {
  items: Instance[]
  total: number
  nextToken?: string
}

export const listInstances = (params?: { nextToken?: string }) =>
  api.get<ListInstancesResponse>(`${BASE}/instances`, params)

export const terminateInstance = (id: string) =>
  api.delete<void>(`${BASE}/instances/${encodeURIComponent(id)}`)

export const startInstance = (id: string) =>
  api.post<void>(`${BASE}/instances/${encodeURIComponent(id)}/start`)

export const stopInstance = (id: string) =>
  api.post<void>(`${BASE}/instances/${encodeURIComponent(id)}/stop`)
