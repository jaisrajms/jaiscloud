import { api } from '../client'

const BASE = '/api/ui/v1/gcp/compute'

export interface Instance {
  name: string
  zone: string
  status: string
  machineType: string
  cpuPlatform?: string
  internalIp?: string
  externalIp?: string
  creationTimestamp?: string
  labels?: Record<string, string>
}

export interface ListInstancesResponse {
  instances: Instance[]
  total: number
}

export interface InstanceDetail extends Instance {
  id?: string
  selfLink?: string
  description?: string
  metadata?: Record<string, unknown>
  disks?: unknown[]
  networkInterfaces?: unknown[]
}

// ─── Instances ───────────────────────────────────────────────────────────────

export const listInstances = () => api.get<ListInstancesResponse>(`${BASE}/instances`)

export const getInstance = (zone: string, instance: string) =>
  api.get<InstanceDetail>(
    `${BASE}/instances/${encodeURIComponent(zone)}/${encodeURIComponent(instance)}`,
  )

export const startInstance = (zone: string, instance: string) =>
  api.post<Record<string, unknown>>(
    `${BASE}/instances/${encodeURIComponent(zone)}/${encodeURIComponent(instance)}/start`,
    {},
  )

export const stopInstance = (zone: string, instance: string) =>
  api.post<Record<string, unknown>>(
    `${BASE}/instances/${encodeURIComponent(zone)}/${encodeURIComponent(instance)}/stop`,
    {},
  )

export const deleteInstance = (zone: string, instance: string) =>
  api.delete<void>(`${BASE}/instances/${encodeURIComponent(zone)}/${encodeURIComponent(instance)}`)
