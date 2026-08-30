import { api } from './client'

const BASE = '/api/ui/v1/route53'

export interface HostedZone { id: string; name: string; recordCount: number; private: boolean }
export interface ListHostedZonesResponse { items: HostedZone[]; total: number }
export interface RecordSet { name: string; type: string; ttl: number; records: string[] }
export interface ListRecordSetsResponse { items: RecordSet[]; total: number }

export const listZones = () => api.get<ListHostedZonesResponse>(`${BASE}/zones`)

export const createZone = (name: string) =>
  api.post<unknown>(`${BASE}/zones`, { name })

export const deleteZone = (id: string) =>
  api.delete<void>(`${BASE}/zones/${encodeURIComponent(id)}`)

export const listRecords = (zoneId: string) =>
  api.get<ListRecordSetsResponse>(`${BASE}/zones/${encodeURIComponent(zoneId)}/records`)
