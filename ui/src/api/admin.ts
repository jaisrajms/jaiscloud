import { api } from './client'

export interface AdminStatus {
  status: string
  cloud: string
  snapshotters: string[]
  backend?: string
  data_dir?: string
  kek_fingerprint?: string
}

export interface ClockState {
  mode: 'real' | 'fixed' | 'offset'
  time?: string
}

export interface Snapshot {
  name: string
  description?: string
  createdAt?: string
  cloud?: string
  version?: string
}

export interface ListSnapshotsResponse {
  snapshots: Snapshot[]
}

const BASE = '/api/ui/v1/admin'

export function getAdminStatus(): Promise<AdminStatus> {
  return api.get<AdminStatus>(`${BASE}/status`)
}

export function resetState(): Promise<{ status: string }> {
  return api.post<{ status: string }>(`${BASE}/reset`)
}

export function getExportInfo(): Promise<{ downloadUrl: string; info: string }> {
  return api.get<{ downloadUrl: string; info: string }>(`${BASE}/export-info`)
}

export function getClock(): Promise<ClockState> {
  return api.get<ClockState>(`${BASE}/clock`)
}

export function setClock(req: ClockState): Promise<ClockState> {
  return api.post<ClockState>(`${BASE}/clock`, req)
}

export function listSnapshots(): Promise<ListSnapshotsResponse> {
  return api.get<ListSnapshotsResponse>(`${BASE}/snapshots`)
}

export function createSnapshot(req: { name: string; description?: string }): Promise<unknown> {
  return api.post<unknown>(`${BASE}/snapshots`, req)
}

export function revertSnapshot(name: string): Promise<unknown> {
  return api.post<unknown>(`${BASE}/snapshots/${encodeURIComponent(name)}/revert`)
}

export function deleteSnapshot(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/snapshots/${encodeURIComponent(name)}`)
}
