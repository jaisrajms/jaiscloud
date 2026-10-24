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

/**
 * The backend's snapshot metadata shape (`admin.SnapshotMetadata`).
 * `GET /api/ui/v1/admin/snapshots` returns a **bare array** of these with the
 * snake_case JSON tags (`created_at`, `jaiscloud_version`), not the console's
 * `{ snapshots: [...] }` / `createdAt` shape. `listSnapshots` adapts one to the
 * other; without it both the GCP Admin page and the Cloudscape `AdminPanel`
 * render an empty table even when snapshots exist.
 */
interface SnapshotMetadata {
  name: string
  description?: string
  created_at?: string
  cloud?: string
  jaiscloud_version?: string
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

export async function listSnapshots(): Promise<ListSnapshotsResponse> {
  const raw = await api.get<SnapshotMetadata[]>(`${BASE}/snapshots`)
  return {
    snapshots: (raw ?? []).map((s) => ({
      name: s.name,
      description: s.description,
      createdAt: s.created_at,
      cloud: s.cloud,
      version: s.jaiscloud_version,
    })),
  }
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
