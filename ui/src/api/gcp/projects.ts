import { api } from '../client'

/** The three ways the emulator can know about a project. */
export type ProjectKind = 'configured' | 'registered' | 'observed'

/** One row of GET /api/ui/v1/gcp/projects. */
export interface DiscoveryProject {
  id: string
  kind: ProjectKind
  state?: string
  projectNumber?: string
  displayName?: string
  createTime?: string
  firstSeen?: string
  lastSeen?: string
  requests?: number
  services?: string[]
}

export interface ProjectDiscoveryResponse {
  projects: DiscoveryProject[]
}

/**
 * Every project the emulator knows about, tagged by how it became known.
 * Observed rows are projects a workload addressed without registering them.
 */
export const listProjectDiscovery = () =>
  api.get<ProjectDiscoveryResponse>('/api/ui/v1/gcp/projects')
