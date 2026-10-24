import { api } from './client'

export interface ServiceChild {
  label: string
  path: string
}

export interface EngineMode {
  /** Backend name: 'mock' | 'docker' | 'k8s' | 'native'. */
  name: string
  /** Whether this build can run that backend. */
  supported: boolean
  note?: string
}

/**
 * Execution backend for an engine-capable service. Separate from `tier` (the
 * behavioural-depth axis): docker and k8s are interchangeable implementations
 * of one executor seam, so the tier never depends on the active mode.
 */
export interface Engine {
  active: boolean
  mode?: string
  /** Where the configured mode came from (env var name or 'default'). */
  source?: string
  modes: EngineMode[]
}

/** The depth vocabulary, shared by every cloud console. */
export const TIERS = ['full', 'metadata', 'shape'] as const
export type Tier = (typeof TIERS)[number]

export interface ServiceDescriptor {
  id: string
  label: string
  category: string
  rootPath: string
  children: ServiceChild[]
  tier: Tier
  note?: string
  engine?: Engine
}

export interface ServicesResponse {
  services: ServiceDescriptor[]
}

/** Services the running JaisCloud binary actually supports. */
export const listServices = () => api.get<ServicesResponse>('/api/ui/v1/services')
